from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from f1_polymarket_lab.common import stable_uuid
from f1_polymarket_lab.models import (
    MultitaskTrainerConfig,
    build_walk_forward_splits,
    train_multitask_split,
)
from f1_polymarket_lab.storage.models import FeatureSnapshot, ModelPrediction, ModelRun
from f1_polymarket_lab.storage.repository import upsert_records

from f1_polymarket_worker.gp_registry import GP_REGISTRY
from f1_polymarket_worker.model_registry import (
    log_run_to_mlflow,
    mlflow_experiment_name_for_stage,
    model_artifact_dir,
    score_promoted_multitask_snapshot,
)
from f1_polymarket_worker.multitask_snapshot import CHECKPOINTS, build_multitask_feature_snapshots
from f1_polymarket_worker.pipeline import PipelineContext


def default_multitask_manifest_path(*, data_root: Path, season: int) -> Path:
    return data_root / "feature_snapshots" / "multitask" / str(season) / "manifest.json"


def build_multitask_training_snapshots(
    ctx: PipelineContext,
    *,
    season: int,
    through_meeting_key: int | None = None,
    checkpoints: tuple[str, ...] = CHECKPOINTS,
    stage: str = "multitask_qr",
) -> dict[str, Any]:
    meeting_keys = sorted(
        {
            config.meeting_key
            for config in GP_REGISTRY
            if config.season == season
            and (through_meeting_key is None or config.meeting_key <= through_meeting_key)
        }
    )
    if not meeting_keys:
        raise ValueError(f"No GP configs found for season={season}.")

    completed: list[dict[str, Any]] = []
    warnings: list[str] = []
    manifest_path: str | None = None
    snapshot_ids: list[str] = []
    job_run_ids: list[str] = []
    total_row_count = 0

    for meeting_key in meeting_keys:
        try:
            result = build_multitask_feature_snapshots(
                ctx,
                meeting_key=meeting_key,
                season=season,
                checkpoints=checkpoints,
                stage=stage,
            )
        except Exception as exc:
            warnings.append(f"meeting_key={meeting_key}: {exc}")
            continue

        manifest_path = str(result.get("manifest_path") or manifest_path or "")
        job_run_id = result.get("job_run_id")
        if isinstance(job_run_id, str):
            job_run_ids.append(job_run_id)
        ids = [str(value) for value in result.get("snapshot_ids", [])]
        snapshot_ids.extend(ids)
        row_count = _snapshot_ids_row_count(ctx, ids)
        total_row_count += row_count
        if row_count == 0:
            warnings.append(
                f"meeting_key={meeting_key}: snapshots were built but contain no training rows"
            )
        completed.append(
            {
                "meeting_key": meeting_key,
                "snapshot_count": len(ids),
                "row_count": row_count,
                "snapshot_ids": ids,
            }
        )

    if not completed:
        raise ValueError(
            "No multitask snapshots were built. Refresh GP data and linked markets first."
        )

    return {
        "status": "completed",
        "stage": stage,
        "season": season,
        "through_meeting_key": through_meeting_key,
        "meeting_keys": meeting_keys,
        "completed_meetings": completed,
        "snapshot_ids": snapshot_ids,
        "snapshot_count": len(snapshot_ids),
        "row_count": total_row_count,
        "manifest_path": manifest_path,
        "job_run_ids": job_run_ids,
        "warnings": warnings,
    }


def train_multitask_walk_forward(
    ctx: PipelineContext,
    *,
    season: int,
    manifest_path: str | None = None,
    stage: str = "multitask_qr",
    min_train_gps: int = 2,
) -> dict[str, Any]:
    manifest = Path(manifest_path) if manifest_path else default_multitask_manifest_path(
        data_root=Path(ctx.settings.data_root),
        season=season,
    )
    if not manifest.exists():
        raise ValueError(
            "No training snapshots exist yet. Build model snapshots first, then train."
        )

    manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in manifest_payload.get("snapshots", []):
        if not isinstance(row, dict) or row.get("meeting_key") is None:
            continue
        grouped.setdefault(int(row["meeting_key"]), []).append(row)

    row_counts_by_meeting = {
        meeting_key: sum(_snapshot_path_row_count(str(row.get("path"))) for row in rows)
        for meeting_key, rows in grouped.items()
    }
    grouped = {
        meeting_key: rows
        for meeting_key, rows in grouped.items()
        if row_counts_by_meeting.get(meeting_key, 0) > 0
    }
    meeting_keys = sorted(grouped)
    splits = build_walk_forward_splits(meeting_keys, min_train=min_train_gps)
    if not splits:
        raise ValueError(
            f"Training needs at least {min_train_gps + 1} GPs with non-empty "
            f"snapshot rows. Only {len(meeting_keys)} GP(s) are ready. "
            "Refresh missing GP data and markets, then build data again."
        )

    results: list[dict[str, Any]] = []
    skipped: list[str] = []
    checkpoint_order = {"FP1": 1, "FP2": 2, "FP3": 3, "Q": 4}

    for split in splits:
        train_paths: list[str] = []
        for meeting_key in split.train_meeting_keys:
            snapshots = sorted(
                grouped[meeting_key],
                key=lambda row: checkpoint_order.get(str(row.get("checkpoint")), 99),
            )
            train_paths.extend(str(row["path"]) for row in snapshots)

        test_snapshots = sorted(
            grouped[split.test_meeting_key],
            key=lambda row: checkpoint_order.get(str(row.get("checkpoint")), 99),
        )
        train_frames = _load_snapshot_frames(train_paths)
        test_frames = _load_snapshot_frames([str(row["path"]) for row in test_snapshots])
        if not train_frames:
            skipped.append(
                f"meeting_key={split.test_meeting_key}: no non-empty training rows"
            )
            continue
        if not test_frames:
            skipped.append(f"meeting_key={split.test_meeting_key}: no non-empty test rows")
            continue

        train_df = pl.concat(train_frames)
        test_df = pl.concat(test_frames)
        model_run_id = stable_uuid("multitask-run", split.test_meeting_key, stage)
        artifact_dir = model_artifact_dir(
            data_root=Path(ctx.settings.data_root),
            model_run_id=model_run_id,
        )
        result = train_multitask_split(
            train_df,
            test_df,
            model_run_id=model_run_id,
            stage=stage,
            config=MultitaskTrainerConfig(),
            artifact_dir=artifact_dir,
        )

        test_timestamps = (
            [value for value in test_df["as_of_ts"].to_list() if isinstance(value, datetime)]
            if "as_of_ts" in test_df.columns
            else []
        )
        train_snapshot_ids = [
            str(row["snapshot_id"])
            for meeting_key in split.train_meeting_keys
            for row in grouped[meeting_key]
        ]
        test_snapshot_ids = [str(row["snapshot_id"]) for row in test_snapshots]

        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "predictions.json").write_text(
            json.dumps(result.predictions, indent=2, default=str),
            encoding="utf-8",
        )
        (artifact_dir / "metrics.json").write_text(
            json.dumps(result.metrics, indent=2, default=str),
            encoding="utf-8",
        )

        registry_run_id = log_run_to_mlflow(
            tracking_uri=ctx.settings.mlflow_tracking_uri,
            experiment_name=mlflow_experiment_name_for_stage(stage),
            run_name=f"{stage}:{split.test_meeting_key}",
            params={
                **result.config,
                "manifest_path": str(manifest),
                "train_snapshot_ids": train_snapshot_ids,
                "test_snapshot_ids": test_snapshot_ids,
            },
            metrics=result.metrics,
            tags={
                "stage": stage,
                "model_family": "torch_multitask",
                "test_meeting_key": str(split.test_meeting_key),
            },
            artifact_dir=artifact_dir,
        )

        upsert_records(
            ctx.db,
            ModelRun,
            [
                {
                    "id": result.model_run_id,
                    "stage": stage,
                    "model_family": "torch_multitask",
                    "model_name": "shared_encoder_multitask_v2",
                    "dataset_version": "multitask_v1",
                    "feature_snapshot_id": None,
                    "test_start": min(test_timestamps) if test_timestamps else None,
                    "test_end": max(test_timestamps) if test_timestamps else None,
                    "config_json": {
                        **result.config,
                        "manifest_path": str(manifest),
                        "train_snapshot_ids": train_snapshot_ids,
                        "test_snapshot_ids": test_snapshot_ids,
                    },
                    "metrics_json": result.metrics,
                    "artifact_uri": str(artifact_dir),
                    "registry_run_id": registry_run_id,
                }
            ],
            conflict_columns=["id"],
        )

        prediction_records = [
            {
                **row,
                "id": stable_uuid(
                    "multitask-prediction",
                    result.model_run_id,
                    row.get("market_id"),
                    row.get("token_id"),
                    str((row.get("explanation_json") or {}).get("as_of_checkpoint", "NA")),
                    _serialize_timestamp(row.get("as_of_ts")),
                ),
            }
            for row in result.predictions
        ]
        upsert_records(
            ctx.db,
            ModelPrediction,
            prediction_records,
            conflict_columns=["id"],
        )
        results.append(
            {
                "model_run_id": result.model_run_id,
                "test_meeting_key": split.test_meeting_key,
                "train_meeting_keys": split.train_meeting_keys,
                "prediction_count": len(result.predictions),
                "metrics": result.metrics,
            }
        )

    if not results:
        raise ValueError(
            "No model runs were trained. Check that the built snapshots contain rows."
        )

    return {
        "status": "completed",
        "stage": stage,
        "season": season,
        "manifest_path": str(manifest),
        "meeting_keys": meeting_keys,
        "split_count": len(splits),
        "model_run_ids": [row["model_run_id"] for row in results],
        "model_run_count": len(results),
        "runs": results,
        "skipped": skipped,
    }


def score_multitask_snapshot(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    stage: str = "multitask_qr",
) -> dict[str, Any]:
    snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
    if snapshot is None:
        raise KeyError(f"snapshot_id={snapshot_id} not found")
    result = score_promoted_multitask_snapshot(
        ctx.db,
        data_root=Path(ctx.settings.data_root),
        snapshot=snapshot,
        stage=stage,
    )
    return {
        "status": "completed",
        "stage": stage,
        "snapshot_id": snapshot_id,
        **result,
    }


def _load_snapshot_frames(paths: list[str]) -> list[pl.DataFrame]:
    frames: list[pl.DataFrame] = []
    for path in paths:
        frame = pl.read_parquet(path)
        if frame.height == 0 or frame.width == 0:
            continue
        frames.append(frame)
    return frames


def _snapshot_ids_row_count(ctx: PipelineContext, snapshot_ids: list[str]) -> int:
    row_count = 0
    for snapshot_id in snapshot_ids:
        snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
        if snapshot is not None and snapshot.row_count is not None:
            row_count += int(snapshot.row_count)
    return row_count


def _snapshot_path_row_count(path: str) -> int:
    if not path:
        return 0
    try:
        return pl.read_parquet(path).height
    except Exception:
        return 0


def _serialize_timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
