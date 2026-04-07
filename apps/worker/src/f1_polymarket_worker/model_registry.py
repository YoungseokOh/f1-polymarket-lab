from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from f1_polymarket_lab.common import stable_uuid, utc_now
from f1_polymarket_lab.storage.models import (
    FeatureSnapshot,
    ModelPrediction,
    ModelRun,
    ModelRunPromotion,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select
from sqlalchemy.orm import Session

try:
    import mlflow
except ImportError as exc:  # pragma: no cover - import availability is environment-specific
    MLFLOW_IMPORT_ERROR = exc
    mlflow = None

MULTITASK_PROMOTION_STAGE = "multitask_qr"
SQ_POLE_LIVE_PROMOTION_STAGE = "sq_pole_live_v1"
SPRINT_WINNER_LIVE_PROMOTION_STAGE = "sprint_winner_live_v1"
MULTITASK_EXPERIMENT_NAME = "weekend_ops.multitask_qr"
SUPPORTED_MULTITASK_SOURCE_CHECKPOINTS = frozenset({"FP1", "FP2", "FP3", "Q"})

PROMOTION_THRESHOLDS = {
    "total_pnl_min": 0.0,
    "roi_pct_min": 0.0,
    "bet_count_min": 20,
    "ece_max": 0.08,
    "family_pnl_share_max": 0.65,
}


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    eligible: bool
    actuals: dict[str, float]
    failed_rules: list[str]


def model_artifact_dir(*, data_root: Path, model_run_id: str) -> Path:
    return data_root / "artifacts" / "model_runs" / model_run_id


def scored_predictions_path(*, artifact_dir: Path, snapshot_id: str) -> Path:
    return artifact_dir / "scored_predictions" / f"{snapshot_id}.json"


def mlflow_experiment_name_for_stage(stage: str) -> str:
    if stage == MULTITASK_PROMOTION_STAGE:
        return MULTITASK_EXPERIMENT_NAME
    return f"weekend_ops.{stage}"


def required_model_stage_for_gp(config: Any) -> str | None:
    explicit_stage = getattr(config, "required_model_stage", None)
    if isinstance(explicit_stage, str) and explicit_stage:
        return explicit_stage
    target_session_code = getattr(config, "target_session_code", None)
    source_session_code = getattr(config, "source_session_code", None)
    if (
        target_session_code in {"Q", "R"}
        and source_session_code in SUPPORTED_MULTITASK_SOURCE_CHECKPOINTS
    ):
        return MULTITASK_PROMOTION_STAGE
    return None


def gp_supports_promoted_multitask(config: Any) -> bool:
    return required_model_stage_for_gp(config) == MULTITASK_PROMOTION_STAGE


def _coerce_float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, int | float):
        return float(value)
    return default


def evaluate_promotion_gate(metrics: dict[str, Any] | None) -> PromotionDecision:
    payload = metrics or {}
    actuals = {
        "total_pnl": _coerce_float(payload.get("total_pnl"), 0.0),
        "roi_pct": _coerce_float(payload.get("roi_pct"), 0.0),
        "bet_count": _coerce_float(payload.get("bet_count"), 0.0),
        "ece": _coerce_float(payload.get("ece"), 1.0),
        "family_pnl_share_max": _coerce_float(payload.get("family_pnl_share_max"), 1.0),
    }
    failed_rules: list[str] = []
    if actuals["total_pnl"] <= PROMOTION_THRESHOLDS["total_pnl_min"]:
        failed_rules.append(
            "total_pnl="
            f"{actuals['total_pnl']:.4f} must be > "
            f"{PROMOTION_THRESHOLDS['total_pnl_min']:.4f}"
        )
    if actuals["roi_pct"] <= PROMOTION_THRESHOLDS["roi_pct_min"]:
        failed_rules.append(
            f"roi_pct={actuals['roi_pct']:.4f} must be > {PROMOTION_THRESHOLDS['roi_pct_min']:.4f}"
        )
    if actuals["bet_count"] < PROMOTION_THRESHOLDS["bet_count_min"]:
        failed_rules.append(
            "bet_count="
            f"{actuals['bet_count']:.0f} must be >= "
            f"{PROMOTION_THRESHOLDS['bet_count_min']}"
        )
    if actuals["ece"] > PROMOTION_THRESHOLDS["ece_max"]:
        failed_rules.append(
            f"ece={actuals['ece']:.4f} must be <= {PROMOTION_THRESHOLDS['ece_max']:.4f}"
        )
    if actuals["family_pnl_share_max"] > PROMOTION_THRESHOLDS["family_pnl_share_max"]:
        failed_rules.append(
            "family_pnl_share_max="
            f"{actuals['family_pnl_share_max']:.4f} must be <= "
            f"{PROMOTION_THRESHOLDS['family_pnl_share_max']:.4f}"
        )
    return PromotionDecision(
        eligible=not failed_rules,
        actuals=actuals,
        failed_rules=failed_rules,
    )


def log_run_to_mlflow(
    *,
    tracking_uri: str,
    experiment_name: str,
    run_name: str,
    params: dict[str, Any],
    metrics: dict[str, Any],
    tags: dict[str, str] | None,
    artifact_dir: Path,
) -> str:
    if mlflow is None:  # pragma: no cover - dependency is expected in configured envs
        raise ImportError(
            "MLflow support requires the 'mlflow' dependency."
        ) from MLFLOW_IMPORT_ERROR

    def flatten_values(
        payload: dict[str, Any],
        *,
        numeric_only: bool,
        prefix: str = "",
    ) -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        for key, value in payload.items():
            composite_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flattened.update(
                    flatten_values(value, numeric_only=numeric_only, prefix=composite_key)
                )
                continue
            if numeric_only:
                if isinstance(value, bool):
                    flattened[composite_key] = float(int(value))
                elif isinstance(value, int | float):
                    flattened[composite_key] = float(value)
                continue
            flattened[composite_key] = (
                json.dumps(value, default=str)
                if isinstance(value, list)
                else str(value)
            )
        return flattened

    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    experiment_id = (
        experiment.experiment_id
        if experiment is not None
        else mlflow.create_experiment(experiment_name)
    )

    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name, tags=tags or {}) as run:
        flat_params = flatten_values(params, numeric_only=False)
        if flat_params:
            mlflow.log_params(flat_params)

        flat_metrics = flatten_values(metrics, numeric_only=True)
        for key, value in flat_metrics.items():
            mlflow.log_metric(key, value)

        mlflow.log_text(
            json.dumps({"params": params, "metrics": metrics}, indent=2, default=str),
            "summary.json",
        )
        if artifact_dir.exists():
            mlflow.log_artifacts(str(artifact_dir), artifact_path="artifacts")
        return run.info.run_id


def get_active_promotion(session: Session, *, stage: str) -> ModelRunPromotion | None:
    return session.scalar(
        select(ModelRunPromotion)
        .where(
            ModelRunPromotion.stage == stage,
            ModelRunPromotion.status == "active",
        )
        .order_by(ModelRunPromotion.promoted_at.desc())
        .limit(1)
    )


def get_active_promoted_model_run(session: Session, *, stage: str) -> ModelRun | None:
    promotion = get_active_promotion(session, stage=stage)
    if promotion is None:
        return None
    return session.get(ModelRun, promotion.model_run_id)


def promotion_state_by_model_run_id(
    session: Session,
    *,
    model_run_ids: list[str],
) -> dict[str, ModelRunPromotion]:
    if not model_run_ids:
        return {}
    rows = session.scalars(
        select(ModelRunPromotion).where(ModelRunPromotion.model_run_id.in_(model_run_ids))
    ).all()
    state: dict[str, ModelRunPromotion] = {}
    for row in rows:
        existing = state.get(row.model_run_id)
        if existing is None or row.promoted_at >= existing.promoted_at:
            state[row.model_run_id] = row
    return state


def promote_model_run(
    session: Session,
    *,
    model_run_id: str,
    stage: str,
) -> ModelRunPromotion:
    model_run = session.get(ModelRun, model_run_id)
    if model_run is None:
        raise KeyError(f"model_run_id={model_run_id} not found")
    if model_run.stage != stage:
        raise ValueError(
            f"model_run_id={model_run_id} has stage={model_run.stage}, expected stage={stage}"
        )
    if not model_run.artifact_uri:
        raise ValueError(f"model_run_id={model_run_id} has no artifact_uri to promote")

    decision = evaluate_promotion_gate(model_run.metrics_json)
    if not decision.eligible:
        raise ValueError("Promotion gate failed: " + "; ".join(decision.failed_rules))

    active_rows = session.scalars(
        select(ModelRunPromotion).where(
            ModelRunPromotion.stage == stage,
            ModelRunPromotion.status == "active",
        )
    ).all()
    for row in active_rows:
        if row.model_run_id != model_run_id:
            row.status = "superseded"

    existing = session.scalar(
        select(ModelRunPromotion).where(
            ModelRunPromotion.model_run_id == model_run_id,
            ModelRunPromotion.stage == stage,
        )
    )
    promotion_id = (
        existing.id
        if existing is not None
        else stable_uuid("promotion", stage, model_run_id)
    )
    promoted_at = utc_now()
    upsert_records(
        session,
        ModelRunPromotion,
        [
            {
                "id": promotion_id,
                "model_run_id": model_run_id,
                "stage": stage,
                "status": "active",
                "gate_metrics_json": decision.actuals,
                "promoted_at": promoted_at,
            }
        ],
        conflict_columns=["model_run_id", "stage"],
    )
    session.flush()
    promotion = session.scalar(
        select(ModelRunPromotion).where(
            ModelRunPromotion.model_run_id == model_run_id,
            ModelRunPromotion.stage == stage,
        )
    )
    if promotion is None:
        raise RuntimeError("Failed to persist model promotion")
    return promotion


def ensure_model_run_allowed_for_paper_trade(session: Session, *, model_run_id: str) -> ModelRun:
    model_run = session.get(ModelRun, model_run_id)
    if model_run is None:
        raise KeyError(f"model_run_id={model_run_id} not found")

    active = get_active_promotion(session, stage=model_run.stage)
    if active is not None and active.model_run_id == model_run.id:
        return model_run

    config = model_run.config_json or {}
    parent_model_run_id = config.get("parent_model_run_id")
    source_stage = config.get("source_promotion_stage")
    if isinstance(parent_model_run_id, str) and isinstance(source_stage, str):
        active_parent = get_active_promotion(session, stage=source_stage)
        if active_parent is not None and active_parent.model_run_id == parent_model_run_id:
            return model_run

    raise ValueError(
        f"model_run_id={model_run_id} is not an active promoted run or a scored derivative of one"
    )


def score_promoted_multitask_snapshot(
    session: Session,
    *,
    data_root: Path,
    snapshot: FeatureSnapshot,
    stage: str,
) -> dict[str, Any]:
    import polars as pl
    from f1_polymarket_lab.models import score_multitask_frame

    champion_run = get_active_promoted_model_run(session, stage=stage)
    if champion_run is None:
        raise ValueError(f"No active promoted champion exists for stage={stage}")
    if not champion_run.artifact_uri:
        raise ValueError(f"Active promoted champion {champion_run.id} is missing artifact_uri")
    if snapshot.storage_path is None:
        raise ValueError(f"snapshot_id={snapshot.id} is missing storage_path")
    if champion_run.dataset_version and snapshot.feature_version != champion_run.dataset_version:
        raise ValueError(
            "Snapshot feature version does not match the active promoted champion: "
            f"snapshot={snapshot.feature_version}, champion={champion_run.dataset_version}"
        )

    artifact_dir = Path(champion_run.artifact_uri)
    if not artifact_dir.exists():
        raise ValueError(
            f"Active promoted champion artifact directory does not exist: {artifact_dir}"
        )

    frame = pl.read_parquet(snapshot.storage_path)
    if frame.height == 0:
        raise ValueError(f"snapshot_id={snapshot.id} contains no rows to score")

    scored_run_id = stable_uuid("scored-multitask-run", champion_run.id, snapshot.id)
    predictions = score_multitask_frame(
        frame,
        artifact_dir=artifact_dir,
        model_run_id=scored_run_id,
        stage=stage,
        feature_snapshot_id=snapshot.id,
    )

    scored_artifact_dir = model_artifact_dir(data_root=data_root, model_run_id=scored_run_id)
    scored_artifact_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = scored_predictions_path(
        artifact_dir=scored_artifact_dir,
        snapshot_id=snapshot.id,
    )
    predictions_path.parent.mkdir(parents=True, exist_ok=True)
    predictions_path.write_text(json.dumps(predictions, indent=2, default=str), encoding="utf-8")

    run_record = {
        "id": scored_run_id,
        "stage": stage,
        "model_family": "torch_multitask_scored",
        "model_name": f"{champion_run.model_name}_scored",
        "dataset_version": snapshot.feature_version,
        "feature_snapshot_id": snapshot.id,
        "test_start": snapshot.as_of_ts,
        "test_end": snapshot.as_of_ts,
        "config_json": {
            "parent_model_run_id": champion_run.id,
            "source_promotion_stage": stage,
            "source_registry_run_id": champion_run.registry_run_id,
            "source_feature_snapshot_id": snapshot.id,
        },
        "metrics_json": {
            "row_count": len(predictions),
            "source_model_run_id": champion_run.id,
        },
        "artifact_uri": str(scored_artifact_dir),
        "registry_run_id": champion_run.registry_run_id,
    }
    upsert_records(session, ModelRun, [run_record], conflict_columns=["id"])

    prediction_rows: list[dict[str, Any]] = []
    for row in predictions:
        explanation = row.get("explanation_json") or {}
        prediction_rows.append(
            {
                **row,
                "id": stable_uuid(
                    "scored-multitask-prediction",
                    scored_run_id,
                    snapshot.id,
                    row.get("market_id"),
                    row.get("token_id"),
                    explanation.get("as_of_checkpoint"),
                ),
            }
        )
    upsert_records(session, ModelPrediction, prediction_rows, conflict_columns=["id"])
    session.flush()

    return {
        "model_run_id": scored_run_id,
        "source_model_run_id": champion_run.id,
        "artifact_path": str(predictions_path),
        "prediction_count": len(predictions),
    }
