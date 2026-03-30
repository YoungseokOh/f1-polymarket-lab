from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from random import Random
from typing import Any

import polars as pl
from f1_polymarket_lab.models import (
    MultitaskTrainerConfig,
    build_walk_forward_splits,
    train_multitask_split,
)

from .tracking import ExperimentSpec, ExperimentTracker

CHECKPOINT_ORDER = {"FP1": 1, "FP2": 2, "FP3": 3, "Q": 4}


@dataclass(frozen=True, slots=True)
class AutoResearchConfig:
    iterations: int = 20
    seed: int = 7
    stage: str = "multitask_qr"
    base_hidden_dim: int = 128
    min_train_gps: int = 2
    min_edge: float = 0.05


def promotion_gate(
    metrics: dict[str, Any],
    *,
    baseline_metrics: dict[str, Any] | None = None,
) -> bool:
    keep = (
        float(metrics.get("realized_pnl_total", 0.0) or 0.0) > 0.0
        and float(metrics.get("roi_pct", 0.0) or 0.0) > 0.0
        and int(metrics.get("bet_count", 0) or 0) >= 10
        and float(metrics.get("family_pnl_share_max", 1.0) or 1.0) <= 0.80
    )
    if not keep or baseline_metrics is None:
        return keep
    baseline_log_loss = float(baseline_metrics.get("log_loss", 0.0) or 0.0)
    baseline_brier = float(baseline_metrics.get("brier_score", 0.0) or 0.0)
    log_loss = float(metrics.get("log_loss", 0.0) or 0.0)
    brier = float(metrics.get("brier_score", 0.0) or 0.0)
    return (
        log_loss <= baseline_log_loss * 1.10
        and brier <= baseline_brier * 1.10
    )


def hybrid_sort_key(metrics: dict[str, Any]) -> tuple[float, float, float, float]:
    return (
        -float(metrics.get("realized_pnl_total", 0.0) or 0.0),
        -float(metrics.get("roi_pct", 0.0) or 0.0),
        float(metrics.get("log_loss", 999.0) or 999.0),
        float(metrics.get("brier_score", 999.0) or 999.0),
    )


def base_candidate(*, hidden_dim: int = 128) -> dict[str, Any]:
    return {
        "hidden_dim": hidden_dim,
        "depth": 2,
        "dropout": 0.1,
        "lr": 1e-3,
        "batch_size": 64,
        "head_weights": {
            "pole": 1.0,
            "constructor_pole": 1.0,
            "winner": 1.0,
            "h2h": 1.0,
        },
    }


def mutate_config(rng: Random, base_config: dict[str, Any]) -> dict[str, Any]:
    return {
        **base_config,
        "hidden_dim": rng.choice([64, 96, 128, 160]),
        "depth": rng.choice([1, 2, 3]),
        "dropout": rng.choice([0.0, 0.05, 0.1, 0.2]),
        "lr": rng.choice([5e-4, 1e-3, 2e-3]),
        "batch_size": rng.choice([32, 64, 96]),
        "head_weights": {
            "pole": rng.choice([0.8, 1.0, 1.2, 1.4]),
            "constructor_pole": rng.choice([0.8, 1.0, 1.2]),
            "winner": rng.choice([0.8, 1.0, 1.2, 1.4]),
            "h2h": rng.choice([0.8, 1.0, 1.2, 1.4]),
        },
    }


def trainer_config_from_candidate(candidate: dict[str, Any]) -> MultitaskTrainerConfig:
    return MultitaskTrainerConfig(
        hidden_dim=int(candidate["hidden_dim"]),
        depth=int(candidate["depth"]),
        dropout=float(candidate["dropout"]),
        lr=float(candidate["lr"]),
        batch_size=int(candidate["batch_size"]),
        head_weights={
            str(key): float(value)
            for key, value in dict(candidate.get("head_weights") or {}).items()
        },
    )


def load_manifest_grouped(path: Path) -> dict[int, list[dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in payload.get("snapshots", []):
        grouped.setdefault(int(row["meeting_key"]), []).append(dict(row))
    return grouped


def _sorted_snapshot_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: CHECKPOINT_ORDER.get(str(row.get("checkpoint")), 99),
    )


def _load_snapshot_frames(paths: list[str]) -> list[pl.DataFrame]:
    frames: list[pl.DataFrame] = []
    for path in paths:
        frame = pl.read_parquet(path)
        if frame.height == 0 or frame.width == 0:
            continue
        frames.append(frame)
    return frames


def aggregate_fold_metrics(folds: list[dict[str, Any]]) -> dict[str, Any]:
    if not folds:
        return {
            "fold_count": 0,
            "row_count": 0,
            "bet_count": 0,
            "realized_pnl_total": 0.0,
            "roi_pct": 0.0,
            "average_edge": None,
            "paper_edge_hit_rate": None,
            "log_loss": 999.0,
            "brier_score": 999.0,
            "family_pnl_share_max": 1.0,
            "family_metrics": {},
            "stake_total": 0.0,
        }

    row_count = sum(int(fold.get("row_count", 0) or 0) for fold in folds)
    bet_count = sum(int(fold.get("bet_count", 0) or 0) for fold in folds)
    stake_total = sum(float(fold.get("stake_total", 0.0) or 0.0) for fold in folds)
    realized_pnl_total = sum(
        float(fold.get("realized_pnl_total", 0.0) or 0.0) for fold in folds
    )
    weighted_log_loss = sum(
        float(fold.get("log_loss", 0.0) or 0.0) * int(fold.get("row_count", 0) or 0)
        for fold in folds
    )
    weighted_brier = sum(
        float(fold.get("brier_score", 0.0) or 0.0) * int(fold.get("row_count", 0) or 0)
        for fold in folds
    )
    weighted_edge = sum(
        float(fold.get("average_edge", 0.0) or 0.0) * int(fold.get("bet_count", 0) or 0)
        for fold in folds
        if fold.get("average_edge") is not None
    )
    weighted_hit_rate = sum(
        float(fold.get("paper_edge_hit_rate", 0.0) or 0.0) * int(fold.get("bet_count", 0) or 0)
        for fold in folds
        if fold.get("paper_edge_hit_rate") is not None
    )
    family_metrics: dict[str, dict[str, Any]] = {}
    for fold in folds:
        for family, metrics in dict(fold.get("family_metrics") or {}).items():
            bucket = family_metrics.setdefault(
                str(family),
                {
                    "row_count": 0,
                    "bet_count": 0,
                    "realized_pnl_total": 0.0,
                    "stake_total": 0.0,
                    "weighted_brier": 0.0,
                },
            )
            bucket["row_count"] += int(metrics.get("row_count", 0) or 0)
            bucket["bet_count"] += int(metrics.get("bet_count", 0) or 0)
            bucket["realized_pnl_total"] += float(
                metrics.get("realized_pnl_total", 0.0) or 0.0
            )
            bucket["stake_total"] += float(metrics.get("stake_total", 0.0) or 0.0)
            bucket["weighted_brier"] += float(metrics.get("brier_score", 0.0) or 0.0) * int(
                metrics.get("row_count", 0) or 0
            )

    family_pnl_share_max = 0.0
    total_abs_family_pnl = sum(
        abs(float(metrics["realized_pnl_total"])) for metrics in family_metrics.values()
    )
    finalized_family_metrics: dict[str, dict[str, Any]] = {}
    for family, metrics in family_metrics.items():
        pnl = float(metrics["realized_pnl_total"])
        stake = float(metrics["stake_total"])
        family_pnl_share_max = max(
            family_pnl_share_max,
            abs(pnl) / max(total_abs_family_pnl, 1e-12),
        )
        finalized_family_metrics[family] = {
            "row_count": int(metrics["row_count"]),
            "bet_count": int(metrics["bet_count"]),
            "realized_pnl_total": pnl,
            "stake_total": stake,
            "roi_pct": (pnl / stake * 100.0) if stake > 0 else None,
            "brier_score": (
                float(metrics["weighted_brier"]) / int(metrics["row_count"])
                if int(metrics["row_count"]) > 0
                else None
            ),
        }

    return {
        "fold_count": len(folds),
        "row_count": row_count,
        "bet_count": bet_count,
        "stake_total": stake_total,
        "realized_pnl_total": realized_pnl_total,
        "roi_pct": (realized_pnl_total / stake_total * 100.0) if stake_total > 0 else 0.0,
        "average_edge": (weighted_edge / bet_count) if bet_count > 0 else None,
        "paper_edge_hit_rate": (weighted_hit_rate / bet_count) if bet_count > 0 else None,
        "log_loss": (weighted_log_loss / row_count) if row_count > 0 else 999.0,
        "brier_score": (weighted_brier / row_count) if row_count > 0 else 999.0,
        "family_pnl_share_max": family_pnl_share_max,
        "family_metrics": finalized_family_metrics,
    }


def evaluate_multitask_candidate(
    *,
    manifest_path: Path,
    candidate: dict[str, Any],
    stage: str,
    min_train_gps: int,
    min_edge: float,
) -> dict[str, Any]:
    grouped = load_manifest_grouped(manifest_path)
    meeting_keys = sorted(grouped)
    splits = build_walk_forward_splits(meeting_keys, min_train=min_train_gps)
    if not splits:
        raise ValueError(f"Need at least {min_train_gps + 1} meetings in the manifest.")

    trainer_config = trainer_config_from_candidate(candidate)
    folds: list[dict[str, Any]] = []
    predictions_count = 0

    for split_index, split in enumerate(splits, start=1):
        train_paths: list[str] = []
        for meeting_key in split.train_meeting_keys:
            train_paths.extend(
                str(row["path"]) for row in _sorted_snapshot_rows(grouped[meeting_key])
            )
        test_paths = [
            str(row["path"])
            for row in _sorted_snapshot_rows(grouped[split.test_meeting_key])
        ]
        train_frames = _load_snapshot_frames(train_paths)
        test_frames = _load_snapshot_frames(test_paths)
        if not train_frames or not test_frames:
            continue

        train_df = pl.concat(train_frames)
        test_df = pl.concat(test_frames)
        if train_df.height == 0 or test_df.height == 0:
            continue

        result = train_multitask_split(
            train_df,
            test_df,
            model_run_id=f"autoresearch-{stage}-{split.test_meeting_key}-{split_index}",
            stage=stage,
            config=trainer_config,
            min_edge=min_edge,
        )
        predictions_count += len(result.predictions)
        folds.append(
            {
                "test_meeting_key": split.test_meeting_key,
                "train_meeting_keys": list(split.train_meeting_keys),
                **dict(result.metrics),
            }
        )

    aggregate_metrics = aggregate_fold_metrics(folds)
    aggregate_metrics["predictions_count"] = predictions_count
    return {
        "metrics": aggregate_metrics,
        "folds": folds,
        "predictions_count": predictions_count,
    }


def write_candidate_artifacts(
    *,
    storage_dir: Path,
    experiment_id: str,
    candidate: dict[str, Any],
    evaluation: dict[str, Any],
) -> Path:
    artifact_dir = storage_dir / experiment_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "config.json").write_text(
        json.dumps(candidate, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (artifact_dir / "metrics.json").write_text(
        json.dumps(evaluation["metrics"], indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (artifact_dir / "folds.json").write_text(
        json.dumps(evaluation["folds"], indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return artifact_dir


def save_autoresearch_summary(
    *,
    storage_dir: Path,
    history: list[dict[str, Any]],
) -> None:
    comparison_rows = sorted(history, key=lambda entry: hybrid_sort_key(entry["metrics"]))
    lines = [
        "# Multitask Autoresearch",
        "",
        "| Rank | ID | Decision | Total PnL | ROI % | Log loss | Brier | Bets |",
        "|------|----|----------|-----------|-------|----------|-------|------|",
    ]
    for rank, entry in enumerate(comparison_rows, start=1):
        metrics = entry["metrics"]
        lines.append(
            f"| {rank} | {entry['experiment_id']} | {entry['decision']} "
            f"| {float(metrics.get('realized_pnl_total', 0.0)):.3f} "
            f"| {float(metrics.get('roi_pct', 0.0)):.3f} "
            f"| {float(metrics.get('log_loss', 0.0)):.4f} "
            f"| {float(metrics.get('brier_score', 0.0)):.4f} "
            f"| {int(metrics.get('bet_count', 0))} |"
        )
    (storage_dir / "comparison_report.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    if comparison_rows:
        (storage_dir / "best_run.json").write_text(
            json.dumps(comparison_rows[0], indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )


def run_autoresearch_loop(
    *,
    tracker: ExperimentTracker,
    config: AutoResearchConfig,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    rng = Random(config.seed)
    baseline = base_candidate(hidden_dim=config.base_hidden_dim)
    candidates = [baseline]
    parent_id: str | None = None

    for _ in range(max(config.iterations - 1, 0)):
        candidates.append(mutate_config(rng, baseline))

    history: list[dict[str, Any]] = []
    baseline_metrics: dict[str, Any] | None = None
    for idx, candidate in enumerate(candidates):
        experiment_id = f"exp-{idx:04d}"
        evaluation = evaluate_multitask_candidate(
            manifest_path=manifest_path,
            candidate=candidate,
            stage=config.stage,
            min_train_gps=config.min_train_gps,
            min_edge=config.min_edge,
        )
        metrics = dict(evaluation["metrics"])
        if idx == 0:
            decision = "baseline"
            baseline_metrics = metrics
        else:
            decision = (
                "keep"
                if promotion_gate(metrics, baseline_metrics=baseline_metrics)
                else "discard"
            )
            if decision == "keep":
                parent_id = history[0]["experiment_id"] if parent_id is None else parent_id
        artifact_dir = write_candidate_artifacts(
            storage_dir=tracker.storage_dir,
            experiment_id=experiment_id,
            candidate=candidate,
            evaluation=evaluation,
        )
        entry = tracker.log_run(
            experiment_id=experiment_id,
            spec=ExperimentSpec(
                stage=config.stage,
                model_family="torch_multitask",
                config_path=str(artifact_dir / "config.json"),
            ),
            config=candidate,
            metrics=metrics,
            predictions_count=int(evaluation.get("predictions_count", 0) or 0),
            artifact_path=str(artifact_dir),
            parent_experiment_id=parent_id,
            search_phase="baseline" if idx == 0 else "screening",
            decision=decision,
        )
        history.append(entry)
        if decision == "keep" and idx != 0:
            parent_id = experiment_id

    save_autoresearch_summary(storage_dir=tracker.storage_dir, history=history)
    return history
