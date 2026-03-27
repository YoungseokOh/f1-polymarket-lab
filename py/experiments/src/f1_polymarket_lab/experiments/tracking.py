from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ExperimentSpec:
    stage: str
    model_family: str
    config_path: str
    description: str | None = None


@dataclass(slots=True)
class ExperimentTracker:
    """Track experiment runs: configs, metrics, and comparison reports."""

    storage_dir: Path
    runs: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def log_run(
        self,
        *,
        experiment_id: str,
        spec: ExperimentSpec,
        config: dict[str, Any],
        metrics: dict[str, Any],
        predictions_count: int = 0,
        artifact_path: str | None = None,
        parent_experiment_id: str | None = None,
        search_phase: str = "screening",
        decision: str = "discard",
    ) -> dict[str, Any]:
        """Record a single experiment run."""
        entry = {
            "experiment_id": experiment_id,
            "parent_experiment_id": parent_experiment_id,
            "search_phase": search_phase,
            "decision": decision,
            "stage": spec.stage,
            "model_family": spec.model_family,
            "config_path": spec.config_path,
            "description": spec.description,
            "config": config,
            "metrics": metrics,
            "predictions_count": predictions_count,
            "artifact_path": artifact_path,
            "logged_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self.runs.append(entry)

        run_file = self.storage_dir / f"{experiment_id}.json"
        run_file.write_text(json.dumps(entry, indent=2, default=str), encoding="utf-8")
        return entry

    def compare_runs(
        self,
        metric_key: str = "log_loss",
        *,
        higher_is_better: bool = False,
    ) -> list[dict[str, Any]]:
        """Return runs sorted by a metric."""
        scored = [
            r for r in self.runs
            if r.get("metrics", {}).get(metric_key) is not None
        ]
        return sorted(
            scored,
            key=lambda r: float(r["metrics"][metric_key]),
            reverse=higher_is_better,
        )

    def best_run(
        self,
        metric_key: str = "log_loss",
        *,
        higher_is_better: bool = False,
    ) -> dict[str, Any] | None:
        """Return the best run by the given metric."""
        compared = self.compare_runs(metric_key, higher_is_better=higher_is_better)
        return compared[0] if compared else None

    def save_comparison_report(
        self,
        metric_key: str = "log_loss",
        *,
        higher_is_better: bool = False,
    ) -> Path:
        """Write a markdown comparison table to disk."""
        compared = self.compare_runs(metric_key, higher_is_better=higher_is_better)
        direction = "descending" if higher_is_better else "ascending"
        lines = [
            "# Experiment Comparison",
            "",
            f"Sorted by `{metric_key}` ({direction})",
            "",
            f"| Rank | ID | Family | Stage | {metric_key} | Brier | Bet Count |",
            "|------|-------|--------|-------|---------|-------|-----------|",
        ]
        for i, run in enumerate(compared, 1):
            m = run.get("metrics", {})
            metric_value = m.get(metric_key)
            brier = m.get("brier_score")
            metric_text = f"{metric_value:.4f}" if isinstance(metric_value, int | float) else "N/A"
            brier_text = f"{brier:.4f}" if isinstance(brier, int | float) else "N/A"
            lines.append(
                f"| {i} | {run['experiment_id'][:12]} | {run['model_family']} "
                f"| {run['stage']} | {metric_text} "
                f"| {brier_text} "
                f"| {m.get('bet_count', 0)} |"
            )

        report_path = self.storage_dir / "comparison_report.md"
        report_path.write_text("\n".join(lines), encoding="utf-8")
        return report_path
