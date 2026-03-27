from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from random import Random
from typing import Any

from .tracking import ExperimentSpec, ExperimentTracker


@dataclass(frozen=True, slots=True)
class AutoResearchConfig:
    iterations: int = 20
    seed: int = 7
    stage: str = "multitask_qr"
    base_hidden_dim: int = 128


def promotion_gate(metrics: dict[str, Any]) -> bool:
    return (
        float(metrics.get("total_pnl", 0.0)) > 0.0
        and float(metrics.get("roi_pct", 0.0)) > 0.0
        and int(metrics.get("bet_count", 0)) >= 10
        and float(metrics.get("family_pnl_share_max", 1.0)) <= 0.80
    )


def mutate_config(rng: Random, base_config: dict[str, Any]) -> dict[str, Any]:
    return {
        **base_config,
        "hidden_dim": rng.choice([64, 96, 128, 160]),
        "depth": rng.choice([1, 2, 3]),
        "dropout": rng.choice([0.0, 0.05, 0.1, 0.2]),
        "winner_weight": rng.choice([1.0, 1.25, 1.5]),
        "h2h_weight": rng.choice([1.0, 1.25, 1.5]),
    }


def run_autoresearch_loop(
    *,
    tracker: ExperimentTracker,
    config: AutoResearchConfig,
    scoring_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> list[dict[str, Any]]:
    rng = Random(config.seed)
    incumbent = {
        "hidden_dim": config.base_hidden_dim,
        "depth": 2,
        "dropout": 0.1,
        "winner_weight": 1.0,
        "h2h_weight": 1.0,
    }
    history: list[dict[str, Any]] = []
    parent_id: str | None = None

    for idx in range(config.iterations):
        candidate = mutate_config(rng, incumbent)
        metrics = scoring_fn(candidate)
        decision = "keep" if promotion_gate(metrics) else "discard"
        experiment_id = f"exp-{idx:04d}"
        entry = tracker.log_run(
            experiment_id=experiment_id,
            spec=ExperimentSpec(
                stage=config.stage,
                model_family="torch_multitask",
                config_path=f"generated/{experiment_id}.json",
            ),
            config=candidate,
            metrics=metrics,
            predictions_count=int(metrics.get("bet_count", 0)),
            artifact_path=f"artifacts/{experiment_id}",
            parent_experiment_id=parent_id,
            search_phase="screening",
            decision=decision,
        )
        history.append(entry)
        if decision == "keep":
            incumbent = candidate
            parent_id = experiment_id

    return history
