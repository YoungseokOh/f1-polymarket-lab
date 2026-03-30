from __future__ import annotations

from pathlib import Path

from f1_polymarket_lab.experiments.autoresearch import promotion_gate
from f1_polymarket_lab.experiments.tracking import ExperimentSpec, ExperimentTracker
from f1_polymarket_worker.cli import app
from typer.testing import CliRunner


def test_promotion_gate_requires_positive_ev_and_bet_support() -> None:
    metrics = {
        "total_pnl": 42.0,
        "roi_pct": 12.5,
        "bet_count": 28,
        "family_pnl_share_max": 0.55,
    }

    assert promotion_gate(metrics) is True


def test_tracker_logs_parent_and_decision(tmp_path: Path) -> None:
    tracker = ExperimentTracker(storage_dir=tmp_path)

    entry = tracker.log_run(
        experiment_id="exp-001",
        spec=ExperimentSpec(
            stage="multitask_qr",
            model_family="torch_multitask",
            config_path="configs/base.json",
        ),
        config={"hidden_dim": 128},
        metrics={"total_pnl": 12.0},
        predictions_count=24,
        artifact_path="artifacts/exp-001",
        parent_experiment_id="exp-000",
        search_phase="screening",
        decision="keep",
    )

    assert entry["parent_experiment_id"] == "exp-000"
    assert entry["decision"] == "keep"


def test_best_run_maximizes_total_pnl(tmp_path: Path) -> None:
    tracker = ExperimentTracker(storage_dir=tmp_path)
    spec = ExperimentSpec(
        stage="multitask_qr",
        model_family="torch_multitask",
        config_path="configs/base.json",
    )
    tracker.log_run(
        experiment_id="exp-001",
        spec=spec,
        config={"hidden_dim": 64},
        metrics={"total_pnl": 10.0},
        decision="discard",
    )
    tracker.log_run(
        experiment_id="exp-002",
        spec=spec,
        config={"hidden_dim": 128},
        metrics={"total_pnl": 25.0},
        decision="keep",
    )

    best = tracker.best_run(metric_key="total_pnl", higher_is_better=True)

    assert best is not None
    assert best["experiment_id"] == "exp-002"


def test_multitask_autoresearch_cli_calls_out_mock_scoring() -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["run-multitask-autoresearch", "--iterations", "1"],
    )

    assert result.exit_code == 0
    assert "experimental" in result.stdout.lower()
    assert "mock scoring" in result.stdout.lower()
