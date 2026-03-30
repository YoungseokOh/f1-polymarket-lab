from __future__ import annotations

import json
from pathlib import Path

from f1_polymarket_lab.experiments.autoresearch import (
    aggregate_fold_metrics,
    hybrid_sort_key,
    promotion_gate,
)
from f1_polymarket_lab.experiments.tracking import ExperimentSpec, ExperimentTracker
from f1_polymarket_worker.cli import app
from typer.testing import CliRunner


def test_promotion_gate_requires_positive_ev_support_and_baseline_guard() -> None:
    baseline_metrics = {
        "log_loss": 0.40,
        "brier_score": 0.18,
    }
    metrics = {
        "realized_pnl_total": 42.0,
        "roi_pct": 12.5,
        "bet_count": 28,
        "family_pnl_share_max": 0.55,
        "log_loss": 0.42,
        "brier_score": 0.19,
    }

    assert promotion_gate(metrics, baseline_metrics=baseline_metrics) is True
    assert promotion_gate(
        {**metrics, "log_loss": 0.45},
        baseline_metrics=baseline_metrics,
    ) is False


def test_aggregate_fold_metrics_combines_pnl_and_family_concentration() -> None:
    aggregate = aggregate_fold_metrics(
        [
            {
                "row_count": 10,
                "bet_count": 4,
                "stake_total": 1.2,
                "realized_pnl_total": 0.30,
                "average_edge": 0.08,
                "paper_edge_hit_rate": 0.75,
                "log_loss": 0.41,
                "brier_score": 0.19,
                "family_metrics": {
                    "winner": {
                        "row_count": 5,
                        "bet_count": 3,
                        "stake_total": 0.8,
                        "realized_pnl_total": 0.20,
                        "brier_score": 0.18,
                    }
                },
            },
            {
                "row_count": 8,
                "bet_count": 2,
                "stake_total": 0.6,
                "realized_pnl_total": 0.10,
                "average_edge": 0.05,
                "paper_edge_hit_rate": 0.50,
                "log_loss": 0.39,
                "brier_score": 0.17,
                "family_metrics": {
                    "pole": {
                        "row_count": 8,
                        "bet_count": 2,
                        "stake_total": 0.6,
                        "realized_pnl_total": 0.10,
                        "brier_score": 0.17,
                    }
                },
            },
        ]
    )

    assert aggregate["row_count"] == 18
    assert aggregate["bet_count"] == 6
    assert aggregate["realized_pnl_total"] == 0.4
    assert aggregate["roi_pct"] > 0
    assert aggregate["family_pnl_share_max"] > 0


def test_hybrid_sort_key_prefers_pnl_then_roi_then_predictive_metrics() -> None:
    better = hybrid_sort_key(
        {
            "realized_pnl_total": 2.0,
            "roi_pct": 10.0,
            "log_loss": 0.40,
            "brier_score": 0.20,
        }
    )
    worse = hybrid_sort_key(
        {
            "realized_pnl_total": 1.0,
            "roi_pct": 20.0,
            "log_loss": 0.30,
            "brier_score": 0.10,
        }
    )

    assert better < worse


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
        metrics={"realized_pnl_total": 12.0},
        predictions_count=24,
        artifact_path="artifacts/exp-001",
        parent_experiment_id="exp-000",
        search_phase="screening",
        decision="keep",
    )

    assert entry["parent_experiment_id"] == "exp-000"
    assert entry["decision"] == "keep"


def test_multitask_autoresearch_cli_requires_manifest_and_reports_best(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"snapshots": []}), encoding="utf-8")

    def fake_run_autoresearch_loop(*, tracker, config, manifest_path):
        assert manifest_path == manifest
        entry = tracker.log_run(
            experiment_id="exp-0000",
            spec=ExperimentSpec(
                stage=config.stage,
                model_family="torch_multitask",
                config_path="generated/exp-0000.json",
            ),
            config={"hidden_dim": 128},
            metrics={
                "realized_pnl_total": 1.5,
                "roi_pct": 6.0,
                "log_loss": 0.41,
                "brier_score": 0.18,
                "bet_count": 12,
            },
            predictions_count=12,
            artifact_path="artifacts/exp-0000",
            decision="baseline",
        )
        return [entry]

    monkeypatch.setattr(
        "f1_polymarket_lab.experiments.run_autoresearch_loop",
        fake_run_autoresearch_loop,
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run-multitask-autoresearch",
            "--manifest",
            str(manifest),
            "--iterations",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert '"runs": 1' in result.stdout
    assert '"best"' in result.stdout
