"""Smoke tests for the LightGBM walk-forward training harness."""

from __future__ import annotations

import numpy as np
import polars as pl
from f1_polymarket_lab.models.lgbm_trainer import (
    LGBMTrainerConfig,
    train_one_split_lgbm,
)


def _make_snapshot_df(meeting_key: int, n_drivers: int = 10) -> pl.DataFrame:
    rng = np.random.default_rng(seed=meeting_key)
    rows: list[dict[str, object]] = []
    event_id = f"event-{meeting_key}"
    winner_idx = rng.integers(0, n_drivers)

    for i in range(n_drivers):
        rows.append({
            "row_id": f"row-{meeting_key}-{i}",
            "meeting_key": meeting_key,
            "event_id": event_id,
            "market_id": f"market-{meeting_key}-{i}",
            "token_id": f"token-{meeting_key}-{i}",
            "driver_name": f"Driver {i}",
            "entry_yes_price": rng.uniform(0.03, 0.30),
            "entry_spread": rng.uniform(0.01, 0.08),
            "entry_midpoint": rng.uniform(0.03, 0.30),
            "trade_count_pre_entry": int(rng.integers(5, 100)),
            "last_trade_age_seconds": rng.uniform(10, 3600),
            "fp1_position": i + 1,
            "fp1_gap_to_leader_seconds": 0.0 if i == 0 else rng.uniform(0.1, 2.0),
            "fp1_teammate_gap_seconds": rng.uniform(-0.5, 0.5),
            "fp1_team_best_gap_to_leader_seconds": rng.uniform(0.0, 1.0),
            "fp1_lap_count": int(rng.integers(15, 30)),
            "fp1_stint_count": int(rng.integers(1, 4)),
            "fp1_result_time_seconds": rng.uniform(60.0, 90.0),
            "label_yes": 1 if i == winner_idx else 0,
        })
    return pl.DataFrame(rows)


def test_train_one_split_lgbm_returns_predictions_and_metrics() -> None:
    train_df = pl.concat([_make_snapshot_df(mk) for mk in [100, 200, 300]])
    test_df = _make_snapshot_df(400)

    result = train_one_split_lgbm(
        train_df,
        test_df,
        model_run_id="lgbm-test-run",
        stage="lgbm_stage",
        config=LGBMTrainerConfig(
            num_boost_round=10,
            early_stopping_rounds=5,
            calibrate=False,
        ),
    )

    assert len(result.predictions) == 10
    assert result.metrics["row_count"] == 10
    assert "brier_score" in result.metrics
    assert "log_loss" in result.metrics
    assert "calibration_buckets" in result.metrics
    assert result.config["feature_importance"]
