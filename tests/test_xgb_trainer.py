"""Tests for the XGBoost walk-forward training harness."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from f1_polymarket_lab.models.xgb_trainer import (
    TrainResult,
    XGBTrainerConfig,
    _evaluate,
    _prepare_features,
    build_walk_forward_splits,
    train_one_split,
)

# ---------------------------------------------------------------------------
# build_walk_forward_splits
# ---------------------------------------------------------------------------

class TestBuildWalkForwardSplits:
    def test_basic_three_gps(self) -> None:
        splits = build_walk_forward_splits([1, 2, 3], min_train=2)
        assert len(splits) == 1
        assert splits[0].train_meeting_keys == [1, 2]
        assert splits[0].test_meeting_key == 3

    def test_four_gps(self) -> None:
        splits = build_walk_forward_splits([10, 20, 30, 40], min_train=2)
        assert len(splits) == 2
        assert splits[0].train_meeting_keys == [10, 20]
        assert splits[0].test_meeting_key == 30
        assert splits[1].train_meeting_keys == [10, 20, 30]
        assert splits[1].test_meeting_key == 40

    def test_too_few_gps(self) -> None:
        splits = build_walk_forward_splits([1, 2], min_train=2)
        assert splits == []

    def test_single_gp(self) -> None:
        splits = build_walk_forward_splits([1], min_train=1)
        assert splits == []

    def test_min_train_one(self) -> None:
        splits = build_walk_forward_splits([1, 2, 3], min_train=1)
        assert len(splits) == 2
        assert splits[0].train_meeting_keys == [1]
        assert splits[0].test_meeting_key == 2


# ---------------------------------------------------------------------------
# _prepare_features
# ---------------------------------------------------------------------------

class TestPrepareFeatures:
    def test_fills_nulls(self) -> None:
        df = pl.DataFrame({"a": [1.0, None, 3.0], "b": [None, 2.0, None]})
        X = _prepare_features(df, ["a", "b"])
        assert X.shape == (3, 2)
        assert not np.isnan(X).any()

    def test_correct_shape(self) -> None:
        df = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
        X = _prepare_features(df, ["x", "z"])
        assert X.shape == (3, 2)


# ---------------------------------------------------------------------------
# _evaluate
# ---------------------------------------------------------------------------

class TestEvaluate:
    def test_perfect_predictions(self) -> None:
        y_true = np.array([1.0, 0.0, 1.0, 0.0])
        y_prob = np.array([0.99, 0.01, 0.99, 0.01])
        prices = np.array([0.5, 0.5, 0.5, 0.5])
        metrics = _evaluate(y_true, y_prob, prices, min_edge=0.05)
        assert metrics["brier_score"] < 0.01
        assert metrics["row_count"] == 4
        assert metrics["bet_count"] == 2  # only label_yes=1 rows have edge > 0.05

    def test_no_edge_bets(self) -> None:
        y_true = np.array([1.0, 0.0])
        y_prob = np.array([0.50, 0.50])
        prices = np.array([0.50, 0.50])
        metrics = _evaluate(y_true, y_prob, prices, min_edge=0.05)
        assert metrics["bet_count"] == 0
        assert metrics["realized_pnl_total"] == 0.0
        assert metrics["paper_edge_hit_rate"] is None


# ---------------------------------------------------------------------------
# train_one_split (integration)
# ---------------------------------------------------------------------------

def _make_snapshot_df(meeting_key: int, n_drivers: int = 10) -> pl.DataFrame:
    """Build a synthetic snapshot DataFrame."""
    rng = np.random.default_rng(seed=meeting_key)
    rows: list[dict] = []
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


class TestTrainOneSplit:
    def test_basic_training(self) -> None:
        train_df = pl.concat([_make_snapshot_df(mk) for mk in [100, 200]])
        test_df = _make_snapshot_df(300)

        result = train_one_split(
            train_df,
            test_df,
            model_run_id="test-run-001",
            stage="test_stage",
            config=XGBTrainerConfig(
                num_boost_round=10,
                early_stopping_rounds=5,
                calibrate=False,
            ),
        )
        assert isinstance(result, TrainResult)
        assert result.model_run_id == "test-run-001"
        assert result.test_meeting_key == 300
        assert len(result.predictions) == 10
        assert "brier_score" in result.metrics
        assert "log_loss" in result.metrics

        # Probabilities should be valid
        for pred in result.predictions:
            assert 0.0 <= pred["probability_yes"] <= 1.0
            assert pred["probability_no"] == pytest.approx(1.0 - pred["probability_yes"])

    def test_with_calibration(self) -> None:
        train_df = pl.concat([_make_snapshot_df(mk) for mk in [100, 200, 300]])
        test_df = _make_snapshot_df(400)

        result = train_one_split(
            train_df,
            test_df,
            model_run_id="test-run-cal",
            stage="test_stage",
            config=XGBTrainerConfig(
                num_boost_round=10,
                early_stopping_rounds=5,
                calibrate=True,
            ),
        )
        assert isinstance(result, TrainResult)
        assert len(result.predictions) == 10
        assert result.config["calibrate"] is True

    def test_predictions_have_required_fields(self) -> None:
        train_df = pl.concat([_make_snapshot_df(mk) for mk in [100, 200]])
        test_df = _make_snapshot_df(300)

        result = train_one_split(
            train_df, test_df,
            model_run_id="test-run-fields",
            stage="test_stage",
            config=XGBTrainerConfig(num_boost_round=5, calibrate=False),
        )
        required_keys = {"model_run_id", "market_id", "token_id", "as_of_ts",
                         "probability_yes", "probability_no", "raw_score"}
        for pred in result.predictions:
            assert required_keys.issubset(pred.keys())
