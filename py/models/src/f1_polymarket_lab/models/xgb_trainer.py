"""XGBoost walk-forward training harness.

Trains an XGBoost classifier on feature snapshots, produces calibrated
probability predictions, and stores ``ModelRun`` + ``ModelPrediction``
records for downstream backtest evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

# Feature columns expected in every snapshot parquet
PACE_FEATURES = [
    "fp1_position",
    "fp1_gap_to_leader_seconds",
    "fp1_teammate_gap_seconds",
    "fp1_lap_count",
    "fp1_stint_count",
    "fp1_result_time_seconds",
    "fp1_team_best_gap_to_leader_seconds",
]
MARKET_FEATURES = [
    "entry_yes_price",
    "entry_spread",
    "entry_midpoint",
    "trade_count_pre_entry",
    "last_trade_age_seconds",
]
ALL_FEATURES = PACE_FEATURES + MARKET_FEATURES

LABEL_COL = "label_yes"
GROUP_COL = "event_id"

EPSILON = 1e-15


@dataclass(frozen=True, slots=True)
class WalkForwardSplit:
    """One fold of a walk-forward split."""

    train_meeting_keys: list[int]
    test_meeting_key: int


@dataclass(frozen=True, slots=True)
class TrainResult:
    """Summary returned after training + prediction on one split."""

    model_run_id: str
    test_meeting_key: int
    metrics: dict[str, Any]
    predictions: list[dict[str, Any]]
    config: dict[str, Any]


@dataclass(slots=True)
class XGBTrainerConfig:
    """Tunable hyperparameters for the training harness."""

    feature_cols: list[str] = field(default_factory=lambda: list(ALL_FEATURES))
    xgb_params: dict[str, Any] = field(default_factory=lambda: {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 4,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 3,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
    })
    num_boost_round: int = 200
    early_stopping_rounds: int = 20
    calibrate: bool = True
    min_train_rows: int = 30


def build_walk_forward_splits(
    meeting_keys: list[int],
    min_train: int = 2,
) -> list[WalkForwardSplit]:
    """Create chronological walk-forward splits.

    ``meeting_keys`` must be ordered chronologically.  Each split trains on
    all preceding GPs and tests on the current one.
    """
    splits: list[WalkForwardSplit] = []
    for i in range(min_train, len(meeting_keys)):
        splits.append(
            WalkForwardSplit(
                train_meeting_keys=list(meeting_keys[:i]),
                test_meeting_key=meeting_keys[i],
            )
        )
    return splits


def _prepare_features(
    df: pl.DataFrame,
    feature_cols: list[str],
) -> np.ndarray:
    """Extract feature matrix, filling nulls with column medians."""
    subset = df.select(feature_cols)
    filled = subset.fill_null(strategy="forward").fill_null(0)
    return filled.to_numpy().astype(np.float32)


def _evaluate(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    prices: np.ndarray,
    min_edge: float = 0.05,
) -> dict[str, Any]:
    """Compute evaluation metrics matching the baseline evaluator."""
    n = len(y_true)
    brier = float(np.mean((y_prob - y_true) ** 2))
    log_loss = float(-np.mean(
        y_true * np.log(np.clip(y_prob, EPSILON, 1.0))
        + (1 - y_true) * np.log(np.clip(1 - y_prob, EPSILON, 1.0))
    ))
    edges = y_prob - prices
    selected = edges >= min_edge
    bet_count = int(np.sum(selected))
    if bet_count > 0:
        sel_labels = y_true[selected]
        sel_prices = prices[selected]
        pnl = np.where(sel_labels == 1, 1.0 - sel_prices, -sel_prices)
        hit_rate = float(np.mean(sel_labels))
        avg_edge = float(np.mean(edges[selected]))
        pnl_total = float(np.sum(pnl))
        pnl_avg = float(np.mean(pnl))
    else:
        hit_rate = None
        avg_edge = None
        pnl_total = 0.0
        pnl_avg = None

    return {
        "row_count": n,
        "brier_score": brier,
        "log_loss": log_loss,
        "bet_count": bet_count,
        "paper_edge_hit_rate": hit_rate,
        "average_edge": avg_edge,
        "realized_pnl_total": pnl_total,
        "realized_pnl_avg": pnl_avg,
    }


def train_one_split(
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    *,
    config: XGBTrainerConfig | None = None,
    model_run_id: str,
    stage: str,
    min_edge: float = 0.05,
) -> TrainResult:
    """Train XGBoost on *train_df*, predict on *test_df*, return results.

    This is a pure function — it does NOT touch the database.  The caller
    is responsible for persisting ``ModelRun`` and ``ModelPrediction`` rows.
    """
    cfg = config or XGBTrainerConfig()
    feature_cols = [c for c in cfg.feature_cols if c in train_df.columns and c in test_df.columns]

    X_train = _prepare_features(train_df, feature_cols)
    y_train = train_df[LABEL_COL].to_numpy().astype(np.float32)
    X_test = _prepare_features(test_df, feature_cols)
    y_test = test_df[LABEL_COL].to_numpy().astype(np.float32)

    # Group-aware eval split — hold out latest group from train for early stopping
    train_groups = train_df[GROUP_COL].to_list()
    unique_groups = list(dict.fromkeys(train_groups))  # preserve order

    if len(unique_groups) >= 2:
        last_group = unique_groups[-1]
        eval_mask = np.array([g == last_group for g in train_groups])
        dtrain = xgb.DMatrix(X_train[~eval_mask], label=y_train[~eval_mask])
        deval = xgb.DMatrix(X_train[eval_mask], label=y_train[eval_mask])
        evals = [(dtrain, "train"), (deval, "eval")]
    else:
        dtrain = xgb.DMatrix(X_train, label=y_train)
        evals = [(dtrain, "train")]

    booster = xgb.train(
        cfg.xgb_params,
        dtrain,
        num_boost_round=cfg.num_boost_round,
        evals=evals,
        early_stopping_rounds=cfg.early_stopping_rounds if len(evals) > 1 else None,
        verbose_eval=False,
    )

    dtest = xgb.DMatrix(X_test)
    raw_probs = booster.predict(dtest)

    # Optional isotonic calibration using the training set
    if cfg.calibrate and len(X_train) >= cfg.min_train_rows:
        from sklearn.isotonic import IsotonicRegression

        train_preds = booster.predict(dtrain)
        iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        iso.fit(train_preds, y_train[~eval_mask] if len(unique_groups) >= 2 else y_train)
        raw_probs = iso.predict(raw_probs)

    prices = test_df["entry_yes_price"].to_numpy().astype(np.float32)
    metrics = _evaluate(y_test, raw_probs, prices, min_edge=min_edge)

    test_meeting_key = int(test_df["meeting_key"][0])
    now = datetime.now(tz=timezone.utc)

    predictions: list[dict[str, Any]] = []
    for i in range(len(test_df)):
        row = test_df.row(i, named=True)
        prob = float(raw_probs[i])
        predictions.append({
            "model_run_id": model_run_id,
            "market_id": row.get("market_id"),
            "token_id": row.get("token_id"),
            "as_of_ts": now,
            "probability_yes": prob,
            "probability_no": 1.0 - prob,
            "raw_score": prob,
            "calibration_version": "isotonic_v1" if cfg.calibrate else None,
        })

    run_config = {
        "feature_cols": feature_cols,
        "xgb_params": cfg.xgb_params,
        "num_boost_round": cfg.num_boost_round,
        "calibrate": cfg.calibrate,
        "min_edge": min_edge,
        "train_rows": len(X_train),
        "test_rows": len(X_test),
    }

    return TrainResult(
        model_run_id=model_run_id,
        test_meeting_key=test_meeting_key,
        metrics=metrics,
        predictions=predictions,
        config=run_config,
    )
