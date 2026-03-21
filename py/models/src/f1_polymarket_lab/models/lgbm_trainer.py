"""LightGBM walk-forward training harness.

Same interface as ``xgb_trainer`` but backed by LightGBM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import lightgbm as lgb
import numpy as np
import polars as pl

from .xgb_trainer import (
    ALL_FEATURES,
    GROUP_COL,
    LABEL_COL,
    TrainResult,
    _evaluate,
    _prepare_features,
)


@dataclass(slots=True)
class LGBMTrainerConfig:
    """Tunable hyperparameters for the LightGBM harness."""

    feature_cols: list[str] = field(default_factory=lambda: list(ALL_FEATURES))
    lgbm_params: dict[str, Any] = field(default_factory=lambda: {
        "objective": "binary",
        "metric": "binary_logloss",
        "num_leaves": 15,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_child_samples": 5,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "verbose": -1,
    })
    num_boost_round: int = 200
    early_stopping_rounds: int = 20
    calibrate: bool = True
    min_train_rows: int = 30


def train_one_split_lgbm(
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    *,
    config: LGBMTrainerConfig | None = None,
    model_run_id: str,
    stage: str,
    min_edge: float = 0.05,
) -> TrainResult:
    """Train LightGBM on *train_df*, predict on *test_df*, return results."""
    cfg = config or LGBMTrainerConfig()
    feature_cols = [c for c in cfg.feature_cols if c in train_df.columns and c in test_df.columns]

    X_train = _prepare_features(train_df, feature_cols)
    y_train = train_df[LABEL_COL].to_numpy().astype(np.float32)
    X_test = _prepare_features(test_df, feature_cols)
    y_test = test_df[LABEL_COL].to_numpy().astype(np.float32)

    train_groups = train_df[GROUP_COL].to_list()
    unique_groups = list(dict.fromkeys(train_groups))

    if len(unique_groups) >= 2:
        last_group = unique_groups[-1]
        eval_mask = np.array([g == last_group for g in train_groups])
        dtrain = lgb.Dataset(X_train[~eval_mask], label=y_train[~eval_mask])
        deval = lgb.Dataset(X_train[eval_mask], label=y_train[eval_mask], reference=dtrain)
        callbacks = [lgb.early_stopping(cfg.early_stopping_rounds, verbose=False)]
        valid_sets = [dtrain, deval]
        valid_names = ["train", "eval"]
    else:
        dtrain = lgb.Dataset(X_train, label=y_train)
        callbacks = []
        valid_sets = [dtrain]
        valid_names = ["train"]

    booster = lgb.train(
        cfg.lgbm_params,
        dtrain,
        num_boost_round=cfg.num_boost_round,
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=callbacks,
    )

    raw_probs = booster.predict(X_test).astype(np.float64)

    if cfg.calibrate and len(X_train) >= cfg.min_train_rows:
        from sklearn.isotonic import IsotonicRegression

        train_preds = booster.predict(X_train[~eval_mask] if len(unique_groups) >= 2 else X_train)
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

    gain = booster.feature_importance(importance_type="gain").tolist()
    importance = dict(zip(feature_cols, gain, strict=True))

    run_config = {
        "feature_cols": feature_cols,
        "lgbm_params": cfg.lgbm_params,
        "num_boost_round": cfg.num_boost_round,
        "calibrate": cfg.calibrate,
        "min_edge": min_edge,
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "feature_importance": importance,
    }

    return TrainResult(
        model_run_id=model_run_id,
        test_meeting_key=test_meeting_key,
        metrics=metrics,
        predictions=predictions,
        config=run_config,
    )
