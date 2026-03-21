"""Optuna hyperparameter tuner for walk-forward training.

Uses walk-forward cross-validation as the objective, optimising log-loss
on validation folds.  Supports both XGBoost and LightGBM.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import optuna
import polars as pl

from .xgb_trainer import (
    WalkForwardSplit,
    XGBTrainerConfig,
    build_walk_forward_splits,
    train_one_split,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)


def _xgb_objective(
    trial: optuna.Trial,
    splits: list[WalkForwardSplit],
    dataframes: dict[int, pl.DataFrame],
    stage: str,
) -> float:
    """Optuna objective: train XGBoost walk-forward and return mean log-loss."""
    config = XGBTrainerConfig(
        xgb_params={
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "max_depth": trial.suggest_int("max_depth", 2, 8),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        },
        num_boost_round=trial.suggest_int("num_boost_round", 50, 500),
        calibrate=True,
    )

    losses: list[float] = []
    for sp in splits:
        train_dfs = [dataframes[mk] for mk in sp.train_meeting_keys if mk in dataframes]
        test_df = dataframes.get(sp.test_meeting_key)
        if not train_dfs or test_df is None:
            continue
        train_df = pl.concat(train_dfs)
        if len(train_df) < config.min_train_rows:
            continue

        result = train_one_split(
            train_df,
            test_df,
            model_run_id=f"optuna-{trial.number}-{sp.test_meeting_key}",
            stage=stage,
            config=config,
        )
        ll = result.metrics.get("log_loss")
        if ll is not None:
            losses.append(ll)

    if not losses:
        return float("inf")
    return float(np.mean(losses))


def tune_xgb(
    dataframes: dict[int, pl.DataFrame],
    meeting_keys: list[int],
    *,
    stage: str = "xgb_pole_quicktest",
    n_trials: int = 50,
    min_train_gps: int = 2,
) -> dict[str, Any]:
    """Run Optuna hyperparameter search for XGBoost.

    Returns the best trial params and log-loss score.
    """
    splits = build_walk_forward_splits(meeting_keys, min_train=min_train_gps)
    if not splits:
        return {"error": "not enough GPs for walk-forward splits"}

    study = optuna.create_study(direction="minimize")
    study.optimize(
        lambda trial: _xgb_objective(trial, splits, dataframes, stage),
        n_trials=n_trials,
    )

    return {
        "best_params": study.best_params,
        "best_log_loss": study.best_value,
        "n_trials": len(study.trials),
        "best_trial_number": study.best_trial.number,
    }
