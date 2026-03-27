from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import polars as pl
import torch
from sklearn.isotonic import IsotonicRegression
from torch import nn

from .multitask_model import HEADS, MultitaskModelConfig, MultitaskTabularModel
from .xgb_trainer import EPSILON, TrainResult


@dataclass(slots=True)
class MultitaskTrainerConfig:
    feature_cols: list[str] = field(default_factory=list)
    hidden_dim: int = 128
    depth: int = 2
    dropout: float = 0.1
    lr: float = 1e-3
    max_epochs: int = 20
    batch_size: int = 64
    head_weights: dict[str, float] = field(
        default_factory=lambda: {
            "pole": 1.0,
            "constructor_pole": 1.0,
            "winner": 1.0,
            "h2h": 1.0,
        }
    )


def _default_feature_cols(df: pl.DataFrame) -> list[str]:
    excluded = {
        "meeting_key",
        "event_id",
        "market_id",
        "token_id",
        "target_market_family",
        "as_of_checkpoint",
        "label_yes",
    }
    return [
        col
        for col, dtype in zip(df.columns, df.dtypes, strict=True)
        if col not in excluded and dtype.is_numeric()
    ]


def _feature_tensor(df: pl.DataFrame, feature_cols: list[str]) -> torch.Tensor:
    return torch.tensor(df.select(feature_cols).rows(), dtype=torch.float32)


def _label_tensor(df: pl.DataFrame) -> torch.Tensor:
    return torch.tensor(df["label_yes"].to_list(), dtype=torch.float32)


def _family_array(df: pl.DataFrame) -> np.ndarray:
    return np.asarray(df["target_market_family"].to_list(), dtype=object)


def train_multitask_split(
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    *,
    model_run_id: str,
    stage: str,
    config: MultitaskTrainerConfig | None = None,
) -> TrainResult:
    cfg = config or MultitaskTrainerConfig()
    feature_cols = cfg.feature_cols or _default_feature_cols(train_df)

    x_train = _feature_tensor(train_df, feature_cols)
    y_train = _label_tensor(train_df)
    x_test = _feature_tensor(test_df, feature_cols)
    y_test = np.asarray(test_df["label_yes"].to_list(), dtype=np.float64)
    family_train = _family_array(train_df)
    family_test = _family_array(test_df)

    model = MultitaskTabularModel(
        MultitaskModelConfig(
            input_dim=len(feature_cols),
            hidden_dim=cfg.hidden_dim,
            depth=cfg.depth,
            dropout=cfg.dropout,
        )
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    loss_fn = nn.BCEWithLogitsLoss(reduction="none")

    for _ in range(cfg.max_epochs):
        model.train()
        logits = model(x_train)
        total_loss = torch.tensor(0.0, dtype=torch.float32)
        for head in HEADS:
            mask = torch.tensor((family_train == head).tolist(), dtype=torch.bool)
            if int(mask.sum()) == 0:
                continue
            head_loss = loss_fn(logits[head][mask], y_train[mask]).mean()
            total_loss = total_loss + cfg.head_weights[head] * head_loss
        optimizer.zero_grad()
        torch.autograd.backward(total_loss)
        optimizer.step()

    model.eval()
    with torch.no_grad():
        raw_train = model(x_train)
        raw_test = model(x_test)

    probs_by_head: dict[str, np.ndarray] = {}
    calibrators: dict[str, IsotonicRegression] = {}
    y_train_np = np.asarray(y_train.tolist(), dtype=np.float64)

    for head in HEADS:
        train_mask = family_train == head
        raw_train_probs = np.asarray(torch.sigmoid(raw_train[head]).tolist(), dtype=np.float64)
        raw_test_probs = np.asarray(torch.sigmoid(raw_test[head]).tolist(), dtype=np.float64)
        if int(train_mask.sum()) >= 10:
            calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
            calibrator.fit(raw_train_probs[train_mask], y_train_np[train_mask])
            calibrators[head] = calibrator
            probs_by_head[head] = calibrator.predict(raw_test_probs)
        else:
            probs_by_head[head] = raw_test_probs

    combined_probs = np.zeros(len(test_df), dtype=np.float64)
    predictions: list[dict[str, Any]] = []
    for idx, row in enumerate(test_df.to_dicts()):
        family = str(row["target_market_family"])
        prob = float(probs_by_head[family][idx])
        combined_probs[idx] = prob
        predictions.append(
            {
                "model_run_id": model_run_id,
                "market_id": row["market_id"],
                "token_id": row["token_id"],
                "as_of_ts": datetime.now(tz=timezone.utc),
                "probability_yes": prob,
                "probability_no": 1.0 - prob,
                "raw_score": prob,
                "calibration_version": "isotonic_v1" if family in calibrators else "identity",
                "explanation_json": {
                    "target_market_family": family,
                    "as_of_checkpoint": row["as_of_checkpoint"],
                    "stage": stage,
                },
            }
        )

    family_metrics: dict[str, Any] = {}
    for head in HEADS:
        mask = family_test == head
        if int(mask.sum()) == 0:
            continue
        family_metrics[head] = {
            "row_count": int(mask.sum()),
            "brier_score": float(np.mean((combined_probs[mask] - y_test[mask]) ** 2)),
        }

    metrics = {
        "row_count": len(test_df),
        "brier_score": float(np.mean((combined_probs - y_test) ** 2)),
        "log_loss": float(
            -np.mean(
                y_test * np.log(np.clip(combined_probs, EPSILON, 1.0))
                + (1.0 - y_test) * np.log(np.clip(1.0 - combined_probs, EPSILON, 1.0))
            )
        ),
        "family_metrics": family_metrics,
    }

    return TrainResult(
        model_run_id=model_run_id,
        test_meeting_key=int(test_df["meeting_key"][0]),
        metrics=metrics,
        predictions=predictions,
        config={
            "feature_cols": feature_cols,
            "hidden_dim": cfg.hidden_dim,
            "depth": cfg.depth,
            "head_weights": cfg.head_weights,
        },
    )
