from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import torch
from sklearn.isotonic import IsotonicRegression
from torch import nn

from .calibration import expected_calibration_error, serialize_reliability_diagram
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
    patience: int = 5
    min_delta: float = 1e-4
    validation_group_count: int = 1
    min_calibration_rows: int = 10
    min_edge: float = 0.05
    seed: int = 7
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
    return torch.tensor(df.select(feature_cols).fill_null(0).rows(), dtype=torch.float32)


def _label_tensor(df: pl.DataFrame) -> torch.Tensor:
    return torch.tensor(df["label_yes"].to_list(), dtype=torch.float32)


def _family_array(df: pl.DataFrame) -> np.ndarray:
    return np.asarray(df["target_market_family"].to_list(), dtype=object)


def _checkpoint_array(df: pl.DataFrame) -> np.ndarray:
    return np.asarray(df["as_of_checkpoint"].to_list(), dtype=object)


def _train_validation_split(
    train_df: pl.DataFrame,
    *,
    validation_group_count: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if validation_group_count <= 0 or "event_id" not in train_df.columns:
        return train_df, train_df.head(0)

    groups = list(dict.fromkeys(str(value) for value in train_df["event_id"].to_list()))
    if len(groups) <= validation_group_count:
        return train_df, train_df.head(0)

    validation_groups = groups[-validation_group_count:]
    val_df = train_df.filter(pl.col("event_id").cast(pl.String).is_in(validation_groups))
    fit_df = train_df.filter(~pl.col("event_id").cast(pl.String).is_in(validation_groups))
    if fit_df.height == 0 or val_df.height == 0:
        return train_df, train_df.head(0)
    return fit_df, val_df


def _masked_head_loss(
    *,
    logits: dict[str, torch.Tensor],
    labels: torch.Tensor,
    family_names: np.ndarray,
    loss_fn: nn.Module,
    head_weights: dict[str, float],
) -> torch.Tensor:
    total_loss = torch.tensor(0.0, dtype=torch.float32)
    for head in HEADS:
        mask = torch.tensor((family_names == head).tolist(), dtype=torch.bool)
        if int(mask.sum()) == 0:
            continue
        head_loss = loss_fn(logits[head][mask], labels[mask]).mean()
        total_loss = total_loss + float(head_weights[head]) * head_loss
    return total_loss


def _predict_head_probabilities(
    model: MultitaskTabularModel,
    frame: pl.DataFrame,
    *,
    feature_cols: list[str],
) -> dict[str, np.ndarray]:
    if frame.height == 0:
        return {head: np.array([], dtype=np.float64) for head in HEADS}
    x = _feature_tensor(frame, feature_cols)
    model.eval()
    with torch.no_grad():
        raw_outputs = model(x)
    return {
        head: np.asarray(torch.sigmoid(raw_outputs[head]).tolist(), dtype=np.float64)
        for head in HEADS
    }


def _serialize_calibrator(calibrator: IsotonicRegression) -> dict[str, list[float]]:
    return {
        "x_thresholds": [float(value) for value in calibrator.X_thresholds_],
        "y_thresholds": [float(value) for value in calibrator.y_thresholds_],
    }


def _apply_serialized_calibrator(
    raw_probs: np.ndarray,
    payload: dict[str, list[float]] | None,
) -> np.ndarray:
    if payload is None:
        return raw_probs
    x_thresholds = np.asarray(payload["x_thresholds"], dtype=np.float64)
    y_thresholds = np.asarray(payload["y_thresholds"], dtype=np.float64)
    if x_thresholds.size == 0 or y_thresholds.size == 0:
        return raw_probs
    clipped = np.clip(raw_probs, x_thresholds[0], x_thresholds[-1])
    return np.interp(clipped, x_thresholds, y_thresholds)


def _evaluate_subset(
    *,
    y_true: np.ndarray,
    y_prob: np.ndarray,
    prices: np.ndarray,
    min_edge: float,
) -> dict[str, Any]:
    if len(y_true) == 0:
        return {
            "row_count": 0,
            "brier_score": None,
            "log_loss": None,
            "calibration_buckets": {},
            "ece": None,
            "bet_count": 0,
            "paper_edge_hit_rate": None,
            "total_pnl": 0.0,
            "realized_pnl_total": 0.0,
            "realized_pnl_avg": None,
            "stake_total": 0.0,
            "roi_pct": None,
            "average_edge": None,
        }

    brier_score = float(np.mean((y_prob - y_true) ** 2))
    log_loss = float(
        -np.mean(
            y_true * np.log(np.clip(y_prob, EPSILON, 1.0))
            + (1.0 - y_true) * np.log(np.clip(1.0 - y_prob, EPSILON, 1.0))
        )
    )
    ece = float(expected_calibration_error(y_true, y_prob))
    calibration_buckets = serialize_reliability_diagram(y_true, y_prob)
    edges = y_prob - prices
    selected = edges >= min_edge
    bet_count = int(np.sum(selected))
    if bet_count > 0:
        selected_labels = y_true[selected]
        selected_prices = prices[selected]
        pnl = np.where(selected_labels == 1, 1.0 - selected_prices, -selected_prices)
        total_pnl = float(np.sum(pnl))
        average_pnl = float(np.mean(pnl))
        stake_total = float(np.sum(selected_prices))
        roi_pct = float((total_pnl / stake_total) * 100.0) if stake_total > 0 else None
        average_edge = float(np.mean(edges[selected]))
        hit_rate = float(np.mean(selected_labels))
    else:
        total_pnl = 0.0
        average_pnl = None
        stake_total = 0.0
        roi_pct = None
        hit_rate = None
        average_edge = None

    return {
        "row_count": len(y_true),
        "brier_score": brier_score,
        "log_loss": log_loss,
        "calibration_buckets": calibration_buckets,
        "ece": ece,
        "bet_count": bet_count,
        "paper_edge_hit_rate": hit_rate,
        "total_pnl": total_pnl,
        "realized_pnl_total": total_pnl,
        "realized_pnl_avg": average_pnl,
        "stake_total": stake_total,
        "roi_pct": roi_pct,
        "average_edge": average_edge,
    }


def _breakdown_metrics(
    *,
    labels: np.ndarray,
    probabilities: np.ndarray,
    prices: np.ndarray,
    grouping_values: np.ndarray,
    min_edge: float,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for group_value in dict.fromkeys(str(value) for value in grouping_values):
        mask = grouping_values == group_value
        results[group_value] = _evaluate_subset(
            y_true=labels[mask],
            y_prob=probabilities[mask],
            prices=prices[mask],
            min_edge=min_edge,
        )
    return results


def _family_checkpoint_metrics(
    *,
    labels: np.ndarray,
    probabilities: np.ndarray,
    prices: np.ndarray,
    families: np.ndarray,
    checkpoints: np.ndarray,
    min_edge: float,
) -> dict[str, dict[str, dict[str, Any]]]:
    nested: dict[str, dict[str, dict[str, Any]]] = {}
    for family in dict.fromkeys(str(value) for value in families):
        family_mask = families == family
        family_results: dict[str, dict[str, Any]] = {}
        for checkpoint in dict.fromkeys(str(value) for value in checkpoints[family_mask]):
            checkpoint_mask = family_mask & (checkpoints == checkpoint)
            family_results[checkpoint] = _evaluate_subset(
                y_true=labels[checkpoint_mask],
                y_prob=probabilities[checkpoint_mask],
                prices=prices[checkpoint_mask],
                min_edge=min_edge,
            )
        nested[family] = family_results
    return nested


def _family_pnl_share_max(
    family_metrics: dict[str, dict[str, Any]],
    *,
    total_pnl: float,
) -> float:
    if total_pnl <= 0:
        return 1.0
    shares = [
        float(metrics.get("total_pnl", 0.0)) / total_pnl
        for metrics in family_metrics.values()
        if metrics.get("total_pnl") is not None
    ]
    return float(max(shares)) if shares else 1.0


def save_multitask_artifacts(
    *,
    artifact_dir: Path,
    model: MultitaskTabularModel,
    model_config: MultitaskModelConfig,
    feature_cols: list[str],
    calibrators: dict[str, dict[str, list[float]]],
    trainer_config: MultitaskTrainerConfig,
) -> dict[str, str]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = artifact_dir / "model_bundle.pt"
    calibrators_path = artifact_dir / "calibrators.json"
    config_path = artifact_dir / "trainer_config.json"

    torch.save(
        {
            "model_config": {
                "input_dim": model_config.input_dim,
                "hidden_dim": model_config.hidden_dim,
                "depth": model_config.depth,
                "dropout": model_config.dropout,
            },
            "feature_cols": feature_cols,
            "state_dict": model.state_dict(),
        },
        bundle_path,
    )
    calibrators_path.write_text(
        json.dumps(calibrators, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "feature_cols": feature_cols,
                "hidden_dim": trainer_config.hidden_dim,
                "depth": trainer_config.depth,
                "dropout": trainer_config.dropout,
                "lr": trainer_config.lr,
                "max_epochs": trainer_config.max_epochs,
                "batch_size": trainer_config.batch_size,
                "patience": trainer_config.patience,
                "validation_group_count": trainer_config.validation_group_count,
                "seed": trainer_config.seed,
                "head_weights": trainer_config.head_weights,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {
        "bundle_path": str(bundle_path),
        "calibrators_path": str(calibrators_path),
        "config_path": str(config_path),
    }


def load_multitask_artifacts(
    artifact_dir: Path,
) -> tuple[MultitaskTabularModel, list[str], dict[str, dict[str, list[float]]]]:
    bundle = torch.load(artifact_dir / "model_bundle.pt", map_location="cpu")
    model_config = MultitaskModelConfig(**bundle["model_config"])
    model = MultitaskTabularModel(model_config)
    model.load_state_dict(bundle["state_dict"])
    calibrators_path = artifact_dir / "calibrators.json"
    calibrators = json.loads(calibrators_path.read_text(encoding="utf-8"))
    return model, list(bundle["feature_cols"]), calibrators


def score_multitask_frame(
    frame: pl.DataFrame,
    *,
    artifact_dir: Path,
    model_run_id: str,
    stage: str,
    feature_snapshot_id: str | None = None,
) -> list[dict[str, Any]]:
    if frame.height == 0:
        return []

    model, feature_cols, calibrators = load_multitask_artifacts(artifact_dir)
    raw_probabilities = _predict_head_probabilities(model, frame, feature_cols=feature_cols)
    probabilities_by_head = {
        head: _apply_serialized_calibrator(raw_probabilities[head], calibrators.get(head))
        for head in HEADS
    }

    predictions: list[dict[str, Any]] = []
    for index, row in enumerate(frame.to_dicts()):
        family = str(row["target_market_family"])
        probability_yes = float(probabilities_by_head[family][index])
        predictions.append(
            {
                "model_run_id": model_run_id,
                "market_id": row["market_id"],
                "token_id": row["token_id"],
                "as_of_ts": datetime.now(tz=timezone.utc),
                "probability_yes": probability_yes,
                "probability_no": 1.0 - probability_yes,
                "raw_score": float(raw_probabilities[family][index]),
                "calibration_version": "isotonic_v1" if calibrators.get(family) else "identity",
                "explanation_json": {
                    "target_market_family": family,
                    "as_of_checkpoint": row["as_of_checkpoint"],
                    "stage": stage,
                    "feature_snapshot_id": feature_snapshot_id,
                },
            }
        )
    return predictions
def train_multitask_split(
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    *,
    model_run_id: str,
    stage: str,
    config: MultitaskTrainerConfig | None = None,
    artifact_dir: Path | None = None,
    min_edge: float | None = None,
) -> TrainResult:
    cfg = config or MultitaskTrainerConfig()
    effective_min_edge = cfg.min_edge if min_edge is None else min_edge
    feature_cols = cfg.feature_cols or _default_feature_cols(train_df)
    fit_df, val_df = _train_validation_split(
        train_df,
        validation_group_count=cfg.validation_group_count,
    )

    torch.manual_seed(cfg.seed)
    np.random.seed(cfg.seed)
    torch.set_num_threads(1)

    x_fit = _feature_tensor(fit_df, feature_cols)
    y_fit = _label_tensor(fit_df)
    families_fit = _family_array(fit_df)

    x_val = _feature_tensor(val_df, feature_cols)
    y_val = _label_tensor(val_df)
    families_val = _family_array(val_df)

    test_labels = np.asarray(test_df["label_yes"].to_list(), dtype=np.float64)
    test_prices = test_df["entry_yes_price"].to_numpy().astype(np.float64)
    test_families = _family_array(test_df)
    test_checkpoints = _checkpoint_array(test_df)

    model_config = MultitaskModelConfig(
        input_dim=len(feature_cols),
        hidden_dim=cfg.hidden_dim,
        depth=cfg.depth,
        dropout=cfg.dropout,
    )
    model = MultitaskTabularModel(model_config)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    loss_fn = nn.BCEWithLogitsLoss(reduction="none")
    batch_size = max(int(cfg.batch_size), 1)

    generator = torch.Generator().manual_seed(cfg.seed)
    best_state = deepcopy(model.state_dict())
    best_val_loss = float("inf")
    best_epoch = 0
    stale_epochs = 0
    use_validation = val_df.height > 0

    for epoch in range(cfg.max_epochs):
        model.train()
        permutation = torch.randperm(len(x_fit), generator=generator)
        for start in range(0, len(permutation), batch_size):
            batch_idx = permutation[start : start + batch_size]
            logits = model(x_fit[batch_idx])
            total_loss = _masked_head_loss(
                logits=logits,
                labels=y_fit[batch_idx],
                family_names=families_fit[batch_idx.tolist()],
                loss_fn=loss_fn,
                head_weights=cfg.head_weights,
            )
            optimizer.zero_grad()
            total_loss.backward()  # type: ignore[no-untyped-call]
            optimizer.step()

        if not use_validation:
            best_state = deepcopy(model.state_dict())
            best_epoch = epoch + 1
            continue

        model.eval()
        with torch.no_grad():
            validation_logits = model(x_val)
        validation_loss = float(
            _masked_head_loss(
                logits=validation_logits,
                labels=y_val,
                family_names=families_val,
                loss_fn=loss_fn,
                head_weights=cfg.head_weights,
            ).item()
        )
        if best_val_loss - validation_loss > cfg.min_delta:
            best_val_loss = validation_loss
            best_state = deepcopy(model.state_dict())
            best_epoch = epoch + 1
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= cfg.patience:
                break

    model.load_state_dict(best_state)

    val_probabilities = _predict_head_probabilities(model, val_df, feature_cols=feature_cols)
    test_probabilities = _predict_head_probabilities(model, test_df, feature_cols=feature_cols)

    calibrators: dict[str, dict[str, list[float]]] = {}
    val_labels = np.asarray(y_val.tolist(), dtype=np.float64)
    for head in HEADS:
        val_mask = families_val == head
        if int(val_mask.sum()) < cfg.min_calibration_rows:
            continue
        if len(np.unique(val_labels[val_mask])) < 2:
            continue
        calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        calibrator.fit(val_probabilities[head][val_mask], val_labels[val_mask])
        calibrators[head] = _serialize_calibrator(calibrator)

    combined_probabilities = np.zeros(len(test_df), dtype=np.float64)
    predictions: list[dict[str, Any]] = []
    for index, row in enumerate(test_df.to_dicts()):
        family = str(row["target_market_family"])
        probability_yes = float(
            _apply_serialized_calibrator(
                np.asarray([test_probabilities[family][index]], dtype=np.float64),
                calibrators.get(family),
            )[0]
        )
        combined_probabilities[index] = probability_yes
        predictions.append(
            {
                "model_run_id": model_run_id,
                "market_id": row["market_id"],
                "token_id": row["token_id"],
                "as_of_ts": datetime.now(tz=timezone.utc),
                "probability_yes": probability_yes,
                "probability_no": 1.0 - probability_yes,
                "raw_score": float(test_probabilities[family][index]),
                "calibration_version": "isotonic_v1" if family in calibrators else "identity",
                "explanation_json": {
                    "target_market_family": family,
                    "as_of_checkpoint": row["as_of_checkpoint"],
                    "stage": stage,
                },
            }
        )

    family_metrics = _breakdown_metrics(
        labels=test_labels,
        probabilities=combined_probabilities,
        prices=test_prices,
        grouping_values=test_families,
        min_edge=effective_min_edge,
    )
    checkpoint_metrics = _breakdown_metrics(
        labels=test_labels,
        probabilities=combined_probabilities,
        prices=test_prices,
        grouping_values=test_checkpoints,
        min_edge=effective_min_edge,
    )
    family_checkpoint_metrics = _family_checkpoint_metrics(
        labels=test_labels,
        probabilities=combined_probabilities,
        prices=test_prices,
        families=test_families,
        checkpoints=test_checkpoints,
        min_edge=effective_min_edge,
    )
    overall_metrics = _evaluate_subset(
        y_true=test_labels,
        y_prob=combined_probabilities,
        prices=test_prices,
        min_edge=effective_min_edge,
    )
    overall_metrics.update(
        {
            "family_metrics": family_metrics,
            "checkpoint_metrics": checkpoint_metrics,
            "family_checkpoint_metrics": family_checkpoint_metrics,
            "family_pnl_share_max": _family_pnl_share_max(
                family_metrics,
                total_pnl=float(overall_metrics["total_pnl"]),
            ),
            "fit_row_count": fit_df.height,
            "validation_row_count": val_df.height,
            "best_epoch": best_epoch,
        }
    )

    if artifact_dir is not None:
        save_multitask_artifacts(
            artifact_dir=artifact_dir,
            model=model,
            model_config=model_config,
            feature_cols=feature_cols,
            calibrators=calibrators,
            trainer_config=cfg,
        )

    return TrainResult(
        model_run_id=model_run_id,
        test_meeting_key=int(test_df["meeting_key"][0]),
        metrics=overall_metrics,
        predictions=predictions,
        config={
            "feature_cols": feature_cols,
            "hidden_dim": cfg.hidden_dim,
            "depth": cfg.depth,
            "dropout": cfg.dropout,
            "lr": cfg.lr,
            "max_epochs": cfg.max_epochs,
            "batch_size": cfg.batch_size,
            "patience": cfg.patience,
            "validation_group_count": cfg.validation_group_count,
            "seed": cfg.seed,
            "min_edge": effective_min_edge,
            "head_weights": cfg.head_weights,
        },
    )
