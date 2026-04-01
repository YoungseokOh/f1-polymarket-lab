"""Calibration evaluation and error analysis utilities.

Provides:
  - Reliability diagram computation
  - Expected Calibration Error (ECE)
  - Platt scaling (logistic) calibrator
  - Error analysis breakdown by dimensions
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import numpy as np
from numpy.typing import NDArray


@dataclass(frozen=True, slots=True)
class ReliabilityBin:
    """One bin in a reliability diagram."""

    bin_lower: float
    bin_upper: float
    avg_predicted: float
    avg_actual: float
    count: int


def _bucket_label(bin_lower: float, bin_upper: float) -> str:
    lower = int(round(bin_lower * 100))
    upper = int(round(bin_upper * 100))
    return f"{lower}-{upper}%"


def reliability_diagram(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10,
) -> list[ReliabilityBin]:
    """Compute reliability diagram bins.

    Splits predictions into *n_bins* equally spaced bins and computes
    the average predicted probability vs actual outcome rate per bin.
    """
    bins: list[ReliabilityBin] = []
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)

    for i in range(n_bins):
        lo, hi = float(bin_edges[i]), float(bin_edges[i + 1])
        mask = (y_prob >= lo) & (y_prob < hi) if i < n_bins - 1 else (y_prob >= lo) & (y_prob <= hi)
        count = int(mask.sum())
        if count == 0:
            continue
        bins.append(ReliabilityBin(
            bin_lower=lo,
            bin_upper=hi,
            avg_predicted=float(y_prob[mask].mean()),
            avg_actual=float(y_true[mask].mean()),
            count=count,
        ))
    return bins


def serialize_reliability_diagram(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10,
) -> dict[str, dict[str, float | int]]:
    """Return reliability bins in a JSON-serializable shape."""
    bins = reliability_diagram(y_true, y_prob, n_bins=n_bins)
    return {
        _bucket_label(item.bin_lower, item.bin_upper): {
            "bin_lower": round(item.bin_lower, 6),
            "bin_upper": round(item.bin_upper, 6),
            "avg_predicted": round(item.avg_predicted, 6),
            "avg_actual": round(item.avg_actual, 6),
            "count": item.count,
        }
        for item in bins
    }


def expected_calibration_error(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Compute Expected Calibration Error (ECE).

    ECE = sum(|bin_count / N| * |avg_predicted - avg_actual|) across bins.
    """
    bins = reliability_diagram(y_true, y_prob, n_bins=n_bins)
    n = len(y_true)
    if n == 0:
        return 0.0
    ece = sum(
        (b.count / n) * abs(b.avg_predicted - b.avg_actual)
        for b in bins
    )
    return float(ece)


def platt_scale(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    y_prob_new: np.ndarray | None = None,
) -> NDArray[np.float64]:
    """Apply Platt scaling (logistic regression on predicted probs).

    If *y_prob_new* is provided, fits on (y_prob, y_true) and transforms
    y_prob_new.  Otherwise transforms y_prob in-sample.
    """
    from sklearn.linear_model import LogisticRegression

    lr = LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)
    lr.fit(y_prob.reshape(-1, 1), y_true)

    target = y_prob_new if y_prob_new is not None else y_prob
    calibrated = lr.predict_proba(target.reshape(-1, 1))[:, 1]
    return cast(NDArray[np.float64], calibrated)


def error_analysis(
    predictions: list[dict[str, Any]],
    *,
    group_key: str,
) -> dict[str, dict[str, Any]]:
    """Break down prediction error by a grouping dimension.

    Each prediction dict must have ``probability_yes``, ``label_yes``,
    a ``entry_yes_price`` field, and the *group_key* field.

    Returns a dict keyed by group value with per-group metrics.
    """
    from collections import defaultdict

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in predictions:
        key_val = str(p.get(group_key, "unknown"))
        groups[key_val].append(p)

    results: dict[str, dict[str, Any]] = {}
    for group_val, items in groups.items():
        y_true = np.array([float(it.get("label_yes", 0)) for it in items])
        y_prob = np.array([float(it.get("probability_yes", 0.5)) for it in items])
        prices = np.array([float(it.get("entry_yes_price", 0.5)) for it in items])

        brier = float(np.mean((y_prob - y_true) ** 2))
        ece = expected_calibration_error(y_true, y_prob)
        edges = y_prob - prices
        profitable_bets = int(np.sum(edges >= 0.05))

        results[group_val] = {
            "count": len(items),
            "brier_score": brier,
            "ece": ece,
            "mean_predicted": float(y_prob.mean()),
            "actual_rate": float(y_true.mean()),
            "profitable_bets": profitable_bets,
        }

    return results
