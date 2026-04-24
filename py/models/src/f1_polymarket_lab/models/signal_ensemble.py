from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
from f1_polymarket_lab.common import (
    MarketGroup,
    MarketTaxonomy,
    market_group_for_taxonomy,
    stable_uuid,
    utc_now,
)
from sklearn.isotonic import IsotonicRegression

from .calibration import expected_calibration_error, serialize_reliability_diagram

EPSILON = 1e-6
SIGNAL_ENSEMBLE_STAGE = "signal_ensemble_v1"
SIGNAL_ENSEMBLE_MODEL_NAME = "anchor_ridge_stacking"
SIGNAL_ENSEMBLE_VERSION = "v1"
EXECUTABLE_MARKET_GROUPS = frozenset(
    {"driver_outright", "constructor_outright", "head_to_head"}
)


@dataclass(frozen=True, slots=True)
class SignalDefinition:
    signal_code: str
    signal_family: str
    description: str
    version: str = SIGNAL_ENSEMBLE_VERSION
    applicable_market_groups: tuple[MarketGroup, ...] = ()
    applicable_taxonomies: tuple[MarketTaxonomy, ...] = ()


@dataclass(slots=True)
class SignalEnsembleConfig:
    min_train_groups: int = 2
    min_rows_for_group_model: int = 20
    min_rows_per_class: int = 10
    min_isotonic_rows: int = 200
    min_isotonic_class_rows: int = 20
    min_platt_rows: int = 40
    min_platt_class_rows: int = 10
    ridge_penalty: float = 2.5
    max_newton_iter: int = 60
    min_edge: float = 0.05
    cost_buffer: float = 0.02
    kelly_cap: float = 0.2
    max_spread: float | None = None
    min_group_rows_for_executable: int = 20
    min_group_meetings_for_executable: int = 2


@dataclass(slots=True)
class EnsembleTrainResult:
    model_run_id: str
    predictions: list[dict[str, Any]]
    signal_snapshots: list[dict[str, Any]]
    ensemble_predictions: list[dict[str, Any]]
    trade_decisions: list[dict[str, Any]]
    diagnostics: list[dict[str, Any]]
    metrics: dict[str, Any]
    config: dict[str, Any]


def default_signal_definitions() -> list[SignalDefinition]:
    return [
        SignalDefinition(
            signal_code="market_microstructure_signal",
            signal_family="market_microstructure",
            description=(
                "Anchored orderbook and trade-flow adjustment using spread, "
                "staleness, trade count, and imbalance when present."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "incident_binary",
                "championship",
                "other",
            ),
        ),
        SignalDefinition(
            signal_code="cross_market_consistency_signal",
            signal_family="cross_market_consistency",
            description=(
                "Sibling-market normalization that rescales market anchors within "
                "the same event and taxonomy scope."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "head_to_head",
                "incident_binary",
                "championship",
                "other",
            ),
        ),
        SignalDefinition(
            signal_code="prior_signal",
            signal_family="prior",
            description=(
                "Historical driver or constructor prior built from track affinity, "
                "sector strengths, and profile coverage when available."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "head_to_head",
                "championship",
            ),
        ),
        SignalDefinition(
            signal_code="session_context_signal",
            signal_family="session_context",
            description=(
                "Session-aware probability tilt from the best visible pace context "
                "before the target session."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "head_to_head",
            ),
        ),
        SignalDefinition(
            signal_code="pace_delta_signal",
            signal_family="pace_delta",
            description=(
                "Relative event softmax from the latest available position and gap "
                "columns across FP sessions and qualifying."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "head_to_head",
            ),
        ),
        SignalDefinition(
            signal_code="long_run_consistency_signal",
            signal_family="long_run_consistency",
            description=(
                "Stability signal built from lap counts, stint counts, and team-gap "
                "consistency across visible sessions."
            ),
            applicable_market_groups=(
                "driver_outright",
                "constructor_outright",
                "head_to_head",
            ),
        ),
        SignalDefinition(
            signal_code="driver_affinity_signal",
            signal_family="driver_affinity",
            description=(
                "Driver-centric affinity signal using track affinity and sector "
                "strength columns. Available only for driver or H2H markets."
            ),
            applicable_market_groups=("driver_outright", "head_to_head"),
        ),
    ]


def default_signal_registry_entries() -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for definition in default_signal_definitions():
        scopes: list[tuple[MarketTaxonomy | None, MarketGroup | None]]
        if definition.applicable_taxonomies:
            scopes = [
                (taxonomy, market_group_for_taxonomy(taxonomy))
                for taxonomy in definition.applicable_taxonomies
            ]
        elif definition.applicable_market_groups:
            scopes = [(None, market_group) for market_group in definition.applicable_market_groups]
        else:
            scopes = [(None, None)]
        for market_taxonomy, market_group in scopes:
            scope_key = market_taxonomy or market_group or "global"
            entries.append(
                {
                    "id": stable_uuid(
                        "signal-registry",
                        definition.signal_code,
                        definition.version,
                        scope_key,
                    ),
                    "signal_code": definition.signal_code,
                    "signal_family": definition.signal_family,
                    "market_taxonomy": market_taxonomy,
                    "market_group": market_group,
                    "description": definition.description,
                    "version": definition.version,
                    "config_json": {
                        "applicable_market_groups": list(
                            definition.applicable_market_groups
                        ),
                        "applicable_taxonomies": list(
                            definition.applicable_taxonomies
                        ),
                        "scope_key": scope_key,
                    },
                    "is_active": True,
                }
            )
    return entries


def _clip_probability(value: float | None) -> float:
    if value is None or not math.isfinite(float(value)):
        return 0.5
    return float(min(max(float(value), EPSILON), 1.0 - EPSILON))


def _logit(value: float) -> float:
    clipped = _clip_probability(value)
    return float(math.log(clipped / (1.0 - clipped)))


def _sigmoid(value: float) -> float:
    return float(1.0 / (1.0 + math.exp(-value)))


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, int | float):
        if math.isfinite(float(value)):
            return float(value)
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    parsed = _coerce_float(value)
    return None if parsed is None else int(parsed)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return cast(datetime, utc_now())
    return cast(datetime, utc_now())


def _market_anchor(row: dict[str, Any]) -> float:
    for key in (
        "entry_yes_price",
        "market_implied_probability",
        "last_trade_price",
        "entry_midpoint",
    ):
        value = _coerce_float(row.get(key))
        if value is not None:
            return _clip_probability(value)
    return 0.5


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    taxonomy = str(row.get("market_taxonomy") or row.get("taxonomy") or "other")
    row_id = row.get("row_id")
    if not row_id:
        row_id = stable_uuid(
            "signal-row",
            row.get("market_id"),
            row.get("token_id"),
            row.get("event_id"),
            row.get("driver_id"),
            row.get("team_id"),
        )
    label = _coerce_int(row.get("label_yes"))
    return {
        **row,
        "row_id": str(row_id),
        "market_taxonomy": taxonomy,
        "market_group": market_group_for_taxonomy(taxonomy),
        "meeting_key": _coerce_int(row.get("meeting_key")),
        "p_market_ref": _market_anchor(row),
        "label_yes": label,
        "as_of_ts": _coerce_datetime(row.get("entry_observed_at_utc") or row.get("as_of_ts")),
    }


def _event_scope_key(row: dict[str, Any]) -> tuple[str, str]:
    event_id = row.get("event_id")
    market_taxonomy = str(row.get("market_taxonomy") or "other")
    return (str(event_id or row["market_id"] or row["row_id"]), market_taxonomy)


def _zscore_map(rows: list[dict[str, Any]], feature_name: str) -> dict[str, float | None]:
    values = [
        value
        for value in (_coerce_float(row.get(feature_name)) for row in rows)
        if value is not None
    ]
    if not values:
        return {str(row["row_id"]): None for row in rows}
    mean = float(sum(values) / len(values))
    variance = float(sum((value - mean) ** 2 for value in values) / len(values))
    std = math.sqrt(variance)
    if std <= 0:
        return {
            str(row["row_id"]): (
                0.0 if _coerce_float(row.get(feature_name)) is not None else None
            )
            for row in rows
        }
    scores: dict[str, float | None] = {}
    for row in rows:
        value = _coerce_float(row.get(feature_name))
        scores[str(row["row_id"])] = None if value is None else (value - mean) / std
    return scores


def _event_softmax_probabilities(
    rows: list[dict[str, Any]],
    *,
    feature_specs: list[tuple[str, float, float]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    zscores = {
        feature_name: _zscore_map(rows, feature_name)
        for feature_name, _sign, _weight in feature_specs
    }
    score_by_row_id: dict[str, float | None] = {}
    metadata_by_row_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = str(row["row_id"])
        weighted_sum = 0.0
        total_weight = 0.0
        used_features: list[str] = []
        for feature_name, sign, weight in feature_specs:
            zscore = zscores[feature_name][row_id]
            if zscore is None:
                continue
            weighted_sum += sign * weight * zscore
            total_weight += abs(weight)
            used_features.append(feature_name)
        if total_weight <= 0:
            score_by_row_id[row_id] = None
            metadata_by_row_id[row_id] = {"used_features": []}
            continue
        score_by_row_id[row_id] = weighted_sum / total_weight
        metadata_by_row_id[row_id] = {"used_features": used_features}

    active_scores = [score for score in score_by_row_id.values() if score is not None]
    if len(active_scores) < 2:
        return {
            str(row["row_id"]): (
                row["p_market_ref"],
                False,
                {
                    **metadata_by_row_id[str(row["row_id"])],
                    "reason": "insufficient_event_feature_coverage",
                },
            )
            for row in rows
        }

    max_score = max(active_scores)
    exp_by_row_id: dict[str, float] = {}
    total = 0.0
    for row in rows:
        row_id = str(row["row_id"])
        score = score_by_row_id[row_id]
        if score is None:
            continue
        exp_value = math.exp(score - max_score)
        exp_by_row_id[row_id] = exp_value
        total += exp_value
    if total <= 0:
        return {
            str(row["row_id"]): (
                row["p_market_ref"],
                False,
                {
                    **metadata_by_row_id[str(row["row_id"])],
                    "reason": "degenerate_event_softmax",
                },
            )
            for row in rows
        }
    output: dict[str, tuple[float, bool, dict[str, Any]]] = {}
    for row in rows:
        row_id = str(row["row_id"])
        if row_id not in exp_by_row_id:
            output[row_id] = (
                row["p_market_ref"],
                False,
                {
                    **metadata_by_row_id[row_id],
                    "reason": "missing_event_feature_score",
                },
            )
            continue
        output[row_id] = (
            _clip_probability(exp_by_row_id[row_id] / total),
            True,
            metadata_by_row_id[row_id],
        )
    return output


def _rowwise_logit_adjusted_probabilities(
    rows: list[dict[str, Any]],
    *,
    feature_specs: list[tuple[str, float, float]],
    score_scale: float = 0.65,
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    zscores = {
        feature_name: _zscore_map(rows, feature_name)
        for feature_name, _sign, _weight in feature_specs
    }
    output: dict[str, tuple[float, bool, dict[str, Any]]] = {}
    for row in rows:
        row_id = str(row["row_id"])
        weighted_sum = 0.0
        total_weight = 0.0
        used_features: list[str] = []
        for feature_name, sign, weight in feature_specs:
            zscore = zscores[feature_name][row_id]
            if zscore is None:
                continue
            weighted_sum += sign * weight * zscore
            total_weight += abs(weight)
            used_features.append(feature_name)
        if total_weight <= 0:
            output[row_id] = (
                row["p_market_ref"],
                False,
                {"used_features": [], "reason": "insufficient_pair_feature_coverage"},
            )
            continue
        adjustment = score_scale * (weighted_sum / total_weight)
        output[row_id] = (
            _sigmoid(_logit(row["p_market_ref"]) + adjustment),
            True,
            {"used_features": used_features, "score_scale": score_scale},
        )
    return output


def _event_probability_outputs(
    rows: list[dict[str, Any]],
    *,
    feature_specs: list[tuple[str, float, float]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    market_groups = {str(row["market_group"]) for row in rows}
    if market_groups == {"head_to_head"}:
        return _rowwise_logit_adjusted_probabilities(rows, feature_specs=feature_specs)
    return _event_softmax_probabilities(rows, feature_specs=feature_specs)


def _latest_session_prefix(rows: list[dict[str, Any]]) -> str | None:
    prefixes = ("q", "sq", "fp3", "fp2", "fp1")
    for prefix in prefixes:
        if any(_coerce_float(row.get(f"{prefix}_position")) is not None for row in rows):
            return prefix
    return None


def _microstructure_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    outputs: dict[str, tuple[float, bool, dict[str, Any]]] = {}
    for row in rows:
        anchor = row["p_market_ref"]
        spread = _coerce_float(row.get("entry_spread"))
        trade_count = _coerce_float(row.get("trade_count_pre_entry"))
        age_seconds = _coerce_float(row.get("last_trade_age_seconds"))
        imbalance = _coerce_float(
            row.get("entry_orderbook_imbalance")
            or row.get("orderbook_imbalance")
            or row.get("imbalance")
        )
        features_used: list[str] = []
        score = 0.0
        if spread is not None:
            score += -0.35 * math.tanh(spread / 0.06)
            features_used.append("entry_spread")
        if trade_count is not None:
            score += 0.25 * math.tanh((math.log1p(max(trade_count, 0.0)) / 4.0) - 0.9)
            features_used.append("trade_count_pre_entry")
        if age_seconds is not None:
            score += -0.2 * math.tanh(age_seconds / 1800.0)
            features_used.append("last_trade_age_seconds")
        if imbalance is not None:
            score += 0.2 * math.tanh(imbalance)
            features_used.append("orderbook_imbalance")
        outputs[str(row["row_id"])] = (
            _sigmoid(_logit(anchor) + score),
            bool(features_used),
            {"used_features": features_used},
        )
    return outputs


def _cross_market_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    market_groups = {str(row["market_group"]) for row in rows}
    if market_groups == {"head_to_head"}:
        return {
            str(row["row_id"]): (
                row["p_market_ref"],
                False,
                {"reason": "unsupported_h2h_cross_market_scope"},
            )
            for row in rows
        }
    anchors = [_clip_probability(row["p_market_ref"]) for row in rows]
    total = float(sum(anchors))
    if len(rows) < 2 or total <= 0:
        return {
            str(row["row_id"]): (
                row["p_market_ref"],
                False,
                {"reason": "no_sibling_market_scope"},
            )
            for row in rows
        }
    return {
        str(row["row_id"]): (
            _clip_probability(anchors[index] / total),
            True,
            {"peer_count": len(rows)},
        )
        for index, row in enumerate(rows)
    }


def _prior_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    groups = {str(row["market_group"]) for row in rows}
    if groups == {"constructor_outright"}:
        specs = [
            ("fp1_team_best_gap_to_leader_seconds", -1.0, 1.5),
            ("fp2_team_best_gap_to_leader_seconds", -1.0, 1.2),
            ("fp3_team_best_gap_to_leader_seconds", -1.0, 1.2),
            ("q_team_best_gap_to_leader_seconds", -1.0, 1.4),
        ]
    else:
        specs = [
            ("driver_track_affinity", 1.0, 2.0),
            ("driver_s1_strength", 1.0, 0.7),
            ("driver_s2_strength", 1.0, 0.7),
            ("driver_s3_strength", 1.0, 0.7),
            ("best_practice_position", -1.0, 0.6),
            ("best_practice_gap_to_leader_seconds", -1.0, 0.6),
        ]
    return _event_probability_outputs(rows, feature_specs=specs)


def _session_context_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    target_session = str(rows[0].get("target_session_code") or "")
    if target_session in {"R", "S"}:
        specs = [
            ("q_position", -1.0, 1.6),
            ("q_gap_to_leader_seconds", -1.0, 1.5),
            ("best_practice_position", -1.0, 0.7),
            ("best_practice_gap_to_leader_seconds", -1.0, 0.7),
        ]
    else:
        prefix = _latest_session_prefix(rows)
        if prefix is None:
            return {
                str(row["row_id"]): (
                    row["p_market_ref"],
                    False,
                    {"reason": "no_session_context_features"},
                )
                for row in rows
            }
        specs = [
            (f"{prefix}_position", -1.0, 1.4),
            (f"{prefix}_gap_to_leader_seconds", -1.0, 1.4),
            ("best_practice_position", -1.0, 0.5),
        ]
    return _event_probability_outputs(rows, feature_specs=specs)


def _pace_delta_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    prefix = _latest_session_prefix(rows)
    if prefix is None:
        return {
            str(row["row_id"]): (
                row["p_market_ref"],
                False,
                {"reason": "no_pace_features"},
            )
            for row in rows
        }
    specs = [
        (f"{prefix}_position", -1.0, 1.8),
        (f"{prefix}_gap_to_leader_seconds", -1.0, 1.8),
        (f"{prefix}_teammate_gap_seconds", -1.0, 0.7),
        (f"{prefix}_team_best_gap_to_leader_seconds", -1.0, 0.7),
    ]
    if prefix.startswith("fp"):
        specs.extend(
            [
                (f"{prefix}_lap_count", 1.0, 0.4),
                (f"{prefix}_stint_count", 1.0, 0.3),
            ]
        )
    return _event_probability_outputs(rows, feature_specs=specs)


def _long_run_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    specs = [
        ("fp1_lap_count", 1.0, 0.5),
        ("fp1_stint_count", 1.0, 0.5),
        ("fp2_lap_count", 1.0, 0.7),
        ("fp2_stint_count", 1.0, 0.7),
        ("fp3_lap_count", 1.0, 0.9),
        ("fp3_stint_count", 1.0, 0.9),
        ("fp1_teammate_gap_seconds", -1.0, 0.3),
        ("fp2_teammate_gap_seconds", -1.0, 0.3),
        ("fp3_teammate_gap_seconds", -1.0, 0.3),
        ("fp1_team_best_gap_to_leader_seconds", -1.0, 0.3),
        ("fp2_team_best_gap_to_leader_seconds", -1.0, 0.3),
        ("fp3_team_best_gap_to_leader_seconds", -1.0, 0.3),
    ]
    return _event_probability_outputs(rows, feature_specs=specs)


def _driver_affinity_outputs(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[float, bool, dict[str, Any]]]:
    specs = [
        ("driver_track_affinity", 1.0, 2.2),
        ("driver_s1_strength", 1.0, 0.8),
        ("driver_s2_strength", 1.0, 0.8),
        ("driver_s3_strength", 1.0, 0.8),
    ]
    return _event_probability_outputs(rows, feature_specs=specs)


SignalOutputMap = dict[str, tuple[float, bool, dict[str, Any]]]
SignalBuilder = Callable[[list[dict[str, Any]]], SignalOutputMap]

_SIGNAL_BUILDERS: dict[str, SignalBuilder] = {
    "market_microstructure_signal": _microstructure_outputs,
    "cross_market_consistency_signal": _cross_market_outputs,
    "prior_signal": _prior_outputs,
    "session_context_signal": _session_context_outputs,
    "pace_delta_signal": _pace_delta_outputs,
    "long_run_consistency_signal": _long_run_outputs,
    "driver_affinity_signal": _driver_affinity_outputs,
}


def _signal_applicable(definition: SignalDefinition, row: dict[str, Any]) -> bool:
    group = row["market_group"]
    taxonomy = row["market_taxonomy"]
    if definition.applicable_market_groups and group not in definition.applicable_market_groups:
        return False
    if definition.applicable_taxonomies and taxonomy not in definition.applicable_taxonomies:
        return False
    return True


def compute_signal_matrix(
    frame: pl.DataFrame,
    *,
    definitions: list[SignalDefinition] | None = None,
) -> list[dict[str, Any]]:
    definitions = definitions or default_signal_definitions()
    rows = [_normalize_row(row) for row in frame.to_dicts()]
    if not rows:
        return []
    event_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        event_rows[_event_scope_key(row)].append(row)

    signal_output_by_code: dict[str, dict[str, tuple[float, bool, dict[str, Any]]]] = {
        definition.signal_code: {} for definition in definitions
    }
    for definition in definitions:
        builder = _SIGNAL_BUILDERS[definition.signal_code]
        for scoped_rows in event_rows.values():
            applicable_rows = [row for row in scoped_rows if _signal_applicable(definition, row)]
            if not applicable_rows:
                continue
            outputs = builder(applicable_rows)
            signal_output_by_code[definition.signal_code].update(outputs)

    matrix_rows: list[dict[str, Any]] = []
    for row in rows:
        signal_raw: dict[str, float | None] = {}
        signal_coverage: dict[str, bool] = {}
        signal_metadata: dict[str, dict[str, Any]] = {}
        for definition in definitions:
            row_id = str(row["row_id"])
            output = signal_output_by_code[definition.signal_code].get(row_id)
            if output is None:
                signal_raw[definition.signal_code] = row["p_market_ref"]
                signal_coverage[definition.signal_code] = False
                signal_metadata[definition.signal_code] = {"reason": "not_applicable"}
                continue
            signal_raw[definition.signal_code] = output[0]
            signal_coverage[definition.signal_code] = output[1]
            signal_metadata[definition.signal_code] = output[2]
        matrix_rows.append(
            {
                **row,
                "signal_raw": signal_raw,
                "signal_coverage": signal_coverage,
                "signal_metadata": signal_metadata,
            }
        )
    return matrix_rows


def _fit_platt(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, Any]:
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
    model.fit(y_prob.reshape(-1, 1), y_true)
    return {
        "method": "platt",
        "coef": float(model.coef_[0][0]),
        "intercept": float(model.intercept_[0]),
    }


def _fit_isotonic(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, Any]:
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(y_prob, y_true)
    return {
        "method": "isotonic",
        "x_thresholds": [float(value) for value in calibrator.X_thresholds_],
        "y_thresholds": [float(value) for value in calibrator.y_thresholds_],
    }


def _fit_calibrator_payload(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    *,
    config: SignalEnsembleConfig,
) -> dict[str, Any] | None:
    if y_true.size == 0 or y_prob.size == 0:
        return None
    positives = int(np.sum(y_true))
    negatives = int(y_true.size - positives)
    if (
        y_true.size >= config.min_isotonic_rows
        and positives >= config.min_isotonic_class_rows
        and negatives >= config.min_isotonic_class_rows
    ):
        payload = _fit_isotonic(y_true, y_prob)
        payload["fit_row_count"] = int(y_true.size)
        return payload
    if (
        y_true.size >= config.min_platt_rows
        and positives >= config.min_platt_class_rows
        and negatives >= config.min_platt_class_rows
    ):
        payload = _fit_platt(y_true, y_prob)
        payload["fit_row_count"] = int(y_true.size)
        return payload
    return None


def _apply_calibrator_payload(probability: float, payload: dict[str, Any] | None) -> float:
    probability = _clip_probability(probability)
    if payload is None:
        return probability
    method = str(payload.get("method") or "")
    if method == "isotonic":
        x_thresholds = np.asarray(payload.get("x_thresholds") or [], dtype=np.float64)
        y_thresholds = np.asarray(payload.get("y_thresholds") or [], dtype=np.float64)
        if x_thresholds.size == 0 or y_thresholds.size == 0:
            return probability
        clipped = np.clip(probability, x_thresholds[0], x_thresholds[-1])
        return float(np.interp(clipped, x_thresholds, y_thresholds))
    if method == "platt":
        coef = float(payload.get("coef") or 0.0)
        intercept = float(payload.get("intercept") or 0.0)
        return _sigmoid((coef * probability) + intercept)
    return probability


def _fit_calibration_bundle(
    matrix_rows: list[dict[str, Any]],
    *,
    signal_codes: list[str],
    config: SignalEnsembleConfig,
) -> dict[str, dict[str, dict[str, Any]]]:
    bundle: dict[str, dict[str, dict[str, Any]]] = {}
    for signal_code in signal_codes:
        signal_bundle: dict[str, dict[str, Any]] = {}
        trainable_rows = [
            row
            for row in matrix_rows
            if row["label_yes"] is not None
            and row["signal_coverage"].get(signal_code)
            and row["signal_raw"].get(signal_code) is not None
        ]
        if not trainable_rows:
            bundle[signal_code] = signal_bundle
            continue

        def fit_scope(
            scope_rows: list[dict[str, Any]],
            *,
            current_signal_code: str = signal_code,
        ) -> dict[str, Any] | None:
            y_true = np.asarray([int(row["label_yes"]) for row in scope_rows], dtype=np.float64)
            y_prob = np.asarray(
                [
                    _clip_probability(row["signal_raw"][current_signal_code])
                    for row in scope_rows
                ],
                dtype=np.float64,
            )
            return _fit_calibrator_payload(y_true, y_prob, config=config)

        global_payload = fit_scope(trainable_rows)
        if global_payload is not None:
            signal_bundle["global"] = global_payload

        taxonomy_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        market_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in trainable_rows:
            taxonomy_groups[str(row["market_taxonomy"])].append(row)
            market_groups[str(row["market_group"])].append(row)

        for taxonomy, rows in taxonomy_groups.items():
            payload = fit_scope(rows)
            if payload is not None:
                signal_bundle[f"taxonomy::{taxonomy}"] = payload
        for market_group, rows in market_groups.items():
            payload = fit_scope(rows)
            if payload is not None:
                signal_bundle[f"group::{market_group}"] = payload
        bundle[signal_code] = signal_bundle
    return bundle


def _select_calibrator_payload(
    signal_bundle: dict[str, dict[str, Any]],
    *,
    taxonomy: str,
    market_group: str,
) -> tuple[str, dict[str, Any] | None]:
    for key in (f"taxonomy::{taxonomy}", f"group::{market_group}", "global"):
        payload = signal_bundle.get(key)
        if payload is not None:
            return key, payload
    return "none", None


def _grouped_temporal_folds(
    matrix_rows: list[dict[str, Any]],
    *,
    config: SignalEnsembleConfig,
) -> list[tuple[set[str], set[str]]]:
    meeting_keys = [
        str(key)
        for key in dict.fromkeys(
            row["meeting_key"]
            for row in sorted(
                matrix_rows,
                key=lambda row: row["meeting_key"] or -1,
            )
            if row["meeting_key"] is not None
        )
    ]
    folds: list[tuple[set[str], set[str]]] = []
    if len(meeting_keys) >= config.min_train_groups + 1:
        for index in range(config.min_train_groups, len(meeting_keys)):
            train = set(meeting_keys[:index])
            test = {meeting_keys[index]}
            folds.append((train, test))
        return folds

    event_ids = [
        str(event_id)
        for event_id in dict.fromkeys(
            row["event_id"] for row in matrix_rows if row.get("event_id") is not None
        )
    ]
    if len(event_ids) >= config.min_train_groups + 1:
        for index in range(config.min_train_groups, len(event_ids)):
            folds.append((set(event_ids[:index]), {event_ids[index]}))
        return folds
    return []


def _oof_calibrated_probabilities(
    matrix_rows: list[dict[str, Any]],
    *,
    signal_codes: list[str],
    config: SignalEnsembleConfig,
) -> dict[str, dict[str, float | None]]:
    folds = _grouped_temporal_folds(matrix_rows, config=config)
    if not folds:
        raise ValueError("Signal ensemble training needs at least two temporal groups")

    oof: dict[str, dict[str, float | None]] = {
        str(row["row_id"]): {signal_code: None for signal_code in signal_codes}
        for row in matrix_rows
    }
    use_meeting_key = any(row["meeting_key"] is not None for row in matrix_rows)
    for train_groups, test_groups in folds:
        if use_meeting_key:
            train_rows = [
                row for row in matrix_rows if str(row["meeting_key"]) in train_groups
            ]
            test_rows = [
                row for row in matrix_rows if str(row["meeting_key"]) in test_groups
            ]
        else:
            train_rows = [row for row in matrix_rows if str(row["event_id"]) in train_groups]
            test_rows = [row for row in matrix_rows if str(row["event_id"]) in test_groups]
        bundle = _fit_calibration_bundle(train_rows, signal_codes=signal_codes, config=config)
        for row in test_rows:
            row_id = str(row["row_id"])
            for signal_code in signal_codes:
                raw_prob = row["signal_raw"].get(signal_code)
                if raw_prob is None:
                    continue
                scope_key, payload = _select_calibrator_payload(
                    bundle.get(signal_code, {}),
                    taxonomy=str(row["market_taxonomy"]),
                    market_group=str(row["market_group"]),
                )
                oof[row_id][signal_code] = _apply_calibrator_payload(raw_prob, payload)
                row.setdefault("signal_calibration_scope", {})[signal_code] = scope_key
    return oof


def _delta_vector(
    row: dict[str, Any],
    *,
    calibrated: dict[str, float | None],
    signal_codes: list[str],
) -> np.ndarray:
    z_market = _logit(row["p_market_ref"])
    values: list[float] = []
    for signal_code in signal_codes:
        probability = calibrated.get(signal_code)
        if probability is None:
            values.append(0.0)
            continue
        values.append(_logit(probability) - z_market)
    return np.asarray(values, dtype=np.float64)


def _fit_offset_ridge_logit(
    *,
    z_market: np.ndarray,
    X: np.ndarray,
    y: np.ndarray,
    ridge_penalty: float,
    max_iter: int,
) -> tuple[float, np.ndarray]:
    intercept = 0.0
    weights = np.zeros(X.shape[1], dtype=np.float64)
    identity = np.eye(X.shape[1], dtype=np.float64)
    for _ in range(max_iter):
        eta = z_market + intercept + X @ weights
        p = 1.0 / (1.0 + np.exp(-eta))
        s = np.clip(p * (1.0 - p), 1e-6, None)
        residual = p - y
        grad_intercept = float(np.sum(residual))
        grad_weights = X.T @ residual + (ridge_penalty * weights)

        h_ii = float(np.sum(s))
        h_iw = X.T @ s
        h_ww = (X.T * s) @ X + (ridge_penalty * identity)
        hessian = np.zeros((X.shape[1] + 1, X.shape[1] + 1), dtype=np.float64)
        hessian[0, 0] = h_ii
        hessian[0, 1:] = h_iw
        hessian[1:, 0] = h_iw
        hessian[1:, 1:] = h_ww
        gradient = np.concatenate([[grad_intercept], grad_weights])
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            step = np.linalg.pinv(hessian) @ gradient
        intercept -= float(step[0])
        weights -= step[1:]
        if float(np.max(np.abs(step))) < 1e-6:
            break
    return intercept, weights


def _residual_correlation_matrix(
    matrix_rows: list[dict[str, Any]],
    *,
    signal_codes: list[str],
    calibrated_by_row_id: dict[str, dict[str, float | None]],
) -> dict[str, dict[str, float]]:
    residuals_by_signal: dict[str, list[float]] = {signal_code: [] for signal_code in signal_codes}
    for row in matrix_rows:
        if row["label_yes"] is None:
            continue
        row_id = str(row["row_id"])
        label = float(row["label_yes"])
        for signal_code in signal_codes:
            probability = calibrated_by_row_id[row_id].get(signal_code)
            if probability is None:
                continue
            residuals_by_signal[signal_code].append(probability - label)
    output: dict[str, dict[str, float]] = {signal_code: {} for signal_code in signal_codes}
    for left in signal_codes:
        left_values = np.asarray(residuals_by_signal[left], dtype=np.float64)
        for right in signal_codes:
            right_values = np.asarray(residuals_by_signal[right], dtype=np.float64)
            length = min(left_values.size, right_values.size)
            if length < 2:
                output[left][right] = 1.0 if left == right else 0.0
                continue
            corr = np.corrcoef(left_values[:length], right_values[:length])[0, 1]
            if not np.isfinite(corr):
                corr = 1.0 if left == right else 0.0
            output[left][right] = float(corr)
    return output


def _metrics_from_probabilities(
    *,
    y_true: np.ndarray,
    y_prob: np.ndarray,
    market_ref: np.ndarray,
    min_edge: float,
) -> dict[str, Any]:
    if y_true.size == 0:
        return {
            "row_count": 0,
            "brier_score": None,
            "log_loss": None,
            "ece": None,
            "calibration_buckets": {},
            "bet_count": 0,
            "total_pnl": 0.0,
            "roi_pct": 0.0,
            "average_edge": None,
        }
    brier = float(np.mean((y_prob - y_true) ** 2))
    log_loss = float(
        -np.mean(
            y_true * np.log(np.clip(y_prob, EPSILON, 1.0))
            + (1.0 - y_true) * np.log(np.clip(1.0 - y_prob, EPSILON, 1.0))
        )
    )
    ece = float(expected_calibration_error(y_true, y_prob))
    edges = y_prob - market_ref
    selected = edges >= min_edge
    if int(np.sum(selected)) > 0:
        selected_prices = market_ref[selected]
        selected_labels = y_true[selected]
        pnl = np.where(selected_labels == 1.0, 1.0 - selected_prices, -selected_prices)
        total_pnl = float(np.sum(pnl))
        total_selected_prices = float(np.sum(selected_prices))
        roi_pct = (
            float((total_pnl / total_selected_prices) * 100.0)
            if total_selected_prices > 0
            else 0.0
        )
        average_edge = float(np.mean(edges[selected]))
        bet_count = int(np.sum(selected))
    else:
        total_pnl = 0.0
        roi_pct = 0.0
        average_edge = None
        bet_count = 0
    return {
        "row_count": int(y_true.size),
        "brier_score": brier,
        "log_loss": log_loss,
        "ece": ece,
        "calibration_buckets": serialize_reliability_diagram(y_true, y_prob),
        "bet_count": bet_count,
        "total_pnl": total_pnl,
        "roi_pct": roi_pct,
        "average_edge": average_edge,
    }


def _group_breakdown(
    scored_rows: list[dict[str, Any]],
    *,
    group_key: str,
    probability_key: str,
    min_edge: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scored_rows:
        if row.get("label_yes") is None:
            continue
        grouped[str(row.get(group_key) or "unknown")].append(row)
    output: dict[str, dict[str, Any]] = {}
    for key, rows in grouped.items():
        output[key] = _metrics_from_probabilities(
            y_true=np.asarray([int(row["label_yes"]) for row in rows], dtype=np.float64),
            y_prob=np.asarray([float(row[probability_key]) for row in rows], dtype=np.float64),
            market_ref=np.asarray([float(row["p_market_ref"]) for row in rows], dtype=np.float64),
            min_edge=min_edge,
        )
    return output


def _family_pnl_share_max(breakdown: dict[str, dict[str, Any]], *, total_pnl: float) -> float:
    if total_pnl <= 0:
        return 1.0
    shares = [
        float(metrics.get("total_pnl") or 0.0) / total_pnl
        for metrics in breakdown.values()
    ]
    return float(max(shares)) if shares else 1.0


def _train_group_combiner(
    rows: list[dict[str, Any]],
    *,
    signal_codes: list[str],
    oof_calibrated: dict[str, dict[str, float | None]],
    config: SignalEnsembleConfig,
) -> dict[str, Any] | None:
    labeled_rows = [row for row in rows if row["label_yes"] is not None]
    if len(labeled_rows) < config.min_rows_for_group_model:
        return None
    meetings = {row["meeting_key"] for row in labeled_rows if row["meeting_key"] is not None}
    if len(meetings) < config.min_group_meetings_for_executable:
        return None
    y = np.asarray([int(row["label_yes"]) for row in labeled_rows], dtype=np.float64)
    z_market = np.asarray([_logit(row["p_market_ref"]) for row in labeled_rows], dtype=np.float64)
    X = np.vstack(
        [
            _delta_vector(
                row,
                calibrated=oof_calibrated[str(row["row_id"])],
                signal_codes=signal_codes,
            )
            for row in labeled_rows
        ]
    )
    intercept, weights = _fit_offset_ridge_logit(
        z_market=z_market,
        X=X,
        y=y,
        ridge_penalty=config.ridge_penalty,
        max_iter=config.max_newton_iter,
    )
    residual_correlation = _residual_correlation_matrix(
        labeled_rows,
        signal_codes=signal_codes,
        calibrated_by_row_id=oof_calibrated,
    )
    return {
        "intercept": float(intercept),
        "weights": {
            signal_code: float(weight)
            for signal_code, weight in zip(signal_codes, weights, strict=True)
        },
        "signal_codes": signal_codes,
        "residual_correlation": residual_correlation,
        "train_row_count": len(labeled_rows),
        "meeting_count": len(meetings),
    }


def _effective_n(
    *,
    contributions: dict[str, float],
    residual_correlation: dict[str, dict[str, float]],
) -> float:
    active = [signal_code for signal_code, value in contributions.items() if abs(value) > 0]
    if not active:
        return 1.0
    weights = np.asarray(
        [abs(contributions[signal_code]) for signal_code in active],
        dtype=np.float64,
    )
    weight_sum = float(np.sum(weights))
    if weight_sum <= 0:
        return 1.0
    w_norm = weights / weight_sum
    matrix = np.asarray(
        [
            [residual_correlation.get(left, {}).get(right, 0.0) for right in active]
            for left in active
        ],
        dtype=np.float64,
    )
    for index in range(matrix.shape[0]):
        matrix[index, index] = 1.0
    denominator = float(w_norm.T @ matrix @ w_norm)
    if denominator <= 0:
        return float(len(active))
    return float(min(max(1.0 / denominator, 1.0), float(len(active))))


def _score_one_row(
    row: dict[str, Any],
    *,
    signal_codes: list[str],
    calibrated: dict[str, float | None],
    combiner_payload: dict[str, Any] | None,
    config: SignalEnsembleConfig,
) -> dict[str, Any]:
    z_market = _logit(row["p_market_ref"])
    deltas: dict[str, float] = {}
    for signal_code in signal_codes:
        probability = calibrated.get(signal_code)
        deltas[signal_code] = 0.0 if probability is None else _logit(probability) - z_market

    if combiner_payload is None:
        contributions = {signal_code: 0.0 for signal_code in signal_codes}
        p_yes = row["p_market_ref"]
        uncertainty_score = 1.0
        effective_n = 1.0
        disagreement = 0.0
        z_ensemble = z_market
        intercept = 0.0
        coverage = {
            "supported": False,
            "blocked_reason": "no_trained_combiner_for_market_group",
        }
    else:
        weights = combiner_payload["weights"]
        intercept = float(combiner_payload["intercept"])
        contributions = {
            signal_code: float(weights.get(signal_code, 0.0)) * deltas[signal_code]
            for signal_code in signal_codes
        }
        active_deltas = [value for value in deltas.values() if abs(value) > 0]
        disagreement = float(np.std(active_deltas)) if active_deltas else 0.0
        effective_n = _effective_n(
            contributions=contributions,
            residual_correlation=combiner_payload["residual_correlation"],
        )
        active_count = max(1, len([value for value in contributions.values() if abs(value) > 0]))
        disagreement_norm = min(disagreement / 1.5, 1.0)
        uncertainty_score = float(
            min(
                max(
                    (0.5 * disagreement_norm)
                    + (0.5 * (1.0 - (effective_n / float(active_count)))),
                    0.0,
                ),
                1.0,
            )
        )
        z_ensemble = z_market + intercept + float(sum(contributions.values()))
        p_yes = _sigmoid(z_ensemble)
        coverage = {
            "supported": True,
            "train_row_count": combiner_payload.get("train_row_count"),
            "meeting_count": combiner_payload.get("meeting_count"),
        }

    best_ask = _coerce_float(row.get("entry_best_ask"))
    best_bid = _coerce_float(row.get("entry_best_bid"))
    midpoint = _coerce_float(row.get("entry_midpoint")) or row["p_market_ref"]
    spread = _coerce_float(row.get("entry_spread"))
    yes_entry_price = best_ask or (
        midpoint + (spread / 2.0 if spread is not None else 0.0)
    )
    yes_entry_price = _clip_probability(yes_entry_price)
    if best_bid is not None:
        no_entry_price = _clip_probability(1.0 - best_bid)
    else:
        no_entry_price = _clip_probability(
            1.0 - midpoint + ((spread / 2.0) if spread is not None else 0.0)
        )

    edge_yes = p_yes - yes_entry_price - config.cost_buffer
    edge_no = (1.0 - p_yes) - no_entry_price - config.cost_buffer
    side = "skip"
    edge = max(edge_yes, edge_no)
    chosen_price = yes_entry_price
    decision_reason = "edge_below_threshold"
    decision_status = "skip"

    if row["market_group"] not in EXECUTABLE_MARKET_GROUPS:
        decision_status = "blocked"
        decision_reason = "diagnostics_only_market_group"
        side = "skip"
    elif not coverage.get("supported"):
        decision_status = "blocked"
        decision_reason = str(coverage.get("blocked_reason") or "unsupported_market_group")
        side = "skip"
    else:
        if config.max_spread is not None and spread is not None and spread > config.max_spread:
            decision_status = "blocked"
            decision_reason = "spread_above_max"
        elif edge_yes >= edge_no and edge_yes >= config.min_edge:
            side = "YES"
            chosen_price = yes_entry_price
            decision_reason = "positive_yes_edge"
            decision_status = "trade"
        elif edge_no > edge_yes and edge_no >= config.min_edge:
            side = "NO"
            chosen_price = no_entry_price
            decision_reason = "positive_no_edge"
            decision_status = "trade"

    chosen_probability = p_yes if side != "NO" else 1.0 - p_yes
    payout_odds = max((1.0 - chosen_price) / max(chosen_price, EPSILON), EPSILON)
    kelly_fraction_raw = max(
        ((payout_odds * chosen_probability) - (1.0 - chosen_probability)) / payout_odds,
        0.0,
    )
    disagreement_penalty = max(0.25, 1.0 - uncertainty_score)
    liquidity_value = _coerce_float(row.get("market_liquidity") or row.get("liquidity"))
    if liquidity_value is not None and liquidity_value > 0:
        liquidity_factor = min(1.0, liquidity_value / 1000.0)
        depth = liquidity_value
    else:
        trade_count = _coerce_float(row.get("trade_count_pre_entry"))
        liquidity_factor = (
            min(1.0, math.log1p(max(trade_count or 0.0, 0.0)) / 4.0)
            if trade_count is not None
            else 1.0
        )
        depth = trade_count or 0.0
    size_fraction = (
        min(config.kelly_cap, kelly_fraction_raw)
        * disagreement_penalty
        * liquidity_factor
    )
    if decision_status != "trade":
        size_fraction = 0.0

    return {
        "p_yes_ensemble": p_yes,
        "z_market": z_market,
        "z_ensemble": z_ensemble,
        "intercept": intercept,
        "contributions": contributions,
        "deltas": deltas,
        "disagreement_score": disagreement,
        "effective_n": effective_n,
        "uncertainty_score": uncertainty_score,
        "coverage": coverage,
        "side": side,
        "edge": float(edge),
        "threshold": config.min_edge,
        "spread": spread,
        "depth": depth,
        "kelly_fraction_raw": float(kelly_fraction_raw),
        "disagreement_penalty": float(disagreement_penalty),
        "liquidity_factor": float(liquidity_factor),
        "size_fraction": float(size_fraction),
        "decision_status": decision_status,
        "decision_reason": decision_reason,
        "yes_entry_price": yes_entry_price,
        "no_entry_price": no_entry_price,
    }


def _serialize_model_artifact(
    artifact_dir: Path,
    *,
    config: SignalEnsembleConfig,
    definitions: list[SignalDefinition],
    calibration_bundle: dict[str, dict[str, dict[str, Any]]],
    combiners: dict[str, dict[str, Any]],
    metrics: dict[str, Any],
) -> dict[str, str]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    model_bundle_path = artifact_dir / "signal_ensemble_bundle.json"
    payload = {
        "version": SIGNAL_ENSEMBLE_VERSION,
        "config": {
            "min_edge": config.min_edge,
            "cost_buffer": config.cost_buffer,
            "kelly_cap": config.kelly_cap,
            "max_spread": config.max_spread,
        },
        "signals": [
            {
                "signal_code": definition.signal_code,
                "signal_family": definition.signal_family,
                "description": definition.description,
                "version": definition.version,
                "applicable_market_groups": list(definition.applicable_market_groups),
                "applicable_taxonomies": list(definition.applicable_taxonomies),
            }
            for definition in definitions
        ],
        "calibration_bundle": calibration_bundle,
        "combiners": combiners,
        "metrics": metrics,
        "created_at": utc_now().isoformat(),
    }
    model_bundle_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return {"bundle_path": str(model_bundle_path)}


def load_signal_ensemble_artifacts(
    artifact_dir: Path,
) -> tuple[list[SignalDefinition], SignalEnsembleConfig, dict[str, Any]]:
    payload = json.loads((artifact_dir / "signal_ensemble_bundle.json").read_text(encoding="utf-8"))
    definitions = [
        SignalDefinition(
            signal_code=entry["signal_code"],
            signal_family=entry["signal_family"],
            description=entry["description"],
            version=entry.get("version") or SIGNAL_ENSEMBLE_VERSION,
            applicable_market_groups=tuple(entry.get("applicable_market_groups") or ()),
            applicable_taxonomies=tuple(entry.get("applicable_taxonomies") or ()),
        )
        for entry in payload.get("signals") or []
    ]
    raw_config = payload.get("config") or {}
    config = SignalEnsembleConfig(
        min_edge=float(raw_config.get("min_edge") or 0.05),
        cost_buffer=float(raw_config.get("cost_buffer") or 0.02),
        kelly_cap=float(raw_config.get("kelly_cap") or 0.2),
        max_spread=(
            None
            if raw_config.get("max_spread") is None
            else float(raw_config["max_spread"])
        ),
    )
    return definitions, config, payload


def train_signal_ensemble_split(
    train_df: pl.DataFrame,
    test_df: pl.DataFrame,
    *,
    model_run_id: str,
    stage: str = SIGNAL_ENSEMBLE_STAGE,
    config: SignalEnsembleConfig | None = None,
    artifact_dir: Path | None = None,
    feature_snapshot_id: str | None = None,
) -> EnsembleTrainResult:
    config = config or SignalEnsembleConfig()
    definitions = default_signal_definitions()
    signal_codes = [definition.signal_code for definition in definitions]

    train_rows = compute_signal_matrix(train_df, definitions=definitions)
    test_rows = compute_signal_matrix(test_df, definitions=definitions)
    oof_calibrated = _oof_calibrated_probabilities(
        train_rows,
        signal_codes=signal_codes,
        config=config,
    )
    calibration_bundle = _fit_calibration_bundle(
        train_rows,
        signal_codes=signal_codes,
        config=config,
    )

    combiners: dict[str, dict[str, Any]] = {}
    for market_group in sorted({str(row["market_group"]) for row in train_rows}):
        group_rows = [row for row in train_rows if str(row["market_group"]) == market_group]
        combiner = _train_group_combiner(
            group_rows,
            signal_codes=signal_codes,
            oof_calibrated=oof_calibrated,
            config=config,
        )
        if combiner is not None:
            combiners[market_group] = combiner

    scored_rows: list[dict[str, Any]] = []
    signal_snapshot_rows: list[dict[str, Any]] = []
    ensemble_prediction_rows: list[dict[str, Any]] = []
    trade_decision_rows: list[dict[str, Any]] = []
    model_prediction_rows: list[dict[str, Any]] = []

    for row in test_rows:
        calibrated_by_signal: dict[str, float | None] = {}
        calibration_scope_by_signal: dict[str, str] = {}
        for signal_code in signal_codes:
            raw_probability = row["signal_raw"].get(signal_code)
            scope_key, payload = _select_calibrator_payload(
                calibration_bundle.get(signal_code, {}),
                taxonomy=str(row["market_taxonomy"]),
                market_group=str(row["market_group"]),
            )
            calibrated = (
                None
                if raw_probability is None
                else _apply_calibrator_payload(raw_probability, payload)
            )
            calibrated_by_signal[signal_code] = calibrated
            calibration_scope_by_signal[signal_code] = scope_key
            signal_snapshot_rows.append(
                {
                    "id": stable_uuid(
                        "signal-snapshot",
                        model_run_id,
                        row["market_id"],
                        row.get("token_id"),
                        row["as_of_ts"].isoformat(),
                        signal_code,
                    ),
                    "model_run_id": model_run_id,
                    "feature_snapshot_id": feature_snapshot_id,
                    "market_id": row.get("market_id"),
                    "token_id": row.get("token_id"),
                    "event_id": row.get("event_id"),
                    "market_taxonomy": row["market_taxonomy"],
                    "market_group": row["market_group"],
                    "meeting_key": row["meeting_key"],
                    "as_of_ts": row["as_of_ts"],
                    "signal_code": signal_code,
                    "signal_version": SIGNAL_ENSEMBLE_VERSION,
                    "p_yes_raw": raw_probability,
                    "p_yes_calibrated": calibrated,
                    "p_market_ref": row["p_market_ref"],
                    "delta_logit": (
                        None
                        if calibrated is None
                        else _logit(calibrated) - _logit(row["p_market_ref"])
                    ),
                    "freshness_sec": _coerce_float(row.get("last_trade_age_seconds")),
                    "coverage_flag": bool(row["signal_coverage"].get(signal_code)),
                    "metadata_json": {
                        **row["signal_metadata"].get(signal_code, {}),
                        "calibration_scope": scope_key,
                    },
                }
            )

        score_payload = _score_one_row(
            row,
            signal_codes=signal_codes,
            calibrated=calibrated_by_signal,
            combiner_payload=combiners.get(str(row["market_group"])),
            config=config,
        )
        scored_rows.append(
            {
                **row,
                **score_payload,
                "calibrated_by_signal": calibrated_by_signal,
                "calibration_scope_by_signal": calibration_scope_by_signal,
            }
        )
        ensemble_prediction_id = stable_uuid(
            "ensemble-prediction",
            model_run_id,
            row.get("market_id"),
            row.get("token_id"),
            row["as_of_ts"].isoformat(),
        )
        ensemble_prediction_rows.append(
            {
                "id": ensemble_prediction_id,
                "model_run_id": model_run_id,
                "feature_snapshot_id": feature_snapshot_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "event_id": row.get("event_id"),
                "market_taxonomy": row["market_taxonomy"],
                "market_group": row["market_group"],
                "meeting_key": row["meeting_key"],
                "as_of_ts": row["as_of_ts"],
                "p_market_ref": row["p_market_ref"],
                "p_yes_ensemble": score_payload["p_yes_ensemble"],
                "z_market": score_payload["z_market"],
                "z_ensemble": score_payload["z_ensemble"],
                "intercept": score_payload["intercept"],
                "disagreement_score": score_payload["disagreement_score"],
                "effective_n": score_payload["effective_n"],
                "uncertainty_score": score_payload["uncertainty_score"],
                "contributions_json": score_payload["contributions"],
                "coverage_json": score_payload["coverage"],
                "metadata_json": {
                    "signal_deltas": score_payload["deltas"],
                    "calibration_scopes": calibration_scope_by_signal,
                    "label_yes": row.get("label_yes"),
                },
            }
        )
        trade_decision_rows.append(
            {
                "id": stable_uuid(
                    "trade-decision",
                    model_run_id,
                    row.get("market_id"),
                    row.get("token_id"),
                    row["as_of_ts"].isoformat(),
                ),
                "model_run_id": model_run_id,
                "ensemble_prediction_id": ensemble_prediction_id,
                "feature_snapshot_id": feature_snapshot_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "event_id": row.get("event_id"),
                "market_taxonomy": row["market_taxonomy"],
                "market_group": row["market_group"],
                "meeting_key": row["meeting_key"],
                "as_of_ts": row["as_of_ts"],
                "side": score_payload["side"],
                "edge": score_payload["edge"],
                "threshold": score_payload["threshold"],
                "spread": score_payload["spread"],
                "depth": score_payload["depth"],
                "kelly_fraction_raw": score_payload["kelly_fraction_raw"],
                "disagreement_penalty": score_payload["disagreement_penalty"],
                "liquidity_factor": score_payload["liquidity_factor"],
                "size_fraction": score_payload["size_fraction"],
                "decision_status": score_payload["decision_status"],
                "decision_reason": score_payload["decision_reason"],
                "metadata_json": {
                    "yes_entry_price": score_payload["yes_entry_price"],
                    "no_entry_price": score_payload["no_entry_price"],
                    "coverage": score_payload["coverage"],
                },
            }
        )
        model_prediction_rows.append(
            {
                "id": stable_uuid(
                    "model-prediction",
                    model_run_id,
                    row.get("market_id"),
                    row.get("token_id"),
                    row["as_of_ts"].isoformat(),
                ),
                "model_run_id": model_run_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "as_of_ts": row["as_of_ts"],
                "probability_yes": score_payload["p_yes_ensemble"],
                "probability_no": 1.0 - score_payload["p_yes_ensemble"],
                "raw_score": score_payload["z_ensemble"],
                "calibration_version": SIGNAL_ENSEMBLE_VERSION,
                "explanation_json": {
                    "stage": stage,
                    "market_group": row["market_group"],
                    "market_taxonomy": row["market_taxonomy"],
                    "p_market_ref": row["p_market_ref"],
                    "contributions": score_payload["contributions"],
                    "disagreement_score": score_payload["disagreement_score"],
                    "effective_n": score_payload["effective_n"],
                    "uncertainty_score": score_payload["uncertainty_score"],
                    "decision_status": score_payload["decision_status"],
                    "decision_reason": score_payload["decision_reason"],
                    "feature_snapshot_id": feature_snapshot_id,
                },
            }
        )

    diagnostics: list[dict[str, Any]] = []
    for signal_code in signal_codes:
        grouped = defaultdict(list)
        for row in train_rows:
            row_id = str(row["row_id"])
            probability = oof_calibrated[row_id].get(signal_code)
            if probability is None or row["label_yes"] is None:
                continue
            grouped[(str(row["market_group"]), "overall")].append(
                {
                    "probability": probability,
                    "label": int(row["label_yes"]),
                    "market_ref": row["p_market_ref"],
                    "market_taxonomy": row["market_taxonomy"],
                }
            )
        for (market_group, phase_bucket), items in grouped.items():
            y_true = np.asarray([item["label"] for item in items], dtype=np.float64)
            y_prob = np.asarray([item["probability"] for item in items], dtype=np.float64)
            market_ref = np.asarray([item["market_ref"] for item in items], dtype=np.float64)
            metrics = _metrics_from_probabilities(
                y_true=y_true,
                y_prob=y_prob,
                market_ref=market_ref,
                min_edge=config.min_edge,
            )
            skill_vs_market = None
            market_brier = float(np.mean((market_ref - y_true) ** 2)) if y_true.size > 0 else None
            if metrics["brier_score"] is not None and market_brier is not None:
                skill_vs_market = float(market_brier - metrics["brier_score"])
            diagnostics.append(
                {
                    "id": stable_uuid(
                        "signal-diagnostic",
                        model_run_id,
                        signal_code,
                        market_group,
                        phase_bucket,
                    ),
                    "model_run_id": model_run_id,
                    "signal_code": signal_code,
                    "market_taxonomy": None,
                    "market_group": market_group,
                    "phase_bucket": phase_bucket,
                    "brier": metrics["brier_score"],
                    "log_loss": metrics["log_loss"],
                    "ece": metrics["ece"],
                    "skill_vs_market": skill_vs_market,
                    "coverage_rate": (
                        float(
                            np.mean(
                                [
                                    int(row["signal_coverage"].get(signal_code, False))
                                    for row in train_rows
                                    if str(row["market_group"]) == market_group
                                ]
                            )
                        )
                        if train_rows
                        else None
                    ),
                    "residual_correlation_json": combiners.get(market_group, {}).get(
                        "residual_correlation"
                    ),
                    "stability_json": {
                        "fit_row_count": len(items),
                        "meeting_count": len(
                            {
                                row["meeting_key"]
                                for row in train_rows
                                if str(row["market_group"]) == market_group
                                and row["label_yes"] is not None
                            }
                        ),
                    },
                    "metrics_json": metrics,
                }
            )

    labeled_scored = [row for row in scored_rows if row.get("label_yes") is not None]
    overall_metrics = _metrics_from_probabilities(
        y_true=np.asarray([int(row["label_yes"]) for row in labeled_scored], dtype=np.float64),
        y_prob=np.asarray(
            [float(row["p_yes_ensemble"]) for row in labeled_scored],
            dtype=np.float64,
        ),
        market_ref=np.asarray(
            [float(row["p_market_ref"]) for row in labeled_scored],
            dtype=np.float64,
        ),
        min_edge=config.min_edge,
    )
    market_group_breakdown = _group_breakdown(
        scored_rows,
        group_key="market_group",
        probability_key="p_yes_ensemble",
        min_edge=config.min_edge,
    )
    taxonomy_breakdown = _group_breakdown(
        scored_rows,
        group_key="market_taxonomy",
        probability_key="p_yes_ensemble",
        min_edge=config.min_edge,
    )
    executable_rows = [
        row
        for row in trade_decision_rows
        if row["decision_status"] == "trade"
    ]
    overall_metrics.update(
        {
            "stage": stage,
            "signal_codes": signal_codes,
            "fit_row_count": len(train_rows),
            "test_row_count": len(test_rows),
            "market_group_breakdown": market_group_breakdown,
            "taxonomy_breakdown": taxonomy_breakdown,
            "trade_count": len(executable_rows),
            "decision_status_breakdown": {
                status: sum(1 for row in trade_decision_rows if row["decision_status"] == status)
                for status in sorted({row["decision_status"] for row in trade_decision_rows})
            },
            "coverage_by_market_group": {
                market_group: {
                    "supported": market_group in combiners,
                    "row_count": sum(
                        1
                        for row in test_rows
                        if str(row["market_group"]) == market_group
                    ),
                }
                for market_group in sorted({str(row["market_group"]) for row in test_rows})
            },
            "family_pnl_share_max": _family_pnl_share_max(
                market_group_breakdown,
                total_pnl=float(overall_metrics.get("total_pnl") or 0.0),
            ),
        }
    )

    if artifact_dir is not None:
        _serialize_model_artifact(
            artifact_dir,
            config=config,
            definitions=definitions,
            calibration_bundle=calibration_bundle,
            combiners=combiners,
            metrics=overall_metrics,
        )

    return EnsembleTrainResult(
        model_run_id=model_run_id,
        predictions=model_prediction_rows,
        signal_snapshots=signal_snapshot_rows,
        ensemble_predictions=ensemble_prediction_rows,
        trade_decisions=trade_decision_rows,
        diagnostics=diagnostics,
        metrics=overall_metrics,
        config={
            "stage": stage,
            "min_edge": config.min_edge,
            "cost_buffer": config.cost_buffer,
            "kelly_cap": config.kelly_cap,
            "max_spread": config.max_spread,
            "signal_codes": signal_codes,
        },
    )


def score_signal_ensemble_frame(
    frame: pl.DataFrame,
    *,
    artifact_dir: Path,
    model_run_id: str,
    stage: str = SIGNAL_ENSEMBLE_STAGE,
    feature_snapshot_id: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    definitions, config, payload = load_signal_ensemble_artifacts(artifact_dir)
    signal_codes = [definition.signal_code for definition in definitions]
    matrix_rows = compute_signal_matrix(frame, definitions=definitions)
    calibration_bundle = payload.get("calibration_bundle") or {}
    combiners = payload.get("combiners") or {}

    signal_snapshot_rows: list[dict[str, Any]] = []
    ensemble_prediction_rows: list[dict[str, Any]] = []
    trade_decision_rows: list[dict[str, Any]] = []
    model_prediction_rows: list[dict[str, Any]] = []

    for row in matrix_rows:
        calibrated_by_signal: dict[str, float | None] = {}
        calibration_scope_by_signal: dict[str, str] = {}
        for signal_code in signal_codes:
            raw_probability = row["signal_raw"].get(signal_code)
            scope_key, calibrator = _select_calibrator_payload(
                calibration_bundle.get(signal_code, {}),
                taxonomy=str(row["market_taxonomy"]),
                market_group=str(row["market_group"]),
            )
            calibrated = (
                None
                if raw_probability is None
                else _apply_calibrator_payload(raw_probability, calibrator)
            )
            calibrated_by_signal[signal_code] = calibrated
            calibration_scope_by_signal[signal_code] = scope_key
            signal_snapshot_rows.append(
                {
                    "id": stable_uuid(
                        "signal-snapshot",
                        model_run_id,
                        row["market_id"],
                        row.get("token_id"),
                        row["as_of_ts"].isoformat(),
                        signal_code,
                    ),
                    "model_run_id": model_run_id,
                    "feature_snapshot_id": feature_snapshot_id,
                    "market_id": row.get("market_id"),
                    "token_id": row.get("token_id"),
                    "event_id": row.get("event_id"),
                    "market_taxonomy": row["market_taxonomy"],
                    "market_group": row["market_group"],
                    "meeting_key": row["meeting_key"],
                    "as_of_ts": row["as_of_ts"],
                    "signal_code": signal_code,
                    "signal_version": SIGNAL_ENSEMBLE_VERSION,
                    "p_yes_raw": raw_probability,
                    "p_yes_calibrated": calibrated,
                    "p_market_ref": row["p_market_ref"],
                    "delta_logit": (
                        None
                        if calibrated is None
                        else _logit(calibrated) - _logit(row["p_market_ref"])
                    ),
                    "freshness_sec": _coerce_float(row.get("last_trade_age_seconds")),
                    "coverage_flag": bool(row["signal_coverage"].get(signal_code)),
                    "metadata_json": {
                        **row["signal_metadata"].get(signal_code, {}),
                        "calibration_scope": scope_key,
                    },
                }
            )

        score_payload = _score_one_row(
            row,
            signal_codes=signal_codes,
            calibrated=calibrated_by_signal,
            combiner_payload=combiners.get(str(row["market_group"])),
            config=config,
        )
        ensemble_prediction_id = stable_uuid(
            "ensemble-prediction",
            model_run_id,
            row.get("market_id"),
            row.get("token_id"),
            row["as_of_ts"].isoformat(),
        )
        ensemble_prediction_rows.append(
            {
                "id": ensemble_prediction_id,
                "model_run_id": model_run_id,
                "feature_snapshot_id": feature_snapshot_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "event_id": row.get("event_id"),
                "market_taxonomy": row["market_taxonomy"],
                "market_group": row["market_group"],
                "meeting_key": row["meeting_key"],
                "as_of_ts": row["as_of_ts"],
                "p_market_ref": row["p_market_ref"],
                "p_yes_ensemble": score_payload["p_yes_ensemble"],
                "z_market": score_payload["z_market"],
                "z_ensemble": score_payload["z_ensemble"],
                "intercept": score_payload["intercept"],
                "disagreement_score": score_payload["disagreement_score"],
                "effective_n": score_payload["effective_n"],
                "uncertainty_score": score_payload["uncertainty_score"],
                "contributions_json": score_payload["contributions"],
                "coverage_json": score_payload["coverage"],
                "metadata_json": {
                    "signal_deltas": score_payload["deltas"],
                    "calibration_scopes": calibration_scope_by_signal,
                },
            }
        )
        trade_decision_rows.append(
            {
                "id": stable_uuid(
                    "trade-decision",
                    model_run_id,
                    row.get("market_id"),
                    row.get("token_id"),
                    row["as_of_ts"].isoformat(),
                ),
                "model_run_id": model_run_id,
                "ensemble_prediction_id": ensemble_prediction_id,
                "feature_snapshot_id": feature_snapshot_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "event_id": row.get("event_id"),
                "market_taxonomy": row["market_taxonomy"],
                "market_group": row["market_group"],
                "meeting_key": row["meeting_key"],
                "as_of_ts": row["as_of_ts"],
                "side": score_payload["side"],
                "edge": score_payload["edge"],
                "threshold": score_payload["threshold"],
                "spread": score_payload["spread"],
                "depth": score_payload["depth"],
                "kelly_fraction_raw": score_payload["kelly_fraction_raw"],
                "disagreement_penalty": score_payload["disagreement_penalty"],
                "liquidity_factor": score_payload["liquidity_factor"],
                "size_fraction": score_payload["size_fraction"],
                "decision_status": score_payload["decision_status"],
                "decision_reason": score_payload["decision_reason"],
                "metadata_json": {
                    "yes_entry_price": score_payload["yes_entry_price"],
                    "no_entry_price": score_payload["no_entry_price"],
                    "coverage": score_payload["coverage"],
                },
            }
        )
        model_prediction_rows.append(
            {
                "id": stable_uuid(
                    "model-prediction",
                    model_run_id,
                    row.get("market_id"),
                    row.get("token_id"),
                    row["as_of_ts"].isoformat(),
                ),
                "model_run_id": model_run_id,
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "as_of_ts": row["as_of_ts"],
                "probability_yes": score_payload["p_yes_ensemble"],
                "probability_no": 1.0 - score_payload["p_yes_ensemble"],
                "raw_score": score_payload["z_ensemble"],
                "calibration_version": SIGNAL_ENSEMBLE_VERSION,
                "explanation_json": {
                    "stage": stage,
                    "market_group": row["market_group"],
                    "market_taxonomy": row["market_taxonomy"],
                    "p_market_ref": row["p_market_ref"],
                    "contributions": score_payload["contributions"],
                    "disagreement_score": score_payload["disagreement_score"],
                    "effective_n": score_payload["effective_n"],
                    "uncertainty_score": score_payload["uncertainty_score"],
                    "decision_status": score_payload["decision_status"],
                    "decision_reason": score_payload["decision_reason"],
                    "feature_snapshot_id": feature_snapshot_id,
                },
            }
        )

    return {
        "signal_snapshots": signal_snapshot_rows,
        "ensemble_predictions": ensemble_prediction_rows,
        "trade_decisions": trade_decision_rows,
        "predictions": model_prediction_rows,
    }
