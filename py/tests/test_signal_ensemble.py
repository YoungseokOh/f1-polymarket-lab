from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from f1_polymarket_lab.models.signal_ensemble import (
    SignalEnsembleConfig,
    compute_signal_matrix,
    default_signal_definitions,
    default_signal_registry_entries,
    score_signal_ensemble_frame,
    train_signal_ensemble_split,
)


def _make_event_rows(
    *,
    meeting_key: int,
    taxonomy: str,
    event_suffix: str,
    option_count: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    winner_index = meeting_key % option_count
    event_id = f"{taxonomy}:{meeting_key}:{event_suffix}"
    for index in range(option_count):
        strength = option_count - index
        rows.append(
            {
                "row_id": f"{event_id}:{index}",
                "meeting_key": meeting_key,
                "event_id": event_id,
                "market_id": f"market:{event_id}:{index}",
                "token_id": f"token:{event_id}:{index}",
                "market_taxonomy": taxonomy,
                "target_session_code": "Q" if taxonomy != "red_flag" else "R",
                "entry_observed_at_utc": datetime(
                    2026,
                    4,
                    1 + (meeting_key % 20),
                    12,
                    index,
                    tzinfo=timezone.utc,
                ),
                "entry_yes_price": 0.12 + (strength * 0.05),
                "entry_midpoint": 0.12 + (strength * 0.05),
                "entry_best_bid": 0.1 + (strength * 0.04),
                "entry_best_ask": 0.14 + (strength * 0.05),
                "entry_spread": 0.02 + (index * 0.003),
                "trade_count_pre_entry": 10 + (strength * 4),
                "last_trade_age_seconds": 120.0 + (index * 30.0),
                "driver_track_affinity": 1.6 - (index * 0.25),
                "driver_s1_strength": 1.2 - (index * 0.2),
                "driver_s2_strength": 1.0 - (index * 0.18),
                "driver_s3_strength": 1.1 - (index * 0.2),
                "best_practice_position": index + 1,
                "best_practice_gap_to_leader_seconds": index * 0.15,
                "fp1_position": index + 1,
                "fp1_gap_to_leader_seconds": index * 0.18,
                "fp1_teammate_gap_seconds": (index - 1) * 0.04,
                "fp1_team_best_gap_to_leader_seconds": index * 0.08,
                "fp1_lap_count": 18 - index,
                "fp1_stint_count": 2 + (index % 2),
                "fp2_position": index + 1,
                "fp2_gap_to_leader_seconds": index * 0.12,
                "fp2_teammate_gap_seconds": (index - 1) * 0.03,
                "fp2_team_best_gap_to_leader_seconds": index * 0.06,
                "fp2_lap_count": 20 - index,
                "fp2_stint_count": 3 + (index % 2),
                "fp3_position": index + 1,
                "fp3_gap_to_leader_seconds": index * 0.1,
                "fp3_teammate_gap_seconds": (index - 1) * 0.02,
                "fp3_team_best_gap_to_leader_seconds": index * 0.05,
                "fp3_lap_count": 16 - index,
                "fp3_stint_count": 2 + (index % 2),
                "q_position": index + 1,
                "q_gap_to_leader_seconds": index * 0.09,
                "q_teammate_gap_seconds": (index - 1) * 0.02,
                "q_team_best_gap_to_leader_seconds": index * 0.04,
                "label_yes": 1 if index == winner_index else 0,
            }
        )
    return rows


def build_signal_ensemble_df(
    meeting_key: int,
    *,
    include_incident: bool = False,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    rows.extend(
        _make_event_rows(
            meeting_key=meeting_key,
            taxonomy="driver_pole_position",
            event_suffix="pole",
            option_count=5,
        )
    )
    rows.extend(
        _make_event_rows(
            meeting_key=meeting_key,
            taxonomy="constructor_pole_position",
            event_suffix="constructor-pole",
            option_count=3,
        )
    )
    rows.extend(
        _make_event_rows(
            meeting_key=meeting_key,
            taxonomy="head_to_head_session",
            event_suffix="h2h",
            option_count=2,
        )
    )
    if include_incident:
        rows.extend(
            _make_event_rows(
                meeting_key=meeting_key,
                taxonomy="red_flag",
                event_suffix="incident",
                option_count=1,
            )
        )
    return pl.DataFrame(rows)


def test_train_signal_ensemble_split_returns_generic_outputs(tmp_path: Path) -> None:
    train_df = pl.concat(
        [
            build_signal_ensemble_df(100),
            build_signal_ensemble_df(200),
            build_signal_ensemble_df(300),
        ],
        how="diagonal_relaxed",
    )
    test_df = build_signal_ensemble_df(400, include_incident=True)
    artifact_dir = tmp_path / "ensemble-artifacts"

    result = train_signal_ensemble_split(
        train_df,
        test_df,
        model_run_id="signal-run-400",
        config=SignalEnsembleConfig(
            min_train_groups=2,
            min_rows_for_group_model=6,
            min_group_rows_for_executable=6,
            min_group_meetings_for_executable=2,
            min_isotonic_rows=50,
            min_platt_rows=10,
            min_platt_class_rows=2,
            max_spread=0.08,
        ),
        artifact_dir=artifact_dir,
        feature_snapshot_id="snapshot-400",
    )

    assert result.model_run_id == "signal-run-400"
    assert len(result.predictions) == test_df.height
    assert len(result.ensemble_predictions) == test_df.height
    assert len(result.trade_decisions) == test_df.height
    assert len(result.signal_snapshots) == test_df.height * len(default_signal_definitions())
    assert result.metrics["market_group_breakdown"]["driver_outright"]["row_count"] > 0
    assert result.metrics["taxonomy_breakdown"]["head_to_head_session"]["row_count"] > 0
    assert result.metrics["signal_codes"][0] == "market_microstructure_signal"
    assert (artifact_dir / "signal_ensemble_bundle.json").exists()

    incident_decision = next(
        row for row in result.trade_decisions if row["market_taxonomy"] == "red_flag"
    )
    assert incident_decision["decision_status"] == "blocked"
    assert incident_decision["decision_reason"] == "diagnostics_only_market_group"


def test_score_signal_ensemble_frame_reuses_saved_artifacts(tmp_path: Path) -> None:
    train_df = pl.concat(
        [
            build_signal_ensemble_df(110),
            build_signal_ensemble_df(210),
            build_signal_ensemble_df(310),
        ],
        how="diagonal_relaxed",
    )
    test_df = build_signal_ensemble_df(410, include_incident=True)
    artifact_dir = tmp_path / "ensemble-artifacts"

    train_signal_ensemble_split(
        train_df,
        test_df,
        model_run_id="signal-run-410",
        config=SignalEnsembleConfig(
            min_train_groups=2,
            min_rows_for_group_model=6,
            min_group_rows_for_executable=6,
            min_group_meetings_for_executable=2,
            min_isotonic_rows=50,
            min_platt_rows=10,
            min_platt_class_rows=2,
            max_spread=0.08,
        ),
        artifact_dir=artifact_dir,
        feature_snapshot_id="snapshot-410",
    )

    scored = score_signal_ensemble_frame(
        test_df,
        artifact_dir=artifact_dir,
        model_run_id="signal-run-411",
        feature_snapshot_id="snapshot-411",
    )

    assert len(scored["predictions"]) == test_df.height
    assert len(scored["trade_decisions"]) == test_df.height
    assert len(scored["signal_snapshots"]) == test_df.height * len(default_signal_definitions())
    assert all(
        row["calibration_version"] == "v1" for row in scored["predictions"]
    )
    assert any(
        row["decision_status"] == "trade" for row in scored["trade_decisions"]
    )


def test_head_to_head_signals_are_not_forced_into_event_softmax() -> None:
    frame = pl.DataFrame(
        [
            {
                "row_id": "h2h-1",
                "meeting_key": 500,
                "event_id": "shared-h2h-event",
                "market_id": "market-1",
                "token_id": "token-1",
                "market_taxonomy": "head_to_head_session",
                "entry_yes_price": 0.5,
                "entry_midpoint": 0.5,
                "fp1_position": 1,
                "fp1_gap_to_leader_seconds": 0.1,
                "best_practice_position": 1,
                "best_practice_gap_to_leader_seconds": 0.1,
            },
            {
                "row_id": "h2h-2",
                "meeting_key": 500,
                "event_id": "shared-h2h-event",
                "market_id": "market-2",
                "token_id": "token-2",
                "market_taxonomy": "head_to_head_session",
                "entry_yes_price": 0.5,
                "entry_midpoint": 0.5,
                "fp1_position": 1,
                "fp1_gap_to_leader_seconds": 0.1,
                "best_practice_position": 1,
                "best_practice_gap_to_leader_seconds": 0.1,
            },
            {
                "row_id": "h2h-3",
                "meeting_key": 500,
                "event_id": "shared-h2h-event",
                "market_id": "market-3",
                "token_id": "token-3",
                "market_taxonomy": "head_to_head_session",
                "entry_yes_price": 0.5,
                "entry_midpoint": 0.5,
                "fp1_position": 1,
                "fp1_gap_to_leader_seconds": 0.1,
                "best_practice_position": 1,
                "best_practice_gap_to_leader_seconds": 0.1,
            },
            {
                "row_id": "h2h-4",
                "meeting_key": 500,
                "event_id": "shared-h2h-event",
                "market_id": "market-4",
                "token_id": "token-4",
                "market_taxonomy": "head_to_head_session",
                "entry_yes_price": 0.5,
                "entry_midpoint": 0.5,
                "fp1_position": 1,
                "fp1_gap_to_leader_seconds": 0.1,
                "best_practice_position": 1,
                "best_practice_gap_to_leader_seconds": 0.1,
            },
        ]
    )

    matrix_rows = compute_signal_matrix(frame, definitions=default_signal_definitions())
    pace_probs = [
        row["signal_raw"]["pace_delta_signal"]
        for row in matrix_rows
        if row["market_taxonomy"] == "head_to_head_session"
    ]
    cross_market_coverage = [
        row["signal_coverage"]["cross_market_consistency_signal"]
        for row in matrix_rows
    ]

    assert len(pace_probs) == 4
    assert all(abs(float(probability) - 0.5) < 1e-9 for probability in pace_probs)
    assert sum(float(probability) for probability in pace_probs) == 2.0
    assert cross_market_coverage == [False, False, False, False]


def test_default_signal_registry_entries_store_scope_rows() -> None:
    entries = default_signal_registry_entries()
    by_signal = {}
    for entry in entries:
        by_signal.setdefault(entry["signal_code"], []).append(entry)

    pace_entries = by_signal["pace_delta_signal"]
    assert {entry["market_group"] for entry in pace_entries} == {
        "driver_outright",
        "constructor_outright",
        "head_to_head",
    }
    assert all(entry["config_json"]["scope_key"] for entry in pace_entries)
