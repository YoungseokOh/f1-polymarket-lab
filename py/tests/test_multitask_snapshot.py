from __future__ import annotations

import polars as pl

from f1_polymarket_lab.features import compute_features, default_feature_registry


def test_default_feature_registry_includes_multitask_contract_features() -> None:
    expected_names = {
        "has_fp1",
        "has_fp2",
        "has_fp3",
        "has_q",
        "checkpoint_ordinal",
        "market_family_is_pole",
        "market_family_is_constructor_pole",
        "market_family_is_winner",
        "market_family_is_h2h",
        "qualifying_position",
        "qualifying_gap_to_pole_seconds",
    }

    actual_names = {definition.feature_name for definition in default_feature_registry()}

    assert expected_names <= actual_names


def test_compute_features_adds_multitask_checkpoint_contract_columns() -> None:
    df = pl.DataFrame(
        {
            "event_id": [101, 101],
            "driver_id": ["driver-1", "driver-2"],
            "meeting_key": [2026, 2026],
            "as_of_checkpoint": ["FP1", "Q"],
            "target_market_family": ["winner", "h2h"],
            "has_fp1": [True, True],
            "has_fp2": [False, True],
            "has_fp3": [False, True],
            "has_q": [False, True],
            "fp1_position": [3, 4],
            "fp2_position": [2, 3],
            "fp3_position": [1, 2],
            "qualifying_position": [None, 2],
            "qualifying_gap_to_pole_seconds": [None, 0.123],
        }
    )

    result = compute_features(df, zscore=False, log=False, interactions=False, cross_gp=False)

    assert {
        "checkpoint_ordinal",
        "market_family_is_winner",
        "market_family_is_h2h",
        "availability_sum",
        "pace_x_checkpoint",
    } <= set(result.columns)
    assert result["checkpoint_ordinal"].to_list() == [1, 4]
    assert result["availability_sum"].to_list() == [1, 4]
