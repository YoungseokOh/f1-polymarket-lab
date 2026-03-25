"""Feature compute engine.

Transforms raw feature columns into engineered features:
  - Z-score normalization within each event
  - Log transforms for skewed distributions
  - Interaction features (pace × market)
  - Cross-GP rolling averages
"""

from __future__ import annotations

import polars as pl

PACE_COLS = [
    "fp1_position",
    "fp1_gap_to_leader_seconds",
    "fp1_teammate_gap_seconds",
    "fp1_lap_count",
    "fp1_stint_count",
    "fp1_result_time_seconds",
    "fp1_team_best_gap_to_leader_seconds",
    "fp2_position",
    "fp2_gap_to_leader_seconds",
    "fp2_teammate_gap_seconds",
    "fp2_lap_count",
    "fp2_stint_count",
    "fp2_result_time_seconds",
    "fp2_team_best_gap_to_leader_seconds",
    "fp3_position",
    "fp3_gap_to_leader_seconds",
    "fp3_teammate_gap_seconds",
    "fp3_lap_count",
    "fp3_stint_count",
    "fp3_result_time_seconds",
    "fp3_team_best_gap_to_leader_seconds",
    "best_practice_position",
    "best_practice_gap_to_leader_seconds",
]

MARKET_COLS = [
    "entry_yes_price",
    "entry_spread",
    "entry_midpoint",
    "trade_count_pre_entry",
    "last_trade_age_seconds",
]


def zscore_within_event(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Add z-score normalized columns within each event_id group."""
    available = [c for c in cols if c in df.columns]
    if not available or "event_id" not in df.columns:
        return df

    for col in available:
        mean_alias = f"_mean_{col}"
        std_alias = f"_std_{col}"
        z_alias = f"{col}_zscore"
        df = df.with_columns(
            pl.col(col).mean().over("event_id").alias(mean_alias),
            pl.col(col).std().over("event_id").alias(std_alias),
        )
        df = df.with_columns(
            pl.when(pl.col(std_alias) > 1e-9)
            .then((pl.col(col) - pl.col(mean_alias)) / pl.col(std_alias))
            .otherwise(0.0)
            .alias(z_alias)
        ).drop(mean_alias, std_alias)

    return df


def log_transform(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Add log1p-transformed columns for skewed features."""
    available = [c for c in cols if c in df.columns]
    for col in available:
        df = df.with_columns(
            pl.col(col).abs().log1p().alias(f"{col}_log1p")
        )
    return df


def interaction_features(df: pl.DataFrame) -> pl.DataFrame:
    """Create pace × market interaction features."""
    interactions = []

    # Use latest available FP gap for interaction features
    _gap_candidates = [
        "fp3_gap_to_leader_seconds",
        "fp2_gap_to_leader_seconds",
        "fp1_gap_to_leader_seconds",
    ]
    _pos_candidates = ["fp3_position", "fp2_position", "fp1_position"]
    best_gap_col = next((c for c in _gap_candidates if c in df.columns), None)
    best_pos_col = next((c for c in _pos_candidates if c in df.columns), None)

    if best_pos_col and "entry_yes_price" in df.columns:
        interactions.append(
            (pl.col(best_pos_col).cast(pl.Float64) * pl.col("entry_yes_price"))
            .alias("pace_x_price")
        )

    if best_gap_col and "entry_spread" in df.columns:
        interactions.append(
            (pl.col(best_gap_col) * pl.col("entry_spread"))
            .alias("gap_x_spread")
        )

    if best_pos_col and "entry_spread" in df.columns:
        interactions.append(
            (pl.col(best_pos_col).cast(pl.Float64) * pl.col("entry_spread"))
            .alias("position_x_spread")
        )

    if interactions:
        df = df.with_columns(interactions)

    return df


def rolling_cross_gp_features(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    """Add rolling averages across GPs for position and gap features.

    Requires ``meeting_key`` column to determine GP ordering.
    """
    if "meeting_key" not in df.columns or "driver_id" not in df.columns:
        return df

    new_cols = []
    for col in (
        "fp1_position",
        "fp1_gap_to_leader_seconds",
        "fp2_position",
        "fp2_gap_to_leader_seconds",
        "fp3_position",
        "fp3_gap_to_leader_seconds",
        "best_practice_position",
        "best_practice_gap_to_leader_seconds",
    ):
        if col not in df.columns:
            continue
        alias = f"rolling_{window}gp_{col}"
        new_cols.append(
            pl.col(col)
            .cast(pl.Float64)
            .rolling_mean(window_size=window, min_samples=1)
            .over("driver_id")
            .alias(alias)
        )

    if new_cols:
        df = df.sort("meeting_key").with_columns(new_cols)

    return df


def compute_features(
    df: pl.DataFrame,
    *,
    zscore: bool = True,
    log: bool = True,
    interactions: bool = True,
    cross_gp: bool = True,
    cross_gp_window: int = 3,
) -> pl.DataFrame:
    """Full feature compute pipeline.

    Returns a DataFrame with the original columns plus all engineered features.
    """
    if zscore:
        df = zscore_within_event(df, PACE_COLS + MARKET_COLS)
    if log:
        df = log_transform(
            df,
            [
                "fp1_gap_to_leader_seconds",
                "fp2_gap_to_leader_seconds",
                "fp3_gap_to_leader_seconds",
                "best_practice_gap_to_leader_seconds",
                "last_trade_age_seconds",
                "trade_count_pre_entry",
            ],
        )
    if interactions:
        df = interaction_features(df)
    if cross_gp:
        df = rolling_cross_gp_features(df, window=cross_gp_window)
    return df
