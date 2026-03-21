from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    feature_name: str
    feature_group: str
    data_type: str
    version: str = "v1"
    description: str | None = None


def default_feature_registry() -> list[FeatureDefinition]:
    return [
        # -- FP1 pace features --
        FeatureDefinition(
            feature_name="fp1_position",
            feature_group="session_pace",
            data_type="int",
            description="Driver finishing position in FP1.",
        ),
        FeatureDefinition(
            feature_name="fp1_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Gap to the FP1 session leader in seconds.",
        ),
        FeatureDefinition(
            feature_name="fp1_teammate_gap_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Intra-team gap: driver time minus teammate best time.",
        ),
        FeatureDefinition(
            feature_name="fp1_team_best_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best team gap to FP1 leader (team-level signal).",
        ),
        FeatureDefinition(
            feature_name="fp1_lap_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of laps completed in FP1.",
        ),
        FeatureDefinition(
            feature_name="fp1_stint_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of distinct stints (pit-stop separated) in FP1.",
        ),
        FeatureDefinition(
            feature_name="fp1_result_time_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best lap time achieved in FP1 in seconds.",
        ),
        # -- Market microstructure features --
        FeatureDefinition(
            feature_name="entry_yes_price",
            feature_group="market_microstructure",
            data_type="float",
            description="YES token price at the entry observation point.",
        ),
        FeatureDefinition(
            feature_name="entry_spread",
            feature_group="market_microstructure",
            data_type="float",
            description="Best-ask minus best-bid at entry observation.",
        ),
        FeatureDefinition(
            feature_name="entry_midpoint",
            feature_group="market_microstructure",
            data_type="float",
            description="Midpoint of best bid and best ask at entry.",
        ),
        FeatureDefinition(
            feature_name="trade_count_pre_entry",
            feature_group="market_microstructure",
            data_type="int",
            description="Total trades executed before the entry observation.",
        ),
        FeatureDefinition(
            feature_name="last_trade_age_seconds",
            feature_group="market_microstructure",
            data_type="float",
            description="Seconds since the last trade before entry observation.",
        ),
        # -- Derived probability features --
        FeatureDefinition(
            feature_name="market_normalized_prob",
            feature_group="derived_probability",
            data_type="float",
            description="Normalized market-implied probability (sums to 1 per event).",
        ),
        FeatureDefinition(
            feature_name="fp1_pace_probability",
            feature_group="derived_probability",
            data_type="float",
            description="FP1 pace-based probability via z-score softmax.",
        ),
        FeatureDefinition(
            feature_name="hybrid_probability",
            feature_group="derived_probability",
            data_type="float",
            description="Equal-weight blend of market and FP1 pace probabilities.",
        ),
    ]
