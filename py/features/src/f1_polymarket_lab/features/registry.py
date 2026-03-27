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
        # -- FP2 pace features --
        FeatureDefinition(
            feature_name="fp2_position",
            feature_group="session_pace",
            data_type="int",
            description="Driver finishing position in FP2.",
        ),
        FeatureDefinition(
            feature_name="fp2_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Gap to the FP2 session leader in seconds.",
        ),
        FeatureDefinition(
            feature_name="fp2_teammate_gap_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Intra-team gap: driver time minus teammate best time in FP2.",
        ),
        FeatureDefinition(
            feature_name="fp2_team_best_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best team gap to FP2 leader (team-level signal).",
        ),
        FeatureDefinition(
            feature_name="fp2_lap_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of laps completed in FP2.",
        ),
        FeatureDefinition(
            feature_name="fp2_stint_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of distinct stints (pit-stop separated) in FP2.",
        ),
        FeatureDefinition(
            feature_name="fp2_result_time_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best lap time achieved in FP2 in seconds.",
        ),
        # -- FP3 pace features --
        FeatureDefinition(
            feature_name="fp3_position",
            feature_group="session_pace",
            data_type="int",
            description="Driver finishing position in FP3.",
        ),
        FeatureDefinition(
            feature_name="fp3_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Gap to the FP3 session leader in seconds.",
        ),
        FeatureDefinition(
            feature_name="fp3_teammate_gap_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Intra-team gap: driver time minus teammate best time in FP3.",
        ),
        FeatureDefinition(
            feature_name="fp3_team_best_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best team gap to FP3 leader (team-level signal).",
        ),
        FeatureDefinition(
            feature_name="fp3_lap_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of laps completed in FP3.",
        ),
        FeatureDefinition(
            feature_name="fp3_stint_count",
            feature_group="session_pace",
            data_type="int",
            description="Number of distinct stints (pit-stop separated) in FP3.",
        ),
        FeatureDefinition(
            feature_name="fp3_result_time_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Best lap time achieved in FP3 in seconds.",
        ),
        # -- Best-practice derived features --
        FeatureDefinition(
            feature_name="best_practice_position",
            feature_group="session_pace",
            data_type="int",
            description="Best position across all available practice sessions (FP1/FP2/FP3).",
        ),
        FeatureDefinition(
            feature_name="best_practice_gap_to_leader_seconds",
            feature_group="session_pace",
            data_type="float",
            description="Minimum gap to leader across all available practice sessions.",
        ),
        FeatureDefinition(
            feature_name="latest_fp_number",
            feature_group="session_pace",
            data_type="int",
            description="Highest practice session number with data available (1, 2, or 3).",
        ),
        # -- Checkpoint context features --
        FeatureDefinition(
            feature_name="has_fp1",
            feature_group="checkpoint_context",
            data_type="int",
            description="Indicator that FP1 data is available for the snapshot.",
        ),
        FeatureDefinition(
            feature_name="has_fp2",
            feature_group="checkpoint_context",
            data_type="int",
            description="Indicator that FP2 data is available for the snapshot.",
        ),
        FeatureDefinition(
            feature_name="has_fp3",
            feature_group="checkpoint_context",
            data_type="int",
            description="Indicator that FP3 data is available for the snapshot.",
        ),
        FeatureDefinition(
            feature_name="has_q",
            feature_group="checkpoint_context",
            data_type="int",
            description="Indicator that qualifying data is available for the snapshot.",
        ),
        FeatureDefinition(
            feature_name="checkpoint_ordinal",
            feature_group="checkpoint_context",
            data_type="int",
            description=(
                "Ordinal representation of the snapshot checkpoint "
                "(FP1=1, FP2=2, FP3=3, Q=4)."
            ),
        ),
        # -- Market family features --
        FeatureDefinition(
            feature_name="market_family_is_pole",
            feature_group="market_family",
            data_type="int",
            description="Indicator that the target market family is pole.",
        ),
        FeatureDefinition(
            feature_name="market_family_is_constructor_pole",
            feature_group="market_family",
            data_type="int",
            description="Indicator that the target market family is constructor pole.",
        ),
        FeatureDefinition(
            feature_name="market_family_is_winner",
            feature_group="market_family",
            data_type="int",
            description="Indicator that the target market family is winner.",
        ),
        FeatureDefinition(
            feature_name="market_family_is_h2h",
            feature_group="market_family",
            data_type="int",
            description="Indicator that the target market family is head-to-head.",
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
        # -- Driver sector strength profile features --
        FeatureDefinition(
            feature_name="driver_s1_strength",
            feature_group="driver_profile",
            data_type="float",
            description="Driver S1 pace strength z-score over recent sessions (positive = faster).",
        ),
        FeatureDefinition(
            feature_name="driver_s2_strength",
            feature_group="driver_profile",
            data_type="float",
            description="Driver S2 pace strength z-score over recent sessions (positive = faster).",
        ),
        FeatureDefinition(
            feature_name="driver_s3_strength",
            feature_group="driver_profile",
            data_type="float",
            description="Driver S3 pace strength z-score over recent sessions (positive = faster).",
        ),
        FeatureDefinition(
            feature_name="driver_profile_sessions",
            feature_group="driver_profile",
            data_type="int",
            description="Number of sessions used to compute the driver sector profile.",
        ),
        # -- Target session features --
        FeatureDefinition(
            feature_name="qualifying_position",
            feature_group="target_session",
            data_type="int",
            description="Driver finishing position in qualifying.",
        ),
        FeatureDefinition(
            feature_name="qualifying_gap_to_pole_seconds",
            feature_group="target_session",
            data_type="float",
            description="Gap to pole in qualifying, measured in seconds.",
        ),
        # -- Track sector weight features --
        FeatureDefinition(
            feature_name="track_s1_fraction",
            feature_group="track_profile",
            data_type="float",
            description="Fraction of lap time in S1 at this circuit (from historical Q laps).",
        ),
        FeatureDefinition(
            feature_name="track_s2_fraction",
            feature_group="track_profile",
            data_type="float",
            description="Fraction of lap time in S2 at this circuit (from historical Q laps).",
        ),
        FeatureDefinition(
            feature_name="track_s3_fraction",
            feature_group="track_profile",
            data_type="float",
            description="Fraction of lap time in S3 at this circuit (from historical Q laps).",
        ),
        # -- Driver-track affinity --
        FeatureDefinition(
            feature_name="driver_track_affinity",
            feature_group="driver_profile",
            data_type="float",
            description="Weighted combination of driver sector strengths × track sector fractions.",
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
            description=(
                "Practice pace-based probability via z-score softmax"
                " (uses latest available FP session)."
            ),
        ),
        FeatureDefinition(
            feature_name="hybrid_probability",
            feature_group="derived_probability",
            data_type="float",
            description="Equal-weight blend of market and latest-practice pace probabilities.",
        ),
    ]
