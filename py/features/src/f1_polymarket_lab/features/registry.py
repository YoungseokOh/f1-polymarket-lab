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
        FeatureDefinition(
            feature_name="team_pace_prior",
            feature_group="historical_performance",
            data_type="float",
            description="Stage 1 placeholder prior for team pace strength.",
        ),
        FeatureDefinition(
            feature_name="market_bid_ask_spread",
            feature_group="market_microstructure",
            data_type="float",
            description="Executable spread observed at snapshot time.",
        ),
    ]
