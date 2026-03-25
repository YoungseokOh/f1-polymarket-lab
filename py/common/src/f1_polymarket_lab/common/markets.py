from __future__ import annotations

from typing import Literal, TypeAlias, cast

MARKET_TAXONOMIES = (
    "head_to_head_session",
    "head_to_head_practice",
    "driver_pole_position",
    "constructor_fastest_lap_practice",
    "constructor_fastest_lap_session",
    "constructor_pole_position",
    "constructor_scores_first",
    "constructors_champion",
    "driver_fastest_lap_practice",
    "driver_fastest_lap_session",
    "driver_podium",
    "drivers_champion",
    "qualifying_winner",
    "race_winner",
    "red_flag",
    "safety_car",
    "sprint_winner",
    "other",
)

MarketTaxonomy: TypeAlias = Literal[
    "head_to_head_session",
    "head_to_head_practice",
    "driver_pole_position",
    "constructor_fastest_lap_practice",
    "constructor_fastest_lap_session",
    "constructor_pole_position",
    "constructor_scores_first",
    "constructors_champion",
    "driver_fastest_lap_practice",
    "driver_fastest_lap_session",
    "driver_podium",
    "drivers_champion",
    "qualifying_winner",
    "race_winner",
    "red_flag",
    "safety_car",
    "sprint_winner",
    "other",
]

_MARKET_TAXONOMY_SET = frozenset(MARKET_TAXONOMIES)


def is_market_taxonomy(value: str) -> bool:
    return value in _MARKET_TAXONOMY_SET


def coerce_market_taxonomy(value: str | None) -> MarketTaxonomy:
    if value is None:
        return "other"
    if is_market_taxonomy(value):
        return cast(MarketTaxonomy, value)
    return "other"
