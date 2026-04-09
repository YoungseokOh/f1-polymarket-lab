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

MARKET_GROUPS = (
    "driver_outright",
    "constructor_outright",
    "head_to_head",
    "incident_binary",
    "championship",
    "other",
)

MarketGroup: TypeAlias = Literal[
    "driver_outright",
    "constructor_outright",
    "head_to_head",
    "incident_binary",
    "championship",
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


_MARKET_GROUP_BY_TAXONOMY: dict[MarketTaxonomy, MarketGroup] = {
    "head_to_head_session": "head_to_head",
    "head_to_head_practice": "head_to_head",
    "driver_pole_position": "driver_outright",
    "constructor_fastest_lap_practice": "constructor_outright",
    "constructor_fastest_lap_session": "constructor_outright",
    "constructor_pole_position": "constructor_outright",
    "constructor_scores_first": "constructor_outright",
    "constructors_champion": "championship",
    "driver_fastest_lap_practice": "driver_outright",
    "driver_fastest_lap_session": "driver_outright",
    "driver_podium": "driver_outright",
    "drivers_champion": "championship",
    "qualifying_winner": "driver_outright",
    "race_winner": "driver_outright",
    "red_flag": "incident_binary",
    "safety_car": "incident_binary",
    "sprint_winner": "driver_outright",
    "other": "other",
}


def market_group_for_taxonomy(taxonomy: str | None) -> MarketGroup:
    return _MARKET_GROUP_BY_TAXONOMY.get(coerce_market_taxonomy(taxonomy), "other")


def taxonomies_for_market_group(group: MarketGroup) -> tuple[MarketTaxonomy, ...]:
    return tuple(
        taxonomy
        for taxonomy, market_group in _MARKET_GROUP_BY_TAXONOMY.items()
        if market_group == group
    )
