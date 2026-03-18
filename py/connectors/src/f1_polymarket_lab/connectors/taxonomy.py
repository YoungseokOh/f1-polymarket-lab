from __future__ import annotations

from dataclasses import dataclass, field

DRIVER_NAMES = [
    "verstappen",
    "perez",
    "norris",
    "piastri",
    "leclerc",
    "sainz",
    "hamilton",
    "russell",
    "alonso",
    "stroll",
    "tsunoda",
    "gasly",
    "ocon",
    "albon",
    "sargeant",
    "hulkenberg",
    "magnussen",
    "bottas",
    "zhou",
    "ricciardo",
    "bearman",
    "lawson",
]

TEAM_NAMES = [
    "red bull",
    "mclaren",
    "ferrari",
    "mercedes",
    "aston martin",
    "rb",
    "racing bulls",
    "alpine",
    "williams",
    "haas",
    "kick sauber",
    "sauber",
]

SESSION_PATTERNS = {
    "fp1": "FP1",
    "practice 1": "FP1",
    "fp2": "FP2",
    "practice 2": "FP2",
    "fp3": "FP3",
    "practice 3": "FP3",
}


@dataclass(slots=True)
class ParsedMarket:
    taxonomy: str = "other"
    confidence: float = 0.1
    target_session_code: str | None = None
    driver_a: str | None = None
    driver_b: str | None = None
    team_name: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


def parse_market_taxonomy(question: str, description: str | None = None) -> ParsedMarket:
    text = f"{question} {description or ''}".lower()
    parsed = ParsedMarket()

    for pattern, session_code in SESSION_PATTERNS.items():
        if pattern in text:
            parsed.target_session_code = session_code
            parsed.confidence = max(parsed.confidence, 0.35)
            break

    if "red flag" in text:
        parsed.taxonomy = "red_flag"
        parsed.confidence = 0.95
        return parsed

    if "safety car" in text or "virtual safety car" in text or "vsc" in text:
        parsed.taxonomy = "safety_car"
        parsed.confidence = 0.9
        return parsed

    if (" vs " in text or " beat " in text) and parsed.target_session_code is not None:
        parsed.taxonomy = "head_to_head_practice"
        parsed.confidence = 0.8
        if " vs " in text:
            left, right = question.split(" vs ", maxsplit=1) if " vs " in question else ("", "")
            parsed.driver_a = left.strip() or None
            parsed.driver_b = right.split("?")[0].strip() or None
        return parsed

    if "fastest lap" in text and parsed.target_session_code is not None:
        for team_name in TEAM_NAMES:
            if team_name in text:
                parsed.taxonomy = "constructor_fastest_lap_practice"
                parsed.team_name = team_name.title()
                parsed.confidence = 0.8
                return parsed

        for driver_name in DRIVER_NAMES:
            if driver_name in text:
                parsed.taxonomy = "driver_fastest_lap_practice"
                parsed.driver_a = driver_name.title()
                parsed.confidence = 0.78
                return parsed

    return parsed
