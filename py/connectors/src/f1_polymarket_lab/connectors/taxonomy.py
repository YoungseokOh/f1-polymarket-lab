from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

DRIVER_NAMES = [
    "antonelli",
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
    "hadjar",
    "colapinto",
    "bortoleto",
]

TEAM_NAMES = [
    "mclaren mastercard",
    "audi revolut",
    "tgr haas",
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
    "audi",
    "cadillac",
]

SESSION_PATTERNS = {
    "sprint qualifying": "SQ",
    "sprint shootout": "SQ",
    "fp1": "FP1",
    "practice 1": "FP1",
    "fp2": "FP2",
    "practice 2": "FP2",
    "fp3": "FP3",
    "practice 3": "FP3",
    "pole winner": "Q",
    "pole position": "Q",
    "qualifying": "Q",
    "sprint": "S",
    "grand prix": "R",
    "race": "R",
}

ISO_DATE_PATTERN = re.compile(r"(?<!\d)(20\d{2}-\d{2}-\d{2})(?!\d)")
TEXT_DATE_PATTERN = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    r")\s+(\d{1,2}),\s*(20\d{2})\b",
    flags=re.IGNORECASE,
)
MONTH_LOOKUP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
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


def _match_driver_name(text: str) -> str | None:
    for driver_name in DRIVER_NAMES:
        if re.search(rf"\b{re.escape(driver_name)}\b", text):
            return driver_name.title()
    return None


def _match_team_name(text: str) -> str | None:
    for team_name in TEAM_NAMES:
        if re.search(rf"\b{re.escape(team_name)}\b", text):
            return team_name.title()
    return None


def _extract_driver_pair(question: str, text: str) -> tuple[str | None, str | None]:
    if " vs " in question:
        left, right = question.split(" vs ", maxsplit=1)
        return left.strip() or None, right.split("?")[0].strip() or None
    matches = []
    for driver_name in DRIVER_NAMES:
        if re.search(rf"\b{re.escape(driver_name)}\b", text):
            matches.append(driver_name.title())
    if len(matches) >= 2:
        return matches[0], matches[1]
    return None, None


def infer_market_scheduled_date(*values: str | None) -> date | None:
    for value in values:
        if not value:
            continue
        match = ISO_DATE_PATTERN.search(value)
        if match is not None:
            return date.fromisoformat(match.group(1))
        text_match = TEXT_DATE_PATTERN.search(value)
        if text_match is None:
            continue
        month = MONTH_LOOKUP[text_match.group(1)[:3].lower()]
        day = int(text_match.group(2))
        year = int(text_match.group(3))
        return date(year, month, day)
    return None


def parse_market_taxonomy(
    question: str,
    description: str | None = None,
    *,
    title: str | None = None,
) -> ParsedMarket:
    question_text = question.lower()
    title_text = (title or "").lower()
    description_text = (description or "").lower()
    primary_text = f"{question} {title or ''}".lower()
    session_text = f"{question} {title or ''}".lower()
    text = f"{question} {title or ''} {description or ''}".lower()
    question_mentions_winner = "win" in question_text or "winner" in question_text
    title_mentions_winner = "win" in title_text or "winner" in title_text
    parsed = ParsedMarket()
    placeholder_match = re.search(r"\bdriver [a-z]\b", text)
    if placeholder_match:
        parsed.metadata["contains_placeholder"] = "true"
        parsed.confidence = 0.2

    for pattern, session_code in SESSION_PATTERNS.items():
        if pattern in session_text:
            parsed.target_session_code = session_code
            parsed.confidence = max(parsed.confidence, 0.35)
            break

    if parsed.target_session_code is None:
        for pattern, session_code in SESSION_PATTERNS.items():
            if pattern == "qualifying":
                continue
            if pattern in description_text:
                parsed.target_session_code = session_code
                parsed.confidence = max(parsed.confidence, 0.3)
                break

    if "red flag" in question_text or "red flag" in title_text or (
        "red flag" in description_text
        and (
            "will there be a red flag" in description_text
            or ": red flag" in description_text
            or " red flag during " in description_text
        )
    ):
        parsed.taxonomy = "red_flag"
        parsed.confidence = 0.95
        return parsed

    if (
        "safety car" in question_text
        or "safety car" in title_text
        or "virtual safety car" in question_text
        or "virtual safety car" in title_text
        or "safety car" in description_text and "will there be a safety car" in description_text
        or "virtual safety car" in description_text
        or "vsc" in question_text
    ):
        parsed.taxonomy = "safety_car"
        parsed.confidence = 0.9
        return parsed

    if "drivers champion" in text or "drivers' champion" in text or "driver standings" in text:
        parsed.taxonomy = "drivers_champion"
        parsed.confidence = 0.92
        return parsed

    if "constructors champion" in text or "constructor's championship" in text:
        parsed.taxonomy = "constructors_champion"
        parsed.confidence = 0.92
        return parsed

    if (
        " vs " in text
        or " beat " in text
        or " ahead of " in text
        or "finish ahead of" in text
        or "finish ahead" in text
        or "head-to-head" in text
        or "who will finish ahead" in text
    ) and parsed.target_session_code is not None:
        parsed.taxonomy = "head_to_head_session"
        parsed.confidence = 0.82
        parsed.driver_a, parsed.driver_b = _extract_driver_pair(question, text)
        return parsed

    if "pole position" in text and parsed.target_session_code in {"Q", "SQ"}:
        team_name = _match_team_name(text)
        if team_name is not None:
            parsed.taxonomy = "constructor_pole_position"
            parsed.team_name = team_name
            parsed.confidence = 0.84
            return parsed
        driver_name = _match_driver_name(text)
        parsed.taxonomy = "driver_pole_position"
        parsed.driver_a = driver_name
        parsed.confidence = 0.84 if driver_name is not None else 0.72
        return parsed

    if "fastest lap" in text and parsed.target_session_code is not None:
        team_name = _match_team_name(text)
        if team_name is not None:
            parsed.taxonomy = (
                "constructor_fastest_lap_practice"
                if parsed.target_session_code in {"FP1", "FP2", "FP3"}
                else "constructor_fastest_lap_session"
            )
            parsed.team_name = team_name
            parsed.confidence = 0.8
            return parsed

        driver_name = _match_driver_name(text)
        if driver_name is not None:
            parsed.taxonomy = (
                "driver_fastest_lap_practice"
                if parsed.target_session_code in {"FP1", "FP2", "FP3"}
                else "driver_fastest_lap_session"
            )
            parsed.driver_a = driver_name
            parsed.confidence = 0.78
            return parsed

    if (
        question_mentions_winner
        or (not question_text.strip() and title_mentions_winner)
    ) and parsed.target_session_code in {"Q", "SQ"}:
        parsed.taxonomy = "qualifying_winner"
        parsed.confidence = 0.8 if "contains_placeholder" not in parsed.metadata else 0.3
        return parsed

    if (
        question_mentions_winner
        or (not question_text.strip() and title_mentions_winner)
    ) and parsed.target_session_code == "S":
        parsed.taxonomy = "sprint_winner"
        parsed.confidence = 0.8 if "contains_placeholder" not in parsed.metadata else 0.3
        return parsed

    if (
        question_mentions_winner
        or (not question_text.strip() and title_mentions_winner)
    ) and parsed.target_session_code == "R":
        parsed.taxonomy = "race_winner"
        parsed.confidence = 0.82 if "contains_placeholder" not in parsed.metadata else 0.3
        return parsed

    if (
        "constructor scores 1st" in text
        or "which constructor scores 1st" in text
        or "scores 1st" in text
        or "scores the most points" in text
        or "which constructor scores the most points" in text
    ) and parsed.target_session_code == "R":
        parsed.taxonomy = "constructor_scores_first"
        parsed.team_name = _match_team_name(text)
        parsed.confidence = 0.82 if parsed.team_name is not None else 0.74
        return parsed

    if (
        "podium" in primary_text
        or "podium finish" in description_text
        or "finish on the podium" in description_text
        or "finishes on the podium" in description_text
    ) and parsed.target_session_code == "R":
        parsed.taxonomy = "driver_podium"
        parsed.driver_a = _match_driver_name(text)
        parsed.confidence = 0.8 if parsed.driver_a is not None else 0.72
        return parsed

    return parsed
