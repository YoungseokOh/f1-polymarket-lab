from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

LAPS_BEHIND_PATTERN = re.compile(r"^\+\s*(\d+)\s*LAPS?$", re.IGNORECASE)
RACE_LIKE_SESSION_CODES = {"R", "S"}
BEST_LAP_SESSION_CODES = {
    "PRE_QUALIFYING",
    "FP1",
    "FP2",
    "FP3",
    "FP4",
    "Q1",
    "Q2",
    "Q3",
    "Q",
    "SQ",
    "WU",
}


@dataclass(frozen=True, slots=True)
class ParsedGapValue:
    display: str | None
    seconds: float | None
    laps_behind: int | None
    status: str
    segments_json: list[Any] | None = None


@dataclass(frozen=True, slots=True)
class ParsedResultTimeValue:
    display: str | None
    seconds: float | None
    kind: str
    segments_json: list[Any] | None = None


def normalize_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, list):
        numeric_values = [
            item for item in (normalize_float(item) for item in value) if item is not None
        ]
        return min(numeric_values) if numeric_values else None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    return numeric_value if math.isfinite(numeric_value) else None


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        parts = [str(item) for item in value if item not in (None, "")]
        return " | ".join(parts) or None
    text = str(value)
    return text if text != "" else None


def parse_gap_value(
    value: Any,
    *,
    position: int | None = None,
    null_means_leader: bool = False,
    allow_segments: bool = False,
) -> ParsedGapValue:
    display = normalize_text(value)
    if isinstance(value, list):
        segments = list(value)
        return ParsedGapValue(
            display=display,
            seconds=None,
            laps_behind=None,
            status="segment_array" if allow_segments else "unknown",
            segments_json=segments if allow_segments else None,
        )

    if value in (None, ""):
        return ParsedGapValue(
            display=display,
            seconds=None,
            laps_behind=None,
            status="leader" if null_means_leader or position == 1 else "unknown",
        )

    text = str(value).strip()
    laps_match = LAPS_BEHIND_PATTERN.fullmatch(text)
    if laps_match is not None:
        return ParsedGapValue(
            display=text,
            seconds=None,
            laps_behind=int(laps_match.group(1)),
            status="laps_behind",
        )

    seconds = normalize_float(value)
    if seconds is not None:
        if position == 1 and seconds == 0.0:
            return ParsedGapValue(
                display=display,
                seconds=None,
                laps_behind=None,
                status="leader",
            )
        return ParsedGapValue(
            display=display,
            seconds=seconds,
            laps_behind=None,
            status="time",
        )

    return ParsedGapValue(
        display=display,
        seconds=None,
        laps_behind=None,
        status="unknown",
    )


def infer_result_time_kind(
    *,
    session_code: str | None = None,
    session_type: str | None = None,
) -> str:
    normalized_code = (session_code or "").upper()
    normalized_type = (session_type or "").lower()
    if normalized_code in RACE_LIKE_SESSION_CODES or normalized_type in {"race", "sprint"}:
        return "total_time"
    if (
        normalized_code in BEST_LAP_SESSION_CODES
        or "practice" in normalized_type
        or "qualifying" in normalized_type
    ):
        return "best_lap"
    return "unknown"


def parse_result_time_value(
    value: Any,
    *,
    session_code: str | None = None,
    session_type: str | None = None,
) -> ParsedResultTimeValue:
    display = normalize_text(value)
    if isinstance(value, list):
        return ParsedResultTimeValue(
            display=display,
            seconds=None,
            kind="segment_array",
            segments_json=list(value),
        )

    seconds = normalize_float(value)
    if seconds is None:
        return ParsedResultTimeValue(
            display=display,
            seconds=None,
            kind="unknown",
        )

    return ParsedResultTimeValue(
        display=display,
        seconds=seconds,
        kind=infer_result_time_kind(session_code=session_code, session_type=session_type),
    )
