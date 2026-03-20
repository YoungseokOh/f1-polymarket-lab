from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

from f1_polymarket_lab.common import (
    payload_checksum,
    slugify,
    stable_uuid,
    timestamp_date_variants,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    OpenF1LiveConnector,
    PolymarketConnector,
    PolymarketLiveConnector,
    infer_market_scheduled_date,
    parse_market_taxonomy,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.models import (
    DataQualityResult,
    EntityMappingF1ToPolymarket,
    F1Interval,
    F1Lap,
    F1Meeting,
    F1Pit,
    F1Position,
    F1RaceControl,
    F1Session,
    F1SessionResult,
    F1Stint,
    F1TeamRadioMetadata,
    F1TelemetryIndex,
    F1Weather,
    MappingCandidate,
    MarketTaxonomyLabel,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketMarketRule,
    PolymarketMarketStatusHistory,
    PolymarketOpenInterestHistory,
    PolymarketOrderbookSnapshot,
    PolymarketPriceHistory,
    PolymarketResolution,
    PolymarketToken,
    PolymarketTrade,
    PolymarketWsMessageManifest,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import delete, func, or_, select

from f1_polymarket_worker.historical import (
    JOLPICA_DEFAULT_RESOURCES,
    bootstrap_f1db_history,
    sync_jolpica_history,
)
from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    start_job_run,
    upsert_cursor_state,
)
from f1_polymarket_worker.pipeline import (
    MODERN_WEEKEND_SESSION_CODES,
    PipelineContext,
    ensure_default_feature_registry,
    ensure_taxonomy_version,
    hydrate_f1_session,
    hydrate_polymarket_market,
    parse_dt,
    persist_fetch,
    persist_silver,
    reconcile_mappings,
    run_data_quality_checks,
    sync_f1_calendar,
)

DEFAULT_F1_TAG_IDS = ("435", "100389")
DEFAULT_F1_SEARCH_TERMS = (
    "formula 1",
    "grand prix",
    "practice 1",
    "practice 2",
    "practice 3",
    "qualifying",
    "pole position",
    "sprint",
    "head to head",
    "finish ahead",
    "driver podium finish",
    "driver fastest lap",
    "constructor fastest lap",
    "safety car",
    "red flag",
    "constructor scores 1st",
    "drivers champion",
    "constructors champion",
)
HEAVY_MODES = frozenset({"weekend", "none"})
VALIDATION_MODES = frozenset({"smoke", "full"})
SMOKE_VALIDATION_HEAVY_SESSION_CODES = frozenset({"Q", "SQ", "R"})
DEFAULT_OPENF1_TOPICS = (
    "v1/laps",
    "v1/position",
    "v1/intervals",
    "v1/race_control",
    "v1/pit",
    "v1/team_radio",
    "v1/car_data",
    "v1/location",
    "v1/weather",
)
SESSION_DISCOVERY_FAMILY_SLUGS = {
    "FP1": ("practice-1-fastest-lap",),
    "FP2": ("practice-2-fastest-lap",),
    "FP3": ("practice-3-fastest-lap",),
    "Q": ("driver-pole-position", "constructor-pole-position", "pole-winner", "qualifying"),
    "SQ": ("sprint-qualifying-pole-winner",),
    "S": ("sprint-winner", "sprint"),
    "R": (
        "winner",
        "driver-winner",
        "driver-fastest-lap",
        "constructor-fastest-lap",
        "safety-car",
        "red-flag",
        "h2h",
        "head-to-head-matchups",
        "driver-podium",
        "constructor-scores-1st",
    ),
}
SESSION_DISCOVERY_SEARCH_LABELS = {
    "FP1": ("Practice 1 Fastest Lap", "Practice 1"),
    "FP2": ("Practice 2 Fastest Lap", "Practice 2"),
    "FP3": ("Practice 3 Fastest Lap", "Practice 3"),
    "Q": (
        "Driver Pole Position",
        "Constructor Pole Position",
        "Pole Winner",
        "Qualifying",
        "Pole Position",
    ),
    "SQ": ("Sprint Qualifying Pole Winner", "Sprint Shootout Pole Winner"),
    "S": ("Sprint Winner", "Sprint"),
    "R": (
        "Winner",
        "Driver Winner",
        "Driver Fastest Lap",
        "Constructor Fastest Lap",
        "Safety Car",
        "Red Flag",
        "Head-to-Head",
        "Head-to-Head Matchups",
        "Finish Ahead Of",
        "Who Will Finish Ahead",
        "Driver Podium Finish",
        "Constructor Scores 1st",
        "Which Constructor Scores the Most Points",
    ),
}
SESSION_DISCOVERY_ALLOWED_TAXONOMIES = {
    "FP1": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "FP2": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "FP3": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "Q": {"driver_pole_position", "constructor_pole_position", "qualifying_winner"},
    "SQ": {"driver_pole_position", "constructor_pole_position", "qualifying_winner"},
    "S": {"sprint_winner"},
    "R": {
        "race_winner",
        "driver_fastest_lap_session",
        "constructor_fastest_lap_session",
        "safety_car",
        "red_flag",
        "head_to_head_session",
        "driver_podium",
        "constructor_scores_first",
    },
}
SESSION_DISCOVERY_MIN_CANDIDATE_SCORE = 0.65
SESSION_DISCOVERY_AUTO_MAP_SCORE = 0.85
DISCOVERY_SOURCE_PRIORITY = {"public_search": 0, "tag_feed": 1, "exact_slug": 2}
REGULAR_WEEKEND_SESSION_PATTERN = frozenset({"FP1", "FP2", "FP3", "Q", "R"})
SPRINT_WEEKEND_SESSION_PATTERN = frozenset({"FP1", "SQ", "S", "Q", "R"})
VALID_WEEKEND_SESSION_PATTERNS = (
    REGULAR_WEEKEND_SESSION_PATTERN,
    SPRINT_WEEKEND_SESSION_PATTERN,
)
REQUIRED_WEEKEND_MAPPING_CODES = frozenset({"Q", "SQ", "R"})
VALIDATION_ALLOWED_TAXONOMIES = {
    "FP1": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "FP2": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "FP3": {"driver_fastest_lap_practice", "constructor_fastest_lap_practice"},
    "Q": {"driver_pole_position", "constructor_pole_position", "qualifying_winner"},
    "SQ": {"driver_pole_position", "constructor_pole_position", "qualifying_winner"},
    "S": {"sprint_winner"},
    "R": {
        "race_winner",
        "driver_fastest_lap_session",
        "constructor_fastest_lap_session",
        "safety_car",
        "red_flag",
        "head_to_head_session",
        "driver_podium",
        "constructor_scores_first",
    },
}
MARKET_PROBE_SPECS = (
    (
        "pole",
        frozenset({"Q", "SQ"}),
        frozenset({"driver_pole_position", "constructor_pole_position", "qualifying_winner"}),
    ),
    ("race_head_to_head", frozenset({"R"}), frozenset({"head_to_head_session"})),
    ("race_outcome", frozenset({"R"}), frozenset({"race_winner", "driver_podium"})),
)


def _normalize_heavy_mode(heavy_mode: str) -> str:
    normalized = heavy_mode.strip().lower()
    if normalized not in HEAVY_MODES:
        raise ValueError(
            f"Unsupported heavy_mode={heavy_mode!r}; expected one of {sorted(HEAVY_MODES)}"
        )
    return normalized


def _normalize_validation_mode(validation_mode: str) -> str:
    normalized = validation_mode.strip().lower()
    if normalized not in VALIDATION_MODES:
        raise ValueError(
            "Unsupported validation_mode="
            f"{validation_mode!r}; expected one of {sorted(VALIDATION_MODES)}"
        )
    return normalized


def _validation_requires_heavy(*, session_code: str | None, validation_mode: str) -> bool:
    if session_code is None:
        return False
    if validation_mode == "full":
        return True
    return session_code in SMOKE_VALIDATION_HEAVY_SESSION_CODES


def _report_slug_from_meeting(meeting: F1Meeting) -> str:
    return str(slugify(f"{meeting.season}-{meeting.meeting_name}"))


def _validation_report_dir(*, root: Path, season: int, slug: str) -> Path:
    return root / "reports" / "validation" / str(season) / slug


def _event_as_payload(event: PolymarketEvent | None) -> dict[str, Any]:
    if event is None:
        return {}
    return {
        "id": event.id,
        "slug": event.slug,
        "title": event.title,
        "description": event.description,
        "ticker": event.ticker,
        "startDate": None if event.start_at_utc is None else event.start_at_utc.isoformat(),
        "endDate": None if event.end_at_utc is None else event.end_at_utc.isoformat(),
    }


def _market_as_payload(market: PolymarketMarket) -> dict[str, Any]:
    return {
        "id": market.id,
        "slug": market.slug,
        "question": market.question,
        "description": market.description,
        "startDate": None if market.start_at_utc is None else market.start_at_utc.isoformat(),
        "endDate": None if market.end_at_utc is None else market.end_at_utc.isoformat(),
    }


def _render_validation_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# {report['meeting']['season']} {report['meeting']['meeting_name']} Validation",
        "",
        f"- Overall status: `{report['overall_status']}`",
        f"- Meeting key: `{report['meeting']['meeting_key']}`",
        f"- Validation mode: `{report['validation_mode']}`",
        f"- Heavy sessions: `{','.join(report['heavy_session_codes'])}`",
        f"- Session pattern: `{','.join(report['session_pattern'])}`",
        f"- Report generated at: `{report['generated_at']}`",
        "",
        "## Sessions",
    ]
    for session in report["session_inventory"]:
        counts = report["f1_dataset_counts"].get(str(session["session_key"]), {})
        mapping = report["mapping_summary"].get(str(session["session_key"]), {})
        lines.append(
            f"- `{session['session_code']}` `{session['session_key']}` "
            f"results={counts.get('session_results', 0)} "
            f"laps={counts.get('laps', 0)} "
            f"telemetry={counts.get('telemetry_total', 0)} "
            f"candidates={mapping.get('candidate_count', 0)} "
            f"mappings={mapping.get('mapping_count', 0)}"
        )
    lines.extend(["", "## Probes"])
    for probe in report["market_probes"]:
        history = probe.get("history_counts", {})
        lines.append(
            f"- `{probe['probe_key']}` `{probe['market_id']}` `{probe['taxonomy']}` "
            f"price_history={history.get('price_history', 0)} "
            f"trades={history.get('trades', 0)} "
            f"orderbooks={history.get('orderbook_snapshots', 0)}"
        )
    lines.extend(["", "## Research Readiness"])
    for key, value in report["research_readiness"].items():
        lines.append(f"- `{key}`: `{value}`")
    if report["failures"]:
        lines.extend(["", "## Failures"])
        for item in report["failures"]:
            lines.append(f"- {item}")
    if report["warnings"]:
        lines.extend(["", "## Warnings"])
        for item in report["warnings"]:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _count_session_rows(ctx: PipelineContext, session: F1Session) -> dict[str, Any]:
    telemetry_rows = ctx.db.scalars(
        select(F1TelemetryIndex).where(F1TelemetryIndex.session_id == session.id)
    ).all()
    telemetry_by_dataset = Counter(row.dataset_name for row in telemetry_rows)
    weather_count = ctx.db.scalar(
        select(func.count())
        .select_from(F1Weather)
        .where(F1Weather.meeting_id == session.meeting_id)
    ) or 0
    return {
        "session_results": ctx.db.scalar(
            select(func.count())
            .select_from(F1SessionResult)
            .where(F1SessionResult.session_id == session.id)
        )
        or 0,
        "laps": ctx.db.scalar(
            select(func.count()).select_from(F1Lap).where(F1Lap.session_id == session.id)
        )
        or 0,
        "stints": ctx.db.scalar(
            select(func.count()).select_from(F1Stint).where(F1Stint.session_id == session.id)
        )
        or 0,
        "race_control": ctx.db.scalar(
            select(func.count())
            .select_from(F1RaceControl)
            .where(F1RaceControl.session_id == session.id)
        )
        or 0,
        "positions": ctx.db.scalar(
            select(func.count()).select_from(F1Position).where(F1Position.session_id == session.id)
        )
        or 0,
        "intervals": ctx.db.scalar(
            select(func.count()).select_from(F1Interval).where(F1Interval.session_id == session.id)
        )
        or 0,
        "pit": ctx.db.scalar(
            select(func.count()).select_from(F1Pit).where(F1Pit.session_id == session.id)
        )
        or 0,
        "team_radio": ctx.db.scalar(
            select(func.count())
            .select_from(F1TeamRadioMetadata)
            .where(F1TeamRadioMetadata.session_id == session.id)
        )
        or 0,
        "telemetry_total": len(telemetry_rows),
        "telemetry_by_dataset": dict(sorted(telemetry_by_dataset.items())),
        "weather_rows_for_meeting": weather_count,
    }


def _market_history_counts(ctx: PipelineContext, market_id: str) -> dict[str, int]:
    return {
        "price_history": ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketPriceHistory)
            .where(PolymarketPriceHistory.market_id == market_id)
        )
        or 0,
        "trades": ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketTrade)
            .where(PolymarketTrade.market_id == market_id)
        )
        or 0,
        "orderbook_snapshots": ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketOrderbookSnapshot)
            .where(PolymarketOrderbookSnapshot.market_id == market_id)
        )
        or 0,
        "open_interest": ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketOpenInterestHistory)
            .where(PolymarketOpenInterestHistory.market_id == market_id)
        )
        or 0,
        "resolution": ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketResolution)
            .where(PolymarketResolution.market_id == market_id)
        )
        or 0,
    }


def _research_status(*, ok: bool, warning: bool = False) -> str:
    if ok:
        return "ready"
    return "warning" if warning else "fail"


def _select_market_probes(
    *,
    sessions: list[F1Session],
    mappings: list[EntityMappingF1ToPolymarket],
    candidates: list[MappingCandidate],
    markets_by_id: dict[str, PolymarketMarket],
) -> list[dict[str, Any]]:
    sessions_by_id = {session.id: session for session in sessions}
    probes: list[dict[str, Any]] = []

    for probe_key, session_codes, taxonomies in MARKET_PROBE_SPECS:
        selected: dict[str, Any] | None = None
        for source_name, rows in (("mapping", mappings), ("candidate", candidates)):
            options: list[dict[str, Any]] = []
            for row in rows:
                session = sessions_by_id.get(row.f1_session_id or "")
                market = markets_by_id.get(row.polymarket_market_id or "")
                if session is None or market is None:
                    continue
                if session.session_code not in session_codes:
                    continue
                if market.taxonomy not in taxonomies:
                    continue
                options.append(
                    {
                        "confidence": float(row.confidence or 0.0),
                        "session": session,
                        "market": market,
                        "source": source_name,
                    }
                )
            if options:
                options.sort(
                    key=lambda item: (
                        item["confidence"],
                        _ensure_utc(item["session"].date_start_utc)
                        if item["session"].date_start_utc is not None
                        else datetime.min.replace(tzinfo=timezone.utc),
                    ),
                    reverse=True,
                )
                selected = options[0]
                break
        if selected is None:
            continue
        session = cast(F1Session, selected["session"])
        market = cast(PolymarketMarket, selected["market"])
        probes.append(
            {
                "probe_key": probe_key,
                "session_key": session.session_key,
                "session_code": session.session_code,
                "market_id": market.id,
                "taxonomy": market.taxonomy,
                "question": market.question,
                "slug": market.slug,
                "confidence": selected["confidence"],
                "selected_from": selected["source"],
            }
        )

    deduped: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for probe in probes:
        if probe["market_id"] in seen_market_ids:
            continue
        seen_market_ids.add(probe["market_id"])
        deduped.append(probe)
    return deduped


def _safe_json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return list(value)
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return list(payload) if isinstance(payload, list) else []


def _event_year(event: dict[str, Any]) -> int | None:
    timestamp = parse_dt(event.get("startDate") or event.get("endDate"))
    return None if timestamp is None else timestamp.year


def _event_in_scope(event: dict[str, Any], *, start_year: int, end_year: int) -> bool:
    year = _event_year(event)
    return year is None or (start_year <= year <= end_year)


def _event_looks_f1(event: dict[str, Any]) -> bool:
    tags = event.get("tags") or []
    labels = {
        str(tag.get("label", "")).strip().lower()
        for tag in tags
        if isinstance(tag, dict)
    }
    slugs = {
        str(tag.get("slug", "")).strip().lower()
        for tag in tags
        if isinstance(tag, dict)
    }
    haystack = " ".join(
        str(value or "")
        for value in [
            event.get("title"),
            event.get("slug"),
            event.get("ticker"),
        ]
    ).lower()
    return (
        "formula 1" in labels
        or "f1" in labels
        or "formula1" in slugs
        or "f1" in slugs
        or "formula 1" in haystack
        or " f1 " in f" {haystack} "
        or "grand prix" in haystack
    )


def _catalog_search_terms(
    ctx: PipelineContext,
    *,
    start_year: int,
    end_year: int,
) -> tuple[str, ...]:
    terms: list[str] = []
    for year in range(start_year, end_year + 1):
        terms.extend(
            (
                f"formula 1 {year}",
                f"formula 1 {year} grand prix",
                f"f1 {year} grand prix",
            )
        )

    meetings = ctx.db.scalars(
        select(F1Meeting)
        .where(F1Meeting.season >= start_year, F1Meeting.season <= end_year)
        .order_by(F1Meeting.season.asc(), F1Meeting.meeting_key.asc())
    ).all()
    for meeting in meetings:
        if meeting.meeting_name:
            terms.extend(
                (
                    f"{meeting.meeting_name} {meeting.season}",
                    f"formula 1 {meeting.season} {meeting.meeting_name}",
                    f"f1 {meeting.season} {meeting.meeting_name}",
                )
            )
        if meeting.country_name:
            terms.extend(
                (
                    f"{meeting.country_name} grand prix {meeting.season}",
                    f"formula 1 {meeting.country_name} grand prix {meeting.season}",
                    f"f1 {meeting.country_name} grand prix {meeting.season}",
                )
            )

    terms.extend(DEFAULT_F1_SEARCH_TERMS)
    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        normalized = term.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(term)
    return tuple(deduped)


def _ensure_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _normalize_event_row(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(event["id"]),
        "ticker": event.get("ticker"),
        "slug": event.get("slug") or str(event["id"]),
        "title": event.get("title") or event.get("slug") or str(event["id"]),
        "description": event.get("description"),
        "category": event.get("category"),
        "subcategory": event.get("subcategory"),
        "start_at_utc": parse_dt(event.get("startDate")),
        "end_at_utc": parse_dt(event.get("endDate")),
        "active": bool(event.get("active")),
        "closed": bool(event.get("closed")),
        "archived": bool(event.get("archived")),
        "liquidity": event.get("liquidity"),
        "volume": event.get("volume"),
        "open_interest": event.get("openInterest"),
        "resolution_source": event.get("resolutionSource"),
        "raw_payload": event,
    }


def _unique_strings(values: list[str]) -> tuple[str, ...]:
    ordered: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in ordered:
            continue
        ordered.append(cleaned)
    return tuple(ordered)


def _session_meeting(ctx: PipelineContext, session: F1Session) -> F1Meeting | None:
    return None if session.meeting_id is None else ctx.db.get(F1Meeting, session.meeting_id)


def _session_dates(session: F1Session) -> tuple[date, ...]:
    raw_payload = session.raw_payload or {}
    offset = raw_payload.get("gmt_offset")
    offset_text = str(offset) if isinstance(offset, str) else None
    return cast(
        tuple[date, ...],
        timestamp_date_variants(session.date_start_utc, gmt_offset=offset_text),
    )


def _session_venue_variants(session: F1Session, meeting: F1Meeting | None) -> tuple[str, ...]:
    raw_candidates: list[str] = []
    if meeting is not None:
        if meeting.meeting_name:
            raw_candidates.append(meeting.meeting_name)
        if meeting.country_name:
            raw_candidates.append(f"{meeting.country_name} Grand Prix")
        if meeting.location:
            raw_candidates.append(f"{meeting.location} Grand Prix")
    raw_payload = session.raw_payload or {}
    for key in ("meeting_name", "country_name", "location"):
        value = raw_payload.get(key)
        if isinstance(value, str) and value:
            raw_candidates.append(value)
            if key != "meeting_name":
                raw_candidates.append(f"{value} Grand Prix")
    return _unique_strings([slugify(candidate) for candidate in raw_candidates])


def _session_family_slugs(session: F1Session) -> tuple[str, ...]:
    return SESSION_DISCOVERY_FAMILY_SLUGS.get(session.session_code or "", ())


def _session_search_terms(session: F1Session, meeting: F1Meeting | None) -> tuple[str, ...]:
    year = (
        _ensure_utc(session.date_start_utc).year
        if session.date_start_utc is not None
        else utc_now().year
    )
    base_names: list[str] = []
    if meeting is not None:
        if meeting.meeting_name:
            base_names.append(meeting.meeting_name)
        if meeting.country_name:
            base_names.append(f"{meeting.country_name} Grand Prix")
        if meeting.location:
            base_names.append(f"{meeting.location} Grand Prix")
    if session.session_name:
        base_names.extend(
            [
                session.session_name,
                f"{session.session_name} {year}",
            ]
        )
    date_labels = [candidate.isoformat() for candidate in _session_dates(session)]
    family_labels = SESSION_DISCOVERY_SEARCH_LABELS.get(
        session.session_code or "",
        (session.session_name,),
    )
    queries: list[str] = []
    for base_name in _unique_strings(base_names):
        for label in family_labels:
            queries.append(f"{base_name} {label} {year}")
        if session.session_name:
            queries.append(f"{base_name} {session.session_name} {year}")
        for date_label in date_labels:
            queries.append(f"{base_name} {date_label}")
    return _unique_strings(queries)


def _session_slug_candidates(session: F1Session, meeting: F1Meeting | None) -> tuple[str, ...]:
    venue_variants = _session_venue_variants(session, meeting)
    family_slugs = _session_family_slugs(session)
    date_labels = [candidate.isoformat() for candidate in _session_dates(session)]
    slugs: list[str] = []
    for venue in venue_variants:
        for family_slug in family_slugs:
            for date_label in date_labels:
                slugs.extend(
                    [
                        f"f1-{venue}-{family_slug}-{date_label}",
                        f"{venue}-{family_slug}-{date_label}",
                    ]
                )
            slugs.extend([f"f1-{venue}-{family_slug}", f"{venue}-{family_slug}"])
    return _unique_strings(slugs)


def _market_scheduled_date(
    *,
    event: dict[str, Any],
    market: dict[str, Any],
) -> date | None:
    return cast(
        date | None,
        infer_market_scheduled_date(
            market.get("slug"),
            market.get("question"),
            market.get("description"),
            event.get("slug"),
            event.get("title"),
            event.get("description"),
            event.get("ticker"),
        ),
    )


def _market_session_delta_days(
    *,
    session: F1Session,
    event: dict[str, Any],
    market: dict[str, Any],
    market_start_at: datetime | None = None,
) -> float | None:
    scheduled_date = _market_scheduled_date(event=event, market=market)
    session_dates = _session_dates(session)
    if scheduled_date is not None and session_dates:
        return float(
            min(
                abs((scheduled_date - candidate_date).days)
                for candidate_date in session_dates
            )
        )
    if session.date_start_utc is None:
        return None
    market_start = parse_dt(market.get("startDate")) or market_start_at
    if market_start is None:
        return None
    delta = _ensure_utc(market_start) - _ensure_utc(session.date_start_utc)
    return abs(delta.total_seconds()) / 86400


def _market_search_haystack(event: dict[str, Any], market: dict[str, Any]) -> str:
    return str(
        slugify(
        " ".join(
            str(value or "")
            for value in [
                event.get("slug"),
                event.get("title"),
                event.get("ticker"),
                market.get("slug"),
                market.get("question"),
            ]
        )
    )
    )


def _score_session_market(
    *,
    session: F1Session,
    meeting: F1Meeting | None,
    event: dict[str, Any],
    market: dict[str, Any],
    source: str,
    matched_slug: str | None,
) -> tuple[float, Any, dict[str, Any]]:
    parsed = parse_market_taxonomy(
        market.get("question") or "",
        " ".join(
            str(value or "")
            for value in [
                market.get("description"),
                event.get("description"),
            ]
            if value
        )
        or None,
        title=event.get("title"),
    )
    if parsed.target_session_code != session.session_code or parsed.taxonomy == "other":
        return 0.0, parsed, {}
    haystack = _market_search_haystack(event, market)
    scheduled_date = _market_scheduled_date(event=event, market=market)
    session_dates = _session_dates(session)
    date_delta = None
    if scheduled_date is not None and session_dates:
        date_delta = min(
            abs((scheduled_date - candidate_date).days)
            for candidate_date in session_dates
        )
    has_close_date = bool(date_delta is not None and date_delta <= 3)
    session_year = (
        _ensure_utc(session.date_start_utc).year
        if session.date_start_utc is not None
        else None
    )
    year_match = bool(
        session_year is not None
        and (
            (scheduled_date is not None and scheduled_date.year == session_year)
            or str(session_year) in haystack
        )
    )
    rationale = {
        "source": source,
        "matched_slug": matched_slug,
        "year_match": year_match,
        "date_delta_days": date_delta,
        "scheduled_date": None if scheduled_date is None else scheduled_date.isoformat(),
    }
    if not year_match and not has_close_date:
        return 0.0, parsed, rationale
    if source == "exact_slug":
        return 1.0, parsed, rationale

    venue_variants = _session_venue_variants(session, meeting)
    family_slugs = _session_family_slugs(session)
    venue_match = any(variant in haystack for variant in venue_variants)
    allowed_taxonomies = SESSION_DISCOVERY_ALLOWED_TAXONOMIES.get(
        session.session_code or "",
        set(),
    )
    family_match = (
        parsed.taxonomy in allowed_taxonomies
        or any(family_slug in haystack for family_slug in family_slugs)
    )

    score = 0.25
    if venue_match:
        score += 0.30
    if family_match:
        score += 0.15
    if year_match:
        score += 0.10
    if date_delta is not None and has_close_date:
        score += 0.20 * (1.0 - (date_delta / 3.0))
    rationale["venue_match"] = venue_match
    rationale["family_match"] = family_match
    return min(score, 1.0), parsed, rationale


def _event_matches_session(
    *,
    session: F1Session,
    meeting: F1Meeting | None,
    event: dict[str, Any],
    source: str,
    matched_slug: str | None = None,
) -> bool:
    if source == "exact_slug":
        return True
    for market in event.get("markets", []):
        score, _parsed, _rationale = _score_session_market(
            session=session,
            meeting=meeting,
            event=event,
            market=market,
            source=source,
            matched_slug=matched_slug,
        )
        if score >= SESSION_DISCOVERY_MIN_CANDIDATE_SCORE:
            return True
    return False


def _record_discovered_event(
    discovered_events: dict[str, dict[str, Any]],
    discovery_meta: dict[str, dict[str, Any]],
    *,
    event: dict[str, Any],
    source: str,
    matched_slug: str | None = None,
) -> None:
    event_id = str(event["id"])
    prior = discovery_meta.get(event_id)
    if (
        prior is not None
        and DISCOVERY_SOURCE_PRIORITY[source]
        <= DISCOVERY_SOURCE_PRIORITY[prior["source"]]
    ):
        return
    discovered_events[event_id] = event
    discovery_meta[event_id] = {"source": source, "matched_slug": matched_slug}


def _build_market_records(
    events: list[dict[str, Any]],
    *,
    taxonomy_version_id: str,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    event_rows: dict[str, dict[str, Any]] = {}
    market_rows: dict[str, dict[str, Any]] = {}
    token_rows: dict[str, dict[str, Any]] = {}
    rule_rows: dict[str, dict[str, Any]] = {}
    status_rows: dict[str, dict[str, Any]] = {}
    label_rows: dict[str, dict[str, Any]] = {}
    observed_at = utc_now()

    for event in events:
        event_id = str(event["id"])
        event_rows[event_id] = _normalize_event_row(event)
        for market in event.get("markets", []):
            market_id = str(market["id"])
            parsed = parse_market_taxonomy(
                market.get("question") or "",
                " ".join(
                    str(value or "")
                    for value in [
                        market.get("description"),
                        event.get("description"),
                    ]
                    if value
                )
                or None,
                title=event.get("title"),
            )
            token_ids = _safe_json_list(market.get("clobTokenIds"))
            outcomes = _safe_json_list(market.get("outcomes"))
            prices = _safe_json_list(market.get("outcomePrices"))
            best_bid = float(market["bestBid"]) if market.get("bestBid") is not None else None
            best_ask = float(market["bestAsk"]) if market.get("bestAsk") is not None else None
            market_rows[market_id] = {
                "id": market_id,
                "event_id": event_id,
                "question": market.get("question") or "",
                "slug": market.get("slug"),
                "condition_id": market["conditionId"],
                "question_id": market.get("questionID"),
                "market_type": market.get("marketType"),
                "sports_market_type": market.get("sportsMarketType"),
                "taxonomy": parsed.taxonomy,
                "taxonomy_confidence": parsed.confidence,
                "target_session_code": parsed.target_session_code,
                "driver_a": parsed.driver_a,
                "driver_b": parsed.driver_b,
                "team_name": parsed.team_name,
                "resolution_source": market.get("resolutionSource"),
                "rules_text": market.get("description"),
                "description": market.get("description"),
                "start_at_utc": parse_dt(market.get("startDate")),
                "end_at_utc": parse_dt(market.get("endDate")),
                "accepting_orders": bool(market.get("acceptingOrders")),
                "active": bool(market.get("active")),
                "closed": bool(market.get("closed")),
                "archived": bool(market.get("archived")),
                "enable_order_book": bool(market.get("enableOrderBook")),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": (
                    best_ask - best_bid if best_ask is not None and best_bid is not None else None
                ),
                "last_trade_price": (
                    float(market["lastTradePrice"])
                    if market.get("lastTradePrice") is not None
                    else None
                ),
                "volume": market.get("volumeNum"),
                "liquidity": market.get("liquidityNum"),
                "clob_token_ids": token_ids,
                "raw_payload": market,
            }
            rule_rows[market_id] = {
                "id": f"rule:{market_id}",
                "market_id": market_id,
                "rules_text": market.get("description"),
                "resolution_text": market.get("resolutionSource"),
                "parsed_metadata": {
                    "taxonomy": parsed.taxonomy,
                    "target_session_code": parsed.target_session_code,
                    "driver_a": parsed.driver_a,
                    "driver_b": parsed.driver_b,
                    "team_name": parsed.team_name,
                }
                | parsed.metadata,
                "raw_payload": market,
            }
            status_rows[market_id] = {
                "id": f"{market_id}:{observed_at.strftime('%Y%m%dT%H%M%S')}",
                "market_id": market_id,
                "observed_at_utc": observed_at,
                "active": bool(market.get("active")),
                "closed": bool(market.get("closed")),
                "archived": bool(market.get("archived")),
                "accepting_orders": bool(market.get("acceptingOrders")),
                "raw_payload": market,
            }
            label_rows[market_id] = {
                "id": stable_uuid("market-taxonomy-label", market_id, taxonomy_version_id),
                "market_id": market_id,
                "taxonomy_version_id": taxonomy_version_id,
                "taxonomy": parsed.taxonomy,
                "confidence": parsed.confidence,
                "label_status": "candidate",
                "target_session_code": parsed.target_session_code,
                "parsed_metadata": parsed.metadata
                | {
                    "driver_a": parsed.driver_a or "",
                    "driver_b": parsed.driver_b or "",
                    "team_name": parsed.team_name or "",
                },
                "created_at": observed_at,
            }
            for index, token_id in enumerate(token_ids):
                token_rows[str(token_id)] = {
                    "id": str(token_id),
                    "market_id": market_id,
                    "outcome": outcomes[index] if index < len(outcomes) else None,
                    "outcome_index": index,
                    "latest_price": float(prices[index]) if index < len(prices) else None,
                    "raw_payload": {"token_id": token_id},
                }
    return event_rows, market_rows, token_rows, rule_rows, status_rows, label_rows


def sync_polymarket_f1_catalog(
    ctx: PipelineContext,
    *,
    batch_size: int = 100,
    max_pages: int = 20,
    search_fallback: bool = True,
    start_year: int = 2022,
    end_year: int | None = None,
    tag_ids: tuple[str, ...] = DEFAULT_F1_TAG_IDS,
) -> dict[str, Any]:
    end_year = end_year or utc_now().year
    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-polymarket-f1-catalog",
        source="polymarket",
        dataset="f1_catalog",
        description="Sync F1-tagged Polymarket events, markets, tokens, and rule metadata.",
        default_cursor={"tag_ids": list(tag_ids), "start_year": start_year, "end_year": end_year},
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="f1_catalog",
        cursor_key="events",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "tag_ids": list(tag_ids),
            "batch_size": batch_size,
            "max_pages": max_pages,
            "search_fallback": search_fallback,
            "start_year": start_year,
            "end_year": end_year,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned"}

    connector = PolymarketConnector()
    taxonomy_version = ensure_taxonomy_version(ctx)
    discovered_events: dict[str, dict[str, Any]] = {}

    for tag_id in tag_ids:
        for offset, batch in connector.iterate_events(
            batch_size=batch_size,
            max_pages=max_pages,
            tag_id=tag_id,
        ):
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="polymarket",
                    dataset="f1_events_by_tag",
                    endpoint="/events",
                    params={"tag_id": tag_id, "limit": batch_size, "offset": offset},
                    payload=batch,
                    response_status=200,
                    checkpoint=f"{tag_id}:{offset}",
                ),
                partition={"tag_id": str(tag_id), "offset": str(offset)},
            )
            for event in batch:
                if _event_in_scope(
                    event, start_year=start_year, end_year=end_year
                ) and _event_looks_f1(event):
                    discovered_events[str(event["id"])] = event

    if search_fallback:
        slug_checkpoints: set[str] = set()
        for term in _catalog_search_terms(ctx, start_year=start_year, end_year=end_year):
            try:
                payload = connector.public_search(term)
            except Exception:
                continue
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="polymarket",
                    dataset="f1_public_search",
                    endpoint="/public-search",
                    params={"q": term},
                    payload=payload,
                    response_status=200,
                    checkpoint=term,
                ),
                partition={"query": slugify(term)},
            )
            for event in payload.get("events", []):
                candidates = [event]
                slug = event.get("slug")
                if (
                    not event.get("markets")
                    and isinstance(slug, str)
                    and slug
                    and slug not in slug_checkpoints
                ):
                    canonical_events = connector.list_events(limit=5, slug=slug)
                    persist_fetch(
                        ctx,
                        job_run_id=run.id,
                        batch=FetchBatch(
                            source="polymarket",
                            dataset="f1_event_by_slug",
                            endpoint="/events",
                            params={"slug": slug, "limit": 5},
                            payload=canonical_events,
                            response_status=200,
                            checkpoint=slug,
                        ),
                        partition={"slug": slugify(slug)},
                    )
                    slug_checkpoints.add(slug)
                    if canonical_events:
                        candidates = canonical_events
                for candidate in candidates:
                    if _event_in_scope(
                        candidate, start_year=start_year, end_year=end_year
                    ) and _event_looks_f1(candidate):
                        discovered_events[str(candidate["id"])] = candidate

    (
        event_rows,
        market_rows,
        token_rows,
        rule_rows,
        status_rows,
        label_rows,
    ) = _build_market_records(
        list(discovered_events.values()),
        taxonomy_version_id=taxonomy_version.id,
    )
    upsert_records(ctx.db, PolymarketEvent, event_rows.values())
    upsert_records(ctx.db, PolymarketMarket, market_rows.values())
    upsert_records(ctx.db, PolymarketToken, token_rows.values())
    upsert_records(ctx.db, PolymarketMarketRule, rule_rows.values())
    upsert_records(ctx.db, PolymarketMarketStatusHistory, status_rows.values())
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows.values())
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_events",
        records=list(event_rows.values()),
        partition={"scope": "f1"},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_markets",
        records=list(market_rows.values()),
        partition={"scope": "f1"},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_tokens",
        records=list(token_rows.values()),
        partition={"scope": "f1"},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_market_rules",
        records=list(rule_rows.values()),
        partition={"scope": "f1"},
    )
    upsert_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="f1_catalog",
        cursor_key="events",
        cursor_value={
            "tag_ids": list(tag_ids),
            "start_year": start_year,
            "end_year": end_year,
            "event_count": len(event_rows),
            "synced_at": utc_now().isoformat(),
        },
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={
            "tag_ids": list(tag_ids),
            "event_count": len(event_rows),
            "synced_at": utc_now().isoformat(),
        },
        records_written=len(market_rows) + len(token_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "events": len(event_rows),
        "markets": len(market_rows),
        "tokens": len(token_rows),
    }


def discover_session_polymarket(
    ctx: PipelineContext,
    *,
    session_key: int,
    batch_size: int = 100,
    max_pages: int = 5,
    search_fallback: bool = True,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="discover-session-polymarket",
        source="polymarket",
        dataset="session_market_discovery",
        description="Discover Polymarket events and markets aligned to a single F1 session.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "session_key": session_key,
            "batch_size": batch_size,
            "max_pages": max_pages,
            "search_fallback": search_fallback,
        },
    )
    session = ctx.db.scalar(select(F1Session).where(F1Session.session_key == session_key))
    if session is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"session_key={session_key} not found",
        )
        raise ValueError(f"session_key={session_key} not found")
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "session_key": session_key}

    meeting = _session_meeting(ctx, session)
    connector = PolymarketConnector()
    taxonomy_version = ensure_taxonomy_version(ctx)
    discovered_events: dict[str, dict[str, Any]] = {}
    discovery_meta: dict[str, dict[str, Any]] = {}

    for slug_candidate in _session_slug_candidates(session, meeting):
        payload = connector.list_events(limit=5, slug=slug_candidate)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="polymarket",
                dataset="session_event_by_slug",
                endpoint="/events",
                params={"slug": slug_candidate, "limit": 5},
                payload=payload,
                response_status=200,
                checkpoint=slug_candidate,
            ),
            partition={"session_key": str(session_key), "slug": slug_candidate},
        )
        for event in payload:
            if not _event_looks_f1(event):
                continue
            _record_discovered_event(
                discovered_events,
                discovery_meta,
                event=event,
                source="exact_slug",
                matched_slug=slug_candidate,
            )

    for tag_id in DEFAULT_F1_TAG_IDS:
        for offset, batch in connector.iterate_events(
            batch_size=batch_size,
            max_pages=max_pages,
            tag_id=tag_id,
        ):
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="polymarket",
                    dataset="session_events_by_tag",
                    endpoint="/events",
                    params={"tag_id": tag_id, "limit": batch_size, "offset": offset},
                    payload=batch,
                    response_status=200,
                    checkpoint=f"{tag_id}:{offset}",
                ),
                partition={
                    "session_key": str(session_key),
                    "tag_id": str(tag_id),
                    "offset": str(offset),
                },
            )
            for event in batch:
                if not _event_looks_f1(event):
                    continue
                if not _event_matches_session(
                    session=session,
                    meeting=meeting,
                    event=event,
                    source="tag_feed",
                ):
                    continue
                _record_discovered_event(
                    discovered_events,
                    discovery_meta,
                    event=event,
                    source="tag_feed",
                )

    if search_fallback:
        for query in _session_search_terms(session, meeting):
            try:
                payload = connector.public_search(query)
            except Exception:
                continue
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="polymarket",
                    dataset="session_public_search",
                    endpoint="/public-search",
                    params={"q": query},
                    payload=payload,
                    response_status=200,
                    checkpoint=query,
                ),
                partition={"session_key": str(session_key), "query": slugify(query)},
            )
            for event in payload.get("events", []):
                if not _event_looks_f1(event):
                    continue
                if not _event_matches_session(
                    session=session,
                    meeting=meeting,
                    event=event,
                    source="public_search",
                ):
                    continue
                _record_discovered_event(
                    discovered_events,
                    discovery_meta,
                    event=event,
                    source="public_search",
                )

    (
        event_rows,
        market_rows,
        token_rows,
        rule_rows,
        status_rows,
        label_rows,
    ) = _build_market_records(
        list(discovered_events.values()),
        taxonomy_version_id=taxonomy_version.id,
    )
    upsert_records(ctx.db, PolymarketEvent, event_rows.values())
    upsert_records(ctx.db, PolymarketMarket, market_rows.values())
    upsert_records(ctx.db, PolymarketToken, token_rows.values())
    upsert_records(ctx.db, PolymarketMarketRule, rule_rows.values())
    upsert_records(ctx.db, PolymarketMarketStatusHistory, status_rows.values())
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows.values())
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_events",
        records=list(event_rows.values()),
        partition={"scope": "session_discovery", "session_key": str(session_key)},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_markets",
        records=list(market_rows.values()),
        partition={"scope": "session_discovery", "session_key": str(session_key)},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_tokens",
        records=list(token_rows.values()),
        partition={"scope": "session_discovery", "session_key": str(session_key)},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="polymarket_market_rules",
        records=list(rule_rows.values()),
        partition={"scope": "session_discovery", "session_key": str(session_key)},
    )

    candidate_rows: list[dict[str, Any]] = []
    mapping_rows: list[dict[str, Any]] = []
    for event in discovered_events.values():
        event_id = str(event["id"])
        source_meta = discovery_meta[event_id]
        source = str(source_meta["source"])
        matched_slug = source_meta.get("matched_slug")
        for market in event.get("markets", []):
            score, parsed, rationale = _score_session_market(
                session=session,
                meeting=meeting,
                event=event,
                market=market,
                source=source,
                matched_slug=None if matched_slug is None else str(matched_slug),
            )
            if score < SESSION_DISCOVERY_MIN_CANDIDATE_SCORE:
                continue
            candidate_id = f"{market['id']}:{session.id}"
            rationale_payload = rationale | {
                "market_taxonomy": parsed.taxonomy,
                "session_code": session.session_code,
                "confidence": round(score, 4),
            }
            candidate_rows.append(
                {
                    "id": candidate_id,
                    "f1_meeting_id": session.meeting_id,
                    "f1_session_id": session.id,
                    "polymarket_event_id": event_id,
                    "polymarket_market_id": str(market["id"]),
                    "candidate_type": parsed.taxonomy,
                    "confidence": score,
                    "matched_by": f"session_discovery_{source}",
                    "rationale_json": rationale_payload,
                    "status": "candidate",
                    "created_at": utc_now(),
                }
            )
            if score >= SESSION_DISCOVERY_AUTO_MAP_SCORE:
                mapping_rows.append(
                    {
                        "id": candidate_id,
                        "f1_meeting_id": session.meeting_id,
                        "f1_session_id": session.id,
                        "polymarket_event_id": event_id,
                        "polymarket_market_id": str(market["id"]),
                        "mapping_type": parsed.taxonomy,
                        "confidence": score,
                        "matched_by": f"session_discovery_{source}",
                        "notes": json.dumps(rationale_payload, sort_keys=True),
                        "override_flag": False,
                    }
                )

    if candidate_rows:
        upsert_records(ctx.db, MappingCandidate, candidate_rows)
    if mapping_rows:
        upsert_records(ctx.db, EntityMappingF1ToPolymarket, mapping_rows)

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(market_rows) + len(candidate_rows) + len(mapping_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "session_key": session_key,
        "events": len(event_rows),
        "markets": len(market_rows),
        "mapping_candidates": len(candidate_rows),
        "auto_mappings": len(mapping_rows),
    }


def _session_has_linked_markets(ctx: PipelineContext, session: F1Session) -> bool:
    if ctx.db.scalar(
        select(EntityMappingF1ToPolymarket).where(
            EntityMappingF1ToPolymarket.f1_session_id == session.id
        )
    ):
        return True
    if session.session_code is None or session.date_start_utc is None:
        return False
    market_rows = ctx.db.scalars(
        select(PolymarketMarket).where(
            PolymarketMarket.target_session_code == session.session_code,
            PolymarketMarket.taxonomy_confidence.is_not(None),
        )
    ).all()
    for market in market_rows:
        if market.taxonomy_confidence is not None and market.taxonomy_confidence < 0.6:
            continue
        event_payload: dict[str, Any] = {}
        if market.event_id is not None:
            event = ctx.db.get(PolymarketEvent, market.event_id)
            if event is not None and event.raw_payload is not None:
                event_payload = event.raw_payload
        delta_days = _market_session_delta_days(
            session=session,
            event=event_payload,
            market=market.raw_payload or {},
            market_start_at=market.start_at_utc,
        )
        if delta_days is not None and delta_days <= 3:
            return True
    return False


def backfill_f1_history(
    ctx: PipelineContext,
    *,
    season_start: int = 2023,
    season_end: int | None = None,
    include_extended: bool = True,
    heavy_mode: str = "weekend",
) -> dict[str, Any]:
    season_end = season_end or utc_now().year
    heavy_mode = _normalize_heavy_mode(heavy_mode)
    definition = ensure_job_definition(
        ctx.db,
        job_name="backfill-f1-history",
        source="openf1",
        dataset="f1_history",
        description="Backfill F1 calendar and session datasets for multiple seasons.",
        default_cursor={"season_start": season_start, "season_end": season_end},
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_history",
        cursor_key=f"{season_start}:{season_end}",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season_start": season_start,
            "season_end": season_end,
            "include_extended": include_extended,
            "heavy_mode": heavy_mode,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned"}

    ensure_default_feature_registry(ctx)
    sessions_hydrated = 0
    sessions_skipped = 0
    historical_cutoff = utc_now() - timedelta(minutes=30)
    for season in range(season_start, season_end + 1):
        sync_f1_calendar(ctx, season=season)
        ctx.db.commit()
        season_sessions = ctx.db.scalars(
            select(F1Session)
            .join(F1Meeting, F1Meeting.id == F1Session.meeting_id)
            .where(F1Meeting.season == season)
            .where(F1Session.session_code.in_(tuple(MODERN_WEEKEND_SESSION_CODES)))
            .order_by(F1Session.date_start_utc.asc())
        ).all()
        for session in season_sessions:
            if (
                session.date_end_utc is None
                or _ensure_utc(session.date_end_utc) > historical_cutoff
            ):
                sessions_skipped += 1
                continue
            include_heavy = heavy_mode == "weekend"
            hydrate_f1_session(
                ctx,
                session_key=session.session_key,
                include_extended=include_extended,
                include_heavy=include_heavy,
            )
            sessions_hydrated += 1
            ctx.db.commit()

    upsert_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_history",
        cursor_key=f"{season_start}:{season_end}",
        cursor_value={
            "season_start": season_start,
            "season_end": season_end,
            "sessions_hydrated": sessions_hydrated,
            "sessions_skipped": sessions_skipped,
            "synced_at": utc_now().isoformat(),
        },
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={
            "season_start": season_start,
            "season_end": season_end,
            "sessions_hydrated": sessions_hydrated,
            "sessions_skipped": sessions_skipped,
            "synced_at": utc_now().isoformat(),
        },
        records_written=sessions_hydrated,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "seasons": season_end - season_start + 1,
        "sessions_hydrated": sessions_hydrated,
        "sessions_skipped": sessions_skipped,
    }


def backfill_f1_history_all(
    ctx: PipelineContext,
    *,
    season_start: int = 1950,
    season_end: int | None = None,
    include_extended: bool = True,
    heavy_mode: str = "weekend",
    jolpica_resources: tuple[str, ...] = JOLPICA_DEFAULT_RESOURCES,
) -> dict[str, Any]:
    season_end = season_end or utc_now().year
    heavy_mode = _normalize_heavy_mode(heavy_mode)
    definition = ensure_job_definition(
        ctx.db,
        job_name="backfill-f1-history-all",
        source="hybrid",
        dataset="f1_history_all",
        description="Backfill 1950+ F1 history using F1DB, Jolpica, and OpenF1 by era.",
        default_cursor={"season_start": season_start, "season_end": season_end},
        schedule_hint="manual",
    )
    cursor_key = f"{season_start}:{season_end}"
    cursor_state = get_cursor_state(
        ctx.db,
        source="hybrid",
        dataset="f1_history_all",
        cursor_key=cursor_key,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season_start": season_start,
            "season_end": season_end,
            "include_extended": include_extended,
            "heavy_mode": heavy_mode,
            "jolpica_resources": list(jolpica_resources),
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned"}

    historical_end = min(season_end, 2022)
    openf1_start = max(season_start, 2023)
    records_written = 0
    child_runs: dict[str, Any] = {}

    if season_start <= historical_end:
        f1db_result = bootstrap_f1db_history(
            ctx,
            season_start=season_start,
            season_end=historical_end,
            artifact="sqlite",
        )
        jolpica_result = sync_jolpica_history(
            ctx,
            season_start=season_start,
            season_end=historical_end,
            resources=jolpica_resources,
        )
        child_runs["f1db"] = f1db_result
        child_runs["jolpica"] = jolpica_result
        records_written += int(f1db_result.get("session_results", 0))
        records_written += int(jolpica_result.get("session_results", 0))
        records_written += int(jolpica_result.get("lap_rows", 0))
        records_written += int(jolpica_result.get("pit_rows", 0))

    if openf1_start <= season_end:
        openf1_result = backfill_f1_history(
            ctx,
            season_start=openf1_start,
            season_end=season_end,
            include_extended=include_extended,
            heavy_mode=heavy_mode,
        )
        child_runs["openf1"] = openf1_result
        records_written += int(openf1_result.get("sessions_hydrated", 0))

    cursor_after = {
        "season_start": season_start,
        "season_end": season_end,
        "synced_at": utc_now().isoformat(),
        "child_runs": child_runs,
    }
    upsert_cursor_state(
        ctx.db,
        source="hybrid",
        dataset="f1_history_all",
        cursor_key=cursor_key,
        cursor_value=cursor_after,
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after=cursor_after,
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "season_start": season_start,
        "season_end": season_end,
        "child_runs": child_runs,
        "records_written": records_written,
    }


def hydrate_polymarket_f1_history(
    ctx: PipelineContext,
    *,
    fidelity: int = 60,
    active_only: bool = False,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="hydrate-polymarket-f1-history",
        source="polymarket",
        dataset="f1_market_history",
        description="Hydrate all F1 Polymarket markets with orderbook and history datasets.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"fidelity": fidelity, "active_only": active_only},
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    query = select(PolymarketMarket)
    if active_only:
        query = query.where(PolymarketMarket.active.is_(True))
    market_rows = ctx.db.scalars(query.order_by(PolymarketMarket.start_at_utc.asc())).all()

    hydrated = 0
    records_written = 0
    for market in market_rows:
        event = None if market.event_id is None else ctx.db.get(PolymarketEvent, market.event_id)
        if event is not None and not _event_looks_f1(event.raw_payload or {}):
            continue
        if event is None and not _event_looks_f1(market.raw_payload or {}):
            continue
        result = hydrate_polymarket_market(ctx, market_id=market.id, fidelity=fidelity)
        hydrated += 1
        records_written += int(result.get("records_written", 0))
        ctx.db.commit()

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "markets_hydrated": hydrated,
        "records_written": records_written,
    }


def validate_f1_weekend_subset(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int | None = None,
    report_slug: str | None = None,
    validation_mode: str = "smoke",
) -> dict[str, Any]:
    normalized_validation_mode = _normalize_validation_mode(validation_mode)
    definition = ensure_job_definition(
        ctx.db,
        job_name="validate-f1-weekend-subset",
        source="derived",
        dataset="validation_report",
        description="Validate one F1 weekend subset end-to-end and write a reusable report.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "meeting_key": meeting_key,
            "season": season,
            "report_slug": report_slug,
            "validation_mode": normalized_validation_mode,
        },
    )

    existing_meeting = ctx.db.scalar(select(F1Meeting).where(F1Meeting.meeting_key == meeting_key))
    target_season = season or (
        existing_meeting.season if existing_meeting is not None else utc_now().year
    )
    slug_hint = report_slug or (
        _report_slug_from_meeting(existing_meeting)
        if existing_meeting is not None
        else slugify(f"{target_season}-meeting-{meeting_key}")
    )
    report_dir = _validation_report_dir(
        root=ctx.settings.data_root,
        season=target_season,
        slug=slug_hint,
    )

    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            records_written=0,
        )
        return {
            "job_run_id": run.id,
            "status": "planned",
            "meeting_key": meeting_key,
            "season": target_season,
            "validation_mode": normalized_validation_mode,
            "report_dir": str(report_dir),
        }

    sync_result = sync_f1_calendar(ctx, season=target_season)
    ensure_default_feature_registry(ctx)

    meeting = ctx.db.scalar(select(F1Meeting).where(F1Meeting.meeting_key == meeting_key))
    if meeting is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"meeting_key={meeting_key} not found after calendar sync",
        )
        raise ValueError(f"meeting_key={meeting_key} not found")

    sessions = ctx.db.scalars(
        select(F1Session)
        .where(F1Session.meeting_id == meeting.id)
        .order_by(F1Session.date_start_utc.asc(), F1Session.session_key.asc())
    ).all()
    session_ids = [session.id for session in sessions]
    if session_ids:
        # Validation reruns should not inherit stale auto-generated session mappings.
        auto_candidate_match = or_(
            MappingCandidate.matched_by == "session_code_time_window",
            MappingCandidate.matched_by.like("session_discovery_%"),
        )
        auto_mapping_match = or_(
            EntityMappingF1ToPolymarket.matched_by == "session_code_time_window",
            EntityMappingF1ToPolymarket.matched_by.like("session_discovery_%"),
        )
        ctx.db.execute(
            delete(MappingCandidate).where(
                MappingCandidate.f1_session_id.in_(session_ids),
                auto_candidate_match,
            )
        )
        ctx.db.execute(
            delete(EntityMappingF1ToPolymarket).where(
                EntityMappingF1ToPolymarket.f1_session_id.in_(session_ids),
                EntityMappingF1ToPolymarket.override_flag.is_(False),
                auto_mapping_match,
            )
        )
    actual_session_codes = frozenset(
        session.session_code for session in sessions if session.session_code is not None
    )
    hydrate_results: dict[str, dict[str, Any]] = {}
    discovery_results: dict[str, dict[str, Any]] = {}
    heavy_session_codes = sorted(
        {
            session.session_code
            for session in sessions
            if _validation_requires_heavy(
                session_code=session.session_code,
                validation_mode=normalized_validation_mode,
            )
        }
    )
    for session in sessions:
        include_heavy = _validation_requires_heavy(
            session_code=session.session_code,
            validation_mode=normalized_validation_mode,
        )
        hydrate_results[session.id] = hydrate_f1_session(
            ctx,
            session_key=session.session_key,
            include_extended=True,
            include_heavy=include_heavy,
        )
        discovery_results[session.id] = discover_session_polymarket(
            ctx,
            session_key=session.session_key,
        )

    reconcile_result = reconcile_mappings(ctx)
    dq_result = run_data_quality_checks(ctx)

    candidate_rows = ctx.db.scalars(
        select(MappingCandidate).where(MappingCandidate.f1_session_id.in_(session_ids))
    ).all()
    mapping_rows = ctx.db.scalars(
        select(EntityMappingF1ToPolymarket).where(
            EntityMappingF1ToPolymarket.f1_session_id.in_(session_ids)
        )
    ).all()
    market_ids = sorted(
        {
            row.polymarket_market_id
            for row in [*candidate_rows, *mapping_rows]
            if row.polymarket_market_id is not None
        }
    )
    markets = (
        ctx.db.scalars(select(PolymarketMarket).where(PolymarketMarket.id.in_(market_ids))).all()
        if market_ids
        else []
    )
    markets_by_id = {market.id: market for market in markets}
    event_ids = sorted({market.event_id for market in markets if market.event_id is not None})
    events = (
        ctx.db.scalars(select(PolymarketEvent).where(PolymarketEvent.id.in_(event_ids))).all()
        if event_ids
        else []
    )
    events_by_id = {event.id: event for event in events}
    dq_rows = ctx.db.scalars(
        select(DataQualityResult).where(DataQualityResult.job_run_id == dq_result["job_run_id"])
    ).all()

    failures: list[str] = []
    warnings: list[str] = []
    if actual_session_codes not in VALID_WEEKEND_SESSION_PATTERNS:
        failures.append(
            "Unexpected weekend session pattern: "
            f"{sorted(actual_session_codes)} for meeting_key={meeting_key}"
        )

    f1_dataset_counts: dict[str, dict[str, Any]] = {}
    mapping_summary: dict[str, dict[str, Any]] = {}
    session_inventory: list[dict[str, Any]] = []

    candidates_by_session: dict[str, list[MappingCandidate]] = defaultdict(list)
    mappings_by_session: dict[str, list[EntityMappingF1ToPolymarket]] = defaultdict(list)
    for row in candidate_rows:
        if row.f1_session_id is not None:
            candidates_by_session[row.f1_session_id].append(row)
    for row in mapping_rows:
        if row.f1_session_id is not None:
            mappings_by_session[row.f1_session_id].append(row)

    for session in sessions:
        session_counts = _count_session_rows(ctx, session)
        f1_dataset_counts[str(session.session_key)] = session_counts
        session_candidates = candidates_by_session.get(session.id, [])
        session_mappings = mappings_by_session.get(session.id, [])
        candidate_types = Counter(row.candidate_type for row in session_candidates)
        mapping_taxonomies = Counter(
            markets_by_id[row.polymarket_market_id].taxonomy
            for row in session_mappings
            if row.polymarket_market_id in markets_by_id
        )
        mapping_summary[str(session.session_key)] = {
            "candidate_count": len(session_candidates),
            "mapping_count": len(session_mappings),
            "candidate_types": dict(sorted(candidate_types.items())),
            "mapping_taxonomies": dict(sorted(mapping_taxonomies.items())),
            "hydrate_result": hydrate_results.get(session.id),
            "discovery_result": discovery_results.get(session.id),
        }
        session_inventory.append(
            {
                "session_key": session.session_key,
                "session_name": session.session_name,
                "session_code": session.session_code,
                "date_start_utc": None
                if session.date_start_utc is None
                else _ensure_utc(session.date_start_utc).isoformat(),
                "date_end_utc": None
                if session.date_end_utc is None
                else _ensure_utc(session.date_end_utc).isoformat(),
            }
        )

        if session_counts["session_results"] == 0:
            failures.append(
                f"Session {session.session_code}:{session.session_key} "
                "has no f1_session_results rows"
            )
        if (
            _validation_requires_heavy(
                session_code=session.session_code,
                validation_mode=normalized_validation_mode,
            )
            and session_counts["telemetry_total"] == 0
        ):
            failures.append(
                f"Session {session.session_code}:{session.session_key} has no heavy telemetry rows"
            )

        allowed_taxonomies = VALIDATION_ALLOWED_TAXONOMIES.get(session.session_code or "", set())
        invalid_candidate_types = sorted(set(candidate_types) - allowed_taxonomies)
        if invalid_candidate_types:
            failures.append(
                f"Session {session.session_code}:{session.session_key} "
                f"has invalid candidate taxonomies {invalid_candidate_types}"
            )
        invalid_mapping_types = sorted(set(mapping_taxonomies) - allowed_taxonomies)
        if invalid_mapping_types:
            failures.append(
                f"Session {session.session_code}:{session.session_key} "
                f"has invalid mapped taxonomies {invalid_mapping_types}"
            )

        if session.session_code in REQUIRED_WEEKEND_MAPPING_CODES and not session_mappings:
            failures.append(
                f"Session {session.session_code}:{session.session_key} has no Polymarket mappings"
            )
        elif not session_candidates:
            warnings.append(
                f"Session {session.session_code}:{session.session_key} has no Polymarket candidates"
            )

        for mapping in session_mappings:
            market = markets_by_id.get(mapping.polymarket_market_id or "")
            if market is None:
                failures.append(
                    f"Session {session.session_code}:{session.session_key} "
                    f"has mapping to missing market {mapping.polymarket_market_id}"
                )
                continue
            if market.target_session_code != session.session_code:
                failures.append(
                    f"Session {session.session_code}:{session.session_key} "
                    f"mapped market {market.id} has "
                    f"target_session_code={market.target_session_code}"
                )
            event = events_by_id.get(market.event_id or "")
            delta_days = _market_session_delta_days(
                session=session,
                event=_event_as_payload(event),
                market=_market_as_payload(market),
                market_start_at=market.start_at_utc,
            )
            if delta_days is None or delta_days > 1:
                failures.append(
                    f"Session {session.session_code}:{session.session_key} "
                    f"mapped market {market.id} is outside the allowed date window"
                )

    selected_probes = _select_market_probes(
        sessions=list(sessions),
        mappings=list(mapping_rows),
        candidates=list(candidate_rows),
        markets_by_id=markets_by_id,
    )
    existing_probe_keys = {probe["probe_key"] for probe in selected_probes}
    for probe_key, _session_codes, _taxonomies in MARKET_PROBE_SPECS:
        if probe_key not in existing_probe_keys:
            warnings.append(f"Representative probe {probe_key} could not be selected")

    market_probes: list[dict[str, Any]] = []
    for probe in selected_probes:
        probe_result = hydrate_polymarket_market(ctx, market_id=probe["market_id"])
        history_counts = _market_history_counts(ctx, probe["market_id"])
        probe_payload = probe | {
            "hydrate_result": probe_result,
            "history_counts": history_counts,
        }
        market_probes.append(probe_payload)
        if history_counts["price_history"] == 0:
            failures.append(
                f"Probe {probe['probe_key']} market {probe['market_id']} has no price history"
            )
        if history_counts["trades"] == 0 and history_counts["orderbook_snapshots"] == 0:
            failures.append(
                f"Probe {probe['probe_key']} market {probe['market_id']} "
                "has no trades or orderbook snapshots"
            )

    q_sq_r_sessions = [session for session in sessions if session.session_code in {"Q", "SQ", "R"}]
    f1_data_ready = all(
        f1_dataset_counts[str(session.session_key)]["session_results"] > 0
        for session in sessions
    ) and all(
        f1_dataset_counts[str(session.session_key)]["telemetry_total"] > 0
        for session in sessions
        if _validation_requires_heavy(
            session_code=session.session_code,
            validation_mode=normalized_validation_mode,
        )
    )
    mapping_ready = all(
        mapping_summary[str(session.session_key)]["mapping_count"] > 0
        for session in q_sq_r_sessions
    )
    history_ready = (
        all(
            probe["history_counts"]["price_history"] > 0
            and (
                probe["history_counts"]["trades"] > 0
                or probe["history_counts"]["orderbook_snapshots"] > 0
            )
            for probe in market_probes
        )
        if market_probes
        else False
    )
    research_readiness = {
        "f1_subset_data": _research_status(ok=f1_data_ready),
        "session_market_mapping": _research_status(ok=mapping_ready),
        "market_history_probe": _research_status(
            ok=history_ready,
            warning=not market_probes,
        ),
        "analysis_joinability": _research_status(ok=f1_data_ready and mapping_ready),
    }

    overall_status = "failed" if failures else ("warning" if warnings else "completed")
    report = {
        "generated_at": utc_now().isoformat(),
        "overall_status": overall_status,
        "validation_mode": normalized_validation_mode,
        "heavy_session_codes": heavy_session_codes,
        "meeting": {
            "meeting_key": meeting.meeting_key,
            "meeting_name": meeting.meeting_name,
            "season": meeting.season,
            "country_name": meeting.country_name,
            "location": meeting.location,
        },
        "session_pattern": sorted(actual_session_codes),
        "sync_result": sync_result,
        "reconcile_result": reconcile_result,
        "data_quality_run": dq_result,
        "data_quality_results": [
            {
                "dataset": row.dataset,
                "status": row.status,
                "metrics_json": row.metrics_json,
            }
            for row in dq_rows
        ],
        "session_inventory": session_inventory,
        "f1_dataset_counts": f1_dataset_counts,
        "mapping_summary": mapping_summary,
        "market_probes": market_probes,
        "research_readiness": research_readiness,
        "failures": failures,
        "warnings": warnings,
    }

    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (report_dir / "summary.md").write_text(
        _render_validation_markdown(report),
        encoding="utf-8",
    )

    finish_job_run(
        ctx.db,
        run,
        status=overall_status,
        records_written=(
            len(sessions) + len(candidate_rows) + len(mapping_rows) + len(market_probes)
        ),
    )
    return {
        "job_run_id": run.id,
        "status": overall_status,
        "meeting_key": meeting_key,
        "season": meeting.season,
        "validation_mode": normalized_validation_mode,
        "sessions": len(sessions),
        "report_dir": str(report_dir),
        "failures": len(failures),
        "warnings": len(warnings),
        "market_probes": len(market_probes),
    }


def _matches_openf1_payload(
    payload: dict[str, Any],
    *,
    session_key: int,
    meeting_key: int | None,
) -> bool:
    payload_session_key = payload.get("session_key")
    payload_meeting_key = payload.get("meeting_key")
    if payload_session_key is not None and int(payload_session_key) == session_key:
        return True
    if meeting_key is not None and payload_meeting_key is not None:
        return int(payload_meeting_key) == meeting_key
    return False


def _derive_live_market_ids(
    ctx: PipelineContext,
    *,
    session: F1Session,
    requested_market_ids: list[str] | None,
) -> list[str]:
    if requested_market_ids:
        return requested_market_ids
    linked_ids = ctx.db.scalars(
        select(EntityMappingF1ToPolymarket.polymarket_market_id).where(
            EntityMappingF1ToPolymarket.f1_session_id == session.id
        )
    ).all()
    if linked_ids:
        return [market_id for market_id in linked_ids if market_id is not None]
    if session.session_code is None or session.date_start_utc is None:
        return []
    candidate_ids: list[str] = []
    market_rows = ctx.db.scalars(
        select(PolymarketMarket).where(
            PolymarketMarket.target_session_code == session.session_code,
            PolymarketMarket.taxonomy_confidence.is_not(None),
        )
    ).all()
    for market in market_rows:
        if market.taxonomy_confidence is not None and market.taxonomy_confidence < 0.6:
            continue
        if market.start_at_utc is None:
            continue
        delta_days = abs((market.start_at_utc - session.date_start_utc).total_seconds()) / 86400
        if delta_days <= 3:
            candidate_ids.append(market.id)
    return candidate_ids


def capture_live_weekend(
    ctx: PipelineContext,
    *,
    session_key: int,
    market_ids: list[str] | None = None,
    start_buffer_min: int = 15,
    stop_buffer_min: int = 15,
    openf1_topics: tuple[str, ...] = DEFAULT_OPENF1_TOPICS,
    message_limit: int | None = None,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="capture-live-weekend",
        source="hybrid",
        dataset="live_weekend",
        description=(
            "Capture OpenF1 live MQTT and Polymarket market-channel updates around a session."
        ),
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "session_key": session_key,
            "market_ids": market_ids or [],
            "start_buffer_min": start_buffer_min,
            "stop_buffer_min": stop_buffer_min,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "session_key": session_key}

    session = ctx.db.scalar(select(F1Session).where(F1Session.session_key == session_key))
    if session is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"session_key={session_key} not found",
        )
        raise ValueError(f"session_key={session_key} not found")
    meeting_key = None
    if session.raw_payload is not None and session.raw_payload.get("meeting_key") is not None:
        meeting_key = int(session.raw_payload["meeting_key"])

    start_at = _ensure_utc(session.date_start_utc or utc_now())
    start_at = start_at - timedelta(minutes=start_buffer_min)
    end_anchor = _ensure_utc(session.date_end_utc or session.date_start_utc or utc_now())
    stop_at = end_anchor + timedelta(minutes=stop_buffer_min)
    now = _ensure_utc(utc_now())
    if now < start_at:
        time.sleep((start_at - now).total_seconds())
    capture_seconds = max((stop_at - _ensure_utc(utc_now())).total_seconds(), 1.0)

    if not ctx.settings.openf1_username or not ctx.settings.openf1_password:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message="OPENF1_USERNAME and OPENF1_PASSWORD are required for live capture",
        )
        raise ValueError("OpenF1 live capture requires OPENF1_USERNAME and OPENF1_PASSWORD")

    openf1_payloads: dict[str, list[dict[str, Any]]] = {topic: [] for topic in openf1_topics}
    openf1_live = OpenF1LiveConnector(
        username=ctx.settings.openf1_username,
        password=ctx.settings.openf1_password,
    )

    def _handle_openf1(message: Any) -> None:
        payloads = message.payload if isinstance(message.payload, list) else [message.payload]
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            if _matches_openf1_payload(payload, session_key=session_key, meeting_key=meeting_key):
                openf1_payloads[message.topic].append(
                    {
                        "topic": message.topic,
                        "observed_at": message.observed_at.isoformat(),
                        "payload": payload,
                    }
                )

    openf1_message_count = openf1_live.stream(
        topics=openf1_topics,
        on_message=_handle_openf1,
        stop_after_seconds=capture_seconds,
        message_limit=message_limit,
    )

    records_written = 0
    for topic, payloads in openf1_payloads.items():
        if not payloads:
            continue
        dataset = topic.replace("/", "_")
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="openf1_live",
                dataset=dataset,
                endpoint=f"mqtt:{topic}",
                params={"session_key": session_key},
                payload=payloads,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key), "topic": dataset},
        )
        records_written += len(payloads)

    live_market_ids = _derive_live_market_ids(ctx, session=session, requested_market_ids=market_ids)
    polymarket_tokens = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id.in_(live_market_ids))
    ).all()
    token_ids = [token.id for token in polymarket_tokens]
    condition_to_market_id = {
        market.condition_id: market.id
        for market in ctx.db.scalars(
            select(PolymarketMarket).where(PolymarketMarket.id.in_(live_market_ids))
        ).all()
    }
    if token_ids:
        ws_messages: list[dict[str, Any]] = []
        ws_connector = PolymarketLiveConnector()

        def _handle_polymarket(message: Any) -> None:
            payload = message.payload
            if not isinstance(payload, dict):
                return
            ws_messages.append(
                {
                    "observed_at": message.observed_at.isoformat(),
                    "payload": payload,
                }
            )

        ws_count = ws_connector.stream_market_messages(
            asset_ids=token_ids,
            on_message=_handle_polymarket,
            stop_after_seconds=capture_seconds,
            message_limit=message_limit,
        )
        bronze_object = persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="polymarket_ws",
                dataset="market_channel",
                endpoint="wss:/market",
                params={"market_ids": live_market_ids},
                payload=ws_messages,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key)},
        )
        manifest_rows: list[dict[str, Any]] = []
        for index, item in enumerate(ws_messages):
            payload = item["payload"]
            condition_id = payload.get("market")
            market_id = condition_to_market_id.get(str(condition_id))
            manifest_rows.append(
                {
                    "id": f"{bronze_object.checksum}:{index}",
                    "channel": "market",
                    "market_id": market_id,
                    "token_id": payload.get("asset_id"),
                    "object_path": str(bronze_object.path),
                    "event_type": payload.get("event_type"),
                    "observed_at_utc": parse_dt(item["observed_at"]) or utc_now(),
                    "checksum": payload_checksum(payload),
                    "raw_payload": payload,
                }
            )
        if manifest_rows:
            upsert_records(ctx.db, PolymarketWsMessageManifest, manifest_rows)
            records_written += len(manifest_rows)
        records_written += ws_count

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "session_key": session_key,
        "openf1_messages": openf1_message_count,
        "polymarket_market_ids": live_market_ids,
        "records_written": records_written,
    }
