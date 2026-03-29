from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, cast

from f1_polymarket_lab.common import (
    slugify,
    stable_uuid,
    timestamp_date_variants,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    PolymarketConnector,
    infer_market_scheduled_date,
    parse_market_taxonomy,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    MappingCandidate,
    MarketTaxonomyLabel,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketMarketRule,
    PolymarketMarketStatusHistory,
    PolymarketToken,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    start_job_run,
    upsert_cursor_state,
)
from f1_polymarket_worker.pipeline import (
    PipelineContext,
    ensure_taxonomy_version,
    parse_dt,
    persist_fetch,
    persist_silver,
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
SESSION_DISCOVERY_MIN_CANDIDATE_SCORE = 0.65
SESSION_DISCOVERY_AUTO_MAP_SCORE = 0.85
DISCOVERY_SOURCE_PRIORITY = {"public_search": 0, "tag_feed": 1, "exact_slug": 2}


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


def _flush_polymarket_catalog_dependencies(ctx: PipelineContext) -> None:
    """Persist parent market rows before child tables in SQLite fallback mode."""
    ctx.db.flush()


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
    _flush_polymarket_catalog_dependencies(ctx)
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows.values())
    _flush_polymarket_catalog_dependencies(ctx)
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
    _flush_polymarket_catalog_dependencies(ctx)
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows.values())
    _flush_polymarket_catalog_dependencies(ctx)
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
    if candidate_rows or mapping_rows:
        _flush_polymarket_catalog_dependencies(ctx)

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
