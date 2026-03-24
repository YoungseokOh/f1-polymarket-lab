from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

from f1_polymarket_lab.common import (
    payload_checksum,
    slugify,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    OpenF1LiveConnector,
    PolymarketLiveConnector,
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
    PolymarketEvent,
    PolymarketMarket,
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

from f1_polymarket_worker.f1_backfill import (
    _normalize_validation_mode,
    _validation_requires_heavy,
)
from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)

# Re-export for weekend_ops callers
from f1_polymarket_worker.market_discovery import (
    DEFAULT_OPENF1_TOPICS,
    _ensure_utc,
    _market_session_delta_days,
    discover_session_polymarket,  # noqa: F401
)
from f1_polymarket_worker.pipeline import (
    PipelineContext,
    ensure_default_feature_registry,
    hydrate_f1_session,
    hydrate_polymarket_market,
    parse_dt,
    persist_fetch,
    reconcile_mappings,
    run_data_quality_checks,
    sync_f1_calendar,
)

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
REGULAR_WEEKEND_SESSION_PATTERN = frozenset({"FP1", "FP2", "FP3", "Q", "R"})
SPRINT_WEEKEND_SESSION_PATTERN = frozenset({"FP1", "SQ", "S", "Q", "R"})
VALID_WEEKEND_SESSION_PATTERNS = (
    REGULAR_WEEKEND_SESSION_PATTERN,
    SPRINT_WEEKEND_SESSION_PATTERN,
)
REQUIRED_WEEKEND_MAPPING_CODES = frozenset({"Q", "SQ", "R"})


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
    from datetime import datetime, timezone

    from f1_polymarket_worker.market_discovery import _ensure_utc as _ensure_utc_local

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
                        _ensure_utc_local(item["session"].date_start_utc)
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
