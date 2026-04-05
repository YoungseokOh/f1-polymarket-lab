from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
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
    F1Driver,
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
    FeatureSnapshot,
    MappingCandidate,
    ModelPrediction,
    PaperTradePosition,
    PaperTradeSession,
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
from f1_polymarket_worker.gp_registry import (
    GP_REGISTRY,
    GPConfig,
    build_snapshot,
    config_display_description,
    config_display_label,
    config_stage_label,
    get_gp_config,
    run_baseline,
    select_model_run_id,
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
from f1_polymarket_worker.paper_trading import PaperTradeConfig, PaperTradingEngine
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
COCKPIT_TIMELINE_CODES = ("FP1", "FP2", "FP3", "Q", "R")


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


def _stage_priority(config: GPConfig) -> int:
    return config.stage_rank


def _meeting_sort_key(meeting: F1Meeting | None, *, now: Any) -> tuple[int, float]:
    if meeting is None:
        return (3, float("inf"))
    start_at = _ensure_utc(meeting.start_date_utc) if meeting.start_date_utc else None
    end_at = _ensure_utc(meeting.end_date_utc) if meeting.end_date_utc else start_at
    if start_at and end_at and start_at <= now <= end_at:
        return (0, abs((now - start_at).total_seconds()))
    if start_at and now < start_at:
        return (1, (start_at - now).total_seconds())
    if end_at:
        return (2, abs((now - end_at).total_seconds()))
    return (3, float("inf"))


def _config_payload(config: GPConfig) -> dict[str, Any]:
    return {
        "name": config.name,
        "short_code": config.short_code,
        "meeting_key": config.meeting_key,
        "season": config.season,
        "target_session_code": config.target_session_code,
        "variant": config.variant,
        "source_session_code": config.source_session_code,
        "market_taxonomy": config.market_taxonomy,
        "stage_rank": config.stage_rank,
        "stage_label": config_stage_label(config),
        "display_label": config_display_label(config),
        "display_description": config_display_description(config),
    }


def _step_payload(
    *,
    key: str,
    label: str,
    status: str,
    detail: str,
    session_code: str | None = None,
    session_key: int | None = None,
    count: int | None = None,
    reason_code: str | None = None,
    actionable_after_utc: Any | None = None,
    resource_label: str | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "status": status,
        "detail": detail,
        "session_code": session_code,
        "session_key": session_key,
        "count": count,
        "reason_code": reason_code,
        "actionable_after_utc": actionable_after_utc,
        "resource_label": resource_label,
    }


def _session_display_name(session_code: str | None) -> str:
    return {
        None: "Pre-weekend",
        "FP1": "FP1",
        "FP2": "FP2",
        "FP3": "FP3",
        "Q": "Qualifying",
        "R": "Race",
    }.get(session_code, session_code or "Session")


def _resource_label_for_step(key: str, *, session_code: str | None) -> str:
    session_name = _session_display_name(session_code)
    return {
        "sync_calendar": "Weekend schedule",
        "hydrate_source_session": f"{session_name} results",
        "settle_finished_stage": f"{session_name} tickets",
        "discover_target_markets": f"{session_name} markets",
        "run_paper_trade": "Paper trading",
    }.get(key, session_name)


def _blocked_until_detail(resource_label: str, *, until: Any) -> str:
    return f"Check again after {until.isoformat()} to continue preparing {resource_label}."


def _config_explanation(config: GPConfig) -> str:
    if config.source_session_code is None:
        return "This stage reviews Qualifying markets using pre-practice information only."
    source_name = _session_display_name(config.source_session_code)
    target_name = _session_display_name(config.target_session_code)
    return (
        f"This stage uses {source_name} results to settle finished {source_name} tickets, "
        f"find {target_name} markets, and when ready continue into paper trading."
    )


def _primary_action_payload(
    *,
    config: GPConfig,
    sync_step: dict[str, Any],
    hydrate_step: dict[str, Any],
    settle_step: dict[str, Any],
    discover_step: dict[str, Any],
    run_step: dict[str, Any],
    latest_paper_session: PaperTradeSession | None,
) -> dict[str, str]:
    source_name = _session_display_name(config.source_session_code)
    target_name = _session_display_name(config.target_session_code)
    if run_step["status"] == "blocked":
        return {
            "primary_action_title": "Wait for this stage",
            "primary_action_description": run_step["detail"],
            "primary_action_cta": "Not ready yet",
        }
    if sync_step["status"] == "ready":
        return {
            "primary_action_title": "Update to latest",
            "primary_action_description": (
                "This latest update will load the current Grand Prix schedule first, "
                "then continue the remaining preparation steps."
            ),
            "primary_action_cta": "Update to latest",
        }
    if hydrate_step["status"] == "ready":
        continuation = (
            f", then settle finished {source_name} tickets and prepare {target_name} markets."
            if settle_step["status"] == "ready"
            else f" and prepare {target_name} markets."
        )
        return {
            "primary_action_title": "Update to latest",
            "primary_action_description": (
                f"This latest update will load {source_name} results first"
                f"{continuation}"
            ),
            "primary_action_cta": "Update to latest",
        }
    if settle_step["status"] == "ready":
        return {
            "primary_action_title": "Update to latest",
            "primary_action_description": (
                f"This latest update will settle finished {source_name} tickets, "
                f"prepare {target_name} markets, and continue into paper trading."
            ),
            "primary_action_cta": "Update to latest",
        }
    if discover_step["status"] == "ready":
        return {
            "primary_action_title": "Update to latest",
            "primary_action_description": (
                f"This latest update will discover {target_name} markets first, "
                "then continue into paper trading."
            ),
            "primary_action_cta": "Update to latest",
        }
    rerun_label = (
        "Update to latest"
        if latest_paper_session is not None
        else "Update to latest"
    )
    return {
        "primary_action_title": "Update to latest",
        "primary_action_description": (
            "All prerequisites are complete. You can run the next paper-trading stage now."
        ),
        "primary_action_cta": rerun_label,
    }


def _required_session_codes(config: GPConfig) -> set[str]:
    codes = {config.target_session_code}
    if config.source_session_code:
        codes.add(config.source_session_code)
    return codes


def _meeting_and_sessions_for_config(
    ctx: PipelineContext,
    *,
    config: GPConfig,
) -> tuple[F1Meeting | None, dict[str, F1Session]]:
    meeting = ctx.db.scalar(
        select(F1Meeting).where(
            F1Meeting.meeting_key == config.meeting_key,
            F1Meeting.season == config.season,
        )
    )
    if meeting is None:
        return None, {}
    sessions = ctx.db.scalars(
        select(F1Session).where(F1Session.meeting_id == meeting.id)
    ).all()
    return meeting, {
        session.session_code: session
        for session in sessions
        if session.session_code is not None
    }


def _session_has_started(session: F1Session | None, *, now: Any) -> bool:
    return bool(
        session is not None
        and session.date_start_utc is not None
        and _ensure_utc(session.date_start_utc) <= now
    )


def _session_has_ended(session: F1Session | None, *, now: Any) -> bool:
    return bool(
        session is not None
        and session.date_end_utc is not None
        and _ensure_utc(session.date_end_utc) <= now
    )


def _session_is_live(session: F1Session | None, *, now: Any) -> bool:
    return bool(
        session is not None
        and session.date_start_utc is not None
        and session.date_end_utc is not None
        and _ensure_utc(session.date_start_utc) <= now < _ensure_utc(session.date_end_utc)
    )


def _find_stage_config(
    configs: list[GPConfig],
    *,
    source_session_code: str | None,
    target_session_code: str,
) -> GPConfig | None:
    candidates = [
        config
        for config in configs
        if config.source_session_code == source_session_code
        and config.target_session_code == target_session_code
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda config: (config.stage_rank, config.short_code))


def _focus_session_state(
    sessions_by_code: dict[str, F1Session],
    *,
    now: Any,
) -> tuple[F1Session | None, str, list[str], str | None]:
    ordered_sessions = [
        sessions_by_code[code]
        for code in COCKPIT_TIMELINE_CODES
        if code in sessions_by_code
    ]
    completed_codes = [
        session.session_code
        for session in ordered_sessions
        if session.session_code is not None and _session_has_ended(session, now=now)
    ]
    live_session = next(
        (session for session in ordered_sessions if _session_is_live(session, now=now)),
        None,
    )
    if live_session is not None:
        return live_session, "live", completed_codes, live_session.session_code

    upcoming_session = next(
        (
            session
            for session in ordered_sessions
            if session.date_start_utc is not None and _ensure_utc(session.date_start_utc) > now
        ),
        None,
    )
    if upcoming_session is not None:
        return upcoming_session, "upcoming", completed_codes, upcoming_session.session_code

    if ordered_sessions:
        last_session = ordered_sessions[-1]
        return last_session, "ended", completed_codes, last_session.session_code
    return None, "upcoming", [], None


def _choose_default_config_for_meeting(
    configs: list[GPConfig],
    *,
    sessions_by_code: dict[str, F1Session],
    now: Any,
) -> GPConfig:
    pre_weekend_config = _find_stage_config(
        configs,
        source_session_code=None,
        target_session_code="Q",
    )
    fp1_to_fp2_config = _find_stage_config(
        configs,
        source_session_code="FP1",
        target_session_code="FP2",
    )
    fp2_to_q_config = _find_stage_config(
        configs,
        source_session_code="FP2",
        target_session_code="Q",
    )
    fp3_to_q_config = _find_stage_config(
        configs,
        source_session_code="FP3",
        target_session_code="Q",
    )
    q_to_r_config = _find_stage_config(
        configs,
        source_session_code="Q",
        target_session_code="R",
    )
    fp1_session = sessions_by_code.get("FP1")
    fp2_session = sessions_by_code.get("FP2")
    fp3_session = sessions_by_code.get("FP3")
    q_session = sessions_by_code.get("Q")

    if q_to_r_config is not None and _session_has_ended(q_session, now=now):
        return q_to_r_config
    if fp3_to_q_config is not None and _session_has_ended(fp3_session, now=now):
        return fp3_to_q_config
    if fp2_to_q_config is not None and (
        _session_is_live(fp2_session, now=now) or _session_has_ended(fp2_session, now=now)
    ):
        return fp2_to_q_config
    if (
        fp1_to_fp2_config is not None
        and _session_has_ended(fp1_session, now=now)
        and not _session_has_started(fp2_session, now=now)
    ):
        return fp1_to_fp2_config
    if pre_weekend_config is not None and not _session_has_ended(fp1_session, now=now):
        return pre_weekend_config

    ready_configs: list[tuple[int, GPConfig]] = []
    for config in configs:
        if config.source_session_code is None:
            ready_configs.append((_stage_priority(config), config))
            continue
        source_session = sessions_by_code.get(config.source_session_code)
        if (
            source_session is not None
            and source_session.date_end_utc is not None
            and _ensure_utc(source_session.date_end_utc) <= now
        ):
            ready_configs.append((_stage_priority(config), config))
    if ready_configs:
        return max(ready_configs, key=lambda item: (item[0], item[1].short_code))[1]
    pre_weekend = [config for config in configs if config.source_session_code is None]
    if pre_weekend:
        return sorted(
            pre_weekend,
            key=lambda config: (config.stage_rank, config.short_code),
        )[0]
    return min(configs, key=lambda config: (_stage_priority(config), config.short_code))


def _auto_select_gp_config(ctx: PipelineContext, *, now: Any) -> GPConfig:
    configs_by_meeting: dict[tuple[int, int], list[GPConfig]] = defaultdict(list)
    for config in GP_REGISTRY:
        configs_by_meeting[(config.meeting_key, config.season)].append(config)

    scored: list[tuple[tuple[int, float], tuple[int, int], F1Meeting | None]] = []
    for key in configs_by_meeting:
        meeting = ctx.db.scalar(
            select(F1Meeting).where(
                F1Meeting.meeting_key == key[0],
                F1Meeting.season == key[1],
            )
        )
        scored.append((_meeting_sort_key(meeting, now=now), key, meeting))

    _, selected_key, _ = min(scored, key=lambda item: (item[0], item[1]))
    representative = configs_by_meeting[selected_key][0]
    _, sessions_by_code = _meeting_and_sessions_for_config(ctx, config=representative)
    return _choose_default_config_for_meeting(
        configs_by_meeting[selected_key],
        sessions_by_code=sessions_by_code,
        now=now,
    )


def _session_result_count(ctx: PipelineContext, *, session_id: str | None) -> int:
    if session_id is None:
        return 0
    return int(
        ctx.db.scalar(
            select(func.count())
            .select_from(F1SessionResult)
            .where(F1SessionResult.session_id == session_id)
        )
        or 0
    )


def _mapping_count_for_config(
    ctx: PipelineContext,
    *,
    config: GPConfig,
    target_session_id: str | None,
) -> int:
    if target_session_id is None:
        return 0
    return int(
        ctx.db.scalar(
            select(func.count(func.distinct(EntityMappingF1ToPolymarket.polymarket_market_id)))
            .select_from(EntityMappingF1ToPolymarket)
            .join(
                PolymarketMarket,
                PolymarketMarket.id == EntityMappingF1ToPolymarket.polymarket_market_id,
            )
            .where(
                EntityMappingF1ToPolymarket.f1_session_id == target_session_id,
                PolymarketMarket.taxonomy == config.market_taxonomy,
                PolymarketMarket.target_session_code == config.target_session_code,
            )
        )
        or 0
    )


def _gp_config_for_slug(gp_slug: str) -> GPConfig | None:
    for config in GP_REGISTRY:
        if config.short_code == gp_slug:
            return config
    return None


def _session_winner_driver_id(ctx: PipelineContext, *, session_id: str | None) -> str | None:
    if session_id is None:
        return None
    return ctx.db.scalar(
        select(F1SessionResult.driver_id).where(
            F1SessionResult.session_id == session_id,
            F1SessionResult.position == 1,
        )
    )


def _open_paper_trade_positions(
    ctx: PipelineContext,
    *,
    paper_session_id: str,
) -> list[PaperTradePosition]:
    return list(
        ctx.db.scalars(
            select(PaperTradePosition)
            .where(
                PaperTradePosition.session_id == paper_session_id,
                PaperTradePosition.status == "open",
            )
            .order_by(PaperTradePosition.entry_time.asc(), PaperTradePosition.id.asc())
        ).all()
    )


def _market_within_meeting_window(
    *,
    market: PolymarketMarket,
    meeting: F1Meeting | None,
) -> bool:
    if meeting is None:
        return True
    window_start = (
        None
        if meeting.start_date_utc is None
        else _ensure_utc(meeting.start_date_utc) - timedelta(days=7)
    )
    window_end = (
        None
        if meeting.end_date_utc is None
        else _ensure_utc(meeting.end_date_utc) + timedelta(days=2)
    )
    market_start = None if market.start_at_utc is None else _ensure_utc(market.start_at_utc)
    market_end = None if market.end_at_utc is None else _ensure_utc(market.end_at_utc)
    if window_end is not None and market_start is not None and market_start > window_end:
        return False
    if window_start is not None and market_end is not None and market_end < window_start:
        return False
    return True


def _paper_trade_session_targets_completed_session(
    ctx: PipelineContext,
    *,
    paper_session: PaperTradeSession,
    open_positions: list[PaperTradePosition],
    completed_session: F1Session,
    meeting: F1Meeting | None,
) -> bool:
    gp_config = _gp_config_for_slug(paper_session.gp_slug)
    if (
        gp_config is not None
        and meeting is not None
        and gp_config.meeting_key == meeting.meeting_key
        and gp_config.season == meeting.season
        and gp_config.target_session_code == completed_session.session_code
    ):
        return True

    market_ids = sorted({position.market_id for position in open_positions if position.market_id})
    if not market_ids:
        return False

    mapped_market_ids = set(
        ctx.db.scalars(
            select(EntityMappingF1ToPolymarket.polymarket_market_id).where(
                EntityMappingF1ToPolymarket.f1_session_id == completed_session.id,
                EntityMappingF1ToPolymarket.polymarket_market_id.in_(market_ids),
            )
        ).all()
    )
    if mapped_market_ids:
        return True

    markets = ctx.db.scalars(
        select(PolymarketMarket).where(PolymarketMarket.id.in_(market_ids))
    ).all()
    return any(
        market.target_session_code == completed_session.session_code
        and _market_within_meeting_window(market=market, meeting=meeting)
        for market in markets
    )


def _candidate_paper_trade_sessions_for_completed_session(
    ctx: PipelineContext,
    *,
    completed_session: F1Session,
    meeting: F1Meeting | None,
) -> list[dict[str, Any]]:
    open_sessions = ctx.db.scalars(
        select(PaperTradeSession)
        .where(PaperTradeSession.status == "open")
        .order_by(PaperTradeSession.started_at.asc(), PaperTradeSession.id.asc())
    ).all()

    candidates: list[dict[str, Any]] = []
    for paper_session in open_sessions:
        open_positions = _open_paper_trade_positions(ctx, paper_session_id=paper_session.id)
        if not open_positions:
            continue
        if not _paper_trade_session_targets_completed_session(
            ctx,
            paper_session=paper_session,
            open_positions=open_positions,
            completed_session=completed_session,
            meeting=meeting,
        ):
            continue
        config_json = paper_session.config_json or {}
        gp_config = _gp_config_for_slug(paper_session.gp_slug)
        candidates.append(
            {
                "session": paper_session,
                "open_positions": open_positions,
                "gp_config": gp_config,
                "manual_trade": bool(config_json.get("manual_trade")) or gp_config is None,
            }
        )
    return candidates


def _settlement_preview(
    ctx: PipelineContext,
    *,
    completed_session: F1Session,
    meeting: F1Meeting | None,
) -> dict[str, Any]:
    candidates = _candidate_paper_trade_sessions_for_completed_session(
        ctx,
        completed_session=completed_session,
        meeting=meeting,
    )
    return {
        "candidate_session_ids": [item["session"].id for item in candidates],
        "candidate_gp_slugs": [item["session"].gp_slug for item in candidates],
        "candidate_sessions": len(candidates),
        "candidate_positions": sum(len(item["open_positions"]) for item in candidates),
        "candidate_manual_positions": sum(
            len(item["open_positions"]) for item in candidates if item["manual_trade"]
        ),
    }


def _normalize_driver_alias(value: Any) -> str | None:
    if value is None:
        return None
    normalized = slugify(str(value).strip())
    return normalized or None


def _driver_alias_variants(*, driver_id: str, driver: F1Driver | None) -> set[str]:
    aliases: set[str] = set()

    def add(value: Any) -> None:
        normalized = _normalize_driver_alias(value)
        if normalized is not None:
            aliases.add(normalized)

    add(driver_id)
    if driver is None:
        return aliases

    add(driver.full_name)
    add(driver.broadcast_name)
    add(driver.first_name)
    add(driver.last_name)
    add(driver.name_acronym)
    if driver.first_name and driver.last_name:
        add(f"{driver.first_name} {driver.last_name}")

    normalized_full_name = _normalize_driver_alias(driver.full_name)
    if normalized_full_name is not None:
        tokens = [token for token in normalized_full_name.split("-") if token]
        if len(tokens) >= 2:
            aliases.add("-".join(tokens[:2]))
            aliases.add("-".join(tokens[-2:]))
            aliases.add("-".join(tokens[:-1]))
            aliases.add("-".join(tokens[1:]))
            aliases.add(f"{tokens[0]}-{tokens[-1]}")
            for start in range(len(tokens)):
                aliases.add("-".join(tokens[start:]))
    return {alias for alias in aliases if alias}


def _session_driver_alias_index(
    ctx: PipelineContext,
    *,
    session_id: str,
) -> dict[str, set[str]]:
    alias_to_driver_ids: dict[str, set[str]] = defaultdict(set)
    rows = ctx.db.execute(
        select(F1SessionResult.driver_id, F1Driver)
        .select_from(F1SessionResult)
        .outerjoin(F1Driver, F1Driver.id == F1SessionResult.driver_id)
        .where(F1SessionResult.session_id == session_id)
    ).all()
    for driver_id, driver in rows:
        if driver_id is None:
            continue
        for alias in _driver_alias_variants(driver_id=driver_id, driver=driver):
            alias_to_driver_ids[alias].add(driver_id)
    return alias_to_driver_ids


def _exact_driver_alias_match(
    *,
    value: Any,
    alias_index: dict[str, set[str]],
) -> str | None:
    normalized = _normalize_driver_alias(value)
    if normalized is None:
        return None
    matches = alias_index.get(normalized)
    if not matches or len(matches) != 1:
        return None
    return next(iter(matches))


def _driver_alias_match_from_text(
    *,
    value: Any,
    alias_index: dict[str, set[str]],
) -> str | None:
    normalized_text = _normalize_driver_alias(value)
    if normalized_text is None:
        return None

    candidates: list[tuple[int, int, str]] = []
    for alias, driver_ids in alias_index.items():
        if len(driver_ids) != 1 or alias not in normalized_text:
            continue
        alias_length = len(alias.replace("-", ""))
        alias_token_count = len([token for token in alias.split("-") if token])
        if alias_token_count == 1 and alias_length < 5:
            continue
        candidates.append((alias_token_count, alias_length, next(iter(driver_ids))))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    top_token_count, top_alias_length, top_driver_id = candidates[0]
    top_driver_ids = {
        driver_id
        for token_count, alias_length, driver_id in candidates
        if token_count == top_token_count and alias_length == top_alias_length
    }
    if len(top_driver_ids) != 1:
        return None
    return top_driver_id


def _load_snapshot_market_driver_map(
    ctx: PipelineContext,
    *,
    paper_session: PaperTradeSession,
) -> dict[str, str]:
    if not paper_session.snapshot_id:
        return {}

    snapshot = ctx.db.get(FeatureSnapshot, paper_session.snapshot_id)
    if snapshot is None or not snapshot.storage_path:
        return {}

    storage_path = Path(snapshot.storage_path)
    if not storage_path.exists():
        return {}

    import polars as pl  # noqa: PLC0415

    try:
        rows = pl.read_parquet(storage_path).select(["market_id", "driver_id"]).to_dicts()
    except Exception:
        return {}

    market_driver_map: dict[str, str] = {}
    for row in rows:
        market_id = row.get("market_id")
        driver_id = row.get("driver_id")
        if market_id and driver_id:
            market_driver_map[str(market_id)] = str(driver_id)
    return market_driver_map


def _resolve_position_driver_id(
    *,
    position: PaperTradePosition,
    paper_session: PaperTradeSession,
    market_driver_map: dict[str, str],
    markets_by_id: dict[str, PolymarketMarket],
    alias_index: dict[str, set[str]],
) -> str | None:
    snapshot_driver_id = market_driver_map.get(position.market_id or "")
    if snapshot_driver_id:
        return snapshot_driver_id

    config_json = paper_session.config_json or {}
    summary_json = paper_session.summary_json or {}
    for hint in (
        summary_json.get("selected_driver"),
        config_json.get("driver"),
    ):
        matched_driver_id = _exact_driver_alias_match(value=hint, alias_index=alias_index)
        if matched_driver_id is not None:
            return matched_driver_id

    market = markets_by_id.get(position.market_id)
    for text_hint in (
        config_json.get("market_question"),
        market.question if market is not None else None,
        market.slug if market is not None else None,
    ):
        matched_driver_id = _driver_alias_match_from_text(
            value=text_hint,
            alias_index=alias_index,
        )
        if matched_driver_id is not None:
            return matched_driver_id
    return None


def _paper_trade_position_pnl(
    position: PaperTradePosition,
    *,
    outcome_yes: bool,
    fee_rate: float,
) -> float:
    if position.side == "buy_no":
        pnl = (
            position.quantity * (1.0 - position.entry_price)
            if not outcome_yes
            else -position.quantity * position.entry_price
        )
    else:
        pnl = (
            position.quantity * (1.0 - position.entry_price)
            if outcome_yes
            else -position.quantity * position.entry_price
        )
    return pnl - (position.quantity * fee_rate)


def _refresh_paper_trade_session_summary(
    ctx: PipelineContext,
    *,
    paper_session: PaperTradeSession,
    settled_at: datetime,
) -> None:
    positions = list(
        ctx.db.scalars(
            select(PaperTradePosition).where(PaperTradePosition.session_id == paper_session.id)
        ).all()
    )
    settled_positions = [position for position in positions if position.status == "settled"]
    open_positions = [position for position in positions if position.status == "open"]
    win_count = len(
        [
            position
            for position in settled_positions
            if (position.realized_pnl or 0.0) > 0
        ]
    )
    total_pnl = sum(position.realized_pnl or 0.0 for position in settled_positions)
    existing_summary = dict(paper_session.summary_json or {})
    existing_summary.update(
        {
            "open_positions": len(open_positions),
            "settled_positions": len(settled_positions),
            "win_count": win_count,
            "loss_count": len(settled_positions) - win_count,
            "win_rate": (
                win_count / len(settled_positions) if settled_positions else None
            ),
            "total_pnl": total_pnl,
            "daily_pnl": total_pnl,
        }
    )
    paper_session.summary_json = existing_summary
    paper_session.status = "settled" if not open_positions else "open"
    paper_session.finished_at = settled_at if not open_positions else None


def settle_paper_trade_sessions_for_completed_session(
    ctx: PipelineContext,
    *,
    completed_session: F1Session,
    meeting: F1Meeting | None,
) -> dict[str, Any]:
    settlement_summary = {
        "settled_session_ids": [],
        "settled_gp_slugs": [],
        "settled_positions": 0,
        "manual_positions_settled": 0,
        "unresolved_positions": 0,
        "unresolved_session_ids": [],
        "winner_driver_id": None,
    }
    candidates = _candidate_paper_trade_sessions_for_completed_session(
        ctx,
        completed_session=completed_session,
        meeting=meeting,
    )
    if not candidates:
        return settlement_summary

    winner_driver_id = _session_winner_driver_id(ctx, session_id=completed_session.id)
    if winner_driver_id is None:
        raise ValueError(
            f"No position-1 result found for completed session {completed_session.session_code} "
            f"({completed_session.session_key})."
        )
    settlement_summary["winner_driver_id"] = winner_driver_id
    alias_index = _session_driver_alias_index(ctx, session_id=completed_session.id)
    settled_at = datetime.now(tz=timezone.utc)

    for candidate in candidates:
        paper_session = cast(PaperTradeSession, candidate["session"])
        open_positions = cast(list[PaperTradePosition], candidate["open_positions"])
        manual_trade = bool(candidate["manual_trade"])
        stage_backed = candidate["gp_config"] is not None
        market_driver_map = _load_snapshot_market_driver_map(ctx, paper_session=paper_session)
        market_ids = sorted(
            {
                position.market_id
                for position in open_positions
                if position.market_id
            }
        )
        markets_by_id = {
            market.id: market
            for market in ctx.db.scalars(
                select(PolymarketMarket).where(PolymarketMarket.id.in_(market_ids))
            ).all()
        } if market_ids else {}
        fee_rate = float((paper_session.config_json or {}).get("fee_rate", 0.02))

        resolvable_positions: list[tuple[PaperTradePosition, str]] = []
        unresolved_position_ids: list[str] = []
        for position in open_positions:
            driver_id = _resolve_position_driver_id(
                position=position,
                paper_session=paper_session,
                market_driver_map=market_driver_map,
                markets_by_id=markets_by_id,
                alias_index=alias_index,
            )
            if driver_id is None:
                unresolved_position_ids.append(position.id)
                continue
            resolvable_positions.append((position, driver_id))

        if unresolved_position_ids and stage_backed:
            raise ValueError(
                f"Could not resolve {len(unresolved_position_ids)} open position(s) for "
                f"{paper_session.gp_slug} against {completed_session.session_code} results."
            )

        for position, driver_id in resolvable_positions:
            outcome_yes = driver_id == winner_driver_id
            position.status = "settled"
            position.exit_price = 1.0 if outcome_yes else 0.0
            position.exit_time = settled_at
            position.realized_pnl = _paper_trade_position_pnl(
                position,
                outcome_yes=outcome_yes,
                fee_rate=fee_rate,
            )

        if resolvable_positions:
            _refresh_paper_trade_session_summary(
                ctx,
                paper_session=paper_session,
                settled_at=settled_at,
            )
            settlement_summary["settled_positions"] += len(resolvable_positions)
            if manual_trade:
                settlement_summary["manual_positions_settled"] += len(resolvable_positions)
            if paper_session.id not in settlement_summary["settled_session_ids"]:
                settlement_summary["settled_session_ids"].append(paper_session.id)
            if paper_session.gp_slug not in settlement_summary["settled_gp_slugs"]:
                settlement_summary["settled_gp_slugs"].append(paper_session.gp_slug)

        if unresolved_position_ids:
            settlement_summary["unresolved_positions"] += len(unresolved_position_ids)
            if paper_session.id not in settlement_summary["unresolved_session_ids"]:
                settlement_summary["unresolved_session_ids"].append(paper_session.id)

    return settlement_summary


def get_weekend_cockpit_status(
    ctx: PipelineContext,
    *,
    gp_short_code: str | None = None,
) -> dict[str, Any]:
    now = _ensure_utc(utc_now())
    auto_config = _auto_select_gp_config(ctx, now=now)
    selected_config = get_gp_config(gp_short_code) if gp_short_code else auto_config
    meeting, sessions_by_code = _meeting_and_sessions_for_config(ctx, config=selected_config)
    focus_session, focus_status, timeline_completed_codes, timeline_active_code = (
        _focus_session_state(sessions_by_code, now=now)
    )
    source_session = (
        None
        if selected_config.source_session_code is None
        else sessions_by_code.get(selected_config.source_session_code)
    )
    target_session = sessions_by_code.get(selected_config.target_session_code)
    required_codes = _required_session_codes(selected_config)
    calendar_completed = meeting is not None and required_codes.issubset(sessions_by_code.keys())
    source_result_count = _session_result_count(
        ctx,
        session_id=None if source_session is None else source_session.id,
    )
    target_mapping_count = _mapping_count_for_config(
        ctx,
        config=selected_config,
        target_session_id=None if target_session is None else target_session.id,
    )
    latest_paper_session = ctx.db.scalars(
        select(PaperTradeSession)
        .where(PaperTradeSession.gp_slug == selected_config.short_code)
        .order_by(PaperTradeSession.started_at.desc())
        .limit(1)
    ).first()

    source_name = _session_display_name(selected_config.source_session_code)
    target_name = _session_display_name(selected_config.target_session_code)
    sync_resource = _resource_label_for_step("sync_calendar", session_code=None)
    hydrate_resource = _resource_label_for_step(
        "hydrate_source_session",
        session_code=selected_config.source_session_code,
    )
    discover_resource = _resource_label_for_step(
        "discover_target_markets",
        session_code=selected_config.target_session_code,
    )
    settle_resource = _resource_label_for_step(
        "settle_finished_stage",
        session_code=selected_config.source_session_code,
    )
    run_resource = _resource_label_for_step("run_paper_trade", session_code=None)

    if calendar_completed:
        sync_step = _step_payload(
            key="sync_calendar",
            label="Load weekend schedule",
            status="completed",
            detail="Loaded the Grand Prix schedule and the sessions required for this stage.",
            reason_code="already_loaded",
            resource_label=sync_resource,
        )
    else:
        sync_step = _step_payload(
            key="sync_calendar",
            label="Load weekend schedule",
            status="ready",
            detail="The Grand Prix schedule and required sessions need to be loaded first.",
            reason_code="calendar_required",
            resource_label=sync_resource,
        )

    if selected_config.source_session_code is None:
        hydrate_step = _step_payload(
            key="hydrate_source_session",
            label="Check prior session results",
            status="skipped",
            detail="This stage does not require results from an earlier session.",
            reason_code="not_required",
            resource_label=hydrate_resource,
        )
    elif not calendar_completed:
        hydrate_step = _step_payload(
            key="hydrate_source_session",
            label=f"Load {source_name} results",
            status="pending",
            detail=(
                f"Once the weekend schedule is loaded, {source_name} results "
                "can be loaded next."
            ),
            session_code=selected_config.source_session_code,
            reason_code="waiting_for_calendar",
            resource_label=hydrate_resource,
        )
    elif source_session is None:
        hydrate_step = _step_payload(
            key="hydrate_source_session",
            label=f"Load {source_name} results",
            status="blocked",
            detail=f"{source_name} session details are unavailable, so results cannot be loaded.",
            session_code=selected_config.source_session_code,
            reason_code="missing_source_session",
            resource_label=hydrate_resource,
        )
    elif source_session.date_end_utc is None:
        hydrate_step = _step_payload(
            key="hydrate_source_session",
            label=f"Load {source_name} results",
            status="blocked",
            detail=(
                f"{source_name} end time is missing, so the automatic run "
                "window cannot be determined."
            ),
            session_code=selected_config.source_session_code,
            session_key=source_session.session_key,
            reason_code="missing_session_end_time",
            resource_label=hydrate_resource,
        )
    else:
        source_end = _ensure_utc(source_session.date_end_utc)
        if source_end > now:
            hydrate_step = _step_payload(
                key="hydrate_source_session",
                label=f"Load {source_name} results",
                status="blocked",
                detail=(
                    f"{source_name} is still in progress. This stage becomes available after "
                    f"{source_end.isoformat()}."
                ),
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                reason_code="session_in_progress",
                actionable_after_utc=source_end,
                resource_label=hydrate_resource,
            )
        elif source_result_count > 0:
            hydrate_step = _step_payload(
                key="hydrate_source_session",
                label=f"Load {source_name} results",
                status="completed",
                detail=f"{source_name} results are already available.",
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                count=source_result_count,
                reason_code="already_loaded",
                resource_label=hydrate_resource,
            )
        else:
            hydrate_step = _step_payload(
                key="hydrate_source_session",
                label=f"Load {source_name} results",
                status="ready",
                detail=f"{source_name} has finished, so results can be loaded now.",
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                reason_code="ready_to_hydrate",
                resource_label=hydrate_resource,
            )

    if selected_config.source_session_code is None:
        settle_step = _step_payload(
            key="settle_finished_stage",
            label="Settle finished stage",
            status="skipped",
            detail="This stage does not have an earlier ticket set to settle.",
            reason_code="not_required",
            resource_label=settle_resource,
        )
    elif not calendar_completed:
        settle_step = _step_payload(
            key="settle_finished_stage",
            label="Settle finished stage",
            status="pending",
            detail=(
                "Once the weekend schedule is loaded and source results are available, "
                "finished tickets can be settled."
            ),
            session_code=selected_config.source_session_code,
            reason_code="waiting_for_calendar",
            resource_label=settle_resource,
        )
    elif source_session is None:
        settle_step = _step_payload(
            key="settle_finished_stage",
            label="Settle finished stage",
            status="blocked",
            detail=(
                f"{source_name} session details are unavailable, so finished tickets "
                "cannot be settled."
            ),
            session_code=selected_config.source_session_code,
            reason_code="missing_source_session",
            resource_label=settle_resource,
        )
    elif source_result_count == 0:
        settle_step = _step_payload(
            key="settle_finished_stage",
            label="Settle finished stage",
            status="pending",
            detail=(
                f"Once {source_name} results are loaded, finished tickets can be "
                "settled automatically."
            ),
            session_code=selected_config.source_session_code,
            session_key=source_session.session_key,
            reason_code="waiting_for_source_results",
            resource_label=settle_resource,
        )
    else:
        settlement_preview = _settlement_preview(
            ctx,
            completed_session=source_session,
            meeting=meeting,
        )
        if settlement_preview["candidate_positions"] == 0:
            settle_step = _step_payload(
                key="settle_finished_stage",
                label="Settle finished stage",
                status="skipped",
                detail=f"No open tickets are waiting on {source_name} results.",
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                reason_code="nothing_to_settle",
                resource_label=settle_resource,
            )
        elif _session_winner_driver_id(ctx, session_id=source_session.id) is None:
            settle_step = _step_payload(
                key="settle_finished_stage",
                label="Settle finished stage",
                status="blocked",
                detail=(
                    f"{source_name} results are present, but the winning driver is not "
                    "available yet."
                ),
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                count=settlement_preview["candidate_positions"],
                reason_code="missing_winner_result",
                resource_label=settle_resource,
            )
        else:
            settle_step = _step_payload(
                key="settle_finished_stage",
                label="Settle finished stage",
                status="ready",
                detail=(
                    f"{settlement_preview['candidate_positions']} open tickets across "
                    f"{settlement_preview['candidate_sessions']} prior runs can be "
                    f"settled from {source_name} results now."
                ),
                session_code=selected_config.source_session_code,
                session_key=source_session.session_key,
                count=settlement_preview["candidate_positions"],
                reason_code="ready_to_settle",
                resource_label=settle_resource,
            )

    if not calendar_completed:
        discover_step = _step_payload(
            key="discover_target_markets",
            label=f"Find {target_name} markets",
            status="pending",
            detail=(
                f"Once the weekend schedule is loaded, {target_name} markets "
                "can be searched next."
            ),
            session_code=selected_config.target_session_code,
            reason_code="waiting_for_calendar",
            resource_label=discover_resource,
        )
    elif target_session is None:
        discover_step = _step_payload(
            key="discover_target_markets",
            label=f"Find {target_name} markets",
            status="blocked",
            detail=(
                f"{target_name} session details are unavailable, so market "
                "discovery cannot start."
            ),
            session_code=selected_config.target_session_code,
            reason_code="missing_target_session",
            resource_label=discover_resource,
        )
    elif target_mapping_count > 0:
        discover_step = _step_payload(
            key="discover_target_markets",
            label=f"Find {target_name} markets",
            status="completed",
            detail=f"{target_mapping_count} {target_name} markets are already linked.",
            session_code=selected_config.target_session_code,
            session_key=target_session.session_key,
            count=target_mapping_count,
            reason_code="already_loaded",
            resource_label=discover_resource,
        )
    else:
        discover_step = _step_payload(
            key="discover_target_markets",
            label=f"Find {target_name} markets",
            status="ready",
            detail=f"{target_name} markets have not been found yet. Discovery can run now.",
            session_code=selected_config.target_session_code,
            session_key=target_session.session_key,
            reason_code="ready_to_discover",
            resource_label=discover_resource,
        )

    blockers = [
        step["detail"]
        for step in (hydrate_step, settle_step, discover_step)
        if step["status"] == "blocked"
    ]
    if blockers:
        run_step = _step_payload(
            key="run_paper_trade",
            label="Run paper trading",
            status="blocked",
            detail=blockers[0],
            reason_code="blocked_by_prerequisite",
            resource_label=run_resource,
        )
    else:
        latest_detail = (
            f"A previous run already exists ({latest_paper_session.id[:8]}...)."
            if latest_paper_session is not None
            else "No paper-trading run exists for this stage yet."
        )
        run_step = _step_payload(
            key="run_paper_trade",
            label="Run paper trading",
            status="ready",
            detail=(
                "All prerequisites are complete. "
                f"You can run paper trading now. {latest_detail}"
            ),
            reason_code="ready_to_run",
            resource_label=run_resource,
        )

    available_configs = [
        _config_payload(config)
        for config in GP_REGISTRY
        if (
            config.meeting_key == selected_config.meeting_key
            and config.season == selected_config.season
        )
    ]
    available_configs.sort(
        key=lambda config: (config["stage_rank"], config["short_code"])
    )
    primary_action = _primary_action_payload(
        config=selected_config,
        sync_step=sync_step,
        hydrate_step=hydrate_step,
        settle_step=settle_step,
        discover_step=discover_step,
        run_step=run_step,
        latest_paper_session=latest_paper_session,
    )

    return {
        "now": now,
        "auto_selected_gp_short_code": auto_config.short_code,
        "selected_gp_short_code": selected_config.short_code,
        "selected_config": _config_payload(selected_config),
        "available_configs": available_configs,
        "meeting": meeting,
        "focus_session": focus_session,
        "focus_status": focus_status,
        "timeline_completed_codes": timeline_completed_codes,
        "timeline_active_code": timeline_active_code,
        "source_session": source_session,
        "target_session": target_session,
        "latest_paper_session": latest_paper_session,
        "steps": [sync_step, hydrate_step, settle_step, discover_step, run_step],
        "blockers": blockers,
        "ready_to_run": not blockers,
        "primary_action_title": primary_action["primary_action_title"],
        "primary_action_description": primary_action["primary_action_description"],
        "primary_action_cta": primary_action["primary_action_cta"],
        "explanation": _config_explanation(selected_config),
    }


def run_gp_paper_trade_pipeline(
    ctx: PipelineContext,
    *,
    config: GPConfig,
    snapshot_id: str | None = None,
    baseline: str = "hybrid",
    min_edge: float = 0.05,
    bet_size: float = 10.0,
    max_daily_loss: float = 100.0,
) -> dict[str, Any]:
    import polars as pl

    ensure_default_feature_registry(ctx)

    if snapshot_id is None:
        snap_result = build_snapshot(
            ctx,
            config,
            meeting_key=config.meeting_key,
            season=config.season,
            entry_offset_min=config.entry_offset_min,
            fidelity=config.fidelity,
        )
        used_snapshot_id = snap_result.get("snapshot_id")
        if not used_snapshot_id:
            raise ValueError(f"Snapshot build failed: {snap_result}")
    else:
        used_snapshot_id = snapshot_id

    baseline_result = run_baseline(
        ctx,
        config,
        snapshot_id=used_snapshot_id,
        min_edge=min_edge,
    )
    model_run_ids: list[str] = baseline_result.get("model_runs", [])
    used_model_run_id, resolved_baseline = select_model_run_id(
        config,
        model_run_ids,
        baseline=baseline,
    )

    predictions = ctx.db.scalars(
        select(ModelPrediction).where(ModelPrediction.model_run_id == used_model_run_id)
    ).all()
    snapshot = None if used_snapshot_id is None else ctx.db.get(FeatureSnapshot, used_snapshot_id)
    if not predictions:
        raise ValueError("No predictions found for model run")

    snapshot_df = (
        pl.read_parquet(snapshot.storage_path)
        if snapshot is not None and snapshot.storage_path
        else None
    )
    price_lookup: dict[str, float] = {}
    label_lookup: dict[str, bool] = {}
    if snapshot_df is not None:
        for row in snapshot_df.to_dicts():
            market_id = row.get("market_id")
            if market_id:
                price_lookup[market_id] = float(row.get("entry_yes_price", 0.5))
                label = row.get("label_yes")
                if label is not None:
                    label_lookup[market_id] = bool(int(label))

    engine = PaperTradingEngine(
        config=PaperTradeConfig(
            min_edge=min_edge,
            bet_size=bet_size,
            max_daily_loss=max_daily_loss,
        )
    )
    for prediction in predictions:
        market_price = price_lookup.get(prediction.market_id or "", 0.5)
        engine.evaluate_signal(
            market_id=prediction.market_id or "",
            token_id=prediction.token_id,
            model_prob=prediction.probability_yes or 0.5,
            market_price=market_price,
        )
    for market_id, outcome in label_lookup.items():
        engine.settle_position(market_id, outcome)

    summary = engine.summary()
    log_path = (
        ctx.settings.data_root
        / "reports"
        / "paper_trading"
        / f"{config.short_code}_{used_model_run_id}.json"
    )
    engine.save_log(log_path)
    pt_session_id = engine.persist(
        ctx.db,
        gp_slug=config.short_code,
        snapshot_id=used_snapshot_id,
        model_run_id=used_model_run_id,
        log_path=log_path,
    )
    return {
        "snapshot_id": used_snapshot_id,
        "model_run_id": used_model_run_id,
        "baseline": resolved_baseline,
        "pt_session_id": pt_session_id,
        "log_path": str(log_path),
        **summary,
    }


def execute_manual_live_paper_trade(
    ctx: PipelineContext,
    *,
    gp_short_code: str,
    market_id: str,
    token_id: str | None,
    model_run_id: str | None,
    snapshot_id: str | None,
    model_prob: float,
    market_price: float,
    observed_at_utc: datetime | None = None,
    observed_spread: float | None = None,
    source_event_type: str | None = None,
    min_edge: float = 0.05,
    max_spread: float | None = None,
    bet_size: float = 10.0,
) -> dict[str, Any]:
    config = get_gp_config(gp_short_code)
    market = ctx.db.get(PolymarketMarket, market_id)
    if market is None:
        raise KeyError(f"market_id={market_id} not found")
    if not 0.0 <= model_prob <= 1.0:
        raise ValueError("model_prob must be between 0 and 1")
    if not 0.0 <= market_price <= 1.0:
        raise ValueError("market_price must be between 0 and 1")
    if observed_spread is not None and not 0.0 <= observed_spread <= 1.0:
        raise ValueError("observed_spread must be between 0 and 1")
    if max_spread is not None and not 0.0 <= max_spread <= 1.0:
        raise ValueError("max_spread must be between 0 and 1")

    entry_ts = _ensure_utc(observed_at_utc or utc_now())
    if max_spread is not None:
        if observed_spread is None:
            return {
                "action": "execute-manual-live-paper-trade",
                "status": "skipped",
                "message": (
                    f"Skipped manual paper trade for {market.question}: "
                    "live spread is unavailable."
                ),
                "gp_short_code": config.short_code,
                "market_id": market.id,
                "pt_session_id": None,
                "signal_action": "skip",
                "quantity": None,
                "entry_price": None,
                "stake_cost": None,
                "market_price": market_price,
                "model_prob": model_prob,
                "edge": model_prob - market_price,
                "side_label": None,
                "reason": "spread_unavailable",
            }
        if observed_spread > max_spread:
            return {
                "action": "execute-manual-live-paper-trade",
                "status": "skipped",
                "message": (
                    f"Skipped manual paper trade for {market.question}: "
                    f"spread {observed_spread:.3f} exceeds max {max_spread:.3f}."
                ),
                "gp_short_code": config.short_code,
                "market_id": market.id,
                "pt_session_id": None,
                "signal_action": "skip",
                "quantity": None,
                "entry_price": None,
                "stake_cost": None,
                "market_price": market_price,
                "model_prob": model_prob,
                "edge": model_prob - market_price,
                "side_label": None,
                "reason": "spread_above_max",
            }
    engine = PaperTradingEngine(
        config=PaperTradeConfig(
            min_edge=min_edge,
            bet_size=bet_size,
        )
    )
    signal = engine.evaluate_signal(
        market_id=market.id,
        token_id=token_id,
        model_prob=model_prob,
        market_price=market_price,
        timestamp=entry_ts,
    )
    signal_action = str(signal.get("action") or "skip")
    edge = float(signal.get("edge", 0.0) or 0.0)
    quantity = float(signal["quantity"]) if signal.get("quantity") is not None else None
    entry_price = (
        float(signal["entry_price"]) if signal.get("entry_price") is not None else None
    )
    stake_cost = (
        quantity * entry_price
        if quantity is not None and entry_price is not None
        else None
    )
    side_label = {"buy_yes": "YES", "buy_no": "NO"}.get(signal_action)
    reason = str(signal.get("reason") or "")

    if signal_action == "skip":
        return {
            "action": "execute-manual-live-paper-trade",
            "status": "skipped",
            "message": f"Skipped manual paper trade for {market.question}: {reason}.",
            "gp_short_code": config.short_code,
            "market_id": market.id,
            "pt_session_id": None,
            "signal_action": signal_action,
            "quantity": quantity,
            "entry_price": entry_price,
            "stake_cost": stake_cost,
            "market_price": market_price,
            "model_prob": model_prob,
            "edge": edge,
            "side_label": side_label,
            "reason": reason,
        }

    timestamp_slug = entry_ts.strftime("%Y%m%dT%H%M%SZ")
    log_path = (
        ctx.settings.data_root
        / "reports"
        / "paper_trading"
        / "manual"
        / f"{config.short_code}_{market.id}_{timestamp_slug}.json"
    )
    engine.save_log(log_path)
    pt_session_id = engine.persist(
        ctx.db,
        gp_slug=config.short_code,
        snapshot_id=snapshot_id,
        model_run_id=model_run_id,
        log_path=log_path,
    )
    paper_session = ctx.db.get(PaperTradeSession, pt_session_id)
    if paper_session is not None:
        config_json = dict(paper_session.config_json or {})
        config_json.update(
            {
                "manual_trade": True,
                "manual_execution": True,
                "selected_market_id": market.id,
                "selected_market_question": market.question,
                "source_event_type": source_event_type,
                "observed_at_utc": entry_ts.isoformat(),
                "signal_action": signal_action,
                "side_label": side_label,
                "market_price": market_price,
                "model_prob": model_prob,
                "stake_cost": stake_cost,
                "observed_spread": observed_spread,
                "max_spread": max_spread,
            }
        )
        paper_session.config_json = config_json

    return {
        "action": "execute-manual-live-paper-trade",
        "status": "ok",
        "message": (
            f"Opened manual {side_label} paper trade for {market.question} "
            f"at {entry_price:.3f} with edge {edge:.3f}."
        ),
        "gp_short_code": config.short_code,
        "market_id": market.id,
        "pt_session_id": pt_session_id,
        "signal_action": signal_action,
        "quantity": quantity,
        "entry_price": entry_price,
        "stake_cost": stake_cost,
        "market_price": market_price,
        "model_prob": model_prob,
        "edge": edge,
        "side_label": side_label,
        "reason": reason,
    }


def run_weekend_cockpit(
    ctx: PipelineContext,
    *,
    gp_short_code: str | None = None,
    baseline: str = "hybrid",
    min_edge: float = 0.05,
    bet_size: float = 10.0,
    search_fallback: bool = True,
    discover_max_pages: int = 5,
) -> dict[str, Any]:
    status = get_weekend_cockpit_status(ctx, gp_short_code=gp_short_code)
    if not status["ready_to_run"]:
        raise ValueError("; ".join(status["blockers"]) or "Weekend cockpit is blocked")

    config = get_gp_config(status["selected_gp_short_code"])
    executed_steps: list[dict[str, Any]] = []
    settlement_result = {
        "settled_session_ids": [],
        "settled_gp_slugs": [],
        "settled_positions": 0,
        "manual_positions_settled": 0,
        "unresolved_positions": 0,
        "unresolved_session_ids": [],
        "winner_driver_id": None,
    }

    sync_step = next(step for step in status["steps"] if step["key"] == "sync_calendar")
    if sync_step["status"] == "completed":
        executed_steps.append(
            _step_payload(
                key="sync_calendar",
                label="Sync calendar",
                status="skipped",
                detail="Calendar already loaded.",
            )
        )
    else:
        sync_result = sync_f1_calendar(ctx, season=config.season)
        executed_steps.append(
            _step_payload(
                key="sync_calendar",
                label="Sync calendar",
                status="completed",
                detail=f"Calendar sync finished for season {config.season}.",
                count=int(sync_result.get("sessions", 0)),
            )
        )
        status = get_weekend_cockpit_status(ctx, gp_short_code=config.short_code)

    hydrate_step = next(step for step in status["steps"] if step["key"] == "hydrate_source_session")
    source_session = status["source_session"]
    if hydrate_step["status"] == "skipped":
        executed_steps.append(
            _step_payload(
                key="hydrate_source_session",
                label="Hydrate source session",
                status="skipped",
                detail=hydrate_step["detail"],
            )
        )
    elif hydrate_step["status"] == "completed":
        executed_steps.append(
            _step_payload(
                key="hydrate_source_session",
                label="Hydrate source session",
                status="skipped",
                detail=hydrate_step["detail"],
                session_code=hydrate_step["session_code"],
                session_key=hydrate_step["session_key"],
                count=hydrate_step["count"],
            )
        )
    else:
        if source_session is None:
            raise ValueError("Source session unavailable after calendar sync")
        hydrate_result = hydrate_f1_session(
            ctx,
            session_key=source_session.session_key,
            include_extended=True,
            include_heavy=source_session.session_code in {"Q", "SQ", "R"},
        )
        executed_steps.append(
            _step_payload(
                key="hydrate_source_session",
                label="Hydrate source session",
                status="completed",
                detail=f"Hydrated {source_session.session_code} session data.",
                session_code=source_session.session_code,
                session_key=source_session.session_key,
                count=int(hydrate_result.get("records_written", 0))
                if hydrate_result.get("records_written") is not None
                else None,
            )
        )
        status = get_weekend_cockpit_status(ctx, gp_short_code=config.short_code)
        hydrate_step = next(
            step for step in status["steps"] if step["key"] == "hydrate_source_session"
        )
        if hydrate_step["status"] != "completed":
            raise ValueError(hydrate_step["detail"])

    settle_step = next(step for step in status["steps"] if step["key"] == "settle_finished_stage")
    source_session = status["source_session"]
    if settle_step["status"] == "skipped":
        executed_steps.append(
            _step_payload(
                key="settle_finished_stage",
                label="Settle finished stage",
                status="skipped",
                detail=settle_step["detail"],
                session_code=settle_step["session_code"],
                session_key=settle_step["session_key"],
                count=settle_step["count"],
            )
        )
    else:
        if source_session is None:
            raise ValueError("Source session unavailable before settlement")
        settlement_result = settle_paper_trade_sessions_for_completed_session(
            ctx,
            completed_session=source_session,
            meeting=status["meeting"],
        )
        unresolved_positions = int(settlement_result["unresolved_positions"])
        settlement_detail = (
            f"Settled {settlement_result['settled_positions']} prior tickets across "
            f"{len(settlement_result['settled_session_ids'])} runs."
        )
        if unresolved_positions:
            settlement_detail += (
                f" {unresolved_positions} manual ticket(s) remain open because the "
                "driver mapping could not be resolved automatically."
            )
        executed_steps.append(
            _step_payload(
                key="settle_finished_stage",
                label="Settle finished stage",
                status="completed",
                detail=settlement_detail,
                session_code=source_session.session_code,
                session_key=source_session.session_key,
                count=int(settlement_result["settled_positions"]),
            )
        )
        status = get_weekend_cockpit_status(ctx, gp_short_code=config.short_code)
        settle_step = next(
            step for step in status["steps"] if step["key"] == "settle_finished_stage"
        )
        if settle_step["status"] == "blocked":
            raise ValueError(settle_step["detail"])

    discover_step = next(
        step for step in status["steps"] if step["key"] == "discover_target_markets"
    )
    target_session = status["target_session"]
    if discover_step["status"] == "completed":
        executed_steps.append(
            _step_payload(
                key="discover_target_markets",
                label="Discover target markets",
                status="skipped",
                detail=discover_step["detail"],
                session_code=discover_step["session_code"],
                session_key=discover_step["session_key"],
                count=discover_step["count"],
            )
        )
    else:
        if target_session is None:
            raise ValueError("Target session unavailable after calendar sync")
        discovery_result = discover_session_polymarket(
            ctx,
            session_key=target_session.session_key,
            max_pages=discover_max_pages,
            search_fallback=search_fallback,
        )
        executed_steps.append(
            _step_payload(
                key="discover_target_markets",
                label="Discover target markets",
                status="completed",
                detail=(
                    f"Discovered Polymarket markets for {target_session.session_code}."
                ),
                session_code=target_session.session_code,
                session_key=target_session.session_key,
                count=int(discovery_result.get("auto_mappings", 0))
                if discovery_result.get("auto_mappings") is not None
                else None,
            )
        )
        status = get_weekend_cockpit_status(ctx, gp_short_code=config.short_code)
        discover_step = next(
            step for step in status["steps"] if step["key"] == "discover_target_markets"
        )
        if discover_step["status"] != "completed":
            raise ValueError(discover_step["detail"])

    if status["blockers"]:
        raise ValueError("; ".join(status["blockers"]))

    paper_result = run_gp_paper_trade_pipeline(
        ctx,
        config=config,
        baseline=baseline,
        min_edge=min_edge,
        bet_size=bet_size,
    )
    executed_steps.append(
        _step_payload(
            key="run_paper_trade",
            label="Run paper trade",
            status="completed",
            detail=(
                f"Paper trade complete. Trades: {paper_result['trades_executed']}, "
                f"PnL: ${paper_result['total_pnl']:.2f}"
            ),
            count=int(paper_result["trades_executed"]),
        )
    )
    message = (
        f"Weekend cockpit complete for {config.name} ({config.short_code}). "
        f"Trades: {paper_result['trades_executed']}, "
        f"PnL: ${paper_result['total_pnl']:.2f}"
    )
    if settlement_result["settled_positions"]:
        message += (
            f" Settled {settlement_result['settled_positions']} prior ticket(s)"
        )
        if settlement_result["manual_positions_settled"]:
            message += (
                f", including {settlement_result['manual_positions_settled']} manual ticket(s)"
            )
        message += "."
    if settlement_result["unresolved_positions"]:
        message += (
            f" {settlement_result['unresolved_positions']} manual ticket(s) remain open "
            "because the driver mapping was unresolved."
        )
    details = dict(paper_result)
    details["settlement"] = settlement_result

    return {
        "action": "run-weekend-cockpit",
        "status": "ok",
        "message": message,
        "gp_short_code": config.short_code,
        "snapshot_id": paper_result["snapshot_id"],
        "model_run_id": paper_result["model_run_id"],
        "pt_session_id": paper_result["pt_session_id"],
        "executed_steps": executed_steps,
        "details": details,
    }


def _latest_ended_session_for_meeting(
    ctx: PipelineContext,
    *,
    meeting_id: str,
    now: Any,
) -> F1Session | None:
    sessions = ctx.db.scalars(
        select(F1Session)
        .where(F1Session.meeting_id == meeting_id)
        .order_by(F1Session.date_end_utc.desc(), F1Session.session_key.desc())
    ).all()
    ended_sessions = [session for session in sessions if _session_has_ended(session, now=now)]
    if not ended_sessions:
        return None
    return ended_sessions[0]


def _linked_market_ids_for_session(
    ctx: PipelineContext,
    *,
    session_id: str,
) -> set[str]:
    return {
        market_id
        for market_id in ctx.db.scalars(
            select(EntityMappingF1ToPolymarket.polymarket_market_id).where(
                EntityMappingF1ToPolymarket.f1_session_id == session_id,
                EntityMappingF1ToPolymarket.polymarket_market_id.is_not(None),
            )
        ).all()
        if market_id is not None
    }


def _refresh_artifacts_for_session(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    session_code: str | None,
) -> list[dict[str, Any]]:
    if session_code is None:
        return []

    from f1_polymarket_worker.backtest import backfill_backtests

    updates: list[dict[str, Any]] = []
    for config in GP_REGISTRY:
        if config.meeting_key != meeting_key:
            continue
        if config.target_session_code != session_code:
            continue

        result = backfill_backtests(
            ctx,
            gp_short_code=config.short_code,
            rebuild_missing=True,
        )
        for item in result.get("processed", []):
            updates.append(
                {
                    "gp_short_code": item["gp_short_code"],
                    "status": "processed",
                    "snapshot_id": item.get("snapshot_id"),
                    "rebuilt_snapshot": bool(item.get("rebuilt_snapshot", False)),
                    "bet_count": item.get("bet_count"),
                    "total_pnl": item.get("total_pnl"),
                    "reason": None,
                }
            )
        for item in result.get("skipped", []):
            updates.append(
                {
                    "gp_short_code": item["gp_short_code"],
                    "status": "skipped",
                    "snapshot_id": item.get("snapshot_id"),
                    "rebuilt_snapshot": False,
                    "bet_count": None,
                    "total_pnl": None,
                    "reason": item.get("reason"),
                }
            )

    return updates


def refresh_latest_session_for_meeting(
    ctx: PipelineContext,
    *,
    meeting_id: str,
    search_fallback: bool = True,
    discover_max_pages: int = 5,
    hydrate_market_history: bool = True,
    market_history_fidelity: int = 60,
    sync_calendar: bool = True,
    hydrate_f1_session_data: bool = True,
    include_extended_f1_data: bool = True,
    include_heavy_f1_data: bool = True,
    refresh_artifacts: bool = True,
) -> dict[str, Any]:
    meeting = ctx.db.get(F1Meeting, meeting_id)
    if meeting is None:
        raise KeyError(f"Meeting not found: {meeting_id}")

    if sync_calendar:
        sync_f1_calendar(ctx, season=meeting.season)
        refreshed_meeting = ctx.db.scalar(
            select(F1Meeting).where(
                F1Meeting.meeting_key == meeting.meeting_key,
                F1Meeting.season == meeting.season,
            )
        )
    else:
        refreshed_meeting = meeting
    if refreshed_meeting is None:
        raise KeyError(
            "Meeting missing after calendar sync: "
            f"season={meeting.season} meeting_key={meeting.meeting_key}"
        )

    now = _ensure_utc(utc_now())
    refreshed_session = _latest_ended_session_for_meeting(
        ctx,
        meeting_id=refreshed_meeting.id,
        now=now,
    )
    if refreshed_session is None:
        raise ValueError(
            f"No ended sessions are available yet for {refreshed_meeting.meeting_name}."
        )

    linked_market_ids_before = _linked_market_ids_for_session(ctx, session_id=refreshed_session.id)
    hydrate_result = {"records_written": 0, "status": "skipped"}
    if hydrate_f1_session_data:
        hydrate_result = hydrate_f1_session(
            ctx,
            session_key=refreshed_session.session_key,
            include_extended=include_extended_f1_data,
            include_heavy=include_heavy_f1_data,
        )
    exact_slug_candidate_limit = 64 if (
        refreshed_session.session_code == "R"
        and not search_fallback
        and discover_max_pages <= 1
        and not hydrate_f1_session_data
    ) else None
    discover_result = discover_session_polymarket(
        ctx,
        session_key=refreshed_session.session_key,
        max_pages=discover_max_pages,
        search_fallback=search_fallback,
        exact_slug_candidate_limit=exact_slug_candidate_limit,
    )
    reconcile_mappings(ctx)
    linked_market_ids_after = _linked_market_ids_for_session(ctx, session_id=refreshed_session.id)

    markets_hydrated = 0
    if hydrate_market_history:
        for market_id in sorted(linked_market_ids_after):
            hydrate_polymarket_market(
                ctx,
                market_id=market_id,
                fidelity=market_history_fidelity,
            )
            markets_hydrated += 1

    session_label = refreshed_session.session_code or refreshed_session.session_name
    artifact_updates: list[dict[str, Any]] = []
    if refresh_artifacts:
        artifact_updates = _refresh_artifacts_for_session(
            ctx,
            meeting_key=refreshed_meeting.meeting_key,
            session_code=refreshed_session.session_code,
        )
    processed_artifacts = sum(1 for item in artifact_updates if item["status"] == "processed")
    skipped_artifacts = sum(1 for item in artifact_updates if item["status"] == "skipped")
    artifact_message = (
        f"Artifacts refreshed: {processed_artifacts}, skipped: {skipped_artifacts}."
        if refresh_artifacts
        else "Artifacts refresh skipped."
    )
    return {
        "action": "refresh-latest-session",
        "status": "ok",
        "message": (
            f"Updated latest ended session {session_label} for "
            f"{refreshed_meeting.meeting_name}. "
            f"{artifact_message}"
        ),
        "meeting_id": refreshed_meeting.id,
        "meeting_name": refreshed_meeting.meeting_name,
        "refreshed_session": {
            "id": refreshed_session.id,
            "session_key": refreshed_session.session_key,
            "session_code": refreshed_session.session_code,
            "session_name": refreshed_session.session_name,
            "date_end_utc": refreshed_session.date_end_utc,
        },
        "f1_records_written": int(hydrate_result.get("records_written", 0) or 0),
        "markets_discovered": int(discover_result.get("markets", 0) or 0),
        "mappings_written": len(linked_market_ids_after - linked_market_ids_before),
        "markets_hydrated": markets_hydrated,
        "artifacts_refreshed": artifact_updates,
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


def _sorted_count_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": int(count)}
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _normalize_live_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_live_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _best_book_price(levels: Any, *, side: str) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = [
        price
        for price in (
            _normalize_live_float(level.get("price")) if isinstance(level, dict) else None
            for level in levels
        )
        if price is not None
    ]
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


def _live_payload_timestamp(payload: dict[str, Any], fallback: datetime) -> datetime:
    for key in ("timestamp", "timestamp_ms", "ts", "ts_ms"):
        value = _normalize_live_float(payload.get(key))
        if value is None:
            continue
        try:
            if value > 10_000_000_000:
                value = value / 1000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            continue
    parsed = parse_dt(
        payload.get("observed_at")
        or payload.get("observedAt")
        or payload.get("created_at")
        or payload.get("createdAt")
    )
    return _ensure_utc(parsed or fallback)


def _pick_live_payload_value(
    primary: dict[str, Any],
    fallback: dict[str, Any],
    *keys: str,
) -> Any:
    for key in keys:
        if key in primary and primary.get(key) is not None:
            return primary.get(key)
        if key in fallback and fallback.get(key) is not None:
            return fallback.get(key)
    return None


def _preferred_live_token_ids(tokens: list[PolymarketToken]) -> dict[str, str]:
    preferred: dict[str, PolymarketToken] = {}
    for token in tokens:
        outcome = (token.outcome or "").strip().lower()
        priority = 0 if outcome == "yes" or token.outcome_index == 0 else 1
        current = preferred.get(token.market_id)
        if current is None:
            preferred[token.market_id] = token
            continue
        current_outcome = (current.outcome or "").strip().lower()
        current_priority = 0 if current_outcome == "yes" or current.outcome_index == 0 else 1
        if priority < current_priority:
            preferred[token.market_id] = token
    return {market_id: token.id for market_id, token in preferred.items()}


def _build_live_market_quote(
    *,
    payload: dict[str, Any],
    item: dict[str, Any],
    observed_at: datetime,
    event_type: str,
    token_to_market_id: dict[str, str],
    token_outcomes: dict[str, str | None],
    condition_to_market_id: dict[str, str],
) -> dict[str, Any] | None:
    token_id = _normalize_live_text(
        _pick_live_payload_value(item, payload, "asset_id", "assetId", "token_id", "tokenId")
    )
    condition_id = _normalize_live_text(
        _pick_live_payload_value(item, payload, "market", "condition_id", "conditionId")
    )
    market_id = (
        token_to_market_id.get(token_id) if token_id is not None else None
    ) or (
        condition_to_market_id.get(condition_id) if condition_id is not None else None
    )
    if market_id is None:
        return None

    best_bid = _normalize_live_float(
        _pick_live_payload_value(item, payload, "best_bid", "bestBid")
    )
    best_ask = _normalize_live_float(
        _pick_live_payload_value(item, payload, "best_ask", "bestAsk")
    )
    book_bid = _best_book_price(
        _pick_live_payload_value(item, payload, "bids"),
        side="bid",
    )
    book_ask = _best_book_price(
        _pick_live_payload_value(item, payload, "asks"),
        side="ask",
    )
    if book_bid is not None:
        best_bid = book_bid
    if book_ask is not None:
        best_ask = book_ask

    midpoint = _normalize_live_float(
        _pick_live_payload_value(item, payload, "midpoint", "midPoint", "mid_price", "midPrice")
    )
    if midpoint is None and best_bid is not None and best_ask is not None:
        midpoint = (best_bid + best_ask) / 2

    price = _normalize_live_float(
        _pick_live_payload_value(
            item,
            payload,
            "price",
            "last_trade_price",
            "lastTradePrice",
        )
    )
    if price is None:
        price = midpoint

    if price is None and best_bid is None and best_ask is None and midpoint is None:
        return None

    spread = (
        best_ask - best_bid
        if best_bid is not None and best_ask is not None
        else None
    )
    return {
        "market_id": market_id,
        "token_id": token_id,
        "outcome": token_outcomes.get(token_id) if token_id is not None else None,
        "event_type": event_type,
        "observed_at_utc": _live_payload_timestamp(item, observed_at).isoformat(),
        "price": price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": midpoint,
        "spread": spread,
        "size": _normalize_live_float(
            _pick_live_payload_value(item, payload, "size", "amount", "quantity")
        ),
        "side": _normalize_live_text(_pick_live_payload_value(item, payload, "side")),
    }


def _extract_live_market_quotes(
    *,
    payload: dict[str, Any],
    observed_at: datetime,
    token_to_market_id: dict[str, str],
    token_outcomes: dict[str, str | None],
    condition_to_market_id: dict[str, str],
) -> list[dict[str, Any]]:
    event_type = _normalize_live_text(payload.get("event_type") or payload.get("type")) or "unknown"
    price_changes = payload.get("price_changes")
    if isinstance(price_changes, list):
        quotes = [
            quote
            for quote in (
                _build_live_market_quote(
                    payload=payload,
                    item=item,
                    observed_at=observed_at,
                    event_type=event_type,
                    token_to_market_id=token_to_market_id,
                    token_outcomes=token_outcomes,
                    condition_to_market_id=condition_to_market_id,
                )
                for item in price_changes
                if isinstance(item, dict)
            )
            if quote is not None
        ]
        if quotes:
            return quotes
    quote = _build_live_market_quote(
        payload=payload,
        item=payload,
        observed_at=observed_at,
        event_type=event_type,
        token_to_market_id=token_to_market_id,
        token_outcomes=token_outcomes,
        condition_to_market_id=condition_to_market_id,
    )
    return [] if quote is None else [quote]


def _should_replace_live_market_quote(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
    *,
    preferred_token_id: str | None,
) -> bool:
    if current is None:
        return True

    def quote_priority(quote: dict[str, Any]) -> int:
        token_id = _normalize_live_text(quote.get("token_id"))
        if preferred_token_id is None:
            return 0
        return 0 if token_id == preferred_token_id else 1

    candidate_priority = quote_priority(candidate)
    current_priority = quote_priority(current)
    if candidate_priority != current_priority:
        return candidate_priority < current_priority

    candidate_observed_at = _ensure_utc(
        parse_dt(candidate.get("observed_at_utc")) or datetime.min.replace(tzinfo=timezone.utc)
    )
    current_observed_at = _ensure_utc(
        parse_dt(current.get("observed_at_utc")) or datetime.min.replace(tzinfo=timezone.utc)
    )
    if candidate_observed_at != current_observed_at:
        return candidate_observed_at > current_observed_at

    def completeness(quote: dict[str, Any]) -> int:
        return sum(
            1
            for key in ("price", "best_bid", "best_ask", "midpoint", "spread", "size", "side")
            if quote.get(key) is not None
        )

    return completeness(candidate) > completeness(current)


def capture_live_weekend(
    ctx: PipelineContext,
    *,
    session_key: int,
    market_ids: list[str] | None = None,
    start_buffer_min: int = 15,
    stop_buffer_min: int = 15,
    openf1_topics: tuple[str, ...] = DEFAULT_OPENF1_TOPICS,
    message_limit: int | None = None,
    capture_seconds: int | None = None,
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
            "capture_seconds": capture_seconds,
            "message_limit": message_limit,
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
    if capture_seconds is not None:
        if now < start_at or now > stop_at:
            detail = (
                f"{session.session_name} live capture is available only between "
                f"{start_at.isoformat()} and {stop_at.isoformat()}."
            )
            finish_job_run(
                ctx.db,
                run,
                status="failed",
                error_message=detail,
            )
            raise ValueError(detail)
        resolved_capture_seconds = max(float(capture_seconds), 1.0)
    else:
        if now < start_at:
            time.sleep((start_at - now).total_seconds())
        resolved_capture_seconds = max((stop_at - _ensure_utc(utc_now())).total_seconds(), 1.0)

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
        stop_after_seconds=resolved_capture_seconds,
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
    openf1_topic_counts = Counter(
        {
            topic: len(payloads)
            for topic, payloads in openf1_payloads.items()
            if payloads
        }
    )

    live_market_ids = _derive_live_market_ids(ctx, session=session, requested_market_ids=market_ids)
    polymarket_tokens = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id.in_(live_market_ids))
    ).all()
    token_ids = [token.id for token in polymarket_tokens]
    token_to_market_id = {token.id: token.market_id for token in polymarket_tokens}
    token_outcomes = {token.id: token.outcome for token in polymarket_tokens}
    preferred_token_ids = _preferred_live_token_ids(polymarket_tokens)
    condition_to_market_id = {
        market.condition_id: market.id
        for market in ctx.db.scalars(
            select(PolymarketMarket).where(PolymarketMarket.id.in_(live_market_ids))
        ).all()
    }
    polymarket_event_type_counts: Counter[str] = Counter()
    observed_market_ids: set[str] = set()
    observed_token_ids: set[str] = set()
    live_market_quotes: dict[str, dict[str, Any]] = {}
    if token_ids:
        ws_messages: list[dict[str, Any]] = []
        ws_connector = PolymarketLiveConnector()

        def _handle_polymarket(message: Any) -> None:
            payload = message.payload
            if not isinstance(payload, dict):
                return
            event_type = str(payload.get("event_type") or payload.get("type") or "unknown")
            polymarket_event_type_counts[event_type] += 1
            asset_id = payload.get("asset_id")
            if asset_id is not None:
                observed_token_ids.add(str(asset_id))
            condition_id = payload.get("market")
            if condition_id is not None:
                market_id = condition_to_market_id.get(str(condition_id))
                if market_id is not None:
                    observed_market_ids.add(str(market_id))
            for quote in _extract_live_market_quotes(
                payload=payload,
                observed_at=message.observed_at,
                token_to_market_id=token_to_market_id,
                token_outcomes=token_outcomes,
                condition_to_market_id=condition_to_market_id,
            ):
                market_id = str(quote["market_id"])
                observed_market_ids.add(market_id)
                token_id = _normalize_live_text(quote.get("token_id"))
                if token_id is not None:
                    observed_token_ids.add(token_id)
                current_quote = live_market_quotes.get(market_id)
                if _should_replace_live_market_quote(
                    current_quote,
                    quote,
                    preferred_token_id=preferred_token_ids.get(market_id),
                ):
                    live_market_quotes[market_id] = quote
            ws_messages.append(
                {
                    "observed_at": message.observed_at.isoformat(),
                    "payload": payload,
                }
            )

        ws_count = ws_connector.stream_market_messages(
            asset_ids=token_ids,
            on_message=_handle_polymarket,
            stop_after_seconds=resolved_capture_seconds,
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
    else:
        ws_count = 0

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=records_written,
    )
    market_count = len(live_market_ids)
    duration_seconds = int(round(resolved_capture_seconds))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "message": (
            f"Captured {duration_seconds}s of live data for {session.session_name} "
            f"across {market_count} market(s)."
        ),
        "session_key": session_key,
        "capture_seconds": duration_seconds,
        "openf1_messages": openf1_message_count,
        "polymarket_messages": ws_count,
        "market_count": market_count,
        "polymarket_market_ids": live_market_ids,
        "records_written": records_written,
        "summary": {
            "openf1_topics": _sorted_count_rows(openf1_topic_counts),
            "polymarket_event_types": _sorted_count_rows(polymarket_event_type_counts),
            "observed_market_count": len(observed_market_ids),
            "observed_token_count": len(observed_token_ids),
            "market_quotes": [
                live_market_quotes[market_id]
                for market_id in sorted(live_market_quotes)
            ],
        },
    }
