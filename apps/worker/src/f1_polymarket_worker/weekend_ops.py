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
    FeatureSnapshot,
    MappingCandidate,
    ModelPrediction,
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
        f"This stage uses {source_name} results to find {target_name} markets and, "
        "when ready, continue into paper trading."
    )


def _primary_action_payload(
    *,
    config: GPConfig,
    sync_step: dict[str, Any],
    hydrate_step: dict[str, Any],
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
            "primary_action_title": "Load weekend schedule",
            "primary_action_description": (
                "This will load the current Grand Prix schedule first, "
                "then continue the remaining preparation steps."
            ),
            "primary_action_cta": "Load weekend schedule",
        }
    if hydrate_step["status"] == "ready":
        return {
            "primary_action_title": f"Load {source_name} results",
            "primary_action_description": (
                f"This will load {source_name} results first, then prepare {target_name} markets."
            ),
            "primary_action_cta": f"Load {source_name} results",
        }
    if discover_step["status"] == "ready":
        return {
            "primary_action_title": f"Find {target_name} markets",
            "primary_action_description": (
                f"This will discover {target_name} markets first, then continue into paper trading."
            ),
            "primary_action_cta": f"Find {target_name} markets",
        }
    rerun_label = (
        "Run this stage again"
        if latest_paper_session is not None
        else "Run paper trading"
    )
    return {
        "primary_action_title": f"{config_display_label(config)} is ready",
        "primary_action_description": (
            "All prerequisites are complete. You can run paper trading now."
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
        for step in (hydrate_step, discover_step)
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
        "steps": [sync_step, hydrate_step, discover_step, run_step],
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

    return {
        "action": "run-weekend-cockpit",
        "status": "ok",
        "message": (
            f"Weekend cockpit complete for {config.name} ({config.short_code}). "
            f"Trades: {paper_result['trades_executed']}, "
            f"PnL: ${paper_result['total_pnl']:.2f}"
        ),
        "gp_short_code": config.short_code,
        "snapshot_id": paper_result["snapshot_id"],
        "model_run_id": paper_result["model_run_id"],
        "pt_session_id": paper_result["pt_session_id"],
        "executed_steps": executed_steps,
        "details": paper_result,
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
