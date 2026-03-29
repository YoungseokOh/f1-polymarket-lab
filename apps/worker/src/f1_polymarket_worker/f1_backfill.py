from __future__ import annotations

from datetime import timedelta
from typing import Any

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    PolymarketEvent,
    PolymarketMarket,
)
from sqlalchemy import select

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
from f1_polymarket_worker.market_discovery import (
    _ensure_utc,
    _event_looks_f1,
    _market_session_delta_days,
)
from f1_polymarket_worker.pipeline import (
    MODERN_WEEKEND_SESSION_CODES,
    PipelineContext,
    ensure_default_feature_registry,
    hydrate_f1_session,
    hydrate_polymarket_market,
    sync_f1_calendar,
)

HEAVY_MODES = frozenset({"weekend", "none"})
VALIDATION_MODES = frozenset({"smoke", "full"})
SMOKE_VALIDATION_HEAVY_SESSION_CODES = frozenset({"Q", "SQ", "R"})


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
    linked_markets_only: bool = False,
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
            "linked_markets_only": linked_markets_only,
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
    sessions_filtered_no_markets = 0
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
        eligible_meeting_ids: set[str] | None = None
        if linked_markets_only:
            eligible_meeting_ids = {
                session.meeting_id
                for session in season_sessions
                if session.meeting_id is not None and _session_has_linked_markets(ctx, session)
            }
        for session in season_sessions:
            if (
                session.date_end_utc is None
                or _ensure_utc(session.date_end_utc) > historical_cutoff
            ):
                sessions_skipped += 1
                continue
            if linked_markets_only and (
                session.meeting_id is None
                or eligible_meeting_ids is None
                or session.meeting_id not in eligible_meeting_ids
            ):
                sessions_filtered_no_markets += 1
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
            "sessions_filtered_no_markets": sessions_filtered_no_markets,
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
            "sessions_filtered_no_markets": sessions_filtered_no_markets,
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
        "sessions_filtered_no_markets": sessions_filtered_no_markets,
    }


def backfill_f1_history_all(
    ctx: PipelineContext,
    *,
    season_start: int = 1950,
    season_end: int | None = None,
    include_extended: bool = True,
    heavy_mode: str = "weekend",
    jolpica_resources: tuple[str, ...] = JOLPICA_DEFAULT_RESOURCES,
    linked_markets_only: bool = False,
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
            "linked_markets_only": linked_markets_only,
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
            linked_markets_only=linked_markets_only,
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
