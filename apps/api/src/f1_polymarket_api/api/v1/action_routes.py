from __future__ import annotations

import logging
from typing import Any, NoReturn, cast

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    ActionStatusResponse,
    BackfillBacktestsRequest,
    CancelLiveTradeTicketRequest,
    CancelLiveTradeTicketResponse,
    CaptureLiveWeekendCountResponse,
    CaptureLiveWeekendMarketQuoteResponse,
    CaptureLiveWeekendRequest,
    CaptureLiveWeekendResponse,
    CaptureLiveWeekendSummaryResponse,
    ClearCalendarOverrideRequest,
    CreateLiveTradeTicketRequest,
    CreateLiveTradeTicketResponse,
    ExecuteManualLivePaperTradeRequest,
    ExecuteManualLivePaperTradeResponse,
    IngestDemoRequest,
    RecordLiveTradeFillRequest,
    RecordLiveTradeFillResponse,
    RefreshDriverAffinityRequest,
    RefreshDriverAffinityResponse,
    RefreshLatestSessionRequest,
    RefreshLatestSessionResponse,
    RunBacktestRequest,
    RunPaperTradeRequest,
    RunWeekendCockpitRequest,
    RunWeekendCockpitResponse,
    SetCalendarOverrideRequest,
    SyncCalendarRequest,
    SyncF1MarketsRequest,
)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from sqlalchemy.orm import Session

action_router = APIRouter(prefix="/api/v1", tags=["actions"])

log = logging.getLogger(__name__)

_WRITE_CONFLICT_DETAIL = (
    "Another write action is already in progress or the database is temporarily busy. "
    "Wait for it to finish, then retry."
)
_WRITE_CONFLICT_MARKERS = (
    "database is locked",
    "database table is locked",
    "deadlock detected",
    "could not serialize access due to concurrent update",
    "canceling statement due to lock timeout",
)


def _rollback_quietly(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        log.debug("rollback failed while handling action error", exc_info=True)


def _is_retryable_write_conflict(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()

    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if any(marker in str(current).lower() for marker in _WRITE_CONFLICT_MARKERS):
            return True
        if isinstance(current, SQLAlchemyOperationalError) and current.orig is not None:
            current = current.orig
            continue
        current = current.__cause__ or current.__context__

    return False


def _raise_action_error(
    db: Session,
    *,
    exc: Exception,
    log_message: str,
    key_error_status: int | None = None,
    value_error_status: int | None = None,
) -> NoReturn:
    _rollback_quietly(db)

    if isinstance(exc, HTTPException):
        raise exc

    if key_error_status is not None and isinstance(exc, KeyError):
        detail = str(exc.args[0]) if exc.args else str(exc)
        raise HTTPException(status_code=key_error_status, detail=detail) from exc

    if value_error_status is not None and isinstance(exc, ValueError):
        raise HTTPException(status_code=value_error_status, detail=str(exc)) from exc

    if _is_retryable_write_conflict(exc):
        log.warning("%s", log_message, exc_info=True)
        raise HTTPException(status_code=409, detail=_WRITE_CONFLICT_DETAIL) from exc

    log.exception(log_message)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


def _queue_action_response(
    db: Session,
    *,
    action: str,
    job_name: str,
    planned_inputs: dict[str, object],
    message: str,
    max_attempts: int | None = None,
) -> ActionStatusResponse:
    from f1_polymarket_worker.job_queue import enqueue_job

    run = enqueue_job(
        db,
        job_name=job_name,
        planned_inputs=dict(planned_inputs),
        max_attempts=max_attempts,
    )
    db.commit()
    return ActionStatusResponse(
        action=action,
        status="queued",
        message=message,
        job_run_id=run.id,
        details={
            "queued": True,
            "job_run_id": run.id,
            "job_name": run.job_name,
            "planned_inputs": run.planned_inputs or {},
            "max_attempts": run.max_attempts,
        },
    )


@action_router.post("/actions/ingest-demo", response_model=ActionStatusResponse)
def action_ingest_demo(
    body: IngestDemoRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    try:
        return _queue_action_response(
            db,
            action="ingest-demo",
            job_name="ingest-demo",
            planned_inputs={
                "season": body.season,
                "weekends": body.weekends,
                "market_batches": body.market_batches,
            },
            message=(
                "Demo ingestion queued "
                f"(season={body.season}, weekends={body.weekends}, "
                f"market_batches={body.market_batches})."
            ),
        )
    except Exception as exc:
        _raise_action_error(db, exc=exc, log_message="ingest-demo queue failed")


@action_router.post("/actions/sync-calendar", response_model=ActionStatusResponse)
def action_sync_calendar(
    body: SyncCalendarRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    try:
        return _queue_action_response(
            db,
            action="sync-calendar",
            job_name="sync-f1-calendar",
            planned_inputs={"season": body.season},
            message=f"Calendar sync queued for season {body.season}.",
        )
    except Exception as exc:
        _raise_action_error(db, exc=exc, log_message="sync-calendar queue failed")


@action_router.post("/actions/set-calendar-override", response_model=ActionStatusResponse)
def action_set_calendar_override(
    body: SetCalendarOverrideRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.ops_calendar import set_calendar_override

    try:
        override = set_calendar_override(
            db,
            season=body.season,
            meeting_slug=body.meeting_slug,
            status=body.status,
            ops_slug=body.ops_slug,
            effective_round_number=body.effective_round_number,
            effective_start_date_utc=body.effective_start_date_utc,
            effective_end_date_utc=body.effective_end_date_utc,
            effective_meeting_name=body.effective_meeting_name,
            effective_country_name=body.effective_country_name,
            effective_location=body.effective_location,
            source_label=body.source_label,
            source_url=body.source_url,
            note=body.note,
        )
        db.commit()
        return ActionStatusResponse(
            action="set-calendar-override",
            status="ok",
            message=(
                f"Calendar override saved for {override.season} {override.meeting_slug} "
                f"({override.status})."
            ),
            details={
                "season": override.season,
                "meeting_slug": override.meeting_slug,
                "status": override.status,
                "ops_slug": override.ops_slug,
                "source_url": override.source_url,
            },
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="set-calendar-override failed",
            value_error_status=409,
        )


@action_router.post("/actions/clear-calendar-override", response_model=ActionStatusResponse)
def action_clear_calendar_override(
    body: ClearCalendarOverrideRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.ops_calendar import clear_calendar_override

    try:
        override = clear_calendar_override(
            db,
            season=body.season,
            meeting_slug=body.meeting_slug,
        )
        db.commit()
        return ActionStatusResponse(
            action="clear-calendar-override",
            status="ok",
            message=(
                f"Calendar override cleared for {override.season} {override.meeting_slug}."
            ),
            details={
                "season": override.season,
                "meeting_slug": override.meeting_slug,
                "status": override.status,
                "is_active": override.is_active,
            },
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="clear-calendar-override failed",
            key_error_status=404,
        )


@action_router.post("/actions/run-backtest", response_model=ActionStatusResponse)
def action_run_backtest(
    body: RunBacktestRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.gp_registry import resolve_gp_config

    try:
        config = resolve_gp_config(body.gp_short_code, db=db)
        return _queue_action_response(
            db,
            action="run-backtest",
            job_name="run-backtest",
            planned_inputs={
                "gp_short_code": body.gp_short_code,
                "min_edge": body.min_edge,
                "bet_size": body.bet_size,
            },
            message=(
                f"Backtest queued for {config.name} ({config.short_code}). "
                "Track the job in lineage."
            ),
            max_attempts=1,
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="run-backtest queue failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post("/actions/backfill-backtests", response_model=ActionStatusResponse)
def action_backfill_backtests(
    body: BackfillBacktestsRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    try:
        scope = f" for {body.gp_short_code}" if body.gp_short_code else ""
        return _queue_action_response(
            db,
            action="backfill-backtests",
            job_name="backfill-backtests",
            planned_inputs={
                "gp_short_code": body.gp_short_code,
                "min_edge": body.min_edge,
                "bet_size": body.bet_size,
                "rebuild_missing": body.rebuild_missing,
            },
            message=f"Backtest backfill queued{scope}. Track the job in lineage.",
            max_attempts=1,
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="backfill-backtests queue failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post("/actions/sync-f1-markets", response_model=ActionStatusResponse)
def action_sync_f1_markets(
    body: SyncF1MarketsRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    try:
        return _queue_action_response(
            db,
            action="sync-f1-markets",
            job_name="sync-f1-markets",
            planned_inputs={
                "max_pages": body.max_pages,
                "search_fallback": body.search_fallback,
                "start_year": body.start_year,
                "end_year": body.end_year,
            },
            message="F1 market sync queued. Track the job in lineage.",
        )
    except Exception as exc:
        _raise_action_error(db, exc=exc, log_message="sync-f1-markets queue failed")


@action_router.post(
    "/actions/refresh-latest-session",
    response_model=RefreshLatestSessionResponse,
)
def action_refresh_latest_session(
    body: RefreshLatestSessionRequest,
    db: Session = Depends(get_db_session),
) -> RefreshLatestSessionResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import refresh_latest_session_for_meeting

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = refresh_latest_session_for_meeting(
            ctx,
            meeting_id=body.meeting_id,
            search_fallback=body.search_fallback,
            discover_max_pages=body.discover_max_pages,
            hydrate_market_history=body.hydrate_market_history,
            sync_calendar=body.sync_calendar,
            hydrate_f1_session_data=body.hydrate_f1_session_data,
            include_extended_f1_data=body.include_extended_f1_data,
            include_heavy_f1_data=body.include_heavy_f1_data,
            refresh_artifacts=body.refresh_artifacts,
        )
        db.commit()
        return RefreshLatestSessionResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="refresh-latest-session failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/capture-live-weekend",
    response_model=CaptureLiveWeekendResponse,
)
def action_capture_live_weekend(
    body: CaptureLiveWeekendRequest,
    db: Session = Depends(get_db_session),
) -> CaptureLiveWeekendResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import capture_live_weekend

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = capture_live_weekend(
            ctx,
            session_key=body.session_key,
            market_ids=body.market_ids,
            start_buffer_min=body.start_buffer_min,
            stop_buffer_min=body.stop_buffer_min,
            message_limit=body.message_limit,
            capture_seconds=body.capture_seconds,
        )
        db.commit()
        summary_raw = result.get("summary") or {}
        summary = cast(dict[str, Any], summary_raw if isinstance(summary_raw, dict) else {})
        return CaptureLiveWeekendResponse(
            action="capture-live-weekend",
            status="ok",
            message=str(result.get("message") or "Live capture complete."),
            job_run_id=str(result["job_run_id"]),
            session_key=int(result["session_key"]),
            capture_seconds=int(result["capture_seconds"]),
            openf1_messages=int(result.get("openf1_messages", 0) or 0),
            polymarket_messages=int(result.get("polymarket_messages", 0) or 0),
            market_count=int(result.get("market_count", 0) or 0),
            polymarket_market_ids=[
                str(market_id)
                for market_id in result.get("polymarket_market_ids", [])
                if market_id is not None
            ],
            records_written=int(result.get("records_written", 0) or 0),
            report_path=(
                str(result["report_path"])
                if result.get("report_path") is not None
                else None
            ),
            preflight_summary=result.get("preflight_summary"),
            warnings=[str(item) for item in result.get("warnings", [])],
            summary=CaptureLiveWeekendSummaryResponse(
                openf1_topics=[
                    CaptureLiveWeekendCountResponse(
                        key=str(item.get("key") or "unknown"),
                        count=int(item.get("count", 0) or 0),
                    )
                    for item in summary.get("openf1_topics", [])
                ],
                polymarket_event_types=[
                    CaptureLiveWeekendCountResponse(
                        key=str(item.get("key") or "unknown"),
                        count=int(item.get("count", 0) or 0),
                    )
                    for item in summary.get("polymarket_event_types", [])
                ],
                observed_market_count=int(summary.get("observed_market_count", 0) or 0),
                observed_token_count=int(summary.get("observed_token_count", 0) or 0),
                market_quotes=[
                    CaptureLiveWeekendMarketQuoteResponse(
                        market_id=str(item.get("market_id") or ""),
                        token_id=(
                            str(item.get("token_id"))
                            if item.get("token_id") is not None
                            else None
                        ),
                        outcome=(
                            str(item.get("outcome"))
                            if item.get("outcome") is not None
                            else None
                        ),
                        event_type=str(item.get("event_type") or "unknown"),
                        observed_at_utc=item.get("observed_at_utc"),
                        price=(
                            float(item["price"]) if item.get("price") is not None else None
                        ),
                        best_bid=(
                            float(item["best_bid"])
                            if item.get("best_bid") is not None
                            else None
                        ),
                        best_ask=(
                            float(item["best_ask"])
                            if item.get("best_ask") is not None
                            else None
                        ),
                        midpoint=(
                            float(item["midpoint"])
                            if item.get("midpoint") is not None
                            else None
                        ),
                        spread=(
                            float(item["spread"])
                            if item.get("spread") is not None
                            else None
                        ),
                        size=float(item["size"]) if item.get("size") is not None else None,
                        side=str(item.get("side")) if item.get("side") is not None else None,
                    )
                    for item in summary.get("market_quotes", [])
                    if item.get("market_id") is not None
                ],
            ),
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="capture-live-weekend failed",
            value_error_status=409,
        )


@action_router.post(
    "/actions/create-live-trade-ticket",
    response_model=CreateLiveTradeTicketResponse,
)
def action_create_live_trade_ticket(
    body: CreateLiveTradeTicketRequest,
    db: Session = Depends(get_db_session),
) -> CreateLiveTradeTicketResponse:
    from f1_polymarket_worker.live_trading import create_live_trade_ticket
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = create_live_trade_ticket(
            ctx,
            gp_short_code=body.gp_short_code,
            market_id=body.market_id,
            observed_market_price=body.observed_market_price,
            observed_spread=body.observed_spread,
            observed_at_utc=body.observed_at_utc,
            source_event_type=body.source_event_type,
            bet_size=body.bet_size,
            min_edge=body.min_edge,
            max_spread=body.max_spread,
        )
        db.commit()
        return CreateLiveTradeTicketResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="create-live-trade-ticket failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/record-live-trade-fill",
    response_model=RecordLiveTradeFillResponse,
)
def action_record_live_trade_fill(
    body: RecordLiveTradeFillRequest,
    db: Session = Depends(get_db_session),
) -> RecordLiveTradeFillResponse:
    from f1_polymarket_worker.live_trading import record_live_trade_fill
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = record_live_trade_fill(
            ctx,
            ticket_id=body.ticket_id,
            submitted_size=body.submitted_size,
            actual_fill_size=body.actual_fill_size,
            actual_fill_price=body.actual_fill_price,
            submitted_at=body.submitted_at,
            filled_at=body.filled_at,
            operator_note=body.operator_note,
            external_reference=body.external_reference,
            status=body.status,
            realized_pnl=body.realized_pnl,
        )
        db.commit()
        return RecordLiveTradeFillResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="record-live-trade-fill failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/cancel-live-trade-ticket",
    response_model=CancelLiveTradeTicketResponse,
)
def action_cancel_live_trade_ticket(
    body: CancelLiveTradeTicketRequest,
    db: Session = Depends(get_db_session),
) -> CancelLiveTradeTicketResponse:
    from f1_polymarket_worker.live_trading import cancel_live_trade_ticket
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = cancel_live_trade_ticket(
            ctx,
            ticket_id=body.ticket_id,
            operator_note=body.operator_note,
        )
        db.commit()
        return CancelLiveTradeTicketResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="cancel-live-trade-ticket failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/execute-manual-live-paper-trade",
    response_model=ExecuteManualLivePaperTradeResponse,
)
def action_execute_manual_live_paper_trade(
    body: ExecuteManualLivePaperTradeRequest,
    db: Session = Depends(get_db_session),
) -> ExecuteManualLivePaperTradeResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import execute_manual_live_paper_trade

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = execute_manual_live_paper_trade(
            ctx,
            gp_short_code=body.gp_short_code,
            market_id=body.market_id,
            token_id=body.token_id,
            model_run_id=body.model_run_id,
            snapshot_id=body.snapshot_id,
            model_prob=body.model_prob,
            market_price=body.market_price,
            observed_at_utc=body.observed_at_utc,
            observed_spread=body.observed_spread,
            source_event_type=body.source_event_type,
            min_edge=body.min_edge,
            max_spread=body.max_spread,
            bet_size=body.bet_size,
        )
        db.commit()
        return ExecuteManualLivePaperTradeResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="execute-manual-live-paper-trade failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post("/actions/run-paper-trade", response_model=ActionStatusResponse)
def action_run_paper_trade(
    body: RunPaperTradeRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    """Run the full paper trading pipeline for a GP."""
    from f1_polymarket_worker.gp_registry import resolve_gp_config

    try:
        config = resolve_gp_config(body.gp_short_code, db=db)
        planned_inputs: dict[str, object] = {
            "gp_short_code": config.short_code,
            "baseline": body.baseline,
            "min_edge": body.min_edge,
            "bet_size": body.bet_size,
        }
        if body.snapshot_id is not None:
            planned_inputs["snapshot_id"] = body.snapshot_id
        return _queue_action_response(
            db,
            action="run-paper-trade",
            job_name="run-paper-trade",
            planned_inputs=planned_inputs,
            message=f"Paper trade queued for {config.name}. Track the job in lineage.",
            max_attempts=1,
        )
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="run-paper-trade failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/run-weekend-cockpit",
    response_model=RunWeekendCockpitResponse,
)
def action_run_weekend_cockpit(
    body: RunWeekendCockpitRequest,
    db: Session = Depends(get_db_session),
) -> RunWeekendCockpitResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import run_weekend_cockpit

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = run_weekend_cockpit(
            ctx,
            gp_short_code=body.gp_short_code,
            baseline=body.baseline,
            min_edge=body.min_edge,
            bet_size=body.bet_size,
            search_fallback=body.search_fallback,
            discover_max_pages=body.discover_max_pages,
        )
        db.commit()
        return RunWeekendCockpitResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="run-weekend-cockpit failed",
            key_error_status=404,
            value_error_status=409,
        )


@action_router.post(
    "/actions/refresh-driver-affinity",
    response_model=RefreshDriverAffinityResponse,
)
def action_refresh_driver_affinity(
    body: RefreshDriverAffinityRequest,
    db: Session = Depends(get_db_session),
) -> RefreshDriverAffinityResponse:
    from f1_polymarket_worker.driver_affinity import refresh_driver_affinity
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = refresh_driver_affinity(
            ctx,
            season=body.season,
            meeting_key=body.meeting_key,
            force=body.force,
        )
        db.commit()
        return RefreshDriverAffinityResponse.model_validate(result)
    except Exception as exc:
        _raise_action_error(
            db,
            exc=exc,
            log_message="refresh-driver-affinity failed",
            value_error_status=404,
        )
