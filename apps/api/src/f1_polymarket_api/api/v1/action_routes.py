from __future__ import annotations

import logging

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    ActionStatusResponse,
    CaptureLiveWeekendRequest,
    CaptureLiveWeekendResponse,
    ExecuteManualLivePaperTradeRequest,
    ExecuteManualLivePaperTradeResponse,
    IngestDemoRequest,
    RefreshDriverAffinityRequest,
    RefreshDriverAffinityResponse,
    RefreshLatestSessionRequest,
    RefreshLatestSessionResponse,
    RunBacktestRequest,
    RunPaperTradeRequest,
    RunWeekendCockpitRequest,
    RunWeekendCockpitResponse,
    SyncCalendarRequest,
    SyncF1MarketsRequest,
)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

action_router = APIRouter(prefix="/api/v1", tags=["actions"])

log = logging.getLogger(__name__)


@action_router.post("/actions/ingest-demo", response_model=ActionStatusResponse)
def action_ingest_demo(
    body: IngestDemoRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.demo_ingest import ingest_demo

    try:
        ingest_demo(
            db,
            season=body.season,
            weekends=body.weekends,
            market_batches=body.market_batches,
        )
        db.commit()
        return ActionStatusResponse(
            action="ingest-demo",
            status="ok",
            message=f"Demo ingestion complete (season={body.season}, weekends={body.weekends}).",
        )
    except Exception as exc:
        log.exception("ingest-demo failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@action_router.post("/actions/sync-calendar", response_model=ActionStatusResponse)
def action_sync_calendar(
    body: SyncCalendarRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.pipeline import PipelineContext, sync_f1_calendar

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = sync_f1_calendar(ctx, season=body.season)
        db.commit()
        return ActionStatusResponse(
            action="sync-calendar",
            status="ok",
            message=f"Calendar sync complete for season {body.season}.",
            details={"result": str(result)},
        )
    except Exception as exc:
        log.exception("sync-calendar failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@action_router.post("/actions/run-backtest", response_model=ActionStatusResponse)
def action_run_backtest(
    body: RunBacktestRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.backtest import settle_single_gp
    from f1_polymarket_worker.gp_registry import (
        GP_REGISTRY,
        build_snapshot,
        run_baseline,
    )
    from f1_polymarket_worker.pipeline import PipelineContext

    config = next(
        (gp for gp in GP_REGISTRY if gp.short_code == body.gp_short_code), None
    )
    if config is None:
        raise HTTPException(
            status_code=404,
            detail=f"GP short_code '{body.gp_short_code}' not found in registry.",
        )

    try:
        ctx = PipelineContext(db=db, execute=True)

        # 1. Build feature snapshot
        snap_result = build_snapshot(ctx, config)
        snapshot_id = snap_result.get("snapshot_id")
        if not snapshot_id:
            return ActionStatusResponse(
                action="run-backtest",
                status="error",
                message="Snapshot build returned no snapshot_id.",
                details={"snap_result": {k: str(v) for k, v in snap_result.items()}},
            )

        # 2. Run baseline models
        run_baseline(
            ctx, config, snapshot_id=snapshot_id, min_edge=body.min_edge
        )

        # 3. Collect resolutions & settle backtest
        settle_result = settle_single_gp(
            ctx,
            meeting_key=config.meeting_key,
            season=config.season,
            snapshot_id=snapshot_id,
            min_edge=body.min_edge,
            bet_size=body.bet_size,
        )

        metrics = settle_result.get("backtest", {}).get("metrics", {})
        db.commit()
        return ActionStatusResponse(
            action="run-backtest",
            status="ok",
            message=(
                f"Backtest complete for {config.name} ({config.short_code}). "
                f"Bets: {metrics.get('bet_count', 0)}, "
                f"PnL: ${metrics.get('total_pnl', 0):.2f}"
            ),
            details={
                "snapshot_id": snapshot_id,
                "bet_count": metrics.get("bet_count", 0),
                "total_pnl": metrics.get("total_pnl", 0),
                "roi_pct": metrics.get("roi_pct", 0),
            },
        )
    except Exception as exc:
        log.exception("run-backtest failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@action_router.post("/actions/sync-f1-markets", response_model=ActionStatusResponse)
def action_sync_f1_markets(
    body: SyncF1MarketsRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    from f1_polymarket_worker.orchestration import sync_polymarket_f1_catalog
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        ctx = PipelineContext(db=db, execute=True)
        result = sync_polymarket_f1_catalog(
            ctx,
            max_pages=body.max_pages,
            search_fallback=body.search_fallback,
            start_year=body.start_year,
            end_year=body.end_year,
        )
        db.commit()
        return ActionStatusResponse(
            action="sync-f1-markets",
            status="ok",
            message=(
                f"F1 market sync complete. "
                f"Events: {result.get('events', 0)}, "
                f"Markets: {result.get('markets', 0)}"
            ),
            details=result,
        )
    except Exception as exc:
        log.exception("sync-f1-markets failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        )
        db.commit()
        return RefreshLatestSessionResponse.model_validate(result)
    except KeyError as exc:
        detail = str(exc.args[0]) if exc.args else str(exc)
        raise HTTPException(status_code=404, detail=detail) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("refresh-latest-session failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        summary = result.get("summary") or {}
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
            summary={
                "openf1_topics": [
                    {
                        "key": str(item.get("key") or "unknown"),
                        "count": int(item.get("count", 0) or 0),
                    }
                    for item in summary.get("openf1_topics", [])
                ],
                "polymarket_event_types": [
                    {
                        "key": str(item.get("key") or "unknown"),
                        "count": int(item.get("count", 0) or 0),
                    }
                    for item in summary.get("polymarket_event_types", [])
                ],
                "observed_market_count": int(summary.get("observed_market_count", 0) or 0),
                "observed_token_count": int(summary.get("observed_token_count", 0) or 0),
                "market_quotes": [
                    {
                        "market_id": str(item.get("market_id") or ""),
                        "token_id": (
                            str(item.get("token_id"))
                            if item.get("token_id") is not None
                            else None
                        ),
                        "outcome": (
                            str(item.get("outcome"))
                            if item.get("outcome") is not None
                            else None
                        ),
                        "event_type": str(item.get("event_type") or "unknown"),
                        "observed_at_utc": item.get("observed_at_utc"),
                        "price": (
                            float(item["price"]) if item.get("price") is not None else None
                        ),
                        "best_bid": (
                            float(item["best_bid"])
                            if item.get("best_bid") is not None
                            else None
                        ),
                        "best_ask": (
                            float(item["best_ask"])
                            if item.get("best_ask") is not None
                            else None
                        ),
                        "midpoint": (
                            float(item["midpoint"])
                            if item.get("midpoint") is not None
                            else None
                        ),
                        "spread": (
                            float(item["spread"])
                            if item.get("spread") is not None
                            else None
                        ),
                        "size": float(item["size"]) if item.get("size") is not None else None,
                        "side": str(item.get("side")) if item.get("side") is not None else None,
                    }
                    for item in summary.get("market_quotes", [])
                    if item.get("market_id") is not None
                ],
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("capture-live-weekend failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("execute-manual-live-paper-trade failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@action_router.post("/actions/run-paper-trade", response_model=ActionStatusResponse)
def action_run_paper_trade(
    body: RunPaperTradeRequest,
    db: Session = Depends(get_db_session),
) -> ActionStatusResponse:
    """Run the full paper trading pipeline for a GP."""
    from f1_polymarket_worker.gp_registry import get_gp_config
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import run_gp_paper_trade_pipeline

    try:
        config = get_gp_config(body.gp_short_code)
        ctx = PipelineContext(db=db, execute=True)
        result = run_gp_paper_trade_pipeline(
            ctx,
            config=config,
            snapshot_id=body.snapshot_id,
            baseline=body.baseline,
            min_edge=body.min_edge,
            bet_size=body.bet_size,
        )
        db.commit()

        return ActionStatusResponse(
            action="run-paper-trade",
            status="ok",
            message=(
                f"Paper trade complete for {config.name}. "
                f"Trades: {result['trades_executed']}, "
                f"PnL: ${result['total_pnl']:.2f}"
            ),
            details={
                **result,
            },
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("run-paper-trade failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("run-weekend-cockpit failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log.exception("refresh-driver-affinity failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
