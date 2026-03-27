from __future__ import annotations

import logging

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    ActionStatusResponse,
    IngestDemoRequest,
    RefreshDriverAffinityRequest,
    RefreshDriverAffinityResponse,
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
