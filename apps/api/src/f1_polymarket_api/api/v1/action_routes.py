from __future__ import annotations

import logging

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    ActionStatusResponse,
    IngestDemoRequest,
    RunBacktestRequest,
    RunPaperTradeRequest,
    SyncCalendarRequest,
    SyncF1MarketsRequest,
)
from f1_polymarket_lab.storage.models import (
    FeatureSnapshot,
    ModelPrediction,
)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
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
    from f1_polymarket_worker.gp_registry import GP_REGISTRY, build_snapshot, run_baseline
    from f1_polymarket_worker.paper_trading import PaperTradeConfig, PaperTradingEngine
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        import polars as pl

        config = next(
            (g for g in GP_REGISTRY if g.short_code == body.gp_short_code), None
        )
        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"GP '{body.gp_short_code}' not found in registry",
            )

        ctx = PipelineContext(db=db, execute=True)

        # Build snapshot if not provided
        if body.snapshot_id is None:
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
                raise HTTPException(status_code=500, detail=f"Snapshot build failed: {snap_result}")
        else:
            used_snapshot_id = body.snapshot_id

        # Run baselines
        baseline_result = run_baseline(
            ctx, config, snapshot_id=used_snapshot_id, min_edge=body.min_edge
        )
        model_run_ids: list[str] = baseline_result.get("model_runs", [])
        if not model_run_ids:
            raise HTTPException(status_code=500, detail="No model runs produced")

        baseline_idx = {"market_implied": 0, "fp1_pace": 1, "hybrid": 2}.get(body.baseline, 2)
        used_model_run_id = model_run_ids[min(baseline_idx, len(model_run_ids) - 1)]

        # Paper trade
        preds = db.scalars(
            select(ModelPrediction).where(ModelPrediction.model_run_id == used_model_run_id)
        ).all()
        snap = db.get(FeatureSnapshot, used_snapshot_id)

        if not preds:
            raise HTTPException(status_code=404, detail="No predictions found for model run")

        snapshot_df = (
            pl.read_parquet(snap.storage_path) if snap and snap.storage_path else None
        )
        price_lookup: dict[str, float] = {}
        label_lookup: dict[str, bool] = {}
        if snapshot_df is not None:
            for row in snapshot_df.to_dicts():
                mid = row.get("market_id")
                if mid:
                    price_lookup[mid] = float(row.get("entry_yes_price", 0.5))
                    label = row.get("label_yes")
                    if label is not None:
                        label_lookup[mid] = bool(int(label))

        engine = PaperTradingEngine(
            config=PaperTradeConfig(
                min_edge=body.min_edge,
                bet_size=body.bet_size,
            )
        )
        for pred in preds:
            market_price = price_lookup.get(pred.market_id or "", 0.5)
            engine.evaluate_signal(
                market_id=pred.market_id or "",
                token_id=pred.token_id,
                model_prob=pred.probability_yes or 0.5,
                market_price=market_price,
            )
        for mid, outcome in label_lookup.items():
            engine.settle_position(mid, outcome)

        summary = engine.summary()
        pt_session_id = engine.persist(
            db,
            gp_slug=config.short_code,
            snapshot_id=used_snapshot_id,
            model_run_id=used_model_run_id,
        )
        db.commit()

        return ActionStatusResponse(
            action="run-paper-trade",
            status="ok",
            message=(
                f"Paper trade complete for {config.name}. "
                f"Trades: {summary['trades_executed']}, "
                f"PnL: ${summary['total_pnl']:.2f}"
            ),
            details={
                "pt_session_id": pt_session_id,
                "snapshot_id": used_snapshot_id,
                "model_run_id": used_model_run_id,
                **summary,
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("run-paper-trade failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
