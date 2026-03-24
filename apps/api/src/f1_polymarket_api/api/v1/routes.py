from __future__ import annotations

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    BacktestResultResponse,
    CursorStateResponse,
    DataQualityResultResponse,
    EntityMappingResponse,
    F1DriverResponse,
    F1MeetingResponse,
    F1SessionResponse,
    F1TeamResponse,
    FeatureSnapshotResponse,
    FreshnessResponse,
    GPRegistryItem,
    IngestionJobRunResponse,
    ModelPredictionResponse,
    ModelRunResponse,
    PaperTradePositionResponse,
    PaperTradeSessionResponse,
    PolymarketEventResponse,
    PolymarketMarketResponse,
    PriceHistoryResponse,
)
from f1_polymarket_lab.storage.models import (
    BacktestResult,
    DataQualityResult,
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Meeting,
    F1Session,
    F1Team,
    FeatureSnapshot,
    IngestionJobRun,
    ModelPrediction,
    ModelRun,
    PaperTradePosition,
    PaperTradeSession,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    SourceCursorState,
    SourceFetchLog,
)
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

router = APIRouter(prefix="/api/v1")


@router.get("/freshness", response_model=list[FreshnessResponse])
def freshness(db: Session = Depends(get_db_session)) -> list[FreshnessResponse]:
    logs = db.scalars(
        select(SourceFetchLog).order_by(SourceFetchLog.finished_at.desc()).limit(100)
    ).all()

    latest: dict[tuple[str, str], FreshnessResponse] = {}
    for log in logs:
        key = (log.source, log.dataset)
        if key in latest:
            continue
        latest[key] = FreshnessResponse(
            source=log.source,
            dataset=log.dataset,
            status=log.status,
            last_fetch_at=log.finished_at,
            records_fetched=log.records_fetched,
        )

    return list(latest.values())


@router.get("/f1/meetings", response_model=list[F1MeetingResponse])
def meetings(db: Session = Depends(get_db_session)) -> list[F1MeetingResponse]:
    records = db.scalars(
        select(F1Meeting).order_by(F1Meeting.season.desc(), F1Meeting.meeting_key)
    ).all()
    return [F1MeetingResponse.model_validate(record) for record in records]


@router.get("/f1/meetings/{meeting_id}", response_model=F1MeetingResponse)
def meeting_detail(
    meeting_id: str, db: Session = Depends(get_db_session)
) -> F1MeetingResponse:
    record = db.get(F1Meeting, meeting_id)
    if not record:
        raise HTTPException(status_code=404, detail="Meeting not found")
    return F1MeetingResponse.model_validate(record)


@router.get("/f1/meetings/{meeting_id}/sessions", response_model=list[F1SessionResponse])
def meeting_sessions(
    meeting_id: str, db: Session = Depends(get_db_session)
) -> list[F1SessionResponse]:
    records = db.scalars(
        select(F1Session)
        .where(F1Session.meeting_id == meeting_id)
        .order_by(F1Session.date_start_utc)
    ).all()
    return [F1SessionResponse.model_validate(record) for record in records]


@router.get("/f1/sessions", response_model=list[F1SessionResponse])
def sessions(db: Session = Depends(get_db_session)) -> list[F1SessionResponse]:
    records = db.scalars(select(F1Session).order_by(F1Session.date_start_utc.desc())).all()
    return [F1SessionResponse.model_validate(record) for record in records]


@router.get("/f1/drivers", response_model=list[F1DriverResponse])
def drivers(db: Session = Depends(get_db_session)) -> list[F1DriverResponse]:
    records = db.scalars(
        select(F1Driver).order_by(F1Driver.driver_number)
    ).all()
    return [F1DriverResponse.model_validate(record) for record in records]


@router.get("/f1/teams", response_model=list[F1TeamResponse])
def teams(db: Session = Depends(get_db_session)) -> list[F1TeamResponse]:
    records = db.scalars(select(F1Team).order_by(F1Team.team_name)).all()
    return [F1TeamResponse.model_validate(record) for record in records]


@router.get("/polymarket/events", response_model=list[PolymarketEventResponse])
def polymarket_events(db: Session = Depends(get_db_session)) -> list[PolymarketEventResponse]:
    records = db.scalars(select(PolymarketEvent).order_by(PolymarketEvent.end_at_utc.desc())).all()
    return [PolymarketEventResponse.model_validate(record) for record in records]


@router.get("/polymarket/markets", response_model=list[PolymarketMarketResponse])
def polymarket_markets(db: Session = Depends(get_db_session)) -> list[PolymarketMarketResponse]:
    records = db.scalars(
        select(PolymarketMarket).order_by(PolymarketMarket.end_at_utc.desc())
    ).all()
    return [PolymarketMarketResponse.model_validate(record) for record in records]


@router.get("/polymarket/markets/{market_id}", response_model=PolymarketMarketResponse)
def polymarket_market_detail(
    market_id: str, db: Session = Depends(get_db_session)
) -> PolymarketMarketResponse:
    record = db.get(PolymarketMarket, market_id)
    if not record:
        raise HTTPException(status_code=404, detail="Market not found")
    return PolymarketMarketResponse.model_validate(record)


@router.get("/polymarket/markets/{market_id}/prices", response_model=list[PriceHistoryResponse])
def market_prices(
    market_id: str, db: Session = Depends(get_db_session)
) -> list[PriceHistoryResponse]:
    records = db.scalars(
        select(PolymarketPriceHistory)
        .where(PolymarketPriceHistory.market_id == market_id)
        .order_by(PolymarketPriceHistory.observed_at_utc)
        .limit(2000)
    ).all()
    return [PriceHistoryResponse.model_validate(record) for record in records]


@router.get("/mappings", response_model=list[EntityMappingResponse])
def mappings(db: Session = Depends(get_db_session)) -> list[EntityMappingResponse]:
    records = db.scalars(
        select(EntityMappingF1ToPolymarket).order_by(EntityMappingF1ToPolymarket.confidence.desc())
    ).all()
    return [EntityMappingResponse.model_validate(record) for record in records]


@router.get("/lineage/jobs", response_model=list[IngestionJobRunResponse])
def ingestion_jobs(db: Session = Depends(get_db_session)) -> list[IngestionJobRunResponse]:
    records = db.scalars(select(IngestionJobRun).order_by(IngestionJobRun.started_at.desc())).all()
    return [IngestionJobRunResponse.model_validate(record) for record in records]


@router.get("/lineage/cursors", response_model=list[CursorStateResponse])
def cursor_states(db: Session = Depends(get_db_session)) -> list[CursorStateResponse]:
    records = db.scalars(
        select(SourceCursorState).order_by(SourceCursorState.updated_at.desc())
    ).all()
    return [CursorStateResponse.model_validate(record) for record in records]


@router.get("/quality/results", response_model=list[DataQualityResultResponse])
def quality_results(db: Session = Depends(get_db_session)) -> list[DataQualityResultResponse]:
    records = db.scalars(
        select(DataQualityResult).order_by(DataQualityResult.observed_at.desc())
    ).all()
    return [DataQualityResultResponse.model_validate(record) for record in records]


@router.get("/model-runs", response_model=list[ModelRunResponse])
def model_runs(db: Session = Depends(get_db_session)) -> list[ModelRunResponse]:
    records = db.scalars(
        select(ModelRun).order_by(ModelRun.created_at.desc())
    ).all()
    return [ModelRunResponse.model_validate(record) for record in records]


@router.get("/predictions", response_model=list[ModelPredictionResponse])
def predictions(
    model_run_id: str | None = None,
    db: Session = Depends(get_db_session),
) -> list[ModelPredictionResponse]:
    stmt = select(ModelPrediction).order_by(ModelPrediction.as_of_ts.desc())
    if model_run_id:
        stmt = stmt.where(ModelPrediction.model_run_id == model_run_id)
    records = db.scalars(stmt.limit(500)).all()
    return [ModelPredictionResponse.model_validate(record) for record in records]


@router.get("/backtest/results", response_model=list[BacktestResultResponse])
def backtest_results(db: Session = Depends(get_db_session)) -> list[BacktestResultResponse]:
    records = db.scalars(
        select(BacktestResult).order_by(BacktestResult.created_at.desc())
    ).all()
    return [BacktestResultResponse.model_validate(record) for record in records]


@router.get("/snapshots", response_model=list[FeatureSnapshotResponse])
def snapshots(db: Session = Depends(get_db_session)) -> list[FeatureSnapshotResponse]:
    records = db.scalars(
        select(FeatureSnapshot).order_by(FeatureSnapshot.as_of_ts.desc())
    ).all()
    return [FeatureSnapshotResponse.model_validate(record) for record in records]


# ---------------------------------------------------------------------------
# Action endpoints (GET only — POST actions live in action_routes.py)
# ---------------------------------------------------------------------------


@router.get("/actions/gp-registry", response_model=list[GPRegistryItem])
def gp_registry() -> list[GPRegistryItem]:
    from f1_polymarket_worker.gp_registry import GP_REGISTRY

    return [
        GPRegistryItem(
            name=gp.name,
            short_code=gp.short_code,
            meeting_key=gp.meeting_key,
            season=gp.season,
            target_session_code=gp.target_session_code,
            variant=gp.variant,
        )
        for gp in GP_REGISTRY
    ]


# ---------------------------------------------------------------------------
# Paper trading endpoints
# ---------------------------------------------------------------------------


@router.get("/paper-trading/sessions", response_model=list[PaperTradeSessionResponse])
def paper_trading_sessions(
    gp_slug: str | None = None,
    limit: int = 50,
    db: Session = Depends(get_db_session),
) -> list[PaperTradeSessionResponse]:
    q = select(PaperTradeSession).order_by(PaperTradeSession.started_at.desc()).limit(limit)
    if gp_slug:
        q = q.where(PaperTradeSession.gp_slug == gp_slug)
    records = db.scalars(q).all()
    return [PaperTradeSessionResponse.model_validate(r) for r in records]


@router.get(
    "/paper-trading/sessions/{session_id}", response_model=PaperTradeSessionResponse
)
def paper_trading_session(
    session_id: str, db: Session = Depends(get_db_session)
) -> PaperTradeSessionResponse:
    record = db.get(PaperTradeSession, session_id)
    if not record:
        raise HTTPException(status_code=404, detail="Paper trade session not found")
    return PaperTradeSessionResponse.model_validate(record)


@router.get(
    "/paper-trading/sessions/{session_id}/positions",
    response_model=list[PaperTradePositionResponse],
)
def paper_trading_positions(
    session_id: str, db: Session = Depends(get_db_session)
) -> list[PaperTradePositionResponse]:
    records = db.scalars(
        select(PaperTradePosition)
        .where(PaperTradePosition.session_id == session_id)
        .order_by(PaperTradePosition.entry_time)
    ).all()
    return [PaperTradePositionResponse.model_validate(r) for r in records]
