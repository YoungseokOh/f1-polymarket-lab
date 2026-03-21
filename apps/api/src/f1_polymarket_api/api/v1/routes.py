from __future__ import annotations

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    BacktestResultResponse,
    CursorStateResponse,
    DataQualityResultResponse,
    EntityMappingResponse,
    F1MeetingResponse,
    F1SessionResponse,
    FeatureSnapshotResponse,
    FreshnessResponse,
    IngestionJobRunResponse,
    ModelPredictionResponse,
    ModelRunResponse,
    PolymarketEventResponse,
    PolymarketMarketResponse,
)
from f1_polymarket_lab.storage.models import (
    BacktestResult,
    DataQualityResult,
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    FeatureSnapshot,
    IngestionJobRun,
    ModelPrediction,
    ModelRun,
    PolymarketEvent,
    PolymarketMarket,
    SourceCursorState,
    SourceFetchLog,
)
from fastapi import APIRouter, Depends
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


@router.get("/f1/sessions", response_model=list[F1SessionResponse])
def sessions(db: Session = Depends(get_db_session)) -> list[F1SessionResponse]:
    records = db.scalars(select(F1Session).order_by(F1Session.date_start_utc.desc())).all()
    return [F1SessionResponse.model_validate(record) for record in records]


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
