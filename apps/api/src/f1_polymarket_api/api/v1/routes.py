from __future__ import annotations

from typing import Any

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    BacktestResultResponse,
    CurrentWeekendOperationsReadinessResponse,
    CursorStateResponse,
    DataQualityResultResponse,
    DriverAffinityReportResponse,
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
    WeekendCockpitStatusResponse,
)
from f1_polymarket_lab.common import MarketTaxonomy, coerce_market_taxonomy
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
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

router = APIRouter(prefix="/api/v1")

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000


def _market_response(record: PolymarketMarket) -> PolymarketMarketResponse:
    return PolymarketMarketResponse.model_validate(
        {
            "id": record.id,
            "event_id": record.event_id,
            "question": record.question,
            "slug": record.slug,
            "taxonomy": coerce_market_taxonomy(record.taxonomy),
            "taxonomy_confidence": record.taxonomy_confidence,
            "target_session_code": record.target_session_code,
            "condition_id": record.condition_id,
            "question_id": record.question_id,
            "best_bid": record.best_bid,
            "best_ask": record.best_ask,
            "last_trade_price": record.last_trade_price,
            "volume": record.volume,
            "liquidity": record.liquidity,
            "active": record.active,
            "closed": record.closed,
        }
    )


def _weekend_cockpit_status_response(payload: dict[str, Any]) -> WeekendCockpitStatusResponse:
    return WeekendCockpitStatusResponse(
        now=payload["now"],
        auto_selected_gp_short_code=payload["auto_selected_gp_short_code"],
        selected_gp_short_code=payload["selected_gp_short_code"],
        selected_config=payload["selected_config"],
        available_configs=payload["available_configs"],
        meeting=(
            None
            if payload["meeting"] is None
            else F1MeetingResponse.model_validate(payload["meeting"])
        ),
        focus_session=(
            None
            if payload["focus_session"] is None
            else F1SessionResponse.model_validate(payload["focus_session"])
        ),
        focus_status=payload["focus_status"],
        timeline_completed_codes=payload["timeline_completed_codes"],
        timeline_active_code=payload["timeline_active_code"],
        source_session=(
            None
            if payload["source_session"] is None
            else F1SessionResponse.model_validate(payload["source_session"])
        ),
        target_session=(
            None
            if payload["target_session"] is None
            else F1SessionResponse.model_validate(payload["target_session"])
        ),
        latest_paper_session=(
            None
            if payload["latest_paper_session"] is None
            else PaperTradeSessionResponse.model_validate(payload["latest_paper_session"])
        ),
        steps=payload["steps"],
        blockers=payload["blockers"],
        ready_to_run=payload["ready_to_run"],
        primary_action_title=payload["primary_action_title"],
        primary_action_description=payload["primary_action_description"],
        primary_action_cta=payload["primary_action_cta"],
        explanation=payload["explanation"],
    )


def _driver_affinity_report_response(payload: dict[str, Any]) -> DriverAffinityReportResponse:
    return DriverAffinityReportResponse.model_validate(payload)


def _current_weekend_operations_readiness_response(
    payload: dict[str, Any],
) -> CurrentWeekendOperationsReadinessResponse:
    return CurrentWeekendOperationsReadinessResponse(
        now=payload["now"],
        selected_gp_short_code=payload["selected_gp_short_code"],
        selected_config=payload["selected_config"],
        meeting=(
            None
            if payload["meeting"] is None
            else F1MeetingResponse.model_validate(payload["meeting"])
        ),
        latest_ended_session=(
            None
            if payload["latest_ended_session"] is None
            else F1SessionResponse.model_validate(payload["latest_ended_session"])
        ),
        next_active_session=(
            None
            if payload["next_active_session"] is None
            else F1SessionResponse.model_validate(payload["next_active_session"])
        ),
        openf1_credentials_configured=payload["openf1_credentials_configured"],
        actions=payload["actions"],
        blockers=payload["blockers"],
        warnings=payload["warnings"],
    )


@router.get("/freshness", response_model=list[FreshnessResponse])
def freshness(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[FreshnessResponse]:
    logs = db.scalars(
        select(SourceFetchLog)
        .order_by(SourceFetchLog.finished_at.desc())
        .limit(max(limit * 4, DEFAULT_LIMIT))
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
        if len(latest) >= limit:
            break

    return list(latest.values())


@router.get("/f1/meetings", response_model=list[F1MeetingResponse])
def meetings(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    season: int | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[F1MeetingResponse]:
    stmt = select(F1Meeting)
    if season is not None:
        stmt = stmt.where(F1Meeting.season == season)
    records = db.scalars(
        stmt.order_by(F1Meeting.season.desc(), F1Meeting.meeting_key).limit(limit)
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
def sessions(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    season: int | None = Query(None),
    meeting_id: str | None = Query(None),
    session_code: str | None = Query(None),
    is_practice: bool | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[F1SessionResponse]:
    stmt = select(F1Session)
    if season is not None:
        stmt = stmt.join(F1Meeting, F1Session.meeting_id == F1Meeting.id).where(
            F1Meeting.season == season
        )
    if meeting_id is not None:
        stmt = stmt.where(F1Session.meeting_id == meeting_id)
    if session_code is not None:
        stmt = stmt.where(F1Session.session_code == session_code)
    if is_practice is not None:
        stmt = stmt.where(F1Session.is_practice == is_practice)
    records = db.scalars(stmt.order_by(F1Session.date_start_utc.desc()).limit(limit)).all()
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
def polymarket_events(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[PolymarketEventResponse]:
    records = db.scalars(
        select(PolymarketEvent).order_by(PolymarketEvent.end_at_utc.desc()).limit(limit)
    ).all()
    return [PolymarketEventResponse.model_validate(record) for record in records]


@router.get("/polymarket/markets", response_model=list[PolymarketMarketResponse])
def polymarket_markets(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    event_id: str | None = Query(None),
    market_ids: str | None = Query(None),
    taxonomy: MarketTaxonomy | None = Query(None),
    active: bool | None = Query(None),
    closed: bool | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[PolymarketMarketResponse]:
    stmt = select(PolymarketMarket)
    if event_id is not None:
        stmt = stmt.where(PolymarketMarket.event_id == event_id)
    if market_ids is not None:
        market_id_list = [value.strip() for value in market_ids.split(",") if value.strip()]
        if market_id_list:
            stmt = stmt.where(PolymarketMarket.id.in_(market_id_list))
    if taxonomy is not None:
        stmt = stmt.where(PolymarketMarket.taxonomy == taxonomy)
    if active is not None:
        stmt = stmt.where(PolymarketMarket.active == active)
    if closed is not None:
        stmt = stmt.where(PolymarketMarket.closed == closed)
    records = db.scalars(stmt.order_by(PolymarketMarket.end_at_utc.desc()).limit(limit)).all()
    return [_market_response(record) for record in records]


@router.get("/polymarket/markets/{market_id}", response_model=PolymarketMarketResponse)
def polymarket_market_detail(
    market_id: str, db: Session = Depends(get_db_session)
) -> PolymarketMarketResponse:
    record = db.get(PolymarketMarket, market_id)
    if not record:
        raise HTTPException(status_code=404, detail="Market not found")
    return _market_response(record)


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
def mappings(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    f1_session_id: str | None = Query(None),
    polymarket_market_id: str | None = Query(None),
    min_confidence: float | None = Query(None, ge=0.0, le=1.0),
    db: Session = Depends(get_db_session),
) -> list[EntityMappingResponse]:
    stmt = select(EntityMappingF1ToPolymarket)
    if f1_session_id is not None:
        stmt = stmt.where(EntityMappingF1ToPolymarket.f1_session_id == f1_session_id)
    if polymarket_market_id is not None:
        stmt = stmt.where(EntityMappingF1ToPolymarket.polymarket_market_id == polymarket_market_id)
    if min_confidence is not None:
        stmt = stmt.where(EntityMappingF1ToPolymarket.confidence >= min_confidence)
    records = db.scalars(
        stmt.order_by(EntityMappingF1ToPolymarket.confidence.desc()).limit(limit)
    ).all()
    return [EntityMappingResponse.model_validate(record) for record in records]


@router.get("/lineage/jobs", response_model=list[IngestionJobRunResponse])
def ingestion_jobs(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[IngestionJobRunResponse]:
    records = db.scalars(
        select(IngestionJobRun).order_by(IngestionJobRun.started_at.desc()).limit(limit)
    ).all()
    return [IngestionJobRunResponse.model_validate(record) for record in records]


@router.get("/lineage/cursors", response_model=list[CursorStateResponse])
def cursor_states(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[CursorStateResponse]:
    records = db.scalars(
        select(SourceCursorState).order_by(SourceCursorState.updated_at.desc()).limit(limit)
    ).all()
    return [CursorStateResponse.model_validate(record) for record in records]


@router.get("/quality/results", response_model=list[DataQualityResultResponse])
def quality_results(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[DataQualityResultResponse]:
    records = db.scalars(
        select(DataQualityResult).order_by(DataQualityResult.observed_at.desc()).limit(limit)
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
    from f1_polymarket_worker.gp_registry import (
        GP_REGISTRY,
        config_display_description,
        config_display_label,
        config_stage_label,
    )

    return [
        GPRegistryItem(
            name=gp.name,
            short_code=gp.short_code,
            meeting_key=gp.meeting_key,
            season=gp.season,
            target_session_code=gp.target_session_code,
            variant=gp.variant,
            source_session_code=gp.source_session_code,
            market_taxonomy=gp.market_taxonomy,
            stage_rank=gp.stage_rank,
            stage_label=config_stage_label(gp),
            display_label=config_display_label(gp),
            display_description=config_display_description(gp),
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


@router.get("/weekend-cockpit/status", response_model=WeekendCockpitStatusResponse)
def weekend_cockpit_status(
    gp_short_code: str | None = None,
    db: Session = Depends(get_db_session),
) -> WeekendCockpitStatusResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import get_weekend_cockpit_status

    try:
        return _weekend_cockpit_status_response(
            get_weekend_cockpit_status(
                PipelineContext(db=db, execute=False),
                gp_short_code=gp_short_code,
            )
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/operations/current-weekend-readiness",
    response_model=CurrentWeekendOperationsReadinessResponse,
)
def current_weekend_operations_readiness(
    gp_short_code: str | None = Query(None),
    season: int | None = Query(None),
    meeting_key: int | None = Query(None),
    db: Session = Depends(get_db_session),
) -> CurrentWeekendOperationsReadinessResponse:
    from f1_polymarket_worker.pipeline import PipelineContext
    from f1_polymarket_worker.weekend_ops import (
        get_current_weekend_operations_readiness,
    )

    try:
        return _current_weekend_operations_readiness_response(
            get_current_weekend_operations_readiness(
                PipelineContext(db=db, execute=False),
                gp_short_code=gp_short_code,
                season=season,
                meeting_key=meeting_key,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/driver-affinity", response_model=DriverAffinityReportResponse)
def driver_affinity_report(
    season: int = Query(2026),
    meeting_key: int | None = Query(None),
    db: Session = Depends(get_db_session),
) -> DriverAffinityReportResponse:
    from f1_polymarket_worker.driver_affinity import get_driver_affinity_report
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        payload = get_driver_affinity_report(
            PipelineContext(db=db, execute=False),
            season=season,
            meeting_key=meeting_key,
        )
        return _driver_affinity_report_response(payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
