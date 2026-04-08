from __future__ import annotations

from typing import Any

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.schemas import (
    BacktestResultResponse,
    CursorStateResponse,
    DataQualityResultResponse,
    DriverAffinityReportResponse,
    EnsemblePredictionResponse,
    EntityMappingResponse,
    F1DriverResponse,
    F1MeetingResponse,
    F1SessionResponse,
    F1TeamResponse,
    FeatureSnapshotResponse,
    FreshnessResponse,
    GPRegistryItem,
    IngestionJobRunResponse,
    LiveTradeExecutionResponse,
    LiveTradeSignalBoardResponse,
    LiveTradeTicketResponse,
    ModelPredictionResponse,
    ModelRunResponse,
    OpsCalendarMeetingResponse,
    PaperTradePositionResponse,
    PaperTradeSessionResponse,
    PolymarketEventResponse,
    PolymarketMarketResponse,
    PriceHistoryResponse,
    SignalDiagnosticResponse,
    SignalRegistryResponse,
    SignalSnapshotResponse,
    TradeDecisionResponse,
    WeekendCockpitStatusResponse,
)
from f1_polymarket_lab.common import (
    MarketTaxonomy,
    coerce_market_taxonomy,
    market_group_for_taxonomy,
)
from f1_polymarket_lab.storage.models import (
    BacktestResult,
    DataQualityResult,
    EnsemblePrediction,
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Meeting,
    F1Session,
    F1SessionResult,
    F1Team,
    FeatureSnapshot,
    IngestionJobRun,
    LiveTradeExecution,
    LiveTradeTicket,
    ModelPrediction,
    ModelRun,
    ModelRunPromotion,
    PaperTradePosition,
    PaperTradeSession,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    SignalDiagnostic,
    SignalRegistryEntry,
    SignalSnapshot,
    SourceCursorState,
    SourceFetchLog,
    TradeDecision,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
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
        calendar_status=payload["calendar_status"],
        meeting_slug=payload["meeting_slug"],
        source_conflict=payload["source_conflict"],
        override_source_url=payload["override_source_url"],
        calendar_meetings=[
            OpsCalendarMeetingResponse.model_validate(item)
            for item in payload["calendar_meetings"]
        ],
        cancelled_meetings=[
            OpsCalendarMeetingResponse.model_validate(item)
            for item in payload["cancelled_meetings"]
        ],
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
        model_ready=payload["model_ready"],
        required_stage=payload["required_stage"],
        active_model_run_id=payload["active_model_run_id"],
        model_blockers=payload["model_blockers"],
        session_stage_statuses=payload["session_stage_statuses"],
        live_ticket_summary=payload["live_ticket_summary"],
        live_execution_summary=payload["live_execution_summary"],
        primary_action_title=payload["primary_action_title"],
        primary_action_description=payload["primary_action_description"],
        primary_action_cta=payload["primary_action_cta"],
        explanation=payload["explanation"],
    )


def _driver_affinity_report_response(payload: dict[str, Any]) -> DriverAffinityReportResponse:
    return DriverAffinityReportResponse.model_validate(payload)


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
    taxonomy: MarketTaxonomy | None = Query(None),
    active: bool | None = Query(None),
    closed: bool | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[PolymarketMarketResponse]:
    stmt = select(PolymarketMarket)
    if event_id is not None:
        stmt = stmt.where(PolymarketMarket.event_id == event_id)
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
    if not records:
        return []
    promotions = db.scalars(
        select(ModelRunPromotion).where(
            ModelRunPromotion.model_run_id.in_([record.id for record in records])
        )
    ).all()
    promotion_by_model_run_id: dict[str, ModelRunPromotion] = {}
    for promotion in promotions:
        existing = promotion_by_model_run_id.get(promotion.model_run_id)
        if existing is None or promotion.promoted_at >= existing.promoted_at:
            promotion_by_model_run_id[promotion.model_run_id] = promotion

    return [
        ModelRunResponse.model_validate(
            {
                "id": record.id,
                "stage": record.stage,
                "model_family": record.model_family,
                "model_name": record.model_name,
                "dataset_version": record.dataset_version,
                "feature_snapshot_id": record.feature_snapshot_id,
                "config_json": record.config_json,
                "metrics_json": record.metrics_json,
                "artifact_uri": record.artifact_uri,
                "registry_run_id": record.registry_run_id,
                "promotion_status": (
                    promotion_by_model_run_id[record.id].status
                    if record.id in promotion_by_model_run_id
                    else "inactive"
                ),
                "promoted_at": (
                    promotion_by_model_run_id[record.id].promoted_at
                    if record.id in promotion_by_model_run_id
                    else None
                ),
                "created_at": record.created_at,
            }
        )
        for record in records
    ]


@router.get("/predictions", response_model=list[ModelPredictionResponse])
def predictions(
    model_run_id: str | None = Query(None),
    market_id: str | None = Query(None),
    limit: int = Query(500, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[ModelPredictionResponse]:
    stmt = select(ModelPrediction).order_by(ModelPrediction.as_of_ts.desc())
    if model_run_id is not None:
        stmt = stmt.where(ModelPrediction.model_run_id == model_run_id)
    if market_id is not None:
        stmt = stmt.where(ModelPrediction.market_id == market_id)
    records = db.scalars(stmt.limit(limit)).all()
    return [ModelPredictionResponse.model_validate(record) for record in records]


@router.get("/signals/registry", response_model=list[SignalRegistryResponse])
def signal_registry(db: Session = Depends(get_db_session)) -> list[SignalRegistryResponse]:
    records = db.scalars(
        select(SignalRegistryEntry)
        .where(SignalRegistryEntry.is_active.is_(True))
        .order_by(
            SignalRegistryEntry.signal_code.asc(),
            SignalRegistryEntry.market_group.asc(),
            SignalRegistryEntry.market_taxonomy.asc(),
        )
    ).all()
    return [SignalRegistryResponse.model_validate(record) for record in records]


@router.get("/signals/snapshots", response_model=list[SignalSnapshotResponse])
def signal_snapshots(
    model_run_id: str | None = Query(None),
    market_id: str | None = Query(None),
    signal_code: str | None = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[SignalSnapshotResponse]:
    stmt = select(SignalSnapshot).order_by(SignalSnapshot.as_of_ts.desc())
    if model_run_id is not None:
        stmt = stmt.where(SignalSnapshot.model_run_id == model_run_id)
    if market_id is not None:
        stmt = stmt.where(SignalSnapshot.market_id == market_id)
    if signal_code is not None:
        stmt = stmt.where(SignalSnapshot.signal_code == signal_code)
    records = db.scalars(stmt.limit(limit)).all()
    return [
        SignalSnapshotResponse.model_validate(
            {
                **record.__dict__,
                "market_taxonomy": coerce_market_taxonomy(record.market_taxonomy),
                "market_group": record.market_group
                or market_group_for_taxonomy(record.market_taxonomy),
            }
        )
        for record in records
    ]


@router.get("/signals/diagnostics", response_model=list[SignalDiagnosticResponse])
def signal_diagnostics(
    model_run_id: str | None = Query(None),
    market_group: str | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[SignalDiagnosticResponse]:
    stmt = select(SignalDiagnostic).order_by(SignalDiagnostic.created_at.desc())
    if model_run_id is not None:
        stmt = stmt.where(SignalDiagnostic.model_run_id == model_run_id)
    if market_group is not None:
        stmt = stmt.where(SignalDiagnostic.market_group == market_group)
    records = db.scalars(stmt.limit(500)).all()
    return [
        SignalDiagnosticResponse.model_validate(
            {
                **record.__dict__,
                "market_taxonomy": (
                    None
                    if record.market_taxonomy is None
                    else coerce_market_taxonomy(record.market_taxonomy)
                ),
            }
        )
        for record in records
    ]


@router.get("/ensemble/predictions", response_model=list[EnsemblePredictionResponse])
def ensemble_predictions(
    model_run_id: str | None = Query(None),
    market_id: str | None = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[EnsemblePredictionResponse]:
    stmt = select(EnsemblePrediction).order_by(EnsemblePrediction.as_of_ts.desc())
    if model_run_id is not None:
        stmt = stmt.where(EnsemblePrediction.model_run_id == model_run_id)
    if market_id is not None:
        stmt = stmt.where(EnsemblePrediction.market_id == market_id)
    records = db.scalars(stmt.limit(limit)).all()
    return [
        EnsemblePredictionResponse.model_validate(
            {
                **record.__dict__,
                "market_taxonomy": coerce_market_taxonomy(record.market_taxonomy),
                "market_group": record.market_group
                or market_group_for_taxonomy(record.market_taxonomy),
            }
        )
        for record in records
    ]


@router.get("/trade-decisions", response_model=list[TradeDecisionResponse])
def trade_decisions(
    model_run_id: str | None = Query(None),
    market_id: str | None = Query(None),
    decision_status: str | None = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session),
) -> list[TradeDecisionResponse]:
    stmt = select(TradeDecision).order_by(TradeDecision.as_of_ts.desc())
    if model_run_id is not None:
        stmt = stmt.where(TradeDecision.model_run_id == model_run_id)
    if market_id is not None:
        stmt = stmt.where(TradeDecision.market_id == market_id)
    if decision_status is not None:
        stmt = stmt.where(TradeDecision.decision_status == decision_status)
    records = db.scalars(stmt.limit(limit)).all()
    return [
        TradeDecisionResponse.model_validate(
            {
                **record.__dict__,
                "market_taxonomy": coerce_market_taxonomy(record.market_taxonomy),
                "market_group": record.market_group
                or market_group_for_taxonomy(record.market_taxonomy),
            }
        )
        for record in records
    ]


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


@router.get("/ops-calendar", response_model=list[OpsCalendarMeetingResponse])
def ops_calendar(
    season: int | None = Query(None),
    include_cancelled: bool = Query(False),
    db: Session = Depends(get_db_session),
) -> list[OpsCalendarMeetingResponse]:
    from f1_polymarket_worker.ops_calendar import (
        resolve_effective_ops_calendar,
        resolve_ops_season,
    )

    resolved_season = resolve_ops_season(db) if season is None else season
    return [
        OpsCalendarMeetingResponse.model_validate(
            {
                "season": meeting.season,
                "meeting_key": meeting.meeting_key,
                "meeting_slug": meeting.meeting_slug,
                "ops_slug": meeting.ops_slug,
                "meeting_name": meeting.meeting_name,
                "round_number": meeting.round_number,
                "event_format": meeting.event_format,
                "start_date_utc": meeting.start_date_utc,
                "end_date_utc": meeting.end_date_utc,
                "country_name": meeting.country_name,
                "location": meeting.location,
                "status": meeting.status,
                "source_conflict": meeting.source_conflict,
                "source_label": meeting.source_label,
                "source_url": meeting.source_url,
                "note": meeting.note,
            }
        )
        for meeting in resolve_effective_ops_calendar(
            db,
            season=resolved_season,
            include_cancelled=include_cancelled,
        )
    ]


@router.get("/actions/gp-registry", response_model=list[GPRegistryItem])
def gp_registry(
    season: int | None = Query(None),
    db: Session = Depends(get_db_session),
) -> list[GPRegistryItem]:
    from f1_polymarket_worker.gp_registry import (
        config_display_description,
        config_display_label,
        config_stage_label,
    )
    from f1_polymarket_worker.ops_calendar import list_ops_stage_configs, resolve_ops_season

    resolved_season = resolve_ops_season(db) if season is None else season
    registry_entries = list(list_ops_stage_configs(db, season=resolved_season))
    registry_configs = [config for _, config in registry_entries]
    target_meeting_keys = {meeting.meeting_key for meeting, _ in registry_entries}
    target_seasons = {meeting.season for meeting, _ in registry_entries}
    target_session_codes = {gp.target_session_code for gp in registry_configs}
    target_snapshot_types = {gp.snapshot_type for gp in registry_configs}

    target_sessions = db.execute(
        select(
            F1Meeting.meeting_key,
            F1Meeting.season,
            F1Session.session_code,
            F1Session.id,
        )
        .join(F1Meeting, F1Meeting.id == F1Session.meeting_id)
        .where(
            F1Meeting.meeting_key.in_(target_meeting_keys),
            F1Meeting.season.in_(target_seasons),
            F1Session.session_code.in_(target_session_codes),
        )
    ).all()
    session_ids = [row.id for row in target_sessions]
    session_id_by_target = {
        (row.meeting_key, row.season, row.session_code): row.id
        for row in target_sessions
    }

    mapped_market_counts: dict[str, int] = {}
    result_counts: dict[str, int] = {}
    if session_ids:
        mapped_market_counts = {
            str(row.f1_session_id): int(row.mapped_count)
            for row in db.execute(
                select(
                    EntityMappingF1ToPolymarket.f1_session_id,
                    func.count(
                        func.distinct(EntityMappingF1ToPolymarket.polymarket_market_id)
                    ).label("mapped_count"),
                )
                .where(
                    EntityMappingF1ToPolymarket.f1_session_id.in_(session_ids),
                    EntityMappingF1ToPolymarket.polymarket_market_id.is_not(None),
                )
                .group_by(EntityMappingF1ToPolymarket.f1_session_id)
            ).all()
        }
        result_counts = {
            str(row.session_id): int(row.result_count)
            for row in db.execute(
                select(
                    F1SessionResult.session_id,
                    func.count(F1SessionResult.id).label("result_count"),
                )
                .where(F1SessionResult.session_id.in_(session_ids))
                .group_by(F1SessionResult.session_id)
            ).all()
        }

    snapshot_types_with_data = {
        row[0]
        for row in db.execute(
            select(FeatureSnapshot.snapshot_type)
            .where(FeatureSnapshot.snapshot_type.in_(target_snapshot_types))
            .distinct()
        ).all()
    }

    meeting_by_key = {
        (meeting.meeting_key, meeting.season): meeting for meeting, _ in registry_entries
    }

    def sort_key(gp) -> tuple[bool, bool, int, int, int, int]:
        session_id = session_id_by_target.get(
            (gp.meeting_key, gp.season, gp.target_session_code)
        )
        mapped_count = 0 if session_id is None else mapped_market_counts.get(session_id, 0)
        result_count = 0 if session_id is None else result_counts.get(session_id, 0)
        has_snapshot = int(gp.snapshot_type in snapshot_types_with_data)
        meeting = meeting_by_key[(gp.meeting_key, gp.season)]
        return (
            meeting.status == "cancelled",
            meeting.round_number is None,
            meeting.round_number or 10_000,
            -has_snapshot,
            -(mapped_count + result_count),
            gp.stage_rank,
        )

    return [
        GPRegistryItem(
            name=gp.name,
            short_code=gp.short_code,
            meeting_key=gp.meeting_key,
            season=gp.season,
            meeting_slug=meeting_by_key[(gp.meeting_key, gp.season)].meeting_slug,
            target_session_code=gp.target_session_code,
            variant=gp.variant,
            source_session_code=gp.source_session_code,
            market_taxonomy=gp.market_taxonomy,
            stage_rank=gp.stage_rank,
            stage_label=config_stage_label(gp),
            display_label=config_display_label(gp),
            display_description=config_display_description(gp),
            required_model_stage=gp.required_model_stage,
            live_bet_size=gp.live_bet_size,
            live_min_edge=gp.live_min_edge,
            live_max_daily_loss=gp.live_max_daily_loss,
            live_max_spread=gp.live_max_spread,
            calendar_status=meeting_by_key[(gp.meeting_key, gp.season)].status,
            source_conflict=meeting_by_key[(gp.meeting_key, gp.season)].source_conflict,
            override_source_url=meeting_by_key[(gp.meeting_key, gp.season)].source_url,
        )
        for gp in sorted(registry_configs, key=sort_key)
    ]


# ---------------------------------------------------------------------------
# Live trading endpoints
# ---------------------------------------------------------------------------


@router.get("/live-trading/signal-board", response_model=LiveTradeSignalBoardResponse)
def live_trade_signal_board(
    gp_short_code: str,
    db: Session = Depends(get_db_session),
) -> LiveTradeSignalBoardResponse:
    from f1_polymarket_worker.live_trading import build_live_signal_board
    from f1_polymarket_worker.pipeline import PipelineContext

    try:
        payload = build_live_signal_board(
            PipelineContext(db=db, execute=True),
            gp_short_code=gp_short_code,
        )
        return LiveTradeSignalBoardResponse.model_validate(payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/live-trading/tickets", response_model=list[LiveTradeTicketResponse])
def live_trading_tickets(
    gp_slug: str | None = None,
    status: str | None = None,
    limit: int = 50,
    db: Session = Depends(get_db_session),
) -> list[LiveTradeTicketResponse]:
    stmt = select(LiveTradeTicket).order_by(LiveTradeTicket.created_at.desc()).limit(limit)
    if gp_slug is not None:
        stmt = stmt.where(LiveTradeTicket.gp_slug == gp_slug)
    if status is not None:
        stmt = stmt.where(LiveTradeTicket.status == status)
    records = db.scalars(stmt).all()
    return [LiveTradeTicketResponse.model_validate(record) for record in records]


@router.get("/live-trading/executions", response_model=list[LiveTradeExecutionResponse])
def live_trading_executions(
    gp_slug: str | None = None,
    status: str | None = None,
    limit: int = 50,
    db: Session = Depends(get_db_session),
) -> list[LiveTradeExecutionResponse]:
    stmt = select(LiveTradeExecution).order_by(LiveTradeExecution.submitted_at.desc()).limit(limit)
    if gp_slug is not None:
        stmt = stmt.join(LiveTradeTicket, LiveTradeTicket.id == LiveTradeExecution.ticket_id).where(
            LiveTradeTicket.gp_slug == gp_slug
        )
    if status is not None:
        stmt = stmt.where(LiveTradeExecution.status == status)
    records = db.scalars(stmt).all()
    return [LiveTradeExecutionResponse.model_validate(record) for record in records]


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
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


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
