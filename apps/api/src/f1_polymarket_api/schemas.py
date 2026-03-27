from __future__ import annotations

from datetime import datetime

from f1_polymarket_lab.common import MarketTaxonomy
from pydantic import BaseModel, ConfigDict


class ApiHealthResponse(BaseModel):
    service: str
    status: str
    now: datetime


class FreshnessResponse(BaseModel):
    source: str
    dataset: str
    status: str
    last_fetch_at: datetime | None
    records_fetched: int


class F1MeetingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    meeting_key: int
    season: int
    round_number: int | None
    meeting_name: str
    circuit_short_name: str | None
    country_name: str | None
    location: str | None
    start_date_utc: datetime | None
    end_date_utc: datetime | None


class F1SessionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    session_key: int
    meeting_id: str | None
    session_name: str
    session_code: str | None
    session_type: str | None
    date_start_utc: datetime | None
    date_end_utc: datetime | None
    is_practice: bool


class PolymarketEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    slug: str
    title: str
    start_at_utc: datetime | None
    end_at_utc: datetime | None
    active: bool
    closed: bool


class PolymarketMarketResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_id: str | None
    question: str
    slug: str | None
    taxonomy: MarketTaxonomy
    taxonomy_confidence: float | None
    target_session_code: str | None
    condition_id: str
    question_id: str | None
    best_bid: float | None
    best_ask: float | None
    last_trade_price: float | None
    volume: float | None
    liquidity: float | None
    active: bool
    closed: bool


class EntityMappingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    f1_meeting_id: str | None
    f1_session_id: str | None
    polymarket_event_id: str | None
    polymarket_market_id: str | None
    mapping_type: str
    confidence: float | None
    matched_by: str | None
    override_flag: bool


class IngestionJobRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_name: str
    source: str
    dataset: str
    status: str
    execute_mode: str
    records_written: int | None
    started_at: datetime
    finished_at: datetime | None


class CursorStateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    source: str
    dataset: str
    cursor_key: str
    cursor_value: dict[str, object] | None
    updated_at: datetime


class DataQualityResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset: str
    status: str
    metrics_json: dict[str, object] | None
    observed_at: datetime


class ModelRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    stage: str
    model_family: str
    model_name: str
    dataset_version: str | None
    feature_snapshot_id: str | None
    config_json: dict[str, object] | None
    metrics_json: dict[str, object] | None
    artifact_uri: str | None
    created_at: datetime


class ModelPredictionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    model_run_id: str
    market_id: str | None
    token_id: str | None
    as_of_ts: datetime
    probability_yes: float | None
    probability_no: float | None
    raw_score: float | None
    calibration_version: str | None


class BacktestResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    backtest_run_id: str
    strategy_name: str
    stage: str
    start_at: datetime | None
    end_at: datetime | None
    metrics_json: dict[str, object] | None
    created_at: datetime


class FeatureSnapshotResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    market_id: str | None
    session_id: str | None
    as_of_ts: datetime
    snapshot_type: str
    feature_version: str
    storage_path: str | None
    row_count: int | None


class F1DriverResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    driver_number: int
    broadcast_name: str | None
    full_name: str | None
    first_name: str | None
    last_name: str | None
    name_acronym: str | None
    team_id: str | None
    country_code: str | None
    headshot_url: str | None


class F1TeamResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    team_name: str
    team_color: str | None


class PriceHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    market_id: str
    token_id: str
    observed_at_utc: datetime
    price: float | None
    midpoint: float | None
    best_bid: float | None
    best_ask: float | None


# ---------------------------------------------------------------------------
# Action request / response schemas
# ---------------------------------------------------------------------------


class ActionStatusResponse(BaseModel):
    action: str
    status: str
    message: str
    details: dict[str, object] | None = None


class IngestDemoRequest(BaseModel):
    season: int = 2026
    weekends: int = 2
    market_batches: int = 3


class SyncCalendarRequest(BaseModel):
    season: int = 2026


class SyncF1MarketsRequest(BaseModel):
    max_pages: int = 20
    search_fallback: bool = True
    start_year: int = 2022
    end_year: int | None = None


class RunBacktestRequest(BaseModel):
    gp_short_code: str
    min_edge: float = 0.05
    bet_size: float = 10.0


class GPRegistryItem(BaseModel):
    name: str
    short_code: str
    meeting_key: int
    season: int
    target_session_code: str
    variant: str
    source_session_code: str | None
    market_taxonomy: MarketTaxonomy
    stage_rank: int
    stage_label: str
    display_label: str
    display_description: str


class WeekendCockpitStepResponse(BaseModel):
    key: str
    label: str
    status: str
    detail: str
    session_code: str | None = None
    session_key: int | None = None
    count: int | None = None
    reason_code: str | None = None
    actionable_after_utc: datetime | None = None
    resource_label: str | None = None


class WeekendCockpitStatusResponse(BaseModel):
    now: datetime
    auto_selected_gp_short_code: str
    selected_gp_short_code: str
    selected_config: GPRegistryItem
    available_configs: list[GPRegistryItem]
    meeting: F1MeetingResponse | None
    focus_session: F1SessionResponse | None
    focus_status: str
    timeline_completed_codes: list[str]
    timeline_active_code: str | None
    source_session: F1SessionResponse | None
    target_session: F1SessionResponse | None
    latest_paper_session: PaperTradeSessionResponse | None
    steps: list[WeekendCockpitStepResponse]
    blockers: list[str]
    ready_to_run: bool
    primary_action_title: str
    primary_action_description: str
    primary_action_cta: str
    explanation: str


class RunWeekendCockpitRequest(BaseModel):
    gp_short_code: str | None = None
    baseline: str = "hybrid"
    min_edge: float = 0.05
    bet_size: float = 10.0
    search_fallback: bool = True
    discover_max_pages: int = 5


class RunWeekendCockpitResponse(BaseModel):
    action: str
    status: str
    message: str
    gp_short_code: str
    snapshot_id: str | None
    model_run_id: str | None
    pt_session_id: str | None
    executed_steps: list[WeekendCockpitStepResponse]
    details: dict[str, object] | None = None


class PaperTradePositionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    session_id: str
    market_id: str
    token_id: str | None
    side: str
    quantity: float
    entry_price: float
    entry_time: datetime
    model_prob: float
    market_prob: float
    edge: float
    status: str
    exit_price: float | None
    exit_time: datetime | None
    realized_pnl: float | None


class PaperTradeSessionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    gp_slug: str
    snapshot_id: str | None
    model_run_id: str | None
    status: str
    config_json: dict[str, object] | None
    summary_json: dict[str, object] | None
    log_path: str | None
    started_at: datetime
    finished_at: datetime | None


class RunPaperTradeRequest(BaseModel):
    gp_short_code: str
    snapshot_id: str | None = None
    baseline: str = "hybrid"
    min_edge: float = 0.05
    bet_size: float = 10.0
