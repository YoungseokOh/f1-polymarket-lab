from __future__ import annotations

from datetime import datetime

from f1_polymarket_lab.common import MarketGroup, MarketTaxonomy
from pydantic import BaseModel, ConfigDict, Field


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
    round_number: int | None = None
    meeting_name: str
    meeting_slug: str | None = None
    event_format: str | None = None
    circuit_short_name: str | None = None
    country_name: str | None = None
    location: str | None = None
    start_date_utc: datetime | None = None
    end_date_utc: datetime | None = None


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
    planned_inputs: dict[str, object] | None = None
    cursor_after: dict[str, object] | None = None
    records_written: int | None
    error_message: str | None = None
    started_at: datetime
    finished_at: datetime | None


class IngestionJobRunSummaryResponse(BaseModel):
    id: str
    job_name: str
    status: str
    records_written: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None


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
    registry_run_id: str | None = None
    promotion_status: str = "inactive"
    promoted_at: datetime | None = None
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


class SignalRegistryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    signal_code: str
    signal_family: str
    market_taxonomy: str | None = None
    market_group: str | None = None
    description: str | None = None
    version: str
    config_json: dict[str, object] | None = None
    is_active: bool
    created_at: datetime


class SignalSnapshotResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    model_run_id: str
    feature_snapshot_id: str | None = None
    market_id: str | None = None
    token_id: str | None = None
    event_id: str | None = None
    market_taxonomy: MarketTaxonomy
    market_group: MarketGroup
    meeting_key: int | None = None
    as_of_ts: datetime
    signal_code: str
    signal_version: str
    p_yes_raw: float | None = None
    p_yes_calibrated: float | None = None
    p_market_ref: float | None = None
    delta_logit: float | None = None
    freshness_sec: float | None = None
    coverage_flag: bool
    metadata_json: dict[str, object] | None = None
    created_at: datetime


class SignalDiagnosticResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    model_run_id: str
    signal_code: str
    market_taxonomy: MarketTaxonomy | None = None
    market_group: MarketGroup | None = None
    phase_bucket: str | None = None
    brier: float | None = None
    log_loss: float | None = None
    ece: float | None = None
    skill_vs_market: float | None = None
    coverage_rate: float | None = None
    residual_correlation_json: dict[str, object] | None = None
    stability_json: dict[str, object] | None = None
    metrics_json: dict[str, object] | None = None
    created_at: datetime


class EnsemblePredictionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    model_run_id: str
    feature_snapshot_id: str | None = None
    market_id: str | None = None
    token_id: str | None = None
    event_id: str | None = None
    market_taxonomy: MarketTaxonomy
    market_group: MarketGroup
    meeting_key: int | None = None
    as_of_ts: datetime
    p_market_ref: float | None = None
    p_yes_ensemble: float | None = None
    z_market: float | None = None
    z_ensemble: float | None = None
    intercept: float | None = None
    disagreement_score: float | None = None
    effective_n: float | None = None
    uncertainty_score: float | None = None
    contributions_json: dict[str, object] | None = None
    coverage_json: dict[str, object] | None = None
    metadata_json: dict[str, object] | None = None
    created_at: datetime


class TradeDecisionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    model_run_id: str
    ensemble_prediction_id: str | None = None
    feature_snapshot_id: str | None = None
    market_id: str | None = None
    token_id: str | None = None
    event_id: str | None = None
    market_taxonomy: MarketTaxonomy
    market_group: MarketGroup
    meeting_key: int | None = None
    as_of_ts: datetime
    side: str
    edge: float | None = None
    threshold: float | None = None
    spread: float | None = None
    depth: float | None = None
    kelly_fraction_raw: float | None = None
    disagreement_penalty: float | None = None
    liquidity_factor: float | None = None
    size_fraction: float | None = None
    decision_status: str
    decision_reason: str | None = None
    metadata_json: dict[str, object] | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Action request / response schemas
# ---------------------------------------------------------------------------


class ActionStatusResponse(BaseModel):
    action: str
    status: str
    message: str
    details: dict[str, object] | None = None


class OpsCalendarMeetingResponse(BaseModel):
    season: int
    meeting_key: int
    meeting_slug: str
    ops_slug: str
    meeting_name: str
    round_number: int | None = None
    event_format: str | None = None
    start_date_utc: datetime | None = None
    end_date_utc: datetime | None = None
    country_name: str | None = None
    location: str | None = None
    status: str
    source_conflict: bool = False
    source_label: str | None = None
    source_url: str | None = None
    note: str | None = None


class IngestDemoRequest(BaseModel):
    season: int = 2026
    weekends: int = 1
    market_batches: int = 1


class SyncCalendarRequest(BaseModel):
    season: int = 2026


class SyncF1MarketsRequest(BaseModel):
    max_pages: int = 20
    search_fallback: bool = True
    start_year: int = 2022
    end_year: int | None = None


class SetCalendarOverrideRequest(BaseModel):
    season: int = 2026
    meeting_slug: str
    status: str
    ops_slug: str | None = None
    effective_round_number: int | None = None
    effective_start_date_utc: datetime | None = None
    effective_end_date_utc: datetime | None = None
    effective_meeting_name: str | None = None
    effective_country_name: str | None = None
    effective_location: str | None = None
    source_label: str | None = None
    source_url: str | None = None
    note: str | None = None


class ClearCalendarOverrideRequest(BaseModel):
    season: int = 2026
    meeting_slug: str


class RefreshLatestSessionRequest(BaseModel):
    meeting_id: str
    search_fallback: bool = True
    discover_max_pages: int = 5
    hydrate_market_history: bool = True
    sync_calendar: bool = True
    hydrate_f1_session_data: bool = True
    include_extended_f1_data: bool = True
    include_heavy_f1_data: bool = True
    refresh_artifacts: bool = True


class RefreshedSessionResponse(BaseModel):
    id: str
    session_key: int
    session_code: str | None
    session_name: str
    date_end_utc: datetime | None


class ArtifactRefreshResponse(BaseModel):
    gp_short_code: str
    status: str
    snapshot_id: str | None = None
    rebuilt_snapshot: bool = False
    bet_count: int | None = None
    total_pnl: float | None = None
    reason: str | None = None


class RefreshLatestSessionResponse(BaseModel):
    action: str
    status: str
    message: str
    meeting_id: str
    meeting_name: str
    refreshed_session: RefreshedSessionResponse
    f1_records_written: int
    markets_discovered: int
    mappings_written: int
    markets_hydrated: int
    artifacts_refreshed: list[ArtifactRefreshResponse] = Field(default_factory=list)


class CaptureLiveWeekendRequest(BaseModel):
    session_key: int
    market_ids: list[str] | None = None
    capture_seconds: int = Field(default=20, ge=1, le=300)
    start_buffer_min: int = Field(default=0, ge=0, le=60)
    stop_buffer_min: int = Field(default=0, ge=0, le=60)
    message_limit: int | None = Field(default=250, ge=1, le=5000)


class CaptureLiveWeekendCountResponse(BaseModel):
    key: str
    count: int


class CaptureLiveWeekendMarketQuoteResponse(BaseModel):
    market_id: str
    token_id: str | None = None
    outcome: str | None = None
    event_type: str
    observed_at_utc: datetime
    price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    midpoint: float | None = None
    spread: float | None = None
    size: float | None = None
    side: str | None = None


class CaptureLiveWeekendSummaryResponse(BaseModel):
    openf1_topics: list[CaptureLiveWeekendCountResponse] = Field(default_factory=list)
    polymarket_event_types: list[CaptureLiveWeekendCountResponse] = Field(default_factory=list)
    observed_market_count: int
    observed_token_count: int
    market_quotes: list[CaptureLiveWeekendMarketQuoteResponse] = Field(default_factory=list)


class CaptureLiveWeekendResponse(BaseModel):
    action: str
    status: str
    message: str
    job_run_id: str
    session_key: int
    capture_seconds: int
    openf1_messages: int
    polymarket_messages: int
    market_count: int
    polymarket_market_ids: list[str]
    records_written: int
    report_path: str | None = None
    preflight_summary: OperationReadinessResponse | None = None
    warnings: list[str] = Field(default_factory=list)
    summary: CaptureLiveWeekendSummaryResponse


class ExecuteManualLivePaperTradeRequest(BaseModel):
    gp_short_code: str
    market_id: str
    token_id: str | None = None
    model_run_id: str | None = None
    snapshot_id: str | None = None
    model_prob: float = Field(ge=0.0, le=1.0)
    market_price: float = Field(ge=0.0, le=1.0)
    observed_at_utc: datetime | None = None
    observed_spread: float | None = Field(default=None, ge=0.0, le=1.0)
    source_event_type: str | None = None
    min_edge: float = Field(default=0.05, ge=0.0, le=1.0)
    max_spread: float | None = Field(default=None, ge=0.0, le=1.0)
    bet_size: float = Field(default=10.0, gt=0.0, le=1000.0)


class ExecuteManualLivePaperTradeResponse(BaseModel):
    action: str
    status: str
    message: str
    gp_short_code: str
    market_id: str
    pt_session_id: str | None = None
    signal_action: str
    quantity: float | None = None
    entry_price: float | None = None
    stake_cost: float | None = None
    market_price: float
    model_prob: float
    edge: float
    side_label: str | None = None
    reason: str | None = None


class RunBacktestRequest(BaseModel):
    gp_short_code: str
    min_edge: float = 0.05
    bet_size: float = 10.0


class BackfillBacktestsRequest(BaseModel):
    gp_short_code: str | None = None
    min_edge: float = 0.05
    bet_size: float = 10.0
    rebuild_missing: bool = True


class GPRegistryItem(BaseModel):
    name: str
    short_code: str
    meeting_key: int
    season: int
    meeting_slug: str | None = None
    target_session_code: str
    variant: str
    source_session_code: str | None
    market_taxonomy: MarketTaxonomy
    stage_rank: int
    stage_label: str
    display_label: str
    display_description: str
    required_model_stage: str | None = None
    live_bet_size: float | None = None
    live_min_edge: float | None = None
    live_max_daily_loss: float | None = None
    live_max_spread: float | None = None
    calendar_status: str = "scheduled"
    source_conflict: bool = False
    override_source_url: str | None = None


class WeekendCockpitSessionStageResponse(BaseModel):
    gp_short_code: str
    target_session_code: str
    required_stage: str | None
    model_ready: bool
    active_model_run_id: str | None
    model_blockers: list[str]
    display_label: str


class LiveTradeTicketSummaryResponse(BaseModel):
    ticket_count: int
    open_ticket_count: int
    filled_ticket_count: int
    cancelled_ticket_count: int


class LiveTradeExecutionSummaryResponse(BaseModel):
    execution_count: int
    filled_execution_count: int


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
    calendar_status: str
    meeting_slug: str
    source_conflict: bool
    override_source_url: str | None = None
    calendar_meetings: list[OpsCalendarMeetingResponse] = Field(default_factory=list)
    cancelled_meetings: list[OpsCalendarMeetingResponse] = Field(default_factory=list)
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
    model_ready: bool
    required_stage: str | None
    active_model_run_id: str | None
    model_blockers: list[str]
    session_stage_statuses: list[WeekendCockpitSessionStageResponse]
    live_ticket_summary: LiveTradeTicketSummaryResponse
    live_execution_summary: LiveTradeExecutionSummaryResponse
    primary_action_title: str
    primary_action_description: str
    primary_action_cta: str
    explanation: str


class OperationReadinessResponse(BaseModel):
    key: str
    label: str
    status: str
    message: str
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    meeting_key: int | None = None
    meeting_name: str | None = None
    gp_short_code: str | None = None
    session_code: str | None = None
    session_key: int | None = None
    actionable_after_utc: datetime | None = None
    openf1_credentials_configured: bool
    last_job_run: IngestionJobRunSummaryResponse | None = None
    last_report_path: str | None = None
    linked_market_count: int | None = None
    token_count: int | None = None
    missing_session_keys: list[int] = Field(default_factory=list)
    report_is_fresh: bool | None = None
    latest_ended_session_code: str | None = None
    latest_ended_session_end_utc: datetime | None = None


class CurrentWeekendOperationsReadinessResponse(BaseModel):
    now: datetime
    selected_gp_short_code: str
    selected_config: GPRegistryItem
    meeting: F1MeetingResponse | None = None
    latest_ended_session: F1SessionResponse | None = None
    next_active_session: F1SessionResponse | None = None
    openf1_credentials_configured: bool
    actions: list[OperationReadinessResponse] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DriverAffinityEntryResponse(BaseModel):
    canonical_driver_key: str
    display_driver_id: str | None = None
    display_name: str
    display_broadcast_name: str | None = None
    driver_number: int | None = None
    team_id: str | None = None
    team_name: str | None = None
    country_code: str | None = None
    headshot_url: str | None = None
    rank: int
    affinity_score: float
    s1_strength: float
    s2_strength: float
    s3_strength: float
    track_s1_fraction: float
    track_s2_fraction: float
    track_s3_fraction: float
    contributing_session_count: int
    contributing_session_codes: list[str]
    latest_contributing_session_code: str | None = None
    latest_contributing_session_end_utc: datetime | None = None


class DriverAffinitySegmentResponse(BaseModel):
    key: str
    title: str
    description: str
    source_session_codes_included: list[str] = Field(default_factory=list)
    source_seasons_included: list[int] = Field(default_factory=list)
    entry_count: int
    entries: list[DriverAffinityEntryResponse] = Field(default_factory=list)


class DriverAffinityReportResponse(BaseModel):
    season: int
    meeting_key: int
    meeting: F1MeetingResponse
    computed_at_utc: datetime
    as_of_utc: datetime
    lookback_start_season: int
    session_code_weights: dict[str, float]
    season_weights: dict[str, float]
    track_weights: dict[str, float]
    default_segment_key: str | None = None
    segments: list[DriverAffinitySegmentResponse] = Field(default_factory=list)
    source_session_codes_included: list[str]
    source_max_session_end_utc: datetime | None
    latest_ended_relevant_session_code: str | None = None
    latest_ended_relevant_session_end_utc: datetime | None = None
    entry_count: int
    is_fresh: bool
    stale_reason: str | None = None
    entries: list[DriverAffinityEntryResponse]


class RunWeekendCockpitRequest(BaseModel):
    gp_short_code: str | None = None
    baseline: str = "hybrid"
    min_edge: float = 0.05
    bet_size: float = 10.0
    search_fallback: bool = True
    discover_max_pages: int = 5


class LiveSignalRowResponse(BaseModel):
    market_id: str
    token_id: str | None = None
    question: str
    session_code: str
    promotion_stage: str | None = None
    model_run_id: str | None = None
    snapshot_id: str | None = None
    model_prob: float
    market_price: float | None = None
    edge: float | None = None
    spread: float | None = None
    signal_action: str
    side_label: str | None = None
    recommended_size: float
    max_spread: float | None = None
    observed_at_utc: str | None = None
    event_type: str | None = None


class LiveTradeSignalBoardResponse(BaseModel):
    gp_short_code: str
    required_stage: str | None = None
    active_model_run_id: str | None = None
    model_run_id: str | None = None
    snapshot_id: str | None = None
    rows: list[LiveSignalRowResponse]
    blockers: list[str]


class CreateLiveTradeTicketRequest(BaseModel):
    gp_short_code: str
    market_id: str
    observed_market_price: float | None = Field(default=None, ge=0.0, le=1.0)
    observed_spread: float | None = Field(default=None, ge=0.0, le=1.0)
    observed_at_utc: datetime | None = None
    source_event_type: str | None = None
    bet_size: float | None = Field(default=None, gt=0.0, le=1000.0)
    min_edge: float | None = Field(default=None, ge=0.0, le=1.0)
    max_spread: float | None = Field(default=None, ge=0.0, le=1.0)


class CreateLiveTradeTicketResponse(BaseModel):
    action: str
    status: str
    message: str
    ticket_id: str
    gp_short_code: str
    market_id: str
    model_run_id: str | None = None
    snapshot_id: str | None = None
    promotion_stage: str | None = None
    signal_action: str
    side_label: str
    recommended_size: float
    market_price: float
    model_prob: float
    edge: float
    observed_spread: float | None = None
    max_spread: float | None = None
    observed_at_utc: datetime
    expires_at: datetime | None = None


class RecordLiveTradeFillRequest(BaseModel):
    ticket_id: str
    submitted_size: float = Field(gt=0.0, le=1000.0)
    actual_fill_size: float | None = Field(default=None, gt=0.0, le=1000.0)
    actual_fill_price: float | None = Field(default=None, gt=0.0, le=1.0)
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    operator_note: str | None = None
    external_reference: str | None = None
    status: str = "filled"
    realized_pnl: float | None = None


class RecordLiveTradeFillResponse(BaseModel):
    action: str
    status: str
    message: str
    ticket_id: str
    execution_id: str
    execution_status: str
    ticket_status: str


class CancelLiveTradeTicketRequest(BaseModel):
    ticket_id: str
    operator_note: str | None = None


class CancelLiveTradeTicketResponse(BaseModel):
    action: str
    status: str
    message: str
    ticket_id: str
    ticket_status: str


class WeekendCockpitSettlementSummaryResponse(BaseModel):
    settled_session_ids: list[str] = Field(default_factory=list)
    settled_gp_slugs: list[str] = Field(default_factory=list)
    settled_positions: int = 0
    manual_positions_settled: int = 0
    unresolved_positions: int = 0
    unresolved_session_ids: list[str] = Field(default_factory=list)
    winner_driver_id: str | None = None


class RunWeekendCockpitDetailsResponse(BaseModel):
    snapshot_id: str | None = None
    model_run_id: str | None = None
    baseline: str | None = None
    pt_session_id: str | None = None
    log_path: str | None = None
    total_signals: int | None = None
    trades_executed: int | None = None
    open_positions: int | None = None
    settled_positions: int | None = None
    win_count: int | None = None
    loss_count: int | None = None
    win_rate: float | None = None
    total_pnl: float | None = None
    daily_pnl: float | None = None
    settlement: WeekendCockpitSettlementSummaryResponse | None = None


class RunWeekendCockpitResponse(BaseModel):
    action: str
    status: str
    message: str
    gp_short_code: str
    snapshot_id: str | None
    model_run_id: str | None
    pt_session_id: str | None
    job_run_id: str | None = None
    report_path: str | None = None
    preflight_summary: OperationReadinessResponse | None = None
    warnings: list[str] = Field(default_factory=list)
    executed_steps: list[WeekendCockpitStepResponse]
    details: RunWeekendCockpitDetailsResponse | None = None


class RefreshDriverAffinityRequest(BaseModel):
    season: int = 2026
    meeting_key: int | None = None
    force: bool = False


class RefreshDriverAffinityResponse(BaseModel):
    action: str
    status: str
    message: str
    season: int
    meeting_key: int
    computed_at_utc: datetime | None
    source_max_session_end_utc: datetime | None
    hydrated_session_keys: list[int]
    job_run_id: str | None = None
    report_path: str | None = None
    preflight_summary: OperationReadinessResponse | None = None
    warnings: list[str] = Field(default_factory=list)
    report: DriverAffinityReportResponse | None = None


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


class LiveTradeTicketResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    gp_slug: str
    session_code: str
    market_id: str
    token_id: str | None
    snapshot_id: str | None
    model_run_id: str | None
    promotion_stage: str | None
    question: str
    signal_action: str
    side_label: str
    model_prob: float
    market_price: float
    edge: float
    recommended_size: float
    observed_spread: float | None
    max_spread: float | None
    observed_at_utc: datetime
    source_event_type: str | None
    status: str
    rationale_json: dict[str, object] | None
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime


class LiveTradeExecutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    ticket_id: str
    market_id: str
    side: str
    submitted_size: float
    actual_fill_size: float | None
    actual_fill_price: float | None
    submitted_at: datetime
    filled_at: datetime | None
    operator_note: str | None
    external_reference: str | None
    realized_pnl: float | None
    status: str
    created_at: datetime
    updated_at: datetime


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
