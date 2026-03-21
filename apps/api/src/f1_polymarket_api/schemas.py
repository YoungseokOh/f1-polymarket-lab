from __future__ import annotations

from datetime import datetime

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
    taxonomy: str
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
