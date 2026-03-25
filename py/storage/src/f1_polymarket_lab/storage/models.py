from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def uuid_str() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class SourceFetchLog(Base):
    __tablename__ = "source_fetch_log"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=uuid_str)
    job_run_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("ingestion_job_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source: Mapped[str] = mapped_column(String(50), index=True)
    dataset: Mapped[str] = mapped_column(String(100), index=True)
    endpoint: Mapped[str] = mapped_column(String(255))
    request_params: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(32))
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    records_fetched: Mapped[int] = mapped_column(Integer, default=0)
    bronze_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(128), nullable=True)
    checkpoint: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class IngestionJobDefinition(Base):
    __tablename__ = "ingestion_job_definitions"
    __table_args__ = (UniqueConstraint("job_name", name="uq_ingestion_job_definitions_job"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    job_name: Mapped[str] = mapped_column(String(128), index=True)
    source: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_cursor: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    schedule_hint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class IngestionJobRun(Base):
    __tablename__ = "ingestion_job_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    job_definition_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("ingestion_job_definitions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    job_name: Mapped[str] = mapped_column(String(128), index=True)
    source: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    execute_mode: Mapped[str] = mapped_column(String(32), default="plan")
    planned_inputs: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    cursor_before: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    cursor_after: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    records_written: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SourceCursorState(Base):
    __tablename__ = "source_cursor_state"
    __table_args__ = (UniqueConstraint("source", "dataset", "cursor_key", name="uq_cursor_state"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    source: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    cursor_key: Mapped[str] = mapped_column(String(128), index=True)
    cursor_value: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    cursor_version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class BronzeObjectManifest(Base):
    __tablename__ = "bronze_object_manifest"
    __table_args__ = (UniqueConstraint("object_path", name="uq_bronze_object_path"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    job_run_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("ingestion_job_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    storage_tier: Mapped[str] = mapped_column(String(16), default="bronze")
    source: Mapped[str] = mapped_column(String(64), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    object_path: Mapped[str] = mapped_column(String(512))
    partition_values: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    schema_version: Mapped[str] = mapped_column(String(32), default="v1")
    checksum: Mapped[str | None] = mapped_column(String(128), nullable=True)
    record_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_emitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class SchemaRegistryEntry(Base):
    __tablename__ = "schema_registry"
    __table_args__ = (
        UniqueConstraint("storage_tier", "dataset", "version", name="uq_schema_registry_version"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    storage_tier: Mapped[str] = mapped_column(String(16), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[str] = mapped_column(String(32))
    schema_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DataQualityCheck(Base):
    __tablename__ = "data_quality_checks"
    __table_args__ = (UniqueConstraint("check_name", name="uq_data_quality_check_name"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=uuid_str)
    check_name: Mapped[str] = mapped_column(String(128), index=True)
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    severity: Mapped[str] = mapped_column(String(16), default="warning")
    rule_type: Mapped[str] = mapped_column(String(64))
    rule_config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DataQualityResult(Base):
    __tablename__ = "data_quality_results"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=uuid_str)
    check_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey("data_quality_checks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    job_run_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("ingestion_job_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    dataset: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    sample_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class F1Meeting(Base):
    __tablename__ = "f1_meetings"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(32), default="openf1")
    meeting_key: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    season: Mapped[int] = mapped_column(Integer, index=True)
    round_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    meeting_name: Mapped[str] = mapped_column(String(255))
    meeting_official_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    circuit_short_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    start_date_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_date_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Session(Base):
    __tablename__ = "f1_sessions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(32), default="openf1")
    session_key: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    session_name: Mapped[str] = mapped_column(String(255))
    session_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    session_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    date_start_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    date_end_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    session_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_practice: Mapped[bool] = mapped_column(Boolean, default=False)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Driver(Base):
    __tablename__ = "f1_drivers"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(32), default="openf1")
    driver_number: Mapped[int] = mapped_column(Integer, index=True)
    broadcast_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    name_acronym: Mapped[str | None] = mapped_column(String(16), nullable=True)
    team_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    country_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    headshot_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Team(Base):
    __tablename__ = "f1_teams"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source: Mapped[str] = mapped_column(String(32), default="openf1")
    team_name: Mapped[str] = mapped_column(String(255), unique=True)
    team_color: Mapped[str | None] = mapped_column(String(16), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1SessionResult(Base):
    __tablename__ = "f1_session_results"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_time_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    result_time_kind: Mapped[str | None] = mapped_column(String(24), nullable=True)
    result_time_display: Mapped[str | None] = mapped_column(String(128), nullable=True)
    result_time_segments_json: Mapped[list[Any] | None] = mapped_column(JSON, nullable=True)
    gap_to_leader_display: Mapped[str | None] = mapped_column(String(128), nullable=True)
    gap_to_leader_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    gap_to_leader_laps_behind: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gap_to_leader_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    gap_to_leader_segments_json: Mapped[list[Any] | None] = mapped_column(JSON, nullable=True)
    dnf: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    dns: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    dsq: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    number_of_laps: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Lap(Base):
    __tablename__ = "f1_laps"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    lap_number: Mapped[int] = mapped_column(Integer)
    lap_start_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lap_end_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lap_duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_pit_out_lap: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    stint_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sector_1_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector_2_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector_3_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    speed_trap_kph: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Stint(Base):
    __tablename__ = "f1_stints"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    stint_number: Mapped[int] = mapped_column(Integer)
    compound: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lap_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lap_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tyre_age_at_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Weather(Base):
    __tablename__ = "f1_weather"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    session_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    air_temperature_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    humidity_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    pressure_hpa: Mapped[float | None] = mapped_column(Float, nullable=True)
    rainfall: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    track_temperature_c: Mapped[float | None] = mapped_column(Float, nullable=True)
    wind_direction_deg: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wind_speed_mps: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1RaceControl(Base):
    __tablename__ = "f1_race_control"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    session_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    driver_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    flag: Mapped[str | None] = mapped_column(String(128), nullable=True)
    scope: Mapped[str | None] = mapped_column(String(32), nullable=True)
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Position(Base):
    __tablename__ = "f1_positions"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Interval(Base):
    __tablename__ = "f1_intervals"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gap_to_leader_display: Mapped[str | None] = mapped_column(String(64), nullable=True)
    gap_to_leader_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    gap_to_leader_laps_behind: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gap_to_leader_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    interval_display: Mapped[str | None] = mapped_column(String(64), nullable=True)
    interval_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    interval_laps_behind: Mapped[int | None] = mapped_column(Integer, nullable=True)
    interval_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1Pit(Base):
    __tablename__ = "f1_pit"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lap_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pit_duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1TelemetryIndex(Base):
    __tablename__ = "f1_telemetry_index"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    dataset_name: Mapped[str] = mapped_column(String(64))
    storage_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    sample_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1TeamRadioMetadata(Base):
    __tablename__ = "f1_team_radio_metadata"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    recording_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    transcript_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class F1StartingGrid(Base):
    __tablename__ = "f1_starting_grid"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        index=True,
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    grid_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class CircuitMetadata(Base):
    __tablename__ = "circuit_metadata"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    circuit_name: Mapped[str] = mapped_column(String(255))
    country_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    track_cluster: Mapped[str | None] = mapped_column(String(64), nullable=True)
    length_km: Mapped[float | None] = mapped_column(Float, nullable=True)
    turns: Mapped[int | None] = mapped_column(Integer, nullable=True)
    altitude_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    clockwise: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketEvent(Base):
    __tablename__ = "polymarket_events"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str | None] = mapped_column(String(255), nullable=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(String(128), nullable=True)
    subcategory: Mapped[str | None] = mapped_column(String(128), nullable=True)
    start_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    open_interest: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketMarket(Base):
    __tablename__ = "polymarket_markets"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_events.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    question: Mapped[str] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    condition_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    question_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    market_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sports_market_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    taxonomy: Mapped[str] = mapped_column(String(64), default="other")
    taxonomy_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_session_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    driver_a: Mapped[str | None] = mapped_column(String(128), nullable=True)
    driver_b: Mapped[str | None] = mapped_column(String(128), nullable=True)
    team_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    rules_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    accepting_orders: Mapped[bool] = mapped_column(Boolean, default=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    enable_order_book: Mapped[bool] = mapped_column(Boolean, default=False)
    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_trade_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    clob_token_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketMarketStatusHistory(Base):
    __tablename__ = "polymarket_market_status_history"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    closed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    archived: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    accepting_orders: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketToken(Base):
    __tablename__ = "polymarket_tokens"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    outcome: Mapped[str | None] = mapped_column(String(64), nullable=True)
    outcome_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketMarketRule(Base):
    __tablename__ = "polymarket_market_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    rules_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    parsed_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketPriceHistory(Base):
    __tablename__ = "polymarket_price_history"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    token_id: Mapped[str] = mapped_column(String(128), index=True)
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    midpoint: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_kind: Mapped[str] = mapped_column(String(32), default="clob")
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketOpenInterestHistory(Base):
    __tablename__ = "polymarket_open_interest_history"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    open_interest: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketOrderbookSnapshot(Base):
    __tablename__ = "polymarket_orderbook_snapshots"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    token_id: Mapped[str] = mapped_column(String(128), index=True)
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    bid_depth_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    ask_depth_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    imbalance: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketOrderbookLevel(Base):
    __tablename__ = "polymarket_orderbook_levels"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "side",
            "level_index",
            name="uq_polymarket_orderbook_levels_snapshot_side_level",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("polymarket_orderbook_snapshots.id", ondelete="CASCADE"),
        index=True,
    )
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    token_id: Mapped[str] = mapped_column(String(128), index=True)
    side: Mapped[str] = mapped_column(String(8))
    level_index: Mapped[int] = mapped_column(Integer)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    size: Mapped[float | None] = mapped_column(Float, nullable=True)


class PolymarketTrade(Base):
    __tablename__ = "polymarket_trades"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)
    trade_timestamp_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    side: Mapped[str | None] = mapped_column(String(16), nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    size: Mapped[float | None] = mapped_column(Float, nullable=True)
    outcome: Mapped[str | None] = mapped_column(String(64), nullable=True)
    transaction_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketResolution(Base):
    __tablename__ = "polymarket_resolution"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    resolved_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[str | None] = mapped_column(String(64), nullable=True)
    outcome: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PolymarketWsMessageManifest(Base):
    __tablename__ = "polymarket_ws_message_manifest"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    market_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    object_path: Mapped[str] = mapped_column(String(512))
    event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    observed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    checksum: Mapped[str | None] = mapped_column(String(128), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class MarketTaxonomyVersion(Base):
    __tablename__ = "market_taxonomy_versions"
    __table_args__ = (UniqueConstraint("version_name", name="uq_market_taxonomy_version_name"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    version_name: Mapped[str] = mapped_column(String(64), index=True)
    parser_name: Mapped[str] = mapped_column(String(128))
    rule_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MarketTaxonomyLabel(Base):
    __tablename__ = "market_taxonomy_labels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    taxonomy_version_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("market_taxonomy_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    taxonomy: Mapped[str] = mapped_column(String(64), index=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    label_status: Mapped[str] = mapped_column(String(32), default="candidate")
    target_session_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    parsed_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MappingCandidate(Base):
    __tablename__ = "mapping_candidates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    f1_meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    f1_session_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    polymarket_event_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_events.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    polymarket_market_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    candidate_type: Mapped[str] = mapped_column(String(64))
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    matched_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    rationale_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="candidate")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ManualMappingOverride(Base):
    __tablename__ = "manual_mapping_overrides"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    polymarket_market_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        index=True,
    )
    f1_session_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    f1_meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    mapping_type: Mapped[str] = mapped_column(String(64))
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class EntityMappingF1ToPolymarket(Base):
    __tablename__ = "entity_mapping_f1_to_polymarket"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    f1_meeting_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_meetings.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    f1_session_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("f1_sessions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    polymarket_event_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_events.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    polymarket_market_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("polymarket_markets.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    mapping_type: Mapped[str] = mapped_column(String(64))
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    matched_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    override_flag: Mapped[bool] = mapped_column(Boolean, default=False)


class DatasetVersionManifest(Base):
    __tablename__ = "dataset_version_manifest"
    __table_args__ = (UniqueConstraint("dataset_name", "version", name="uq_dataset_version"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    dataset_name: Mapped[str] = mapped_column(String(128), index=True)
    storage_tier: Mapped[str] = mapped_column(String(16), index=True)
    version: Mapped[str] = mapped_column(String(32))
    manifest_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FeatureRegistry(Base):
    __tablename__ = "feature_registry"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    feature_name: Mapped[str] = mapped_column(String(255), unique=True)
    feature_group: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    data_type: Mapped[str] = mapped_column(String(64))
    version: Mapped[str] = mapped_column(String(32))
    owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FeatureSnapshot(Base):
    __tablename__ = "feature_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    market_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    session_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    as_of_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    snapshot_type: Mapped[str] = mapped_column(String(64))
    feature_version: Mapped[str] = mapped_column(String(32))
    storage_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    source_cutoffs: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)


class SnapshotRunManifest(Base):
    __tablename__ = "snapshot_run_manifest"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    run_name: Mapped[str] = mapped_column(String(128))
    source_cutoffs: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    dataset_version_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ModelRun(Base):
    __tablename__ = "model_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    stage: Mapped[str] = mapped_column(String(64))
    model_family: Mapped[str] = mapped_column(String(128))
    model_name: Mapped[str] = mapped_column(String(128))
    dataset_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    train_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    train_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    val_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    val_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    test_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    test_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ModelPrediction(Base):
    __tablename__ = "model_predictions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    model_run_id: Mapped[str] = mapped_column(String(36), index=True)
    market_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    as_of_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    probability_yes: Mapped[float | None] = mapped_column(Float, nullable=True)
    probability_no: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    calibration_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    explanation_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class BacktestOrder(Base):
    __tablename__ = "backtest_orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    backtest_run_id: Mapped[str] = mapped_column(String(36), index=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    side: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Float)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    executed_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fees: Mapped[float | None] = mapped_column(Float, nullable=True)
    slippage: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class BacktestPosition(Base):
    __tablename__ = "backtest_positions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    backtest_run_id: Mapped[str] = mapped_column(String(36), index=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    quantity: Mapped[float] = mapped_column(Float)
    avg_entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    realized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(32))


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    backtest_run_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    strategy_name: Mapped[str] = mapped_column(String(128))
    stage: Mapped[str] = mapped_column(String(64))
    start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    equity_curve_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    trades_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PaperTradeSession(Base):
    """A paper trading session tied to a GP snapshot and model run."""

    __tablename__ = "paper_trade_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    gp_slug: Mapped[str] = mapped_column(String(128), index=True)
    snapshot_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    model_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    log_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PaperTradePosition(Base):
    """A single simulated position within a paper trading session."""

    __tablename__ = "paper_trade_positions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    session_id: Mapped[str] = mapped_column(String(36), index=True)
    market_id: Mapped[str] = mapped_column(String(64), index=True)
    token_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    side: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    model_prob: Mapped[float] = mapped_column(Float)
    market_prob: Mapped[float] = mapped_column(Float)
    edge: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    realized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
