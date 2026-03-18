from __future__ import annotations

from typing import Any

from f1_polymarket_lab.common import payload_checksum, utc_now
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.lake import LakeObject
from f1_polymarket_lab.storage.models import (
    BronzeObjectManifest,
    IngestionJobDefinition,
    IngestionJobRun,
    SourceCursorState,
    SourceFetchLog,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select
from sqlalchemy.orm import Session


def ensure_job_definition(
    db: Session,
    *,
    job_name: str,
    source: str,
    dataset: str,
    description: str,
    default_cursor: dict[str, Any] | None = None,
    schedule_hint: str | None = None,
) -> IngestionJobDefinition:
    existing = db.scalar(
        select(IngestionJobDefinition).where(IngestionJobDefinition.job_name == job_name)
    )
    if existing is not None:
        existing.source = source
        existing.dataset = dataset
        existing.description = description
        existing.default_cursor = default_cursor
        existing.schedule_hint = schedule_hint
        existing.updated_at = utc_now()
        db.flush()
        return existing

    definition = IngestionJobDefinition(
        job_name=job_name,
        source=source,
        dataset=dataset,
        description=description,
        default_cursor=default_cursor,
        schedule_hint=schedule_hint,
    )
    db.add(definition)
    db.flush()
    return definition


def start_job_run(
    db: Session,
    *,
    definition: IngestionJobDefinition,
    execute: bool,
    planned_inputs: dict[str, Any] | None = None,
    cursor_before: dict[str, Any] | None = None,
) -> IngestionJobRun:
    run = IngestionJobRun(
        job_definition_id=definition.id,
        job_name=definition.job_name,
        source=definition.source,
        dataset=definition.dataset,
        status="running",
        execute_mode="execute" if execute else "plan",
        planned_inputs=planned_inputs,
        cursor_before=cursor_before,
    )
    db.add(run)
    db.flush()
    return run


def finish_job_run(
    db: Session,
    run: IngestionJobRun,
    *,
    status: str,
    cursor_after: dict[str, Any] | None = None,
    records_written: int | None = None,
    error_message: str | None = None,
) -> IngestionJobRun:
    run.status = status
    run.cursor_after = cursor_after
    run.records_written = records_written
    run.error_message = error_message
    run.finished_at = utc_now()
    db.flush()
    return run


def get_cursor_state(
    db: Session,
    *,
    source: str,
    dataset: str,
    cursor_key: str = "default",
) -> SourceCursorState | None:
    return db.scalar(
        select(SourceCursorState).where(
            SourceCursorState.source == source,
            SourceCursorState.dataset == dataset,
            SourceCursorState.cursor_key == cursor_key,
        )
    )


def upsert_cursor_state(
    db: Session,
    *,
    source: str,
    dataset: str,
    cursor_key: str = "default",
    cursor_value: dict[str, Any] | None,
) -> None:
    record = {
        "id": f"{source}:{dataset}:{cursor_key}",
        "source": source,
        "dataset": dataset,
        "cursor_key": cursor_key,
        "cursor_value": cursor_value,
        "cursor_version": 1,
        "updated_at": utc_now(),
    }
    upsert_records(
        db,
        SourceCursorState,
        [record],
        conflict_columns=["source", "dataset", "cursor_key"],
    )


def record_lake_object_manifest(
    db: Session,
    *,
    object_ref: LakeObject,
    job_run_id: str | None,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    record = {
        "id": f"{object_ref.storage_tier}:{payload_checksum(str(object_ref.path))[:24]}",
        "job_run_id": job_run_id,
        "storage_tier": object_ref.storage_tier,
        "source": object_ref.source,
        "dataset": object_ref.dataset,
        "object_path": str(object_ref.path),
        "partition_values": object_ref.partition_values,
        "schema_version": object_ref.schema_version,
        "checksum": object_ref.checksum,
        "record_count": object_ref.record_count,
        "fetched_at": utc_now(),
        "metadata_json": metadata_json,
    }
    upsert_records(
        db,
        BronzeObjectManifest,
        [record],
        conflict_columns=["object_path"],
    )


def record_fetch_batch(
    db: Session,
    *,
    batch: FetchBatch,
    bronze_object: LakeObject | None,
    job_run_id: str | None,
    status: str = "ok",
    error_message: str | None = None,
) -> None:
    now = utc_now()
    bronze_ref = None if bronze_object is None else bronze_object.path
    fetch_id = payload_checksum([batch.endpoint, batch.params, bronze_ref])[:24]
    record = {
        "id": (
            f"{batch.source}:{batch.dataset}:"
            f"{fetch_id}"
        ),
        "job_run_id": job_run_id,
        "source": batch.source,
        "dataset": batch.dataset,
        "endpoint": batch.endpoint,
        "request_params": batch.params,
        "status": status,
        "response_status": batch.response_status,
        "records_fetched": len(batch.payload) if isinstance(batch.payload, list) else 1,
        "bronze_path": None if bronze_object is None else str(bronze_object.path),
        "checksum": None if bronze_object is None else bronze_object.checksum,
        "checkpoint": batch.checkpoint,
        "error_message": error_message,
        "started_at": now,
        "finished_at": now,
    }
    upsert_records(db, SourceFetchLog, [record])
