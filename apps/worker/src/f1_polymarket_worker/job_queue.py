from __future__ import annotations

import json
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.storage.db import db_session
from f1_polymarket_lab.storage.models import IngestionJobRun
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from f1_polymarket_worker.lineage import ensure_job_definition, finish_job_run
from f1_polymarket_worker.pipeline import PipelineContext

JobHandler = Callable[[Session, Any, dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class QueuedJobSpec:
    job_name: str
    source: str
    dataset: str
    description: str
    handler: JobHandler
    default_max_attempts: int = 3


def _json_safe(payload: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(json.dumps(payload, default=str)))


def _records_written(summary: dict[str, Any]) -> int:
    explicit = summary.get("records_written")
    if isinstance(explicit, int | float):
        return int(explicit)
    if isinstance(explicit, str) and explicit.isdigit():
        return int(explicit)
    numeric_values = [
        value
        for value in summary.values()
        if isinstance(value, int) and not isinstance(value, bool)
    ]
    return int(sum(numeric_values)) if numeric_values else 0


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _retry_available_after(attempt_count: int) -> Any:
    delay_seconds = min(300, max(10, 10 * (2 ** max(attempt_count - 1, 0))))
    return utc_now() + timedelta(seconds=delay_seconds)


def _int_input(inputs: dict[str, Any], key: str, default: int) -> int:
    value = inputs.get(key, default)
    if value is None:
        return default
    return int(value)


def _optional_int_input(inputs: dict[str, Any], key: str) -> int | None:
    value = inputs.get(key)
    if value is None or value == "":
        return None
    return int(value)


def _float_input(inputs: dict[str, Any], key: str, default: float) -> float:
    value = inputs.get(key, default)
    if value is None:
        return default
    return float(value)


def _bool_input(inputs: dict[str, Any], key: str, default: bool) -> bool:
    value = inputs.get(key, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _str_input(inputs: dict[str, Any], key: str, default: str | None = None) -> str | None:
    value = inputs.get(key, default)
    if value is None:
        return default
    return str(value)


def _list_str_input(inputs: dict[str, Any], key: str) -> list[str] | None:
    value = inputs.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value]
    raise TypeError(f"{key} must be a string or list of strings")


def _run_ingest_demo(session: Session, _settings: Any, inputs: dict[str, Any]) -> dict[str, Any]:
    from f1_polymarket_worker.demo_ingest import ingest_demo

    return ingest_demo(
        session,
        season=_int_input(inputs, "season", 2024),
        weekends=_int_input(inputs, "weekends", 2),
        market_batches=_int_input(inputs, "market_batches", 3),
    )


def _run_sync_f1_calendar(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.pipeline import sync_f1_calendar

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return sync_f1_calendar(ctx, season=_int_input(inputs, "season", 2024))


def _run_sync_f1_markets(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.orchestration import sync_polymarket_f1_catalog

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return sync_polymarket_f1_catalog(
        ctx,
        max_pages=_int_input(inputs, "max_pages", 20),
        batch_size=_int_input(inputs, "batch_size", 100),
        search_fallback=_bool_input(inputs, "search_fallback", True),
        start_year=_int_input(inputs, "start_year", 2022),
        end_year=(
            _int_input(inputs, "end_year", 0) if inputs.get("end_year") is not None else None
        ),
    )


def _run_backfill_backtests(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.backtest import backfill_backtests

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return backfill_backtests(
        ctx,
        gp_short_code=_str_input(inputs, "gp_short_code"),
        min_edge=_float_input(inputs, "min_edge", 0.05),
        bet_size=_float_input(inputs, "bet_size", 10.0),
        rebuild_missing=_bool_input(inputs, "rebuild_missing", True),
    )


def _run_backtest(session: Session, settings: Any, inputs: dict[str, Any]) -> dict[str, Any]:
    from f1_polymarket_worker.backtest import settle_single_gp
    from f1_polymarket_worker.gp_registry import build_snapshot, resolve_gp_config, run_baseline

    gp_short_code = _str_input(inputs, "gp_short_code")
    if gp_short_code is None:
        raise ValueError("gp_short_code is required")

    config = resolve_gp_config(gp_short_code, db=session)
    ctx = PipelineContext(db=session, execute=True, settings=settings)
    snap_result = build_snapshot(ctx, config)
    snapshot_id = snap_result.get("snapshot_id")
    if not snapshot_id:
        raise ValueError(f"Snapshot build returned no snapshot_id: {snap_result}")

    run_baseline(
        ctx,
        config,
        snapshot_id=str(snapshot_id),
        min_edge=_float_input(inputs, "min_edge", 0.05),
    )
    settle_result = settle_single_gp(
        ctx,
        meeting_key=config.meeting_key,
        season=config.season,
        snapshot_id=str(snapshot_id),
        min_edge=_float_input(inputs, "min_edge", 0.05),
        bet_size=_float_input(inputs, "bet_size", 10.0),
    )
    return {
        "snapshot_id": str(snapshot_id),
        "snapshot": snap_result,
        "settlement": settle_result,
    }


def _run_paper_trade(session: Session, settings: Any, inputs: dict[str, Any]) -> dict[str, Any]:
    from f1_polymarket_worker.gp_registry import resolve_gp_config
    from f1_polymarket_worker.weekend_ops import run_gp_paper_trade_pipeline

    gp_short_code = _str_input(inputs, "gp_short_code")
    if gp_short_code is None:
        raise ValueError("gp_short_code is required")

    config = resolve_gp_config(gp_short_code, db=session)
    ctx = PipelineContext(db=session, execute=True, settings=settings)
    result = run_gp_paper_trade_pipeline(
        ctx,
        config=config,
        snapshot_id=_str_input(inputs, "snapshot_id"),
        baseline=_str_input(inputs, "baseline", "hybrid") or "hybrid",
        min_edge=_float_input(inputs, "min_edge", 0.05),
        bet_size=_float_input(inputs, "bet_size", 10.0),
    )
    return {"gp_short_code": config.short_code, **result}


def _run_weekend_cockpit(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.weekend_ops import run_weekend_cockpit

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return run_weekend_cockpit(
        ctx,
        gp_short_code=_str_input(inputs, "gp_short_code"),
        baseline=_str_input(inputs, "baseline", "hybrid") or "hybrid",
        min_edge=_float_input(inputs, "min_edge", 0.05),
        bet_size=_float_input(inputs, "bet_size", 10.0),
        search_fallback=_bool_input(inputs, "search_fallback", True),
        discover_max_pages=_int_input(inputs, "discover_max_pages", 5),
    )


def _run_refresh_latest_session(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.weekend_ops import refresh_latest_session_for_meeting

    meeting_id = _str_input(inputs, "meeting_id")
    if meeting_id is None:
        raise ValueError("meeting_id is required")

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return refresh_latest_session_for_meeting(
        ctx,
        meeting_id=meeting_id,
        search_fallback=_bool_input(inputs, "search_fallback", True),
        discover_max_pages=_int_input(inputs, "discover_max_pages", 5),
        hydrate_market_history=_bool_input(inputs, "hydrate_market_history", True),
        market_history_fidelity=_int_input(inputs, "market_history_fidelity", 60),
        sync_calendar=_bool_input(inputs, "sync_calendar", True),
        hydrate_f1_session_data=_bool_input(inputs, "hydrate_f1_session_data", True),
        include_extended_f1_data=_bool_input(inputs, "include_extended_f1_data", True),
        include_heavy_f1_data=_bool_input(inputs, "include_heavy_f1_data", True),
        refresh_artifacts=_bool_input(inputs, "refresh_artifacts", True),
    )


def _run_refresh_driver_affinity(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.driver_affinity import refresh_driver_affinity

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return refresh_driver_affinity(
        ctx,
        season=_int_input(inputs, "season", 2026),
        meeting_key=_optional_int_input(inputs, "meeting_key"),
        force=_bool_input(inputs, "force", False),
    )


def _run_capture_live_weekend(
    session: Session,
    settings: Any,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    from f1_polymarket_worker.weekend_ops import capture_live_weekend

    session_key = _optional_int_input(inputs, "session_key")
    if session_key is None:
        raise ValueError("session_key is required")

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return capture_live_weekend(
        ctx,
        session_key=session_key,
        market_ids=_list_str_input(inputs, "market_ids"),
        start_buffer_min=_int_input(inputs, "start_buffer_min", 15),
        stop_buffer_min=_int_input(inputs, "stop_buffer_min", 15),
        message_limit=_optional_int_input(inputs, "message_limit"),
        capture_seconds=_optional_int_input(inputs, "capture_seconds"),
    )


def _run_dq(session: Session, settings: Any, _inputs: dict[str, Any]) -> dict[str, Any]:
    from f1_polymarket_worker.pipeline import run_data_quality_checks

    ctx = PipelineContext(db=session, execute=True, settings=settings)
    return run_data_quality_checks(ctx)


QUEUE_JOB_SPECS: dict[str, QueuedJobSpec] = {
    "ingest-demo": QueuedJobSpec(
        job_name="ingest-demo",
        source="demo",
        dataset="demo_ingest",
        description="Seed a lightweight demo ingestion run for the dashboard.",
        handler=_run_ingest_demo,
    ),
    "sync-f1-calendar": QueuedJobSpec(
        job_name="sync-f1-calendar",
        source="openf1",
        dataset="f1_calendar",
        description="Sync the F1 meeting/session calendar.",
        handler=_run_sync_f1_calendar,
    ),
    "sync-f1-markets": QueuedJobSpec(
        job_name="sync-f1-markets",
        source="polymarket",
        dataset="polymarket_f1_catalog",
        description="Discover and hydrate Polymarket F1 catalog metadata.",
        handler=_run_sync_f1_markets,
    ),
    "run-backtest": QueuedJobSpec(
        job_name="run-backtest",
        source="gold",
        dataset="backtest_results",
        description="Build a GP snapshot, run baseline predictions, and settle a backtest.",
        handler=_run_backtest,
        default_max_attempts=1,
    ),
    "backfill-backtests": QueuedJobSpec(
        job_name="backfill-backtests",
        source="gold",
        dataset="backtest_results",
        description="Settle or rebuild stored GP backtest snapshots.",
        handler=_run_backfill_backtests,
        default_max_attempts=1,
    ),
    "run-paper-trade": QueuedJobSpec(
        job_name="run-paper-trade",
        source="gold",
        dataset="paper_trade_sessions",
        description="Run the full paper-trading pipeline for a GP.",
        handler=_run_paper_trade,
        default_max_attempts=1,
    ),
    "run-weekend-cockpit": QueuedJobSpec(
        job_name="run-weekend-cockpit",
        source="hybrid",
        dataset="weekend_cockpit",
        description="Run the current weekend operations cockpit workflow.",
        handler=_run_weekend_cockpit,
        default_max_attempts=1,
    ),
    "refresh-latest-session": QueuedJobSpec(
        job_name="refresh-latest-session",
        source="hybrid",
        dataset="latest_session_refresh",
        description="Refresh the latest ended session and linked artifacts for a meeting.",
        handler=_run_refresh_latest_session,
        default_max_attempts=1,
    ),
    "refresh-driver-affinity": QueuedJobSpec(
        job_name="refresh-driver-affinity",
        source="derived",
        dataset="driver_affinity_report",
        description="Refresh the current meeting driver-affinity report.",
        handler=_run_refresh_driver_affinity,
        default_max_attempts=1,
    ),
    "capture-live-weekend": QueuedJobSpec(
        job_name="capture-live-weekend",
        source="hybrid",
        dataset="live_weekend",
        description="Capture live OpenF1 and Polymarket messages for a session.",
        handler=_run_capture_live_weekend,
        default_max_attempts=1,
    ),
    "dq-run": QueuedJobSpec(
        job_name="dq-run",
        source="quality",
        dataset="data_quality_results",
        description="Run configured data-quality checks.",
        handler=_run_dq,
    ),
}


def enqueue_job(
    db: Session,
    *,
    job_name: str,
    planned_inputs: dict[str, Any] | None = None,
    max_attempts: int | None = None,
) -> IngestionJobRun:
    spec = QUEUE_JOB_SPECS.get(job_name)
    if spec is None:
        raise KeyError(f"Unknown queued job: {job_name}")

    definition = ensure_job_definition(
        db,
        job_name=spec.job_name,
        source=spec.source,
        dataset=spec.dataset,
        description=spec.description,
        schedule_hint="manual",
    )
    now = utc_now()
    run = IngestionJobRun(
        job_definition_id=definition.id,
        job_name=definition.job_name,
        source=definition.source,
        dataset=definition.dataset,
        status="queued",
        execute_mode="queued",
        planned_inputs=planned_inputs or {},
        queued_at=now,
        available_at=now,
        attempt_count=0,
        max_attempts=max_attempts or spec.default_max_attempts,
        started_at=now,
    )
    db.add(run)
    db.flush()
    return run


def recover_stale_jobs(
    db: Session,
    *,
    stale_after_seconds: int,
    job_names: set[str],
) -> int:
    if stale_after_seconds <= 0:
        return 0

    cutoff = utc_now() - timedelta(seconds=stale_after_seconds)
    rows = db.scalars(
        select(IngestionJobRun).where(
            IngestionJobRun.status == "running",
            IngestionJobRun.execute_mode == "queued",
            IngestionJobRun.job_name.in_(job_names),
            IngestionJobRun.locked_at.is_not(None),
            IngestionJobRun.locked_at < cutoff,
        )
    ).all()
    for run in rows:
        message = (
            f"Recovered stale worker lock held by {run.locked_by or 'unknown'} "
            f"since {run.locked_at}."
        )
        if run.attempt_count >= run.max_attempts:
            finish_job_run(db, run, status="failed", records_written=0, error_message=message)
            continue
        run.status = "queued"
        run.available_at = utc_now()
        run.locked_by = None
        run.locked_at = None
        run.error_message = message
        run.finished_at = None
    db.flush()
    return len(rows)


def claim_next_job(
    db: Session,
    *,
    worker_id: str,
    job_names: set[str],
) -> str | None:
    now = utc_now()
    run = db.scalar(
        select(IngestionJobRun)
        .where(
            IngestionJobRun.status == "queued",
            IngestionJobRun.job_name.in_(job_names),
            or_(
                IngestionJobRun.available_at.is_(None),
                IngestionJobRun.available_at <= now,
            ),
        )
        .order_by(
            IngestionJobRun.available_at.asc(),
            IngestionJobRun.queued_at.asc(),
            IngestionJobRun.started_at.asc(),
            IngestionJobRun.id.asc(),
        )
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if run is None:
        return None

    run.status = "running"
    run.execute_mode = "queued"
    run.attempt_count = (run.attempt_count or 0) + 1
    run.locked_by = worker_id
    run.locked_at = now
    run.started_at = now
    run.finished_at = None
    db.flush()
    return str(run.id)


def execute_claimed_job(
    db: Session,
    *,
    settings: Any,
    run_id: str,
) -> dict[str, Any]:
    run = db.get(IngestionJobRun, run_id)
    if run is None:
        return {"status": "missing", "job_run_id": run_id}

    spec = QUEUE_JOB_SPECS.get(run.job_name)
    if spec is None:
        finish_job_run(
            db,
            run,
            status="failed",
            records_written=0,
            error_message=f"Unknown queued job: {run.job_name}",
        )
        return {"status": "failed", "job_run_id": run_id, "error": "unknown job"}

    try:
        summary = _json_safe(spec.handler(db, settings, run.planned_inputs or {}))
        finish_job_run(
            db,
            run,
            status="completed",
            cursor_after=summary,
            records_written=_records_written(summary),
        )
        return {
            "status": "completed",
            "job_run_id": run.id,
            "job_name": run.job_name,
            "records_written": run.records_written or 0,
        }
    except Exception as exc:
        db.rollback()
        run = db.get(IngestionJobRun, run_id)
        if run is None:
            return {"status": "failed", "job_run_id": run_id, "error": str(exc)}

        if run.attempt_count < run.max_attempts:
            run.status = "queued"
            run.available_at = _retry_available_after(run.attempt_count)
            run.locked_by = None
            run.locked_at = None
            run.error_message = str(exc)
            run.finished_at = None
            db.flush()
            return {
                "status": "retrying",
                "job_run_id": run.id,
                "job_name": run.job_name,
                "attempt_count": run.attempt_count,
                "max_attempts": run.max_attempts,
                "available_at": str(run.available_at),
                "error": str(exc),
            }

        finish_job_run(
            db,
            run,
            status="failed",
            records_written=0,
            error_message=str(exc),
        )
        return {
            "status": "failed",
            "job_run_id": run.id,
            "job_name": run.job_name,
            "attempt_count": run.attempt_count,
            "max_attempts": run.max_attempts,
            "error": str(exc),
        }


def run_worker_once(
    settings: Any,
    *,
    worker_id: str | None = None,
    job_names: set[str] | None = None,
    stale_after_seconds: int = 7200,
) -> dict[str, Any]:
    allowed_jobs = job_names or set(QUEUE_JOB_SPECS)
    resolved_worker_id = worker_id or _worker_id()
    with db_session(settings.database_url) as session:
        recovered = recover_stale_jobs(
            session,
            stale_after_seconds=stale_after_seconds,
            job_names=allowed_jobs,
        )
        run_id = claim_next_job(session, worker_id=resolved_worker_id, job_names=allowed_jobs)
        if run_id is None:
            return {"status": "idle", "processed": 0, "recovered": recovered}

    with db_session(settings.database_url) as session:
        result = execute_claimed_job(session, settings=settings, run_id=run_id)
        result["recovered"] = recovered
        return result
