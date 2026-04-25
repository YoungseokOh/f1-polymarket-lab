from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, cast

from f1_polymarket_lab.common import (
    timestamp_date_variants,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    infer_market_scheduled_date,
)
from f1_polymarket_lab.storage.models import (
    DataQualityCheck,
    DataQualityResult,
    EntityMappingF1ToPolymarket,
    F1Session,
    F1TelemetryIndex,
    IngestionJobRun,
    ManualMappingOverride,
    MappingCandidate,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketWsMessageManifest,
    SourceFetchLog,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import func, select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)

from .context import (
    JobResult,
    PipelineContext,
    best_levels,
    compute_imbalance,
    extract_event_rows,
    normalize_float,
    parse_dt,
    persist_fetch,
    persist_silver,
)
from .f1_sync import (
    MODERN_WEEKEND_SESSION_CODE_BY_NAME,
    MODERN_WEEKEND_SESSION_CODES,
    PRACTICE_SESSION_CODES,
    _delete_meetings_and_children,
    _delete_sessions_and_children,
    ensure_default_feature_registry,
    hydrate_f1_session,
    is_practice_session_name,
    session_code_from_name,
    sync_f1_calendar,
)
from .polymarket_sync import (
    ensure_taxonomy_version,
    hydrate_polymarket_market,
    sync_polymarket_catalog,
)

__all__ = [
    # context
    "JobResult",
    "PipelineContext",
    "best_levels",
    "compute_imbalance",
    "extract_event_rows",
    "normalize_float",
    "parse_dt",
    "persist_fetch",
    "persist_silver",
    # f1_sync
    "MODERN_WEEKEND_SESSION_CODE_BY_NAME",
    "MODERN_WEEKEND_SESSION_CODES",
    "PRACTICE_SESSION_CODES",
    "_delete_meetings_and_children",
    "_delete_sessions_and_children",
    "ensure_default_feature_registry",
    "hydrate_f1_session",
    "is_practice_session_name",
    "session_code_from_name",
    "sync_f1_calendar",
    # polymarket_sync
    "ensure_taxonomy_version",
    "hydrate_polymarket_market",
    "sync_polymarket_catalog",
    # local
    "reconcile_mappings",
    "run_data_quality_checks",
]


def _ensure_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def reconcile_mappings(ctx: PipelineContext, *, min_confidence: float = 0.65) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="reconcile-f1-polymarket-mappings",
        source="derived",
        dataset="entity_mapping",
        description="Create mapping candidates between F1 sessions and Polymarket markets.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=True,
        planned_inputs={"min_confidence": min_confidence},
    )

    sessions = ctx.db.scalars(
        select(F1Session).where(
            F1Session.session_code.in_(["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"])
        )
    ).all()
    markets = ctx.db.scalars(
        select(PolymarketMarket).where(PolymarketMarket.taxonomy != "other")
    ).all()
    existing_candidates = {
        candidate.id: candidate for candidate in ctx.db.scalars(select(MappingCandidate)).all()
    }
    existing_mappings = {
        mapping.id: mapping for mapping in ctx.db.scalars(select(EntityMappingF1ToPolymarket)).all()
    }
    overrides = {
        override.polymarket_market_id: override
        for override in ctx.db.scalars(
            select(ManualMappingOverride).where(ManualMappingOverride.active.is_(True))
        ).all()
    }

    candidate_rows: list[dict[str, Any]] = []
    mapping_rows: list[dict[str, Any]] = []

    def _session_date_variants(session: F1Session) -> tuple[date, ...]:
        raw_payload = session.raw_payload or {}
        offset = raw_payload.get("gmt_offset")
        offset_text = str(offset) if isinstance(offset, str) else None
        return cast(
            tuple[date, ...],
            timestamp_date_variants(session.date_start_utc, gmt_offset=offset_text),
        )

    def _market_delta_days(
        session: F1Session,
        market: PolymarketMarket,
        event: PolymarketEvent | None,
    ) -> tuple[float | None, str]:
        scheduled_date = infer_market_scheduled_date(
            market.slug,
            market.question,
            market.description,
            None if event is None else event.slug,
            None if event is None else event.title,
            None if event is None else event.description,
        )
        session_dates = _session_date_variants(session)
        if scheduled_date is not None and session_dates:
            delta = min(
                abs((scheduled_date - candidate_date).days)
                for candidate_date in session_dates
            )
            return float(delta), "scheduled_date"
        if market.start_at_utc is None or session.date_start_utc is None:
            return None, "unavailable"
        delta = abs((market.start_at_utc - session.date_start_utc).total_seconds()) / 86400
        return delta, "market_start_at_utc"

    for market in markets:
        if market.target_session_code is None:
            continue
        if market.taxonomy_confidence is not None and market.taxonomy_confidence < min_confidence:
            continue
        if market.id in overrides:
            override = overrides[market.id]
            mapping_rows.append(
                {
                    "id": f"override:{market.id}",
                    "f1_meeting_id": override.f1_meeting_id,
                    "f1_session_id": override.f1_session_id,
                    "polymarket_event_id": market.event_id,
                    "polymarket_market_id": market.id,
                    "mapping_type": override.mapping_type,
                    "confidence": 1.0,
                    "matched_by": "manual_override",
                    "notes": override.reason,
                    "override_flag": True,
                }
            )
            continue

        best_match: tuple[F1Session, float, dict[str, Any]] | None = None
        event = None if market.event_id is None else ctx.db.get(PolymarketEvent, market.event_id)
        for session in sessions:
            if session.session_code != market.target_session_code:
                continue
            delta_days, delta_source = _market_delta_days(session, market, event)
            if delta_days is None:
                continue
            if delta_days > 3:
                continue
            date_confidence = max(0.1, 1.0 - (delta_days / 3.0))
            taxonomy_confidence = market.taxonomy_confidence or min_confidence
            confidence = min(0.99, (taxonomy_confidence * 0.55) + (date_confidence * 0.45))
            rationale = {
                "delta_days": round(delta_days, 3),
                "delta_source": delta_source,
                "taxonomy_confidence": taxonomy_confidence,
            }
            if best_match is None or confidence > best_match[1]:
                best_match = (session, confidence, rationale)

        if best_match is None:
            continue
        session, confidence, rationale = best_match
        candidate_id = f"{market.id}:{session.id}"
        existing_candidate = existing_candidates.get(candidate_id)
        if existing_candidate is None or (existing_candidate.confidence or 0.0) < confidence:
            candidate_rows.append(
                {
                    "id": candidate_id,
                    "f1_meeting_id": session.meeting_id,
                    "f1_session_id": session.id,
                    "polymarket_event_id": market.event_id,
                    "polymarket_market_id": market.id,
                    "candidate_type": market.taxonomy,
                    "confidence": confidence,
                    "matched_by": "session_code_time_window",
                    "rationale_json": rationale,
                    "status": "candidate",
                    "created_at": utc_now(),
                }
            )
        if confidence >= min_confidence:
            existing_mapping = existing_mappings.get(candidate_id)
            if existing_mapping is not None and (existing_mapping.confidence or 0.0) >= confidence:
                continue
            mapping_rows.append(
                {
                    "id": candidate_id,
                    "f1_meeting_id": session.meeting_id,
                    "f1_session_id": session.id,
                    "polymarket_event_id": market.event_id,
                    "polymarket_market_id": market.id,
                    "mapping_type": market.taxonomy,
                    "confidence": confidence,
                    "matched_by": "session_code_time_window",
                    "notes": json.dumps(rationale, sort_keys=True),
                    "override_flag": False,
                }
            )

    if candidate_rows:
        upsert_records(ctx.db, MappingCandidate, candidate_rows)
    if mapping_rows:
        upsert_records(ctx.db, EntityMappingF1ToPolymarket, mapping_rows)
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(candidate_rows) + len(mapping_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "candidate_rows": len(candidate_rows),
        "mapping_rows": len(mapping_rows),
    }


def run_data_quality_checks(ctx: PipelineContext) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="run-data-quality-checks",
        source="derived",
        dataset="data_quality",
        description="Run basic row-count and null-rate checks on core tables.",
        schedule_hint="manual",
    )
    run = start_job_run(ctx.db, definition=definition, execute=True)
    checks = [
        {
            "id": "dq:f1_sessions_nonempty",
            "check_name": "f1_sessions_nonempty",
            "dataset": "f1_sessions",
            "severity": "error",
            "rule_type": "min_count",
            "rule_config": {"min_count": 1},
            "active": True,
            "created_at": utc_now(),
        },
        {
            "id": "dq:polymarket_markets_nonempty",
            "check_name": "polymarket_markets_nonempty",
            "dataset": "polymarket_markets",
            "severity": "warning",
            "rule_type": "min_count",
            "rule_config": {"min_count": 1},
            "active": True,
            "created_at": utc_now(),
        },
        {
            "id": "dq:f1_telemetry_nonempty",
            "check_name": "f1_telemetry_nonempty",
            "dataset": "f1_telemetry_index",
            "severity": "warning",
            "rule_type": "min_count",
            "rule_config": {"min_count": 1},
            "active": True,
            "created_at": utc_now(),
        },
        {
            "id": "dq:polymarket_ws_manifest_nonempty",
            "check_name": "polymarket_ws_manifest_nonempty",
            "dataset": "polymarket_ws_message_manifest",
            "severity": "warning",
            "rule_type": "min_count",
            "rule_config": {"min_count": 1},
            "active": True,
            "created_at": utc_now(),
        },
        {
            "id": "dq:polymarket_active_market_freshness",
            "check_name": "polymarket_active_market_freshness",
            "dataset": "source_fetch_log",
            "severity": "warning",
            "rule_type": "freshness_hours",
            "rule_config": {"max_age_hours": 24},
            "active": True,
            "created_at": utc_now(),
        },
    ]
    upsert_records(ctx.db, DataQualityCheck, checks, conflict_columns=["check_name"])

    result_rows: list[dict[str, Any]] = []
    session_count = ctx.db.scalar(select(func.count()).select_from(F1Session)) or 0
    market_count = ctx.db.scalar(select(func.count()).select_from(PolymarketMarket)) or 0
    telemetry_count = ctx.db.scalar(select(func.count()).select_from(F1TelemetryIndex)) or 0
    ws_manifest_count = (
        ctx.db.scalar(select(func.count()).select_from(PolymarketWsMessageManifest)) or 0
    )
    live_capture_job_count = (
        ctx.db.scalar(
            select(func.count())
            .select_from(IngestionJobRun)
            .where(
                IngestionJobRun.job_name == "capture-live-weekend",
                IngestionJobRun.status == "completed",
            )
        )
        or 0
    )
    ws_manifest_expected = live_capture_job_count > 0
    latest_polymarket_fetch = ctx.db.scalar(
        select(SourceFetchLog)
        .where(SourceFetchLog.source.in_(["polymarket", "polymarket_ws"]))
        .order_by(SourceFetchLog.finished_at.desc())
        .limit(1)
    )
    freshness_hours = None
    latest_polymarket_finished_at = (
        None
        if latest_polymarket_fetch is None
        else _ensure_utc_datetime(latest_polymarket_fetch.finished_at)
    )
    if latest_polymarket_finished_at is not None:
        freshness_hours = round(
            (utc_now() - latest_polymarket_finished_at).total_seconds() / 3600,
            3,
        )
    result_rows.append(
        {
            "id": f"dq-result:{run.id}:f1_sessions",
            "check_id": "dq:f1_sessions_nonempty",
            "job_run_id": run.id,
            "dataset": "f1_sessions",
            "status": "pass" if session_count > 0 else "fail",
            "metrics_json": {"row_count": session_count},
            "sample_path": None,
            "observed_at": utc_now(),
        }
    )
    def warning_status(passed: bool) -> str:
        return "pass" if passed else "warning"

    result_rows.append(
        {
            "id": f"dq-result:{run.id}:polymarket_markets",
            "check_id": "dq:polymarket_markets_nonempty",
            "job_run_id": run.id,
            "dataset": "polymarket_markets",
            "status": warning_status(market_count > 0),
            "metrics_json": {"row_count": market_count},
            "sample_path": None,
            "observed_at": utc_now(),
        }
    )
    result_rows.append(
        {
            "id": f"dq-result:{run.id}:f1_telemetry",
            "check_id": "dq:f1_telemetry_nonempty",
            "job_run_id": run.id,
            "dataset": "f1_telemetry_index",
            "status": warning_status(telemetry_count > 0),
            "metrics_json": {"row_count": telemetry_count},
            "sample_path": None,
            "observed_at": utc_now(),
        }
    )
    result_rows.append(
        {
            "id": f"dq-result:{run.id}:polymarket_ws_manifest",
            "check_id": "dq:polymarket_ws_manifest_nonempty",
            "job_run_id": run.id,
            "dataset": "polymarket_ws_message_manifest",
            "status": warning_status(not ws_manifest_expected or ws_manifest_count > 0),
            "metrics_json": {
                "row_count": ws_manifest_count,
                "completed_capture_jobs": live_capture_job_count,
                "expected": ws_manifest_expected,
            },
            "sample_path": None,
            "observed_at": utc_now(),
        }
    )
    result_rows.append(
        {
            "id": f"dq-result:{run.id}:polymarket_freshness",
            "check_id": "dq:polymarket_active_market_freshness",
            "job_run_id": run.id,
            "dataset": "source_fetch_log",
            "status": warning_status(freshness_hours is not None and freshness_hours <= 24),
            "metrics_json": {"freshness_hours": freshness_hours},
            "sample_path": None,
            "observed_at": utc_now(),
        }
    )
    upsert_records(ctx.db, DataQualityResult, result_rows)
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(result_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "results": len(result_rows),
    }
