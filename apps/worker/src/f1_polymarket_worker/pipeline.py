from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, cast

from f1_polymarket_lab.common import (
    Settings,
    get_settings,
    parse_gap_value,
    parse_result_time_value,
    payload_checksum,
    slugify,
    stable_uuid,
    timestamp_date_variants,
    utc_now,
)
from f1_polymarket_lab.common import (
    normalize_float as common_normalize_float,
)
from f1_polymarket_lab.connectors import (
    FastF1ScheduleConnector,
    OpenF1Connector,
    PolymarketConnector,
    infer_market_scheduled_date,
    parse_market_taxonomy,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.features import default_feature_registry
from f1_polymarket_lab.storage.lake import LakeObject, LakeWriter
from f1_polymarket_lab.storage.models import (
    CircuitMetadata,
    DataQualityCheck,
    DataQualityResult,
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Interval,
    F1Lap,
    F1Meeting,
    F1Pit,
    F1Position,
    F1RaceControl,
    F1Session,
    F1SessionResult,
    F1StartingGrid,
    F1Stint,
    F1Team,
    F1TeamRadioMetadata,
    F1TelemetryIndex,
    F1Weather,
    FeatureRegistry,
    ManualMappingOverride,
    MappingCandidate,
    MarketTaxonomyLabel,
    MarketTaxonomyVersion,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketMarketRule,
    PolymarketMarketStatusHistory,
    PolymarketOpenInterestHistory,
    PolymarketOrderbookLevel,
    PolymarketOrderbookSnapshot,
    PolymarketPriceHistory,
    PolymarketResolution,
    PolymarketToken,
    PolymarketTrade,
    PolymarketWsMessageManifest,
    SourceFetchLog,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    record_fetch_batch,
    record_lake_object_manifest,
    start_job_run,
    upsert_cursor_state,
)


@dataclass(slots=True)
class JobResult:
    records_written: int = 0
    cursor_after: dict[str, Any] | None = None
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineContext:
    db: Session
    execute: bool = False
    settings: Settings = field(default_factory=get_settings)
    lake: LakeWriter = field(init=False)

    def __post_init__(self) -> None:
        self.lake = LakeWriter(self.settings.data_root)


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text)


MODERN_WEEKEND_SESSION_CODE_BY_NAME = {
    "Practice 1": "FP1",
    "Practice 2": "FP2",
    "Practice 3": "FP3",
    "Qualifying": "Q",
    "Sprint Qualifying": "SQ",
    "Sprint Shootout": "SQ",
    "Sprint": "S",
    "Race": "R",
}
MODERN_WEEKEND_SESSION_CODES = frozenset(MODERN_WEEKEND_SESSION_CODE_BY_NAME.values())
PRACTICE_SESSION_CODES = frozenset({"FP1", "FP2", "FP3"})


def session_code_from_name(name: str) -> str | None:
    return MODERN_WEEKEND_SESSION_CODE_BY_NAME.get(name)


def is_practice_session_name(name: str) -> bool:
    session_code = session_code_from_name(name)
    return session_code in PRACTICE_SESSION_CODES


def _delete_sessions_and_children(ctx: PipelineContext, session_ids: list[str]) -> None:
    if not session_ids:
        return

    for model in (
        F1SessionResult,
        F1Lap,
        F1Stint,
        F1Weather,
        F1RaceControl,
        F1Position,
        F1Interval,
        F1Pit,
        F1TelemetryIndex,
        F1TeamRadioMetadata,
        F1StartingGrid,
        MappingCandidate,
        EntityMappingF1ToPolymarket,
        ManualMappingOverride,
    ):
        column_name = (
            "f1_session_id"
            if model in {MappingCandidate, EntityMappingF1ToPolymarket, ManualMappingOverride}
            else "session_id"
        )
        ctx.db.execute(delete(model).where(getattr(model, column_name).in_(session_ids)))

    ctx.db.execute(delete(F1Session).where(F1Session.id.in_(session_ids)))


def _delete_meetings_and_children(ctx: PipelineContext, meeting_ids: list[str]) -> None:
    if not meeting_ids:
        return

    for model in (
        F1Weather,
        F1RaceControl,
        MappingCandidate,
        EntityMappingF1ToPolymarket,
        ManualMappingOverride,
    ):
        column_name = (
            "f1_meeting_id"
            if model in {MappingCandidate, EntityMappingF1ToPolymarket, ManualMappingOverride}
            else "meeting_id"
        )
        ctx.db.execute(delete(model).where(getattr(model, column_name).in_(meeting_ids)))

    ctx.db.execute(delete(F1Meeting).where(F1Meeting.id.in_(meeting_ids)))


def normalize_float(value: Any) -> float | None:
    return cast(float | None, common_normalize_float(value))


def best_levels(book: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if book is None:
        return None, None
    bid = float(book["bids"][0]["price"]) if book.get("bids") else None
    ask = float(book["asks"][0]["price"]) if book.get("asks") else None
    return bid, ask


def compute_imbalance(book: dict[str, Any] | None) -> float | None:
    if book is None:
        return None
    bid_depth = sum(float(level["size"]) for level in book.get("bids", [])[:5])
    ask_depth = sum(float(level["size"]) for level in book.get("asks", [])[:5])
    total = bid_depth + ask_depth
    if total == 0:
        return None
    return (bid_depth - ask_depth) / total


def extract_event_rows(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for market in markets:
        for event in market.get("events", []):
            events[str(event["id"])] = {
                "id": str(event["id"]),
                "ticker": event.get("ticker"),
                "slug": event["slug"],
                "title": event["title"],
                "description": event.get("description"),
                "category": event.get("category"),
                "subcategory": event.get("subcategory"),
                "start_at_utc": parse_dt(event.get("startDate")),
                "end_at_utc": parse_dt(event.get("endDate")),
                "active": bool(event.get("active")),
                "closed": bool(event.get("closed")),
                "archived": bool(event.get("archived")),
                "liquidity": event.get("liquidity"),
                "volume": event.get("volume"),
                "open_interest": event.get("openInterest"),
                "resolution_source": event.get("resolutionSource"),
                "raw_payload": event,
            }
    return list(events.values())


def persist_fetch(
    ctx: PipelineContext,
    *,
    job_run_id: str,
    batch: FetchBatch,
    partition: dict[str, str] | None = None,
) -> LakeObject:
    bronze_object = ctx.lake.write_bronze_object(
        batch.source,
        batch.dataset,
        batch.payload,
        partition=partition,
    )
    record_lake_object_manifest(
        ctx.db,
        object_ref=bronze_object,
        job_run_id=job_run_id,
        metadata_json={"endpoint": batch.endpoint, "request_params": batch.params},
    )
    record_fetch_batch(
        ctx.db,
        batch=batch,
        bronze_object=bronze_object,
        job_run_id=job_run_id,
    )
    return bronze_object


def persist_silver(
    ctx: PipelineContext,
    *,
    job_run_id: str,
    dataset: str,
    records: list[dict[str, Any]],
    partition: dict[str, str] | None = None,
) -> None:
    object_ref = ctx.lake.write_silver_object(dataset, records, partition=partition)
    if object_ref is None:
        return
    record_lake_object_manifest(
        ctx.db,
        object_ref=object_ref,
        job_run_id=job_run_id,
        metadata_json={"normalized_dataset": dataset},
    )


def ensure_default_feature_registry(ctx: PipelineContext) -> None:
    records = [
        {
            "id": f"feature:{definition.feature_name}",
            "feature_name": definition.feature_name,
            "feature_group": definition.feature_group,
            "description": definition.description,
            "data_type": definition.data_type,
            "version": definition.version,
            "owner": "platform",
            "created_at": utc_now(),
        }
        for definition in default_feature_registry()
    ]
    upsert_records(
        ctx.db,
        FeatureRegistry,
        records,
        conflict_columns=["feature_name"],
    )


def sync_f1_calendar(ctx: PipelineContext, *, season: int) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-f1-calendar",
        source="openf1",
        dataset="f1_calendar",
        description="Sync F1 meeting and session catalog for a season.",
        default_cursor={"season": season},
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_calendar",
        cursor_key=f"season:{season}",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"season": season},
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned", "season": season}

    openf1 = OpenF1Connector()
    fastf1 = FastF1ScheduleConnector(ctx.settings.data_root / "cache" / "fastf1")
    sessions = openf1.fetch_sessions(season)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="sessions",
            endpoint="/v1/sessions",
            params={"year": season},
            payload=sessions,
            response_status=200,
            checkpoint=str(season),
        ),
        partition={"season": str(season)},
    )
    try:
        schedule = fastf1.fetch_event_schedule(season)
    except Exception:
        schedule = []
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="fastf1",
            dataset="event_schedule",
            endpoint="fastf1:event_schedule",
            params={"year": season},
            payload=schedule,
            response_status=200,
            checkpoint=str(season),
        ),
        partition={"season": str(season)},
    )

    schedule_by_location = {str(record.get("Location", "")).lower(): record for record in schedule}
    sessions_by_meeting: dict[int, list[dict[str, Any]]] = {}
    for record in sessions:
        session_name = str(record.get("session_name") or "")
        if session_code_from_name(session_name) not in MODERN_WEEKEND_SESSION_CODES:
            continue
        sessions_by_meeting.setdefault(int(record["meeting_key"]), []).append(record)

    meeting_rows: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    circuit_rows: dict[str, dict[str, Any]] = {}
    for meeting_key, records in sorted(sessions_by_meeting.items()):
        first_session = sorted(records, key=lambda item: item["date_start"])[0]
        schedule_row = schedule_by_location.get(str(first_session.get("location", "")).lower(), {})
        meeting_id = f"meeting:{meeting_key}"
        meeting_rows.append(
            {
                "id": meeting_id,
                "source": "openf1",
                "meeting_key": meeting_key,
                "season": season,
                "round_number": schedule_row.get("RoundNumber"),
                "meeting_name": str(
                    schedule_row.get("EventName")
                    or first_session.get("country_name")
                    or meeting_key
                ),
                "meeting_official_name": schedule_row.get("OfficialEventName"),
                "circuit_short_name": first_session.get("circuit_short_name"),
                "country_name": first_session.get("country_name"),
                "location": first_session.get("location"),
                "start_date_utc": parse_dt(first_session.get("date_start")),
                "end_date_utc": parse_dt(
                    sorted(records, key=lambda item: item["date_end"])[-1]["date_end"]
                ),
                "raw_payload": first_session,
            }
        )
        circuit_key = str(first_session.get("circuit_key"))
        circuit_rows[circuit_key] = {
            "id": circuit_key,
            "circuit_name": first_session.get("circuit_short_name") or "Unknown",
            "country_name": first_session.get("country_name"),
            "track_cluster": None,
            "length_km": None,
            "turns": None,
            "altitude_m": None,
            "clockwise": None,
            "raw_payload": {"circuit_key": first_session.get("circuit_key")},
        }
        for record in records:
            session_name = str(record.get("session_name") or "")
            session_code = session_code_from_name(session_name)
            if session_code not in MODERN_WEEKEND_SESSION_CODES:
                continue
            session_rows.append(
                {
                    "id": f"session:{record['session_key']}",
                    "source": "openf1",
                    "session_key": int(record["session_key"]),
                    "meeting_id": meeting_id,
                    "session_name": session_name,
                    "session_type": record.get("session_type"),
                    "session_code": session_code,
                    "date_start_utc": parse_dt(record.get("date_start")),
                    "date_end_utc": parse_dt(record.get("date_end")),
                    "status": "complete" if record.get("date_end") else "scheduled",
                    "session_order": None,
                    "is_practice": session_code in PRACTICE_SESSION_CODES,
                    "raw_payload": record,
                }
            )

    stale_session_ids = [
        str(session_id)
        for session_id in ctx.db.scalars(
            select(F1Session.id)
            .join(F1Meeting, F1Meeting.id == F1Session.meeting_id)
            .where(
                F1Meeting.season == season,
                F1Session.source == "openf1",
                (
                    F1Session.session_code.is_(None)
                    | F1Session.session_code.not_in(tuple(MODERN_WEEKEND_SESSION_CODES))
                ),
            )
        ).all()
    ]
    _delete_sessions_and_children(ctx, stale_session_ids)
    current_meeting_ids = {row["id"] for row in meeting_rows}
    existing_openf1_meeting_ids = [
        str(meeting_id)
        for meeting_id in ctx.db.scalars(
            select(F1Meeting.id).where(F1Meeting.season == season, F1Meeting.source == "openf1")
        ).all()
    ]
    stale_meeting_ids = [
        meeting_id
        for meeting_id in existing_openf1_meeting_ids
        if meeting_id not in current_meeting_ids
    ]
    _delete_meetings_and_children(ctx, stale_meeting_ids)

    upsert_records(ctx.db, F1Meeting, meeting_rows)
    upsert_records(ctx.db, F1Session, session_rows)
    upsert_records(ctx.db, CircuitMetadata, circuit_rows.values())
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="f1_meetings",
        records=meeting_rows,
        partition={"season": str(season)},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="f1_sessions",
        records=session_rows,
        partition={"season": str(season)},
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="circuit_metadata",
        records=list(circuit_rows.values()),
        partition={"season": str(season)},
    )
    upsert_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_calendar",
        cursor_key=f"season:{season}",
        cursor_value={"season": season, "synced_at": utc_now().isoformat()},
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={"season": season, "synced_at": utc_now().isoformat()},
        records_written=len(meeting_rows) + len(session_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "meetings": len(meeting_rows),
        "sessions": len(session_rows),
    }


def hydrate_f1_session(
    ctx: PipelineContext,
    *,
    session_key: int,
    include_extended: bool = False,
    include_heavy: bool = False,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="hydrate-f1-session",
        source="openf1",
        dataset="f1_session",
        description="Hydrate one F1 session with core and optional extended datasets.",
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_session",
        cursor_key=str(session_key),
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "session_key": session_key,
            "include_extended": include_extended,
            "include_heavy": include_heavy,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned", "session_key": session_key}

    session_row = ctx.db.scalar(select(F1Session).where(F1Session.session_key == session_key))
    if session_row is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"session_key={session_key} not found; run sync-f1-calendar first",
        )
        raise ValueError(f"session_key={session_key} not found")

    meeting_row = None
    if session_row.meeting_id is not None:
        meeting_row = ctx.db.get(F1Meeting, session_row.meeting_id)
    meeting_key = None
    if session_row.raw_payload is not None:
        raw_meeting_key = session_row.raw_payload.get("meeting_key")
        meeting_key = None if raw_meeting_key is None else int(raw_meeting_key)
    season = None if meeting_row is None else meeting_row.season

    connector = OpenF1Connector()
    driver_rows: dict[str, dict[str, Any]] = {}
    team_rows: dict[str, dict[str, Any]] = {}
    result_rows: list[dict[str, Any]] = []
    lap_rows: list[dict[str, Any]] = []
    stint_rows: list[dict[str, Any]] = []
    weather_rows: list[dict[str, Any]] = []
    race_control_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    interval_rows: list[dict[str, Any]] = []
    pit_rows: list[dict[str, Any]] = []
    radio_rows: list[dict[str, Any]] = []
    grid_rows: list[dict[str, Any]] = []
    telemetry_rows: list[dict[str, Any]] = []

    drivers = connector.fetch_drivers(session_key)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="drivers",
            endpoint="/v1/drivers",
            params={"session_key": session_key},
            payload=drivers,
            response_status=200,
            checkpoint=str(session_key),
        ),
        partition={"session_key": str(session_key)},
    )
    for driver in drivers:
        driver_id = f"driver:{driver['driver_number']}"
        team_id = f"team:{slugify(str(driver.get('team_name', 'unknown')))}"
        driver_rows[driver_id] = {
            "id": driver_id,
            "source": "openf1",
            "driver_number": int(driver["driver_number"]),
            "broadcast_name": driver.get("broadcast_name"),
            "full_name": driver.get("full_name"),
            "first_name": driver.get("first_name"),
            "last_name": driver.get("last_name"),
            "name_acronym": driver.get("name_acronym"),
            "team_id": team_id,
            "country_code": driver.get("country_code"),
            "headshot_url": driver.get("headshot_url"),
            "raw_payload": driver,
        }
        team_rows[team_id] = {
            "id": team_id,
            "source": "openf1",
            "team_name": driver.get("team_name") or "Unknown",
            "team_color": driver.get("team_colour"),
            "raw_payload": driver,
        }

    session_code = session_row.session_code or session_code_from_name(session_row.session_name)
    results = connector.fetch_session_results(session_key)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="session_result",
            endpoint="/v1/session_result",
            params={"session_key": session_key},
            payload=results,
            response_status=200,
            checkpoint=str(session_key),
        ),
        partition={"session_key": str(session_key)},
    )
    for result in results:
        parsed_result_time = parse_result_time_value(
            result.get("duration"),
            session_code=session_code,
            session_type=session_row.session_type,
        )
        parsed_gap = parse_gap_value(
            result.get("gap_to_leader"),
            position=result.get("position"),
            allow_segments=True,
        )
        result_rows.append(
            {
                "id": f"{session_key}:{result.get('driver_number')}",
                "session_id": session_row.id,
                "driver_id": f"driver:{result.get('driver_number')}",
                "position": result.get("position"),
                "result_time_seconds": parsed_result_time.seconds,
                "result_time_kind": parsed_result_time.kind,
                "result_time_display": parsed_result_time.display,
                "result_time_segments_json": parsed_result_time.segments_json,
                "gap_to_leader_display": parsed_gap.display,
                "gap_to_leader_seconds": parsed_gap.seconds,
                "gap_to_leader_laps_behind": parsed_gap.laps_behind,
                "gap_to_leader_status": parsed_gap.status,
                "gap_to_leader_segments_json": parsed_gap.segments_json,
                "dnf": result.get("dnf"),
                "dns": result.get("dns"),
                "dsq": result.get("dsq"),
                "number_of_laps": result.get("number_of_laps"),
                "raw_payload": result,
            }
        )

    laps = connector.fetch_laps(session_key)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="laps",
            endpoint="/v1/laps",
            params={"session_key": session_key},
            payload=laps,
            response_status=200,
            checkpoint=str(session_key),
        ),
        partition={"session_key": str(session_key)},
    )
    for lap in laps:
        lap_rows.append(
            {
                "id": f"{session_key}:{lap.get('driver_number')}:{lap.get('lap_number')}",
                "session_id": session_row.id,
                "driver_id": f"driver:{lap.get('driver_number')}",
                "lap_number": int(lap.get("lap_number", 0)),
                "lap_start_utc": parse_dt(lap.get("date_start")),
                "lap_end_utc": parse_dt(lap.get("date_end")),
                "lap_duration_seconds": lap.get("lap_duration"),
                "is_pit_out_lap": lap.get("is_pit_out_lap"),
                "stint_number": lap.get("stint_number"),
                "sector_1_seconds": lap.get("duration_sector_1"),
                "sector_2_seconds": lap.get("duration_sector_2"),
                "sector_3_seconds": lap.get("duration_sector_3"),
                "speed_trap_kph": lap.get("i1_speed"),
                "raw_payload": lap,
            }
        )

    stints = connector.fetch_stints(session_key)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="stints",
            endpoint="/v1/stints",
            params={"session_key": session_key},
            payload=stints,
            response_status=200,
            checkpoint=str(session_key),
        ),
        partition={"session_key": str(session_key)},
    )
    for stint in stints:
        stint_rows.append(
            {
                "id": f"{session_key}:{stint.get('driver_number')}:{stint.get('stint_number')}",
                "session_id": session_row.id,
                "driver_id": f"driver:{stint.get('driver_number')}",
                "stint_number": int(stint.get("stint_number", 0)),
                "compound": stint.get("compound"),
                "lap_start": stint.get("lap_start"),
                "lap_end": stint.get("lap_end"),
                "tyre_age_at_start": stint.get("tyre_age_at_start"),
                "raw_payload": stint,
            }
        )

    race_control = connector.fetch_race_control(session_key)
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="openf1",
            dataset="race_control",
            endpoint="/v1/race_control",
            params={"session_key": session_key},
            payload=race_control,
            response_status=200,
            checkpoint=str(session_key),
        ),
        partition={"session_key": str(session_key)},
    )
    for message in race_control:
        race_control_rows.append(
            {
                "id": f"{session_key}:{payload_checksum(message)[:16]}",
                "meeting_id": session_row.meeting_id,
                "session_id": session_row.id,
                "driver_number": message.get("driver_number"),
                "category": message.get("category"),
                "message": message.get("message"),
                "flag": message.get("flag"),
                "scope": message.get("scope"),
                "observed_at_utc": parse_dt(message.get("date")),
                "raw_payload": message,
            }
        )

    if meeting_key is not None:
        existing_weather = ctx.db.scalar(
            select(func.count())
            .select_from(F1Weather)
            .where(F1Weather.meeting_id == session_row.meeting_id)
        )
        if not existing_weather:
            weather = connector.fetch_weather(meeting_key)
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="openf1",
                    dataset="weather",
                    endpoint="/v1/weather",
                    params={"meeting_key": meeting_key},
                    payload=weather,
                    response_status=200,
                    checkpoint=str(meeting_key),
                ),
                partition={"meeting_key": str(meeting_key)},
            )
            for item in weather:
                weather_rows.append(
                    {
                        "id": f"{meeting_key}:{item.get('date')}",
                        "meeting_id": session_row.meeting_id,
                        "session_id": None,
                        "observed_at_utc": parse_dt(item.get("date")),
                        "air_temperature_c": item.get("air_temperature"),
                        "humidity_pct": item.get("humidity"),
                        "pressure_hpa": item.get("pressure"),
                        "rainfall": item.get("rainfall"),
                        "track_temperature_c": item.get("track_temperature"),
                        "wind_direction_deg": item.get("wind_direction"),
                        "wind_speed_mps": item.get("wind_speed"),
                        "raw_payload": item,
                    }
                )

    if include_extended:
        positions = connector.fetch_positions(session_key)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="openf1",
                dataset="position",
                endpoint="/v1/position",
                params={"session_key": session_key},
                payload=positions,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key)},
        )
        for item in positions:
            position_rows.append(
                {
                    "id": f"{session_key}:{item.get('driver_number')}:{item.get('date')}",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{item.get('driver_number')}",
                    "observed_at_utc": parse_dt(item.get("date")),
                    "position": item.get("position"),
                    "raw_payload": item,
                }
            )

        intervals = connector.fetch_intervals(session_key)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="openf1",
                dataset="intervals",
                endpoint="/v1/intervals",
                params={"session_key": session_key},
                payload=intervals,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key)},
        )
        for item in intervals:
            parsed_gap = parse_gap_value(item.get("gap_to_leader"), null_means_leader=True)
            parsed_interval = parse_gap_value(item.get("interval"), null_means_leader=True)
            interval_rows.append(
                {
                    "id": f"{session_key}:{item.get('driver_number')}:{item.get('date')}",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{item.get('driver_number')}",
                    "observed_at_utc": parse_dt(item.get("date")),
                    "gap_to_leader_display": parsed_gap.display,
                    "gap_to_leader_seconds": parsed_gap.seconds,
                    "gap_to_leader_laps_behind": parsed_gap.laps_behind,
                    "gap_to_leader_status": parsed_gap.status,
                    "interval_display": parsed_interval.display,
                    "interval_seconds": parsed_interval.seconds,
                    "interval_laps_behind": parsed_interval.laps_behind,
                    "interval_status": parsed_interval.status,
                    "raw_payload": item,
                }
            )

        pit = connector.fetch_pit(session_key)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="openf1",
                dataset="pit",
                endpoint="/v1/pit",
                params={"session_key": session_key},
                payload=pit,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key)},
        )
        for item in pit:
            pit_rows.append(
                {
                    "id": f"{session_key}:{item.get('driver_number')}:{item.get('date')}",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{item.get('driver_number')}",
                    "observed_at_utc": parse_dt(item.get("date")),
                    "lap_number": item.get("lap_number"),
                    "pit_duration_seconds": normalize_float(item.get("pit_duration")),
                    "raw_payload": item,
                }
            )

        team_radio = connector.fetch_team_radio(session_key)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="openf1",
                dataset="team_radio",
                endpoint="/v1/team_radio",
                params={"session_key": session_key},
                payload=team_radio,
                response_status=200,
                checkpoint=str(session_key),
            ),
            partition={"session_key": str(session_key)},
        )
        for item in team_radio:
            radio_rows.append(
                {
                    "id": f"{session_key}:{payload_checksum(item)[:16]}",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{item.get('driver_number')}"
                    if item.get("driver_number") is not None
                    else None,
                    "recording_url": item.get("recording_url"),
                    "transcript_text": item.get("transcript"),
                    "observed_at_utc": parse_dt(item.get("date")),
                    "raw_payload": item,
                }
            )

        grid = connector.fetch_starting_grid(session_key)
        if grid:
            persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="openf1",
                    dataset="starting_grid",
                    endpoint="/v1/starting_grid",
                    params={"session_key": session_key},
                    payload=grid,
                    response_status=200,
                    checkpoint=str(session_key),
                ),
                partition={"session_key": str(session_key)},
            )
            for item in grid:
                grid_rows.append(
                    {
                        "id": f"{session_key}:{item.get('driver_number')}",
                        "session_id": session_row.id,
                        "driver_id": f"driver:{item.get('driver_number')}",
                        "grid_position": item.get("position"),
                        "raw_payload": item,
                    }
                )

    if include_heavy:
        for driver in drivers:
            driver_number = int(driver["driver_number"])
            car_data = connector.fetch_car_data(session_key, driver_number=driver_number)
            car_bronze = persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="openf1",
                    dataset="car_data",
                    endpoint="/v1/car_data",
                    params={"session_key": session_key, "driver_number": driver_number},
                    payload=car_data,
                    response_status=200,
                    checkpoint=f"{session_key}:{driver_number}",
                ),
                partition={"session_key": str(session_key), "driver_number": str(driver_number)},
            )
            telemetry_rows.append(
                {
                    "id": f"{session_key}:{driver_number}:car_data",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{driver_number}",
                    "dataset_name": "car_data",
                    "storage_path": str(car_bronze.path),
                    "sample_count": len(car_data),
                    "started_at_utc": parse_dt(car_data[0].get("date")) if car_data else None,
                    "ended_at_utc": parse_dt(car_data[-1].get("date")) if car_data else None,
                    "raw_payload": {"driver_number": driver_number},
                }
            )

            location = connector.fetch_location(session_key, driver_number=driver_number)
            location_bronze = persist_fetch(
                ctx,
                job_run_id=run.id,
                batch=FetchBatch(
                    source="openf1",
                    dataset="location",
                    endpoint="/v1/location",
                    params={"session_key": session_key, "driver_number": driver_number},
                    payload=location,
                    response_status=200,
                    checkpoint=f"{session_key}:{driver_number}",
                ),
                partition={"session_key": str(session_key), "driver_number": str(driver_number)},
            )
            telemetry_rows.append(
                {
                    "id": f"{session_key}:{driver_number}:location",
                    "session_id": session_row.id,
                    "driver_id": f"driver:{driver_number}",
                    "dataset_name": "location",
                    "storage_path": str(location_bronze.path),
                    "sample_count": len(location),
                    "started_at_utc": parse_dt(location[0].get("date")) if location else None,
                    "ended_at_utc": parse_dt(location[-1].get("date")) if location else None,
                    "raw_payload": {"driver_number": driver_number},
                }
            )

    upsert_records(ctx.db, F1Driver, driver_rows.values())
    upsert_records(ctx.db, F1Team, team_rows.values())
    upsert_records(ctx.db, F1SessionResult, result_rows)
    upsert_records(ctx.db, F1Lap, lap_rows)
    upsert_records(ctx.db, F1Stint, stint_rows)
    upsert_records(ctx.db, F1RaceControl, race_control_rows)
    upsert_records(ctx.db, F1Weather, weather_rows)
    if position_rows:
        upsert_records(ctx.db, F1Position, position_rows)
    if interval_rows:
        upsert_records(ctx.db, F1Interval, interval_rows)
    if pit_rows:
        upsert_records(ctx.db, F1Pit, pit_rows)
    if radio_rows:
        upsert_records(ctx.db, F1TeamRadioMetadata, radio_rows)
    if grid_rows:
        upsert_records(ctx.db, F1StartingGrid, grid_rows)
    if telemetry_rows:
        upsert_records(ctx.db, F1TelemetryIndex, telemetry_rows)

    partition = {"season": str(season or "unknown"), "session_key": str(session_key)}
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="f1_session_results",
        records=result_rows,
        partition=partition,
    )
    persist_silver(ctx, job_run_id=run.id, dataset="f1_laps", records=lap_rows, partition=partition)
    persist_silver(
        ctx, job_run_id=run.id, dataset="f1_stints", records=stint_rows, partition=partition
    )
    persist_silver(
        ctx,
        job_run_id=run.id,
        dataset="f1_race_control",
        records=race_control_rows,
        partition=partition,
    )
    if weather_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_weather",
            records=weather_rows,
            partition={"season": str(season or "unknown")},
        )
    if position_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_positions",
            records=position_rows,
            partition=partition,
        )
    if interval_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_intervals",
            records=interval_rows,
            partition=partition,
        )
    if pit_rows:
        persist_silver(
            ctx, job_run_id=run.id, dataset="f1_pit", records=pit_rows, partition=partition
        )
    if radio_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_team_radio_metadata",
            records=radio_rows,
            partition=partition,
        )
    if telemetry_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_telemetry_index",
            records=telemetry_rows,
            partition=partition,
        )

    upsert_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_session",
        cursor_key=str(session_key),
        cursor_value={
            "session_key": session_key,
            "include_extended": include_extended,
            "include_heavy": include_heavy,
            "synced_at": utc_now().isoformat(),
        },
    )
    records_written = (
        len(driver_rows)
        + len(team_rows)
        + len(result_rows)
        + len(lap_rows)
        + len(stint_rows)
        + len(race_control_rows)
        + len(weather_rows)
        + len(position_rows)
        + len(interval_rows)
        + len(pit_rows)
        + len(radio_rows)
        + len(grid_rows)
        + len(telemetry_rows)
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={
            "session_key": session_key,
            "synced_at": utc_now().isoformat(),
        },
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "session_key": session_key,
        "records_written": records_written,
    }


def ensure_taxonomy_version(ctx: PipelineContext) -> MarketTaxonomyVersion:
    existing = ctx.db.scalar(
        select(MarketTaxonomyVersion).where(MarketTaxonomyVersion.version_name == "heuristic-v1")
    )
    if existing is not None:
        return existing
    version = MarketTaxonomyVersion(
        version_name="heuristic-v1",
        parser_name="parse_market_taxonomy",
        rule_hash=payload_checksum("heuristic-v1"),
        notes="Initial slug/question/rules heuristic parser.",
    )
    ctx.db.add(version)
    ctx.db.flush()
    return version


def sync_polymarket_catalog(
    ctx: PipelineContext,
    *,
    max_pages: int = 1,
    batch_size: int = 100,
    active: bool | None = None,
    closed: bool | None = None,
    archived: bool | None = None,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-polymarket-catalog",
        source="polymarket",
        dataset="catalog",
        description="Sync Polymarket events, markets, tokens, and rule metadata.",
        default_cursor={"offset": 0},
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="catalog",
        cursor_key="markets",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "max_pages": max_pages,
            "batch_size": batch_size,
            "active": active,
            "closed": closed,
            "archived": archived,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned"}

    connector = PolymarketConnector()
    taxonomy_version = ensure_taxonomy_version(ctx)
    event_rows: dict[str, dict[str, Any]] = {}
    market_rows: list[dict[str, Any]] = []
    token_rows: list[dict[str, Any]] = []
    rule_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    last_offset = 0

    for offset, batch in connector.iterate_markets(
        batch_size=batch_size,
        max_pages=max_pages,
        active=active,
        closed=closed,
        archived=archived,
    ):
        last_offset = offset
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="polymarket",
                dataset="markets",
                endpoint="/markets",
                params={
                    "limit": batch_size,
                    "offset": offset,
                    "active": active,
                    "closed": closed,
                    "archived": archived,
                },
                payload=batch,
                response_status=200,
                checkpoint=str(offset),
            ),
            partition={"offset": str(offset)},
        )
        for event in extract_event_rows(batch):
            event_rows[event["id"]] = event
        for market in batch:
            event_context = market.get("events", [{}])[0] if market.get("events") else {}
            parsed = parse_market_taxonomy(
                market.get("question") or "",
                " ".join(
                    str(value or "")
                    for value in [
                        market.get("description"),
                        event_context.get("description"),
                    ]
                    if value
                )
                or None,
                title=event_context.get("title"),
            )
            token_ids = json.loads(market.get("clobTokenIds") or "[]")
            outcomes = json.loads(market.get("outcomes") or "[]")
            prices = json.loads(market.get("outcomePrices") or "[]")
            event_id = str(market["events"][0]["id"]) if market.get("events") else None
            best_bid = float(market["bestBid"]) if market.get("bestBid") is not None else None
            best_ask = float(market["bestAsk"]) if market.get("bestAsk") is not None else None
            market_rows.append(
                {
                    "id": str(market["id"]),
                    "event_id": event_id,
                    "question": market.get("question") or "",
                    "slug": market.get("slug"),
                    "condition_id": market["conditionId"],
                    "question_id": market.get("questionID"),
                    "market_type": market.get("marketType"),
                    "sports_market_type": market.get("sportsMarketType"),
                    "taxonomy": parsed.taxonomy,
                    "taxonomy_confidence": parsed.confidence,
                    "target_session_code": parsed.target_session_code,
                    "driver_a": parsed.driver_a,
                    "driver_b": parsed.driver_b,
                    "team_name": parsed.team_name,
                    "resolution_source": market.get("resolutionSource"),
                    "rules_text": market.get("description"),
                    "description": market.get("description"),
                    "start_at_utc": parse_dt(market.get("startDate")),
                    "end_at_utc": parse_dt(market.get("endDate")),
                    "accepting_orders": bool(market.get("acceptingOrders")),
                    "active": bool(market.get("active")),
                    "closed": bool(market.get("closed")),
                    "archived": bool(market.get("archived")),
                    "enable_order_book": bool(market.get("enableOrderBook")),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": (best_ask - best_bid)
                    if best_ask is not None and best_bid is not None
                    else None,
                    "last_trade_price": (
                        float(market["lastTradePrice"])
                        if market.get("lastTradePrice") is not None
                        else None
                    ),
                    "volume": market.get("volumeNum"),
                    "liquidity": market.get("liquidityNum"),
                    "clob_token_ids": token_ids,
                    "raw_payload": market,
                }
            )
            rule_rows.append(
                {
                    "id": f"rule:{market['id']}",
                    "market_id": str(market["id"]),
                    "rules_text": market.get("description"),
                    "resolution_text": market.get("resolutionSource"),
                    "parsed_metadata": {
                        "taxonomy": parsed.taxonomy,
                        "target_session_code": parsed.target_session_code,
                        "driver_a": parsed.driver_a,
                        "driver_b": parsed.driver_b,
                        "team_name": parsed.team_name,
                    },
                    "raw_payload": market,
                }
            )
            status_rows.append(
                {
                    "id": f"{market['id']}:{offset}",
                    "market_id": str(market["id"]),
                    "observed_at_utc": utc_now(),
                    "active": bool(market.get("active")),
                    "closed": bool(market.get("closed")),
                    "archived": bool(market.get("archived")),
                    "accepting_orders": bool(market.get("acceptingOrders")),
                    "raw_payload": market,
                }
            )
            label_rows.append(
                {
                    "id": stable_uuid(
                        "market-taxonomy-label",
                        str(market["id"]),
                        taxonomy_version.id,
                    ),
                    "market_id": str(market["id"]),
                    "taxonomy_version_id": taxonomy_version.id,
                    "taxonomy": parsed.taxonomy,
                    "confidence": parsed.confidence,
                    "label_status": "candidate",
                    "target_session_code": parsed.target_session_code,
                    "parsed_metadata": parsed.metadata
                    | {
                        "driver_a": parsed.driver_a or "",
                        "driver_b": parsed.driver_b or "",
                        "team_name": parsed.team_name or "",
                    },
                    "created_at": utc_now(),
                }
            )
            for index, token_id in enumerate(token_ids):
                token_rows.append(
                    {
                        "id": token_id,
                        "market_id": str(market["id"]),
                        "outcome": outcomes[index] if index < len(outcomes) else None,
                        "outcome_index": index,
                        "latest_price": float(prices[index]) if index < len(prices) else None,
                        "raw_payload": {"token_id": token_id},
                    }
                )

    upsert_records(ctx.db, PolymarketEvent, event_rows.values())
    upsert_records(ctx.db, PolymarketMarket, market_rows)
    upsert_records(ctx.db, PolymarketToken, token_rows)
    upsert_records(ctx.db, PolymarketMarketRule, rule_rows)
    upsert_records(ctx.db, PolymarketMarketStatusHistory, status_rows)
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows)
    persist_silver(
        ctx, job_run_id=run.id, dataset="polymarket_events", records=list(event_rows.values())
    )
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_markets", records=market_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_tokens", records=token_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_market_rules", records=rule_rows)

    upsert_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="catalog",
        cursor_key="markets",
        cursor_value={"last_offset": last_offset, "synced_at": utc_now().isoformat()},
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={"last_offset": last_offset, "synced_at": utc_now().isoformat()},
        records_written=len(market_rows) + len(token_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "markets": len(market_rows),
        "events": len(event_rows),
        "tokens": len(token_rows),
    }


def hydrate_polymarket_market(
    ctx: PipelineContext, *, market_id: str, fidelity: int = 60
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="hydrate-polymarket-market",
        source="polymarket",
        dataset="market_history",
        description="Hydrate one Polymarket market with executable microstructure history.",
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="market_history",
        cursor_key=market_id,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"market_id": market_id, "fidelity": fidelity},
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned", "market_id": market_id}

    connector = PolymarketConnector()
    market = ctx.db.get(PolymarketMarket, market_id)
    if market is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"market_id={market_id} not found; run sync-polymarket-catalog first",
        )
        raise ValueError(f"market_id={market_id} not found")

    tokens = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id == market_id)
    ).all()
    history_rows: list[dict[str, Any]] = []
    orderbook_rows: list[dict[str, Any]] = []
    orderbook_level_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    open_interest_rows: list[dict[str, Any]] = []
    resolution_rows: list[dict[str, Any]] = []

    open_interest = connector.get_open_interest(market.condition_id)
    if open_interest is not None:
        open_interest_rows.append(
            {
                "id": f"{market_id}:oi:{utc_now().strftime('%Y%m%dT%H%M%S')}",
                "market_id": market_id,
                "token_id": None,
                "observed_at_utc": utc_now(),
                "open_interest": open_interest,
                "raw_payload": {
                    "condition_id": market.condition_id,
                    "open_interest": open_interest,
                },
            }
        )

    for token in tokens:
        book = connector.get_order_book(token.id)
        midpoint = connector.get_midpoint(token.id)
        spread = connector.get_spread(token.id)
        last_trade_price = connector.get_last_trade_price(token.id)
        if book is not None:
            observed_at = datetime.fromtimestamp(
                int(book["timestamp"]) / 1000,
                tz=timezone.utc,
            )
            best_bid, best_ask = best_levels(book)
            snapshot_id = f"{market_id}:{token.id}:{book.get('timestamp')}"
            orderbook_rows.append(
                {
                    "id": snapshot_id,
                    "market_id": market_id,
                    "token_id": token.id,
                    "observed_at_utc": observed_at,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": spread,
                    "bid_depth_json": book.get("bids"),
                    "ask_depth_json": book.get("asks"),
                    "imbalance": compute_imbalance(book),
                    "raw_payload": book,
                }
            )
            for side, levels in (("bid", book.get("bids", [])), ("ask", book.get("asks", []))):
                for index, level in enumerate(levels):
                    orderbook_level_rows.append(
                        {
                            "id": f"{snapshot_id}:{side}:{index}",
                            "snapshot_id": snapshot_id,
                            "market_id": market_id,
                            "token_id": token.id,
                            "side": side,
                            "level_index": index,
                            "price": float(level["price"]),
                            "size": float(level["size"]),
                        }
                    )
        for point in connector.get_price_history(token.id, fidelity=fidelity):
            history_rows.append(
                {
                    "id": f"{market_id}:{token.id}:{point['t']}",
                    "market_id": market_id,
                    "token_id": token.id,
                    "observed_at_utc": datetime.fromtimestamp(
                        int(point["t"]),
                        tz=timezone.utc,
                    ),
                    "price": point.get("p"),
                    "midpoint": midpoint,
                    "best_bid": None,
                    "best_ask": None,
                    "source_kind": "clob",
                    "raw_payload": point | {"last_trade_price": last_trade_price},
                }
            )

    for trade in connector.get_trades(market.condition_id, limit=500):
        trade_rows.append(
            {
                "id": f"{market_id}:{trade.get('transactionHash') or payload_checksum(trade)[:16]}",
                "market_id": market_id,
                "token_id": trade.get("asset"),
                "condition_id": market.condition_id,
                "trade_timestamp_utc": datetime.fromtimestamp(
                    int(trade["timestamp"]) / 1000,
                    tz=timezone.utc,
                ),
                "side": trade.get("side"),
                "price": trade.get("price"),
                "size": trade.get("size"),
                "outcome": trade.get("outcome"),
                "transaction_hash": trade.get("transactionHash"),
                "raw_payload": trade,
            }
        )

    if market.closed and market.raw_payload is not None:
        resolution_rows.append(
            {
                "id": f"resolution:{market_id}",
                "market_id": market_id,
                "resolved_at_utc": parse_dt(
                    market.raw_payload.get("resolveDate") or market.raw_payload.get("endDate")
                ),
                "result": market.raw_payload.get("result"),
                "outcome": market.raw_payload.get("outcome"),
                "raw_payload": market.raw_payload,
            }
        )

    if orderbook_rows:
        upsert_records(ctx.db, PolymarketOrderbookSnapshot, orderbook_rows)
        upsert_records(ctx.db, PolymarketOrderbookLevel, orderbook_level_rows)
    if history_rows:
        upsert_records(ctx.db, PolymarketPriceHistory, history_rows)
    if trade_rows:
        upsert_records(ctx.db, PolymarketTrade, trade_rows)
    if open_interest_rows:
        upsert_records(ctx.db, PolymarketOpenInterestHistory, open_interest_rows)
    if resolution_rows:
        upsert_records(ctx.db, PolymarketResolution, resolution_rows)

    partition = {"market_id": market_id}
    if orderbook_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_orderbook_snapshots",
            records=orderbook_rows,
            partition=partition,
        )
    if history_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_price_history",
            records=history_rows,
            partition=partition,
        )
    if trade_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_trades",
            records=trade_rows,
            partition=partition,
        )
    if open_interest_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_open_interest_history",
            records=open_interest_rows,
            partition=partition,
        )

    upsert_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="market_history",
        cursor_key=market_id,
        cursor_value={"market_id": market_id, "synced_at": utc_now().isoformat()},
    )
    records_written = (
        len(orderbook_rows)
        + len(orderbook_level_rows)
        + len(history_rows)
        + len(trade_rows)
        + len(open_interest_rows)
        + len(resolution_rows)
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={"market_id": market_id, "synced_at": utc_now().isoformat()},
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "market_id": market_id,
        "records_written": records_written,
    }


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
        select(F1Session).where(F1Session.session_code.in_(["FP1", "FP2", "FP3", "Q", "S", "R"]))
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
    latest_polymarket_fetch = ctx.db.scalar(
        select(SourceFetchLog)
        .where(SourceFetchLog.source.in_(["polymarket", "polymarket_ws"]))
        .order_by(SourceFetchLog.finished_at.desc())
        .limit(1)
    )
    freshness_hours = None
    if latest_polymarket_fetch is not None and latest_polymarket_fetch.finished_at is not None:
        freshness_hours = round(
            (utc_now() - latest_polymarket_fetch.finished_at).total_seconds() / 3600,
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
    result_rows.append(
        {
            "id": f"dq-result:{run.id}:polymarket_markets",
            "check_id": "dq:polymarket_markets_nonempty",
            "job_run_id": run.id,
            "dataset": "polymarket_markets",
            "status": "pass" if market_count > 0 else "fail",
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
            "status": "pass" if telemetry_count > 0 else "fail",
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
            "status": "pass" if ws_manifest_count > 0 else "fail",
            "metrics_json": {"row_count": ws_manifest_count},
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
            "status": "pass"
            if freshness_hours is not None and freshness_hours <= 24
            else "fail",
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
