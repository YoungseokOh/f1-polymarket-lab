from __future__ import annotations

from typing import Any

from f1_polymarket_lab.common import (
    parse_gap_value,
    parse_result_time_value,
    payload_checksum,
    slugify,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    FastF1ScheduleConnector,
    OpenF1Connector,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.features import default_feature_registry
from f1_polymarket_lab.storage.models import (
    CircuitMetadata,
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
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import delete, func, select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    start_job_run,
    upsert_cursor_state,
)

from .context import PipelineContext, normalize_float, parse_dt, persist_fetch, persist_silver

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
COUNTRY_FALLBACK_MEETING_NAMES = {
    "saudi arabia": "Saudi Arabian Grand Prix",
}


def session_code_from_name(name: str) -> str | None:
    return MODERN_WEEKEND_SESSION_CODE_BY_NAME.get(name)


def is_practice_session_name(name: str) -> bool:
    session_code = session_code_from_name(name)
    return session_code in PRACTICE_SESSION_CODES


def _normalize_event_format(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text.replace(" ", "_").replace("-", "_")


def _infer_event_format(records: list[dict[str, Any]]) -> str | None:
    session_codes = {
        code
        for code in (
            session_code_from_name(str(item.get("session_name") or "")) for item in records
        )
        if code is not None
    }
    if {"FP1", "SQ", "S", "Q", "R"}.issubset(session_codes):
        return "sprint"
    if {"FP1", "FP2", "FP3", "Q", "R"}.issubset(session_codes):
        return "conventional"
    return None


def _legacy_meeting_slug(first_session: dict[str, Any], meeting_key: int, season: int) -> str:
    country_name = str(first_session.get("country_name") or "").strip()
    if country_name:
        fallback_name = COUNTRY_FALLBACK_MEETING_NAMES.get(
            country_name.lower(),
            f"{country_name} Grand Prix",
        )
        return str(slugify(fallback_name))
    location = str(first_session.get("location") or "").strip()
    if location:
        return str(slugify(f"{location} Grand Prix"))
    return str(slugify(f"{season}-meeting-{meeting_key}"))


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
    schedule_by_event_name = {
        str(record.get("EventName", "")).lower(): record for record in schedule
    }
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
        if not schedule_row:
            schedule_row = schedule_by_event_name.get(
                str(first_session.get("meeting_name", "")).lower(),
                {},
            )
        meeting_name = str(
            schedule_row.get("EventName")
            or first_session.get("meeting_name")
            or first_session.get("country_name")
            or meeting_key
        )
        meeting_slug = (
            slugify(str(schedule_row.get("EventName")))
            if schedule_row.get("EventName")
            else _legacy_meeting_slug(first_session, meeting_key, season)
        )
        meeting_id = f"meeting:{meeting_key}"
        meeting_rows.append(
            {
                "id": meeting_id,
                "source": "openf1",
                "meeting_key": meeting_key,
                "season": season,
                "round_number": schedule_row.get("RoundNumber"),
                "meeting_name": meeting_name,
                "meeting_slug": meeting_slug,
                "meeting_official_name": schedule_row.get("OfficialEventName"),
                "event_format": _normalize_event_format(schedule_row.get("EventFormat"))
                or _infer_event_format(records),
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
    upsert_records(ctx.db, F1Team, team_rows.values(), conflict_columns=["team_name"])
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
