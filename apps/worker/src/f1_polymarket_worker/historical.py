from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from f1_polymarket_lab.common import (
    ParsedResultTimeValue,
    normalize_text,
    parse_gap_value,
    payload_checksum,
    slugify,
    utc_now,
)
from f1_polymarket_lab.connectors import F1DBConnector, JolpicaConnector
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.models import (
    F1Driver,
    F1Lap,
    F1Meeting,
    F1Pit,
    F1Session,
    F1SessionResult,
    F1StartingGrid,
    F1Team,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import inspect, select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    start_job_run,
    upsert_cursor_state,
)
from f1_polymarket_worker.pipeline import (
    PipelineContext,
    hydrate_f1_session,
    parse_dt,
    persist_fetch,
    persist_silver,
    sync_f1_calendar,
)


@dataclass(frozen=True, slots=True)
class HistoricalSessionDefinition:
    token: str
    session_name: str
    session_type: str
    session_code: str | None
    session_order: int
    is_practice: bool
    f1db_result_type: str | None = None
    f1db_date_field: str | None = None
    f1db_time_field: str | None = None
    jolpica_schedule_field: str | None = None


HISTORICAL_SESSION_DEFINITIONS = {
    "PRE_QUALIFYING": HistoricalSessionDefinition(
        token="PRE_QUALIFYING",
        session_name="Pre-Qualifying",
        session_type="pre_qualifying",
        session_code=None,
        session_order=5,
        is_practice=True,
        f1db_result_type="PRE_QUALIFYING_RESULT",
        f1db_date_field="pre_qualifying_date",
        f1db_time_field="pre_qualifying_time",
    ),
    "FP1": HistoricalSessionDefinition(
        token="FP1",
        session_name="Practice 1",
        session_type="practice",
        session_code="FP1",
        session_order=11,
        is_practice=True,
        f1db_result_type="FREE_PRACTICE_1_RESULT",
        f1db_date_field="free_practice_1_date",
        f1db_time_field="free_practice_1_time",
        jolpica_schedule_field="FirstPractice",
    ),
    "FP2": HistoricalSessionDefinition(
        token="FP2",
        session_name="Practice 2",
        session_type="practice",
        session_code="FP2",
        session_order=12,
        is_practice=True,
        f1db_result_type="FREE_PRACTICE_2_RESULT",
        f1db_date_field="free_practice_2_date",
        f1db_time_field="free_practice_2_time",
        jolpica_schedule_field="SecondPractice",
    ),
    "FP3": HistoricalSessionDefinition(
        token="FP3",
        session_name="Practice 3",
        session_type="practice",
        session_code="FP3",
        session_order=13,
        is_practice=True,
        f1db_result_type="FREE_PRACTICE_3_RESULT",
        f1db_date_field="free_practice_3_date",
        f1db_time_field="free_practice_3_time",
        jolpica_schedule_field="ThirdPractice",
    ),
    "FP4": HistoricalSessionDefinition(
        token="FP4",
        session_name="Practice 4",
        session_type="practice_4",
        session_code=None,
        session_order=14,
        is_practice=True,
        f1db_result_type="FREE_PRACTICE_4_RESULT",
        f1db_date_field="free_practice_4_date",
        f1db_time_field="free_practice_4_time",
    ),
    "Q1": HistoricalSessionDefinition(
        token="Q1",
        session_name="Qualifying 1",
        session_type="qualifying_1",
        session_code=None,
        session_order=21,
        is_practice=False,
        f1db_result_type="QUALIFYING_1_RESULT",
        f1db_date_field="qualifying_1_date",
        f1db_time_field="qualifying_1_time",
    ),
    "Q2": HistoricalSessionDefinition(
        token="Q2",
        session_name="Qualifying 2",
        session_type="qualifying_2",
        session_code=None,
        session_order=22,
        is_practice=False,
        f1db_result_type="QUALIFYING_2_RESULT",
        f1db_date_field="qualifying_2_date",
        f1db_time_field="qualifying_2_time",
    ),
    "Q": HistoricalSessionDefinition(
        token="Q",
        session_name="Qualifying",
        session_type="qualifying",
        session_code="Q",
        session_order=23,
        is_practice=False,
        f1db_result_type="QUALIFYING_RESULT",
        f1db_date_field="qualifying_date",
        f1db_time_field="qualifying_time",
        jolpica_schedule_field="Qualifying",
    ),
    "SQ": HistoricalSessionDefinition(
        token="SQ",
        session_name="Sprint Qualifying",
        session_type="sprint_qualifying",
        session_code=None,
        session_order=24,
        is_practice=False,
        f1db_result_type="SPRINT_QUALIFYING_RESULT",
        f1db_date_field="sprint_qualifying_date",
        f1db_time_field="sprint_qualifying_time",
    ),
    "S": HistoricalSessionDefinition(
        token="S",
        session_name="Sprint",
        session_type="sprint",
        session_code="S",
        session_order=31,
        is_practice=False,
        f1db_result_type="SPRINT_RACE_RESULT",
        f1db_date_field="sprint_race_date",
        f1db_time_field="sprint_race_time",
        jolpica_schedule_field="Sprint",
    ),
    "WU": HistoricalSessionDefinition(
        token="WU",
        session_name="Warm-Up",
        session_type="warming_up",
        session_code=None,
        session_order=41,
        is_practice=True,
        f1db_result_type="WARMING_UP_RESULT",
        f1db_date_field="warming_up_date",
        f1db_time_field="warming_up_time",
    ),
    "R": HistoricalSessionDefinition(
        token="R",
        session_name="Race",
        session_type="race",
        session_code="R",
        session_order=51,
        is_practice=False,
        f1db_result_type="RACE_RESULT",
    ),
}
F1DB_RESULT_TYPE_TO_TOKEN = {
    definition.f1db_result_type: token
    for token, definition in HISTORICAL_SESSION_DEFINITIONS.items()
    if definition.f1db_result_type is not None
}
F1DB_GRID_TYPE_TO_TOKEN = {
    "STARTING_GRID_POSITION": "R",
    "SPRINT_STARTING_GRID_POSITION": "S",
}
JOLPICA_RESOURCE_TO_TOKEN = {
    "results": "R",
    "qualifying": "Q",
    "sprint": "S",
}
JOLPICA_DEFAULT_RESOURCES = ("races", "results", "qualifying", "sprint", "pitstops", "laps")


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _stable_negative_number(seed: str) -> int:
    checksum = payload_checksum(seed)
    return -(100000 + int(checksum[:8], 16) % 900000)


def historical_meeting_id(season: int, round_number: int) -> str:
    return f"meeting:historical:{season}:{round_number}"


def historical_meeting_key(season: int, round_number: int) -> int:
    return -((season * 100) + round_number)


def historical_session_id(season: int, round_number: int, token: str) -> str:
    return f"session:historical:{season}:{round_number}:{token.lower()}"


def historical_session_key(season: int, round_number: int, token: str) -> int:
    definition = HISTORICAL_SESSION_DEFINITIONS[token]
    return -((season * 100000) + (round_number * 100) + definition.session_order)


def canonical_driver_id(external_id: Any, full_name: Any = None) -> str:
    base = external_id or full_name or "unknown-driver"
    return f"driver:{slugify(str(base))}"


def canonical_team_id(external_id: Any, full_name: Any = None) -> str:
    base = external_id or full_name or "unknown-team"
    return f"team:{slugify(str(base))}"


def _combine_date_time(date_value: Any, time_value: Any) -> datetime | None:
    if not _has_value(date_value):
        return None
    date_text = str(date_value)
    if not _has_value(time_value):
        return parse_dt(f"{date_text}T00:00:00+00:00")
    time_text = str(time_value)
    if "Z" not in time_text and "+" not in time_text:
        time_text = f"{time_text}+00:00"
    return parse_dt(f"{date_text}T{time_text}")


def _parse_numeric_int(value: Any) -> int | None:
    if not _has_value(value):
        return None
    return int(str(value))


def _parse_millis_to_seconds(value: Any) -> float | None:
    if not _has_value(value):
        return None
    return float(value) / 1000.0


def _parse_clock_to_seconds(value: Any) -> float | None:
    if not _has_value(value):
        return None
    text = str(value).strip()
    parts = text.split(":")
    try:
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
        if len(parts) == 2:
            minutes, seconds = parts
            return int(minutes) * 60 + float(seconds)
        if len(parts) == 1:
            return float(parts[0])
    except ValueError:
        return None
    return None


def _result_time_value(
    *,
    kind: str,
    seconds: float | None = None,
    display: str | None = None,
    segments_json: list[Any] | None = None,
) -> ParsedResultTimeValue:
    return ParsedResultTimeValue(
        display=display,
        seconds=seconds,
        kind=kind,
        segments_json=segments_json,
    )


def _merge_with_existing(
    ctx: PipelineContext,
    model: type[Any],
    record: dict[str, Any],
) -> dict[str, Any]:
    existing = ctx.db.get(model, record["id"])
    if existing is None:
        # For F1Team, fall back to name-based lookup to avoid UNIQUE constraint
        # violations when different sources use different IDs for the same team.
        if model is F1Team and record.get("team_name"):
            existing = ctx.db.scalar(
                select(F1Team).where(F1Team.team_name == record["team_name"])
            )
            if existing is not None:
                record = {**record, "id": existing.id}
        # For F1Driver, fall back to broadcast_name-based lookup similarly.
        elif model is F1Driver and record.get("broadcast_name"):
            existing = ctx.db.scalar(
                select(F1Driver).where(F1Driver.broadcast_name == record["broadcast_name"])
            )
            if existing is not None:
                record = {**record, "id": existing.id}
        if existing is None:
            return record

    merged = {column.name: getattr(existing, column.name) for column in inspect(model).columns}
    for key, value in record.items():
        if key == "id":
            merged[key] = value
            continue
        if not _has_value(merged.get(key)):
            merged[key] = value
    return merged


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        deduped[str(record["id"])] = record
    return list(deduped.values())


def _session_result_time_from_f1db(
    row: dict[str, Any],
    token: str,
) -> ParsedResultTimeValue:
    if token in {"PRE_QUALIFYING", "FP1", "FP2", "FP3", "FP4", "WU"}:
        return _result_time_value(
            kind="best_lap",
            seconds=_parse_millis_to_seconds(row.get("practice_time_millis")),
            display=normalize_text(row.get("practice_time")),
        )
    if token in {"Q1", "Q2"}:
        return _result_time_value(
            kind="best_lap",
            seconds=_parse_millis_to_seconds(row.get("qualifying_time_millis")),
            display=normalize_text(row.get("qualifying_time")),
        )
    if token == "Q":
        segment_values = [
            row.get("qualifying_q1"),
            row.get("qualifying_q2"),
            row.get("qualifying_q3"),
        ]
        if any(_has_value(value) for value in segment_values):
            return _result_time_value(
                kind="segment_array",
                display=normalize_text(segment_values),
                segments_json=segment_values,
            )
        millis = row.get("qualifying_time_millis")
        if _has_value(millis):
            return _result_time_value(
                kind="best_lap",
                seconds=_parse_millis_to_seconds(millis),
                display=normalize_text(row.get("qualifying_time")),
            )
        segment_millis = [
            row.get("qualifying_q1_millis"),
            row.get("qualifying_q2_millis"),
            row.get("qualifying_q3_millis"),
        ]
        if any(_has_value(value) for value in segment_millis):
            return _result_time_value(
                kind="segment_array",
                segments_json=segment_millis,
            )
    if token in {"S", "R"}:
        return _result_time_value(
            kind="total_time",
            seconds=_parse_millis_to_seconds(row.get("race_time_millis")),
            display=normalize_text(row.get("race_time")),
        )
    return _result_time_value(kind="unknown")


def _session_result_gap_from_f1db(row: dict[str, Any], token: str) -> str | None:
    if token in {"PRE_QUALIFYING", "FP1", "FP2", "FP3", "FP4", "WU"}:
        return row.get("practice_gap") or row.get("practice_interval")
    if token in {"Q1", "Q2", "Q"}:
        return row.get("qualifying_gap") or row.get("qualifying_interval")
    if token in {"S", "R"}:
        return row.get("race_gap") or row.get("race_interval") or row.get("race_reason_retired")
    return None


def _session_result_laps_from_f1db(row: dict[str, Any], token: str) -> int | None:
    if token in {"PRE_QUALIFYING", "FP1", "FP2", "FP3", "FP4", "WU"}:
        return _parse_numeric_int(row.get("practice_laps"))
    if token in {"Q1", "Q2", "Q"}:
        return _parse_numeric_int(row.get("qualifying_laps"))
    if token in {"S", "R"}:
        return _parse_numeric_int(row.get("race_laps"))
    return None


def _build_f1db_driver_rows(drivers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for driver in drivers:
        canonical_id = canonical_driver_id(driver.get("id"), driver.get("full_name"))
        driver_number = _parse_numeric_int(driver.get("permanent_number"))
        rows.append(
            {
                "id": canonical_id,
                "source": "f1db",
                "driver_number": driver_number
                if driver_number is not None
                else _stable_negative_number(canonical_id),
                "broadcast_name": driver.get("abbreviation") or driver.get("full_name"),
                "full_name": driver.get("full_name") or driver.get("name"),
                "first_name": driver.get("first_name"),
                "last_name": driver.get("last_name"),
                "name_acronym": driver.get("abbreviation"),
                "team_id": None,
                "country_code": driver.get("nationality_alpha2_code"),
                "headshot_url": None,
                "raw_payload": driver,
            }
        )
    return rows


def _build_f1db_team_rows(constructors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for constructor in constructors:
        rows.append(
            {
                "id": canonical_team_id(constructor.get("id"), constructor.get("full_name")),
                "source": "f1db",
                "team_name": constructor.get("full_name") or constructor.get("name") or "Unknown",
                "team_color": None,
                "raw_payload": constructor,
            }
        )
    return rows


def _build_f1db_meeting_row(race: dict[str, Any]) -> dict[str, Any]:
    season = int(race["year"])
    round_number = int(race["round"])
    race_start = _combine_date_time(race.get("date"), race.get("time"))
    return {
        "id": historical_meeting_id(season, round_number),
        "source": "f1db",
        "meeting_key": historical_meeting_key(season, round_number),
        "season": season,
        "round_number": round_number,
        "meeting_name": race.get("grand_prix_full_name")
        or race.get("official_name")
        or race.get("grand_prix_name")
        or f"Round {round_number}",
        "meeting_official_name": race.get("official_name") or race.get("grand_prix_full_name"),
        "circuit_short_name": race.get("circuit_name") or race.get("circuit_full_name"),
        "country_name": race.get("country_name"),
        "location": race.get("circuit_place_name"),
        "start_date_utc": race_start,
        "end_date_utc": race_start,
        "raw_payload": race,
    }


def _build_f1db_session_rows(
    race: dict[str, Any],
    *,
    available_tokens: set[str],
) -> list[dict[str, Any]]:
    season = int(race["year"])
    round_number = int(race["round"])
    meeting_id = historical_meeting_id(season, round_number)
    session_rows: list[dict[str, Any]] = []
    for token, definition in sorted(
        HISTORICAL_SESSION_DEFINITIONS.items(),
        key=lambda item: item[1].session_order,
    ):
        if token not in available_tokens:
            continue
        if token == "R":
            start_at = _combine_date_time(race.get("date"), race.get("time"))
        else:
            start_at = _combine_date_time(
                race.get(definition.f1db_date_field) if definition.f1db_date_field else None,
                race.get(definition.f1db_time_field) if definition.f1db_time_field else None,
            )
        session_rows.append(
            {
                "id": historical_session_id(season, round_number, token),
                "source": "f1db",
                "session_key": historical_session_key(season, round_number, token),
                "meeting_id": meeting_id,
                "session_name": definition.session_name,
                "session_type": definition.session_type,
                "session_code": definition.session_code,
                "date_start_utc": start_at,
                "date_end_utc": start_at,
                "status": "complete",
                "session_order": definition.session_order,
                "is_practice": definition.is_practice,
                "raw_payload": {
                    "race_id": race.get("id"),
                    "session_token": token,
                    "race": race,
                },
            }
        )
    return session_rows


def _f1db_available_tokens(
    race: dict[str, Any],
    race_rows: list[dict[str, Any]],
) -> set[str]:
    available_tokens = {
        token
        for row in race_rows
        if (token := F1DB_RESULT_TYPE_TO_TOKEN.get(str(row.get("type")))) is not None
    }
    for row in race_rows:
        token = F1DB_GRID_TYPE_TO_TOKEN.get(str(row.get("type")))
        if token is not None:
            available_tokens.add(token)
    for token, definition in HISTORICAL_SESSION_DEFINITIONS.items():
        if token == "R":
            available_tokens.add(token)
            continue
        if definition.f1db_date_field and _has_value(race.get(definition.f1db_date_field)):
            available_tokens.add(token)
    return set(available_tokens)


def _build_f1db_fact_rows(
    race: dict[str, Any],
    race_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    season = int(race["year"])
    round_number = int(race["round"])
    result_rows: list[dict[str, Any]] = []
    pit_rows: list[dict[str, Any]] = []
    grid_rows: list[dict[str, Any]] = []

    for row in race_rows:
        row_type = str(row.get("type"))
        if row_type in F1DB_RESULT_TYPE_TO_TOKEN:
            token = F1DB_RESULT_TYPE_TO_TOKEN[row_type]
            session_id = historical_session_id(season, round_number, token)
            driver_id = canonical_driver_id(row.get("driver_id"), row.get("driver_id"))
            position = _parse_numeric_int(row.get("position_number"))
            parsed_result_time = _session_result_time_from_f1db(row, token)
            parsed_gap = parse_gap_value(
                _session_result_gap_from_f1db(row, token),
                position=position,
                allow_segments=True,
            )
            result_rows.append(
                {
                    "id": f"{session_id}:{driver_id}",
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "position": position,
                    "result_time_seconds": parsed_result_time.seconds,
                    "result_time_kind": parsed_result_time.kind,
                    "result_time_display": parsed_result_time.display,
                    "result_time_segments_json": parsed_result_time.segments_json,
                    "gap_to_leader_display": parsed_gap.display,
                    "gap_to_leader_seconds": parsed_gap.seconds,
                    "gap_to_leader_laps_behind": parsed_gap.laps_behind,
                    "gap_to_leader_status": parsed_gap.status,
                    "gap_to_leader_segments_json": parsed_gap.segments_json,
                    "dnf": None,
                    "dns": None,
                    "dsq": None,
                    "number_of_laps": _session_result_laps_from_f1db(row, token),
                    "raw_payload": row,
                }
            )
            continue

        if row_type == "RACE_RESULT":
            continue

        if row_type == "PIT_STOP":
            driver_id = canonical_driver_id(row.get("driver_id"), row.get("driver_id"))
            session_id = historical_session_id(season, round_number, "R")
            pit_rows.append(
                {
                    "id": f"{session_id}:{driver_id}:{row.get('pit_stop_stop')}",
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "observed_at_utc": None,
                    "lap_number": _parse_numeric_int(row.get("pit_stop_lap")),
                    "pit_duration_seconds": _parse_millis_to_seconds(
                        row.get("pit_stop_time_millis")
                    ),
                    "raw_payload": row,
                }
            )
            continue

        if row_type in F1DB_GRID_TYPE_TO_TOKEN:
            token = F1DB_GRID_TYPE_TO_TOKEN[row_type]
            driver_id = canonical_driver_id(row.get("driver_id"), row.get("driver_id"))
            session_id = historical_session_id(season, round_number, token)
            grid_rows.append(
                {
                    "id": f"{session_id}:{driver_id}",
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "grid_position": _parse_numeric_int(row.get("position_number")),
                    "raw_payload": row,
                }
            )

    return result_rows, pit_rows, grid_rows


def bootstrap_f1db_history(
    ctx: PipelineContext,
    *,
    season_start: int = 1950,
    season_end: int = 2022,
    artifact: str = "sqlite",
) -> dict[str, Any]:
    if artifact != "sqlite":
        raise ValueError("Only artifact=sqlite is currently supported")

    definition = ensure_job_definition(
        ctx.db,
        job_name="bootstrap-f1db-history",
        source="f1db",
        dataset="f1_history",
        description="Bootstrap pre-2023 F1 history from the F1DB SQLite artifact.",
        default_cursor={
            "season_start": season_start,
            "season_end": season_end,
            "artifact": artifact,
        },
        schedule_hint="manual",
    )
    cursor_key = f"{season_start}:{season_end}:{artifact}"
    cursor_state = get_cursor_state(
        ctx.db,
        source="f1db",
        dataset="f1_history",
        cursor_key=cursor_key,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season_start": season_start,
            "season_end": season_end,
            "artifact": artifact,
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

    connector = F1DBConnector(ctx.settings.data_root / "cache" / "f1db")
    driver_rows = _build_f1db_driver_rows(connector.fetch_drivers())
    constructor_rows = _build_f1db_team_rows(connector.fetch_constructors())

    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="f1db",
            dataset="drivers",
            endpoint="f1db:driver",
            params={"artifact": artifact},
            payload=[row["raw_payload"] for row in driver_rows],
            response_status=200,
            checkpoint=artifact,
        ),
    )
    persist_fetch(
        ctx,
        job_run_id=run.id,
        batch=FetchBatch(
            source="f1db",
            dataset="constructors",
            endpoint="f1db:constructor",
            params={"artifact": artifact},
            payload=[row["raw_payload"] for row in constructor_rows],
            response_status=200,
            checkpoint=artifact,
        ),
    )
    upsert_records(ctx.db, F1Driver, driver_rows)
    upsert_records(ctx.db, F1Team, constructor_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="f1_drivers", records=driver_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="f1_teams", records=constructor_rows)

    meetings_written = 0
    sessions_written = 0
    results_written = 0
    pit_written = 0
    grid_written = 0

    for season in range(season_start, season_end + 1):
        races = connector.fetch_races(season)
        race_data = connector.fetch_race_data(season)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="f1db",
                dataset="races",
                endpoint="f1db:race",
                params={"season": season, "artifact": artifact},
                payload=races,
                response_status=200,
                checkpoint=str(season),
            ),
            partition={"season": str(season)},
        )
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="f1db",
                dataset="race_data",
                endpoint="f1db:race_data",
                params={"season": season, "artifact": artifact},
                payload=race_data,
                response_status=200,
                checkpoint=str(season),
            ),
            partition={"season": str(season)},
        )

        race_data_by_race_id: dict[int, list[dict[str, Any]]] = {}
        for row in race_data:
            race_id = int(row["race_id"])
            race_data_by_race_id.setdefault(race_id, []).append(row)

        meeting_rows: list[dict[str, Any]] = []
        session_rows: list[dict[str, Any]] = []
        result_rows: list[dict[str, Any]] = []
        pit_rows: list[dict[str, Any]] = []
        grid_rows: list[dict[str, Any]] = []

        for race in races:
            race_rows = race_data_by_race_id.get(int(race["id"]), [])
            available_tokens = _f1db_available_tokens(race, race_rows)
            meeting_rows.append(_build_f1db_meeting_row(race))
            session_rows.extend(_build_f1db_session_rows(race, available_tokens=available_tokens))
            race_results, race_pit, race_grid = _build_f1db_fact_rows(race, race_rows)
            result_rows.extend(race_results)
            pit_rows.extend(race_pit)
            grid_rows.extend(race_grid)

        upsert_records(ctx.db, F1Meeting, meeting_rows)
        upsert_records(ctx.db, F1Session, session_rows)
        upsert_records(ctx.db, F1SessionResult, result_rows)
        if pit_rows:
            upsert_records(ctx.db, F1Pit, pit_rows)
        if grid_rows:
            upsert_records(ctx.db, F1StartingGrid, grid_rows)

        season_partition = {"season": str(season)}
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_meetings",
            records=meeting_rows,
            partition=season_partition,
        )
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_sessions",
            records=session_rows,
            partition=season_partition,
        )
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="f1_session_results",
            records=result_rows,
            partition=season_partition,
        )
        if pit_rows:
            persist_silver(
                ctx,
                job_run_id=run.id,
                dataset="f1_pit",
                records=pit_rows,
                partition=season_partition,
            )
        if grid_rows:
            persist_silver(
                ctx,
                job_run_id=run.id,
                dataset="f1_starting_grid",
                records=grid_rows,
                partition=season_partition,
            )

        meetings_written += len(meeting_rows)
        sessions_written += len(session_rows)
        results_written += len(result_rows)
        pit_written += len(pit_rows)
        grid_written += len(grid_rows)

    cursor_after = {
        "season_start": season_start,
        "season_end": season_end,
        "artifact": artifact,
        "synced_at": utc_now().isoformat(),
    }
    upsert_cursor_state(
        ctx.db,
        source="f1db",
        dataset="f1_history",
        cursor_key=cursor_key,
        cursor_value=cursor_after,
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after=cursor_after,
        records_written=(
            len(driver_rows)
            + len(constructor_rows)
            + meetings_written
            + sessions_written
            + results_written
            + pit_written
            + grid_written
        ),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "drivers": len(driver_rows),
        "teams": len(constructor_rows),
        "meetings": meetings_written,
        "sessions": sessions_written,
        "session_results": results_written,
        "pit_rows": pit_written,
        "grid_rows": grid_written,
    }


def _jolpica_meeting_row(race: dict[str, Any]) -> dict[str, Any]:
    season = int(race["season"])
    round_number = int(race["round"])
    circuit = race.get("Circuit") or {}
    location = circuit.get("Location") or {}
    start_at = _combine_date_time(race.get("date"), race.get("time"))
    return {
        "id": historical_meeting_id(season, round_number),
        "source": "jolpica",
        "meeting_key": historical_meeting_key(season, round_number),
        "season": season,
        "round_number": round_number,
        "meeting_name": race.get("raceName") or f"Round {round_number}",
        "meeting_official_name": race.get("raceName"),
        "circuit_short_name": circuit.get("circuitName"),
        "country_name": location.get("country"),
        "location": location.get("locality"),
        "start_date_utc": start_at,
        "end_date_utc": start_at,
        "raw_payload": race,
    }


def _jolpica_session_rows(race: dict[str, Any]) -> list[dict[str, Any]]:
    season = int(race["season"])
    round_number = int(race["round"])
    meeting_id = historical_meeting_id(season, round_number)
    session_rows: list[dict[str, Any]] = []
    for token, definition in sorted(
        HISTORICAL_SESSION_DEFINITIONS.items(),
        key=lambda item: item[1].session_order,
    ):
        schedule: dict[str, Any] | None
        if token in {"PRE_QUALIFYING", "FP4", "Q1", "Q2", "SQ", "WU"}:
            continue
        if token == "R":
            schedule = race
        else:
            field = definition.jolpica_schedule_field
            schedule_raw = race.get(field) if field else None
            schedule = dict(schedule_raw) if isinstance(schedule_raw, dict) else None
        if not isinstance(schedule, dict):
            continue
        start_at = _combine_date_time(schedule.get("date"), schedule.get("time"))
        session_rows.append(
            {
                "id": historical_session_id(season, round_number, token),
                "source": "jolpica",
                "session_key": historical_session_key(season, round_number, token),
                "meeting_id": meeting_id,
                "session_name": definition.session_name,
                "session_type": definition.session_type,
                "session_code": definition.session_code,
                "date_start_utc": start_at,
                "date_end_utc": start_at,
                "status": "complete",
                "session_order": definition.session_order,
                "is_practice": definition.is_practice,
                "raw_payload": {"race": race, "session_token": token},
            }
        )
    return session_rows


def _jolpica_driver_row(
    driver: dict[str, Any],
    constructor: dict[str, Any] | None,
) -> dict[str, Any]:
    driver_id = canonical_driver_id(driver.get("driverId"), driver.get("familyName"))
    permanent_number = _parse_numeric_int(driver.get("permanentNumber"))
    team_id = None
    if constructor is not None:
        team_id = canonical_team_id(constructor.get("constructorId"), constructor.get("name"))
    full_name = " ".join(
        str(part)
        for part in [driver.get("givenName"), driver.get("familyName")]
        if _has_value(part)
    )
    return {
        "id": driver_id,
        "source": "jolpica",
        "driver_number": (
            permanent_number
            if permanent_number is not None
            else _stable_negative_number(driver_id)
        ),
        "broadcast_name": driver.get("code") or full_name or driver.get("familyName"),
        "full_name": full_name or None,
        "first_name": driver.get("givenName"),
        "last_name": driver.get("familyName"),
        "name_acronym": driver.get("code"),
        "team_id": team_id,
        "country_code": None,
        "headshot_url": None,
        "raw_payload": driver,
    }


def _jolpica_team_row(constructor: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": canonical_team_id(constructor.get("constructorId"), constructor.get("name")),
        "source": "jolpica",
        "team_name": constructor.get("name") or "Unknown",
        "team_color": None,
        "raw_payload": constructor,
    }


def _jolpica_result_time(
    entry: dict[str, Any],
    resource: str,
) -> ParsedResultTimeValue:
    if resource == "qualifying":
        segments = [entry.get("Q1"), entry.get("Q2"), entry.get("Q3")]
        if any(_has_value(value) for value in segments):
            return _result_time_value(
                kind="segment_array",
                display=normalize_text(segments),
                segments_json=segments,
            )
        return _result_time_value(kind="unknown")

    time_payload = entry.get("Time") or {}
    time_text = time_payload.get("time")
    time_millis = time_payload.get("millis")
    position = _parse_numeric_int(entry.get("position"))
    if position == 1 and _has_value(time_text):
        text = str(time_text)
        if not text.startswith("+"):
            return _result_time_value(
                kind="total_time",
                seconds=_parse_clock_to_seconds(time_text)
                or _parse_millis_to_seconds(time_millis),
                display=normalize_text(time_text),
            )
    return _result_time_value(kind="unknown")


def _jolpica_gap_source(entry: dict[str, Any], resource: str) -> Any:
    if resource == "qualifying":
        return None

    position = _parse_numeric_int(entry.get("position"))
    time_text = ((entry.get("Time") or {}) or {}).get("time")
    if position == 1:
        return None
    return time_text or entry.get("status") or entry.get("positionText")


def _jolpica_result_rows(
    race: dict[str, Any],
    *,
    resource: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    token = JOLPICA_RESOURCE_TO_TOKEN[resource]
    season = int(race["season"])
    round_number = int(race["round"])
    session_id = historical_session_id(season, round_number, token)
    if resource == "results":
        entries = race.get("Results") or []
    elif resource == "qualifying":
        entries = race.get("QualifyingResults") or []
    else:
        entries = race.get("SprintResults") or []

    driver_rows: list[dict[str, Any]] = []
    team_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, Any]] = []
    for entry in entries:
        driver = entry.get("Driver") or {}
        constructor = entry.get("Constructor") or {}
        driver_row = _jolpica_driver_row(driver, constructor)
        team_row = _jolpica_team_row(constructor) if constructor else None
        driver_rows.append(driver_row)
        if team_row is not None:
            team_rows.append(team_row)

        position = _parse_numeric_int(entry.get("position"))
        parsed_result_time = _jolpica_result_time(entry, resource)
        parsed_gap = parse_gap_value(
            _jolpica_gap_source(entry, resource),
            position=position,
            allow_segments=True,
        )
        number_of_laps = None if resource == "qualifying" else _parse_numeric_int(entry.get("laps"))

        result_rows.append(
            {
                "id": f"{session_id}:{driver_row['id']}",
                "session_id": session_id,
                "driver_id": driver_row["id"],
                "position": position,
                "result_time_seconds": parsed_result_time.seconds,
                "result_time_kind": parsed_result_time.kind,
                "result_time_display": parsed_result_time.display,
                "result_time_segments_json": parsed_result_time.segments_json,
                "gap_to_leader_display": parsed_gap.display,
                "gap_to_leader_seconds": parsed_gap.seconds,
                "gap_to_leader_laps_behind": parsed_gap.laps_behind,
                "gap_to_leader_status": parsed_gap.status,
                "gap_to_leader_segments_json": parsed_gap.segments_json,
                "dnf": None,
                "dns": None,
                "dsq": None,
                "number_of_laps": number_of_laps,
                "raw_payload": entry,
            }
        )
    return driver_rows, team_rows, result_rows


def _jolpica_pit_rows(race: dict[str, Any]) -> list[dict[str, Any]]:
    season = int(race["season"])
    round_number = int(race["round"])
    session_id = historical_session_id(season, round_number, "R")
    pit_rows: list[dict[str, Any]] = []
    for item in race.get("PitStops") or []:
        driver_id = canonical_driver_id(item.get("driverId"), item.get("driverId"))
        pit_rows.append(
            {
                "id": f"{session_id}:{driver_id}:{item.get('stop')}",
                "session_id": session_id,
                "driver_id": driver_id,
                "observed_at_utc": None,
                "lap_number": _parse_numeric_int(item.get("lap")),
                "pit_duration_seconds": _parse_clock_to_seconds(item.get("duration")),
                "raw_payload": item,
            }
        )
    return pit_rows


def _jolpica_lap_rows(race: dict[str, Any]) -> list[dict[str, Any]]:
    season = int(race["season"])
    round_number = int(race["round"])
    session_id = historical_session_id(season, round_number, "R")
    lap_rows: list[dict[str, Any]] = []
    for lap in race.get("Laps") or []:
        lap_number = _parse_numeric_int(lap.get("number"))
        if lap_number is None:
            continue
        for timing in lap.get("Timings") or []:
            driver_id = canonical_driver_id(timing.get("driverId"), timing.get("driverId"))
            lap_rows.append(
                {
                    "id": f"{session_id}:{driver_id}:{lap_number}",
                    "session_id": session_id,
                    "driver_id": driver_id,
                    "lap_number": lap_number,
                    "lap_start_utc": None,
                    "lap_end_utc": None,
                    "lap_duration_seconds": _parse_clock_to_seconds(timing.get("time")),
                    "is_pit_out_lap": None,
                    "stint_number": None,
                    "sector_1_seconds": None,
                    "sector_2_seconds": None,
                    "sector_3_seconds": None,
                    "speed_trap_kph": None,
                    "raw_payload": timing | {"lap_number": lap_number},
                }
            )
    return lap_rows


def sync_jolpica_history(
    ctx: PipelineContext,
    *,
    season_start: int = 1950,
    season_end: int = 2022,
    resources: tuple[str, ...] = JOLPICA_DEFAULT_RESOURCES,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-jolpica-history",
        source="jolpica",
        dataset="f1_history",
        description="Repair and validate pre-2023 F1 history from Jolpica/Ergast-compatible APIs.",
        default_cursor={
            "season_start": season_start,
            "season_end": season_end,
            "resources": list(resources),
        },
        schedule_hint="manual",
    )
    cursor_key = f"{season_start}:{season_end}:{','.join(resources)}"
    cursor_state = get_cursor_state(
        ctx.db,
        source="jolpica",
        dataset="f1_history",
        cursor_key=cursor_key,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season_start": season_start,
            "season_end": season_end,
            "resources": list(resources),
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

    connector = JolpicaConnector()
    meetings_written = 0
    sessions_written = 0
    results_written = 0
    pit_written = 0
    lap_written = 0

    for season in range(season_start, season_end + 1):
        races = connector.fetch_races(season)
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="jolpica",
                dataset="races",
                endpoint=f"/{season}.json",
                params={},
                payload=races,
                response_status=200,
                checkpoint=str(season),
            ),
            partition={"season": str(season)},
        )

        meeting_rows: list[dict[str, Any]] = []
        session_rows: list[dict[str, Any]] = []
        for race in races:
            meeting_rows.append(_merge_with_existing(ctx, F1Meeting, _jolpica_meeting_row(race)))
            for session_row in _jolpica_session_rows(race):
                session_rows.append(_merge_with_existing(ctx, F1Session, session_row))
        if meeting_rows:
            upsert_records(ctx.db, F1Meeting, meeting_rows)
            persist_silver(
                ctx,
                job_run_id=run.id,
                dataset="f1_meetings",
                records=meeting_rows,
                partition={"season": str(season)},
            )
            meetings_written += len(meeting_rows)
        if session_rows:
            upsert_records(ctx.db, F1Session, session_rows)
            persist_silver(
                ctx,
                job_run_id=run.id,
                dataset="f1_sessions",
                records=session_rows,
                partition={"season": str(season)},
            )
            sessions_written += len(session_rows)

        for race in races:
            round_number = int(race["round"])
            if "results" in resources:
                results_payload = connector.fetch_results(season, round_number)
                persist_fetch(
                    ctx,
                    job_run_id=run.id,
                    batch=FetchBatch(
                        source="jolpica",
                        dataset="results",
                        endpoint=f"/{season}/{round_number}/results.json",
                        params={"limit": 100},
                        payload=results_payload,
                        response_status=200,
                        checkpoint=f"{season}:{round_number}",
                    ),
                    partition={"season": str(season), "round": str(round_number)},
                )
                if results_payload:
                    driver_rows, team_rows, result_rows = _jolpica_result_rows(
                        results_payload[0],
                        resource="results",
                    )
                    merged_drivers = _dedupe_records(
                        [_merge_with_existing(ctx, F1Driver, row) for row in driver_rows]
                    )
                    merged_teams = _dedupe_records(
                        [_merge_with_existing(ctx, F1Team, row) for row in team_rows]
                    )
                    merged_results = [
                        _merge_with_existing(ctx, F1SessionResult, row) for row in result_rows
                    ]
                    upsert_records(ctx.db, F1Driver, merged_drivers)
                    upsert_records(ctx.db, F1Team, merged_teams)
                    upsert_records(ctx.db, F1SessionResult, merged_results)
                    persist_silver(
                        ctx,
                        job_run_id=run.id,
                        dataset="f1_session_results",
                        records=merged_results,
                        partition={"season": str(season)},
                    )
                    results_written += len(merged_results)

            if "qualifying" in resources:
                qualifying_payload = connector.fetch_qualifying(season, round_number)
                persist_fetch(
                    ctx,
                    job_run_id=run.id,
                    batch=FetchBatch(
                        source="jolpica",
                        dataset="qualifying",
                        endpoint=f"/{season}/{round_number}/qualifying.json",
                        params={"limit": 100},
                        payload=qualifying_payload,
                        response_status=200,
                        checkpoint=f"{season}:{round_number}",
                    ),
                    partition={"season": str(season), "round": str(round_number)},
                )
                if qualifying_payload:
                    driver_rows, team_rows, result_rows = _jolpica_result_rows(
                        qualifying_payload[0],
                        resource="qualifying",
                    )
                    merged_drivers = _dedupe_records(
                        [_merge_with_existing(ctx, F1Driver, row) for row in driver_rows]
                    )
                    merged_teams = _dedupe_records(
                        [_merge_with_existing(ctx, F1Team, row) for row in team_rows]
                    )
                    merged_results = [
                        _merge_with_existing(ctx, F1SessionResult, row) for row in result_rows
                    ]
                    upsert_records(ctx.db, F1Driver, merged_drivers)
                    upsert_records(ctx.db, F1Team, merged_teams)
                    upsert_records(ctx.db, F1SessionResult, merged_results)
                    persist_silver(
                        ctx,
                        job_run_id=run.id,
                        dataset="f1_session_results",
                        records=merged_results,
                        partition={"season": str(season)},
                    )
                    results_written += len(merged_results)

            if "sprint" in resources:
                sprint_payload = connector.fetch_sprint(season, round_number)
                persist_fetch(
                    ctx,
                    job_run_id=run.id,
                    batch=FetchBatch(
                        source="jolpica",
                        dataset="sprint",
                        endpoint=f"/{season}/{round_number}/sprint.json",
                        params={"limit": 100},
                        payload=sprint_payload,
                        response_status=200,
                        checkpoint=f"{season}:{round_number}",
                    ),
                    partition={"season": str(season), "round": str(round_number)},
                )
                if sprint_payload:
                    driver_rows, team_rows, result_rows = _jolpica_result_rows(
                        sprint_payload[0],
                        resource="sprint",
                    )
                    merged_drivers = _dedupe_records(
                        [_merge_with_existing(ctx, F1Driver, row) for row in driver_rows]
                    )
                    merged_teams = _dedupe_records(
                        [_merge_with_existing(ctx, F1Team, row) for row in team_rows]
                    )
                    merged_results = [
                        _merge_with_existing(ctx, F1SessionResult, row) for row in result_rows
                    ]
                    upsert_records(ctx.db, F1Driver, merged_drivers)
                    upsert_records(ctx.db, F1Team, merged_teams)
                    upsert_records(ctx.db, F1SessionResult, merged_results)
                    persist_silver(
                        ctx,
                        job_run_id=run.id,
                        dataset="f1_session_results",
                        records=merged_results,
                        partition={"season": str(season)},
                    )
                    results_written += len(merged_results)

            if "pitstops" in resources:
                pit_payload = connector.fetch_pitstops(season, round_number)
                persist_fetch(
                    ctx,
                    job_run_id=run.id,
                    batch=FetchBatch(
                        source="jolpica",
                        dataset="pitstops",
                        endpoint=f"/{season}/{round_number}/pitstops.json",
                        params={"limit": 2000},
                        payload=pit_payload,
                        response_status=200,
                        checkpoint=f"{season}:{round_number}",
                    ),
                    partition={"season": str(season), "round": str(round_number)},
                )
                if pit_payload:
                    pit_rows = [
                        _merge_with_existing(ctx, F1Pit, row)
                        for row in _jolpica_pit_rows(pit_payload[0])
                    ]
                    if pit_rows:
                        upsert_records(ctx.db, F1Pit, pit_rows)
                        persist_silver(
                            ctx,
                            job_run_id=run.id,
                            dataset="f1_pit",
                            records=pit_rows,
                            partition={"season": str(season)},
                        )
                        pit_written += len(pit_rows)

            if "laps" in resources:
                lap_payload = connector.fetch_laps(season, round_number)
                persist_fetch(
                    ctx,
                    job_run_id=run.id,
                    batch=FetchBatch(
                        source="jolpica",
                        dataset="laps",
                        endpoint=f"/{season}/{round_number}/laps.json",
                        params={"limit": 5000},
                        payload=lap_payload,
                        response_status=200,
                        checkpoint=f"{season}:{round_number}",
                    ),
                    partition={"season": str(season), "round": str(round_number)},
                )
                if lap_payload:
                    lap_rows = [
                        _merge_with_existing(ctx, F1Lap, row)
                        for row in _jolpica_lap_rows(lap_payload[0])
                    ]
                    if lap_rows:
                        upsert_records(ctx.db, F1Lap, lap_rows)
                        persist_silver(
                            ctx,
                            job_run_id=run.id,
                            dataset="f1_laps",
                            records=lap_rows,
                            partition={"season": str(season)},
                        )
                        lap_written += len(lap_rows)

    cursor_after = {
        "season_start": season_start,
        "season_end": season_end,
        "resources": list(resources),
        "synced_at": utc_now().isoformat(),
    }
    upsert_cursor_state(
        ctx.db,
        source="jolpica",
        dataset="f1_history",
        cursor_key=cursor_key,
        cursor_value=cursor_after,
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after=cursor_after,
        records_written=(
            meetings_written
            + sessions_written
            + results_written
            + pit_written
            + lap_written
        ),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "meetings": meetings_written,
        "sessions": sessions_written,
        "session_results": results_written,
        "pit_rows": pit_written,
        "lap_rows": lap_written,
    }


# Session codes that carry signal useful for pole-position snapshots.
_OPENF1_SNAPSHOT_SESSION_CODES = frozenset({"FP1", "Q", "SQ"})


def sync_openf1_season_range(
    ctx: PipelineContext,
    *,
    season_start: int = 2023,
    season_end: int = 2025,
    force_rehydrate: bool = False,
) -> dict[str, Any]:
    """Bulk-sync OpenF1 FP1/Q/SQ sessions for a range of seasons.

    Steps for each season:
    1. Sync meeting + session catalog via sync_f1_calendar.
    2. For each F1Session with code in {FP1, Q, SQ} that has no existing
       session results (or force_rehydrate=True), call hydrate_f1_session
       to pull laps, drivers, and per-driver results from OpenF1.

    The function is idempotent: sessions that already have result rows are
    skipped unless force_rehydrate=True.
    """
    from sqlalchemy import func as sa_func

    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-openf1-season-range",
        source="openf1",
        dataset="f1_openf1_bulk",
        description=(
            "Bulk sync OpenF1 FP1/Q/SQ sessions for a range of seasons. "
            "Calls sync_f1_calendar then hydrate_f1_session per session."
        ),
        schedule_hint="manual",
    )
    cursor_key = f"{season_start}:{season_end}"
    cursor_state = get_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_openf1_bulk",
        cursor_key=cursor_key,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season_start": season_start,
            "season_end": season_end,
            "force_rehydrate": force_rehydrate,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    seasons_synced: list[int] = []
    sessions_hydrated: int = 0
    sessions_skipped: int = 0

    for season in range(season_start, season_end + 1):
        # Step 1: ensure meeting + session catalog exists.
        sync_f1_calendar(ctx, season=season)
        seasons_synced.append(season)

        # Step 2: find FP1/Q/SQ sessions for this season.
        session_keys: list[int] = list(
            ctx.db.scalars(
                select(F1Session.session_key)
                .join(F1Meeting, F1Meeting.id == F1Session.meeting_id)
                .where(
                    F1Meeting.season == season,
                    F1Session.source == "openf1",
                    F1Session.session_code.in_(tuple(_OPENF1_SNAPSHOT_SESSION_CODES)),
                    F1Session.session_key.isnot(None),
                )
            ).all()
        )

        # Find session keys that already have results to skip.
        hydrated_session_ids: set[str] = set()
        if not force_rehydrate and session_keys:
            result_counts = ctx.db.execute(
                select(F1SessionResult.session_id, sa_func.count())
                .join(F1Session, F1Session.id == F1SessionResult.session_id)
                .where(
                    F1Session.session_key.in_(session_keys),
                )
                .group_by(F1SessionResult.session_id)
            ).all()
            hydrated_session_ids = {row[0] for row in result_counts if row[1] > 0}

        for session_key in session_keys:
            session_id = f"session:{session_key}"
            if session_id in hydrated_session_ids:
                sessions_skipped += 1
                continue

            hydrate_f1_session(ctx, session_key=session_key)
            sessions_hydrated += 1

    cursor_after = {
        "season_start": season_start,
        "season_end": season_end,
        "sessions_hydrated": sessions_hydrated,
        "sessions_skipped": sessions_skipped,
        "synced_at": utc_now().isoformat(),
    }
    upsert_cursor_state(
        ctx.db,
        source="openf1",
        dataset="f1_openf1_bulk",
        cursor_key=cursor_key,
        cursor_value=cursor_after,
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after=cursor_after,
        records_written=sessions_hydrated,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "seasons_synced": seasons_synced,
        "sessions_hydrated": sessions_hydrated,
        "sessions_skipped": sessions_skipped,
    }


def sweep_polymarket_historical_poles(
    ctx: PipelineContext,
    *,
    max_pages: int = 300,
    batch_size: int = 100,
    fidelity: int = 60,
) -> dict[str, Any]:
    """Sweep Polymarket closed markets for F1 Q/SQ pole position history.

    Step 1: sync_polymarket_catalog with closed=True to discover historical
    F1 pole markets (slug pattern: f1-*-grand-prix-*pole-position* or
    f1-*-sprint-qualifying*).

    Step 2: hydrate_polymarket_f1_history to fill price history, trades,
    and resolution data for all discovered F1 markets.

    max_pages controls how far back to sweep (each page = ~100 markets).
    Use max_pages=300 to reach roughly 3 years of history.
    """
    from f1_polymarket_worker.orchestration import hydrate_polymarket_f1_history
    from f1_polymarket_worker.pipeline import sync_polymarket_catalog

    definition = ensure_job_definition(
        ctx.db,
        job_name="sweep-polymarket-historical-poles",
        source="polymarket",
        dataset="historical_poles",
        description=(
            "Sweep closed Polymarket F1 pole markets and hydrate price/resolution history. "
            "Calls sync_polymarket_catalog(closed=True) then hydrate_polymarket_f1_history."
        ),
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "max_pages": max_pages,
            "batch_size": batch_size,
            "fidelity": fidelity,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    catalog_result = sync_polymarket_catalog(
        ctx,
        closed=True,
        max_pages=max_pages,
        batch_size=batch_size,
    )
    hydrate_result = hydrate_polymarket_f1_history(ctx, fidelity=fidelity)

    total_written = int(catalog_result.get("markets", 0)) + int(
        hydrate_result.get("records_written", 0)
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=total_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "catalog_markets_discovered": catalog_result.get("markets", 0),
        "catalog_events_discovered": catalog_result.get("events", 0),
        "markets_hydrated": hydrate_result.get("markets_hydrated", 0),
        "records_written": hydrate_result.get("records_written", 0),
    }
