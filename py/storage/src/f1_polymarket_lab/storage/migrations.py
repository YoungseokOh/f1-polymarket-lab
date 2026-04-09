from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from f1_polymarket_lab.common import slugify
from sqlalchemy import inspect, select, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

from .db import Base, build_engine

if TYPE_CHECKING:
    from alembic.config import Config

_BOOTSTRAP_SEEDED_TABLE_ROWS: dict[str, int] = {
    "f1_calendar_overrides": 2,
}
_COUNTRY_FALLBACK_MEETING_NAMES: dict[str, str] = {
    "saudi arabia": "Saudi Arabian Grand Prix",
}


def _repo_root() -> Path:
    start = Path(__file__).resolve()
    for candidate in (start.parent, *start.parents):
        if (candidate / "alembic.ini").exists() and (candidate / "db" / "alembic").exists():
            return candidate
    raise FileNotFoundError("Could not locate repository root containing alembic.ini")


def _alembic_config(database_url: str) -> Config:
    from alembic.config import Config

    root = _repo_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "db" / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def masked_database_url(database_url: str) -> str:
    try:
        return make_url(database_url).render_as_string(hide_password=True)
    except Exception:
        return database_url


def stamp_database(database_url: str, revision: str = "head") -> None:
    from alembic import command

    command.stamp(_alembic_config(database_url), revision)


def upgrade_database(database_url: str, revision: str = "head") -> None:
    from alembic import command

    command.upgrade(_alembic_config(database_url), revision)


def ensure_database_schema(database_url: str) -> dict[str, Any]:
    if database_url.startswith("sqlite"):
        engine = build_engine(database_url)
        Base.metadata.create_all(engine)
        stamp_database(database_url, "head")
        return {
            "database_url": masked_database_url(database_url),
            "database_kind": "sqlite",
            "initialization_mode": "create_all_and_stamp",
            "revision": "head",
        }

    upgrade_database(database_url, "head")
    return {
        "database_url": masked_database_url(database_url),
        "database_kind": "postgresql",
        "initialization_mode": "alembic_upgrade",
        "revision": "head",
    }


def _normalize_event_format(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip().lower()
    if not text_value:
        return None
    return text_value.replace(" ", "_").replace("-", "_")


def _infer_event_format_from_session_codes(session_codes: set[str]) -> str | None:
    if {"FP1", "SQ", "S", "Q", "R"}.issubset(session_codes):
        return "sprint"
    if {"FP1", "FP2", "FP3", "Q", "R"}.issubset(session_codes):
        return "conventional"
    return None


def _load_session_codes_by_meeting(source_engine: Engine) -> dict[str, set[str]]:
    inspector = inspect(source_engine)
    if not inspector.has_table("f1_sessions"):
        return {}
    columns = {column["name"] for column in inspector.get_columns("f1_sessions")}
    if not {"meeting_id", "session_code"}.issubset(columns):
        return {}

    meeting_id_column = Base.metadata.tables["f1_sessions"].c.meeting_id
    session_code_column = Base.metadata.tables["f1_sessions"].c.session_code
    session_codes: dict[str, set[str]] = defaultdict(set)
    with source_engine.connect() as connection:
        for meeting_id, session_code in connection.execute(
            select(meeting_id_column, session_code_column)
        ):
            if meeting_id and session_code:
                session_codes[str(meeting_id)].add(str(session_code))
    return session_codes


def _source_table_count(source_engine: Engine, table_name: str) -> int:
    inspector = inspect(source_engine)
    if not inspector.has_table(table_name):
        return 0
    with source_engine.connect() as connection:
        return int(connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one())


def _target_table_count(target_engine: Engine, table_name: str) -> int:
    with target_engine.connect() as connection:
        return int(connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one())


def _derive_meeting_slug(record: dict[str, Any]) -> str | None:
    raw_payload = record.get("raw_payload") or {}
    for candidate in (
        raw_payload.get("grand_prix_full_name"),
        raw_payload.get("EventName"),
        raw_payload.get("raceName"),
    ):
        if candidate:
            return str(slugify(str(candidate)))

    meeting_name = str(record.get("meeting_name") or "").strip()
    if meeting_name and "grand prix" in meeting_name.lower():
        return str(slugify(meeting_name))

    country_name = str(record.get("country_name") or "").strip()
    if country_name:
        fallback_name = _COUNTRY_FALLBACK_MEETING_NAMES.get(
            country_name.lower(),
            f"{country_name} Grand Prix",
        )
        return str(slugify(fallback_name))

    if meeting_name:
        return str(slugify(meeting_name))

    location = str(record.get("location") or "").strip()
    if location:
        return str(slugify(f"{location} Grand Prix"))

    season = record.get("season")
    meeting_key = record.get("meeting_key")
    if season is not None and meeting_key is not None:
        return str(slugify(f"{season}-meeting-{meeting_key}"))
    return None


def _derive_event_format(record: dict[str, Any], session_codes: set[str]) -> str | None:
    raw_payload = record.get("raw_payload") or {}
    normalized = _normalize_event_format(raw_payload.get("EventFormat"))
    if normalized is not None:
        return normalized

    if any(
        raw_payload.get(key)
        for key in (
            "sprint_qualifying_date",
            "sprint_qualifying_time",
            "sprint_race_date",
            "sprint_race_time",
        )
    ):
        return "sprint"

    inferred = _infer_event_format_from_session_codes(session_codes)
    if inferred is not None:
        return inferred

    if any(
        raw_payload.get(key)
        for key in (
            "qualifying_format",
            "date",
            "qualifying_date",
            "free_practice_1_date",
            "free_practice_2_date",
            "free_practice_3_date",
            "grand_prix_id",
        )
    ):
        return "conventional"

    return None


def _adapt_record(
    table_name: str,
    record: dict[str, Any],
    *,
    session_codes_by_meeting: dict[str, set[str]],
    derived_columns: set[str],
) -> dict[str, Any]:
    adapted = dict(record)

    if table_name == "f1_meetings":
        if "meeting_slug" not in adapted:
            adapted["meeting_slug"] = _derive_meeting_slug(adapted)
            derived_columns.add("meeting_slug")
        if "event_format" not in adapted:
            adapted["event_format"] = _derive_event_format(
                adapted,
                session_codes_by_meeting.get(str(adapted["id"]), set()),
            )
            derived_columns.add("event_format")

    if table_name == "model_runs" and "registry_run_id" not in adapted:
        adapted["registry_run_id"] = None
        derived_columns.add("registry_run_id")

    return adapted


def _truncate_target_tables(target_engine: Engine) -> None:
    table_names = ", ".join(table.name for table in Base.metadata.sorted_tables)
    with target_engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE"))


def _target_nonempty_tables(target_engine: Engine) -> list[dict[str, Any]]:
    nonempty: list[dict[str, Any]] = []
    for table in Base.metadata.sorted_tables:
        count = _target_table_count(target_engine, table.name)
        if count:
            nonempty.append({"table_name": table.name, "target_count": count})
    return nonempty


def _expected_bootstrap_target_rows(table_name: str, source_count: int) -> int:
    if source_count == 0:
        return _BOOTSTRAP_SEEDED_TABLE_ROWS.get(table_name, 0)
    return 0


def _unexpected_target_nonempty_tables(
    target_engine: Engine,
    source_counts: dict[str, int],
) -> list[dict[str, Any]]:
    unexpected: list[dict[str, Any]] = []
    for row in _target_nonempty_tables(target_engine):
        expected_bootstrap_rows = _expected_bootstrap_target_rows(
            row["table_name"],
            source_counts.get(row["table_name"], 0),
        )
        if row["target_count"] != expected_bootstrap_rows:
            unexpected.append(row)
    return unexpected


def _migrate_table(
    source_engine: Engine,
    target_engine: Engine,
    *,
    table_name: str,
    batch_size: int,
    session_codes_by_meeting: dict[str, set[str]],
) -> tuple[int, list[str], list[str]]:
    table = Base.metadata.tables[table_name]
    inspector = inspect(source_engine)
    if not inspector.has_table(table_name):
        return 0, [column.name for column in table.columns], []

    source_columns = {column["name"] for column in inspector.get_columns(table_name)}
    selected_columns = [
        table.c[column.name] for column in table.columns if column.name in source_columns
    ]
    missing_columns = [column.name for column in table.columns if column.name not in source_columns]
    derived_columns: set[str] = set()
    migrated_rows = 0

    statement = select(*selected_columns)
    order_by_columns = [
        table.c[column.name]
        for column in table.primary_key.columns
        if column.name in source_columns
    ]
    if order_by_columns:
        statement = statement.order_by(*order_by_columns)

    with source_engine.connect() as source_connection, Session(target_engine) as target_session:
        result = source_connection.execution_options(stream_results=True).execute(statement)
        while rows := result.fetchmany(batch_size):
            payload = [
                _adapt_record(
                    table_name,
                    dict(row._mapping),
                    session_codes_by_meeting=session_codes_by_meeting,
                    derived_columns=derived_columns,
                )
                for row in rows
            ]
            target_session.execute(table.insert(), payload)
            migrated_rows += len(payload)
        target_session.commit()

    return migrated_rows, missing_columns, sorted(derived_columns)


def migrate_sqlite_to_postgres(
    *,
    sqlite_url: str,
    postgres_url: str,
    batch_size: int = 1000,
    truncate_target: bool = False,
    execute: bool = False,
) -> dict[str, Any]:
    if not sqlite_url.startswith("sqlite"):
        raise ValueError("sqlite_url must be a SQLite SQLAlchemy URL")
    if not postgres_url.startswith("postgresql"):
        raise ValueError("postgres_url must be a PostgreSQL SQLAlchemy URL")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    source_engine = build_engine(sqlite_url)
    target_engine = build_engine(postgres_url)
    session_codes_by_meeting = _load_session_codes_by_meeting(source_engine)
    source_counts = {
        table.name: _source_table_count(source_engine, table.name)
        for table in Base.metadata.sorted_tables
    }

    upgrade_database(postgres_url, "head")

    target_precheck = _target_nonempty_tables(target_engine)
    unexpected_target_precheck = _unexpected_target_nonempty_tables(target_engine, source_counts)
    if execute:
        if truncate_target:
            _truncate_target_tables(target_engine)
        elif unexpected_target_precheck:
            tables = ", ".join(
                f"{row['table_name']}={row['target_count']}" for row in unexpected_target_precheck
            )
            raise ValueError(
                "PostgreSQL target is not empty. "
                "Re-run with --truncate-target to replace existing data: "
                f"{tables}"
            )

    table_summaries: list[dict[str, Any]] = []
    total_source_rows = 0
    migrated_tables = 0

    for table in Base.metadata.sorted_tables:
        source_count = source_counts[table.name]
        total_source_rows += source_count
        if execute:
            migrated_rows, missing_columns, derived_columns = _migrate_table(
                source_engine,
                target_engine,
                table_name=table.name,
                batch_size=batch_size,
                session_codes_by_meeting=session_codes_by_meeting,
            )
        else:
            inspector = inspect(source_engine)
            if inspector.has_table(table.name):
                source_columns = {column["name"] for column in inspector.get_columns(table.name)}
                missing_columns = [
                    column.name for column in table.columns if column.name not in source_columns
                ]
            else:
                missing_columns = [column.name for column in table.columns]
            derived_columns = []
            if table.name == "f1_meetings":
                if "meeting_slug" in missing_columns:
                    derived_columns.append("meeting_slug")
                if "event_format" in missing_columns:
                    derived_columns.append("event_format")
            if table.name == "model_runs" and "registry_run_id" in missing_columns:
                derived_columns.append("registry_run_id")
            migrated_rows = 0

        target_count = _target_table_count(target_engine, table.name)
        if source_count:
            migrated_tables += 1
        table_summaries.append(
            {
                "table_name": table.name,
                "source_count": source_count,
                "target_count": target_count,
                "missing_source_columns": missing_columns,
                "derived_columns": derived_columns,
                "migrated_rows": migrated_rows,
            }
        )

    row_count_mismatches = [
        {
            "table_name": table["table_name"],
            "source_count": table["source_count"],
            "target_count": table["target_count"],
        }
        for table in table_summaries
        if table["target_count"]
        != (
            table["source_count"]
            + _expected_bootstrap_target_rows(table["table_name"], table["source_count"])
        )
    ]

    if execute and row_count_mismatches:
        mismatches = ", ".join(
            f"{row['table_name']}:{row['source_count']}!={row['target_count']}"
            for row in row_count_mismatches
        )
        raise ValueError(f"Row count mismatch after migration: {mismatches}")

    source_schema_drift = {
        table["table_name"]: table["missing_source_columns"]
        for table in table_summaries
        if table["missing_source_columns"]
    }
    derived_column_defaults = {
        table["table_name"]: table["derived_columns"]
        for table in table_summaries
        if table["derived_columns"]
    }

    return {
        "source_url": masked_database_url(sqlite_url),
        "target_url": masked_database_url(postgres_url),
        "execute": execute,
        "batch_size": batch_size,
        "target_was_truncated": truncate_target if execute else False,
        "target_nonempty_tables": target_precheck,
        "unexpected_target_nonempty_tables": unexpected_target_precheck,
        "migrated_tables": migrated_tables,
        "source_nonempty_tables": sum(1 for table in table_summaries if table["source_count"]),
        "total_source_rows": total_source_rows,
        "row_count_mismatches": row_count_mismatches,
        "source_schema_drift": source_schema_drift,
        "derived_column_defaults": derived_column_defaults,
        "tables": table_summaries,
    }
