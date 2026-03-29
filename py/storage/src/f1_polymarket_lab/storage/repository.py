from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from sqlalchemy import inspect, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session


def _dedupe_records(
    records: Sequence[dict[str, Any]],
    *,
    conflict_keys: Sequence[str],
) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for record in records:
        key = tuple(record[column] for column in conflict_keys)
        if key in deduped:
            deduped.pop(key)
        deduped[key] = record
    return list(deduped.values())


def _fallback_upsert(
    session: Session,
    model: type[Any],
    records: Sequence[dict[str, Any]],
    *,
    primary_key: str,
    conflict_columns: Sequence[str] | None = None,
) -> None:
    mapper = inspect(model)
    primary_keys = {column.name for column in mapper.primary_key}
    conflict_keys = list(conflict_columns or [primary_key])
    payload = _dedupe_records(records, conflict_keys=conflict_keys)

    def matches(instance: Any, record: dict[str, Any]) -> bool:
        return all(getattr(instance, key) == record.get(key) for key in conflict_keys)

    def lookup_existing(record: dict[str, Any]) -> Any | None:
        for instance in session.identity_map.values():
            if isinstance(instance, model) and matches(instance, record):
                return instance
        for instance in session.new:
            if isinstance(instance, model) and matches(instance, record):
                return instance

        identifier = record.get(primary_key)
        if identifier is not None:
            existing = session.get(model, identifier)
            if existing is not None:
                return existing

        if conflict_keys == [primary_key]:
            return None

        statement = select(model)
        for key in conflict_keys:
            value = record.get(key)
            column = getattr(model, key)
            statement = statement.where(column.is_(None) if value is None else column == value)
        return session.scalar(statement)

    with session.no_autoflush:
        for record in payload:
            existing = lookup_existing(record)
            if existing is None:
                session.add(model(**record))
                continue

            for key, value in record.items():
                if key in primary_keys:
                    continue
                setattr(existing, key, value)


def upsert_records(
    session: Session,
    model: type[Any],
    records: Iterable[dict[str, Any]],
    *,
    primary_key: str = "id",
    conflict_columns: Sequence[str] | None = None,
) -> None:
    payload = list(records)
    if not payload:
        return

    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        _fallback_upsert(
            session,
            model,
            payload,
            primary_key=primary_key,
            conflict_columns=conflict_columns,
        )
        return

    mapper = inspect(model)
    table = mapper.local_table
    conflict_keys = list(conflict_columns or [column.name for column in mapper.primary_key])
    if not conflict_keys:
        _fallback_upsert(
            session,
            model,
            payload,
            primary_key=primary_key,
            conflict_columns=conflict_columns,
        )
        return
    payload = _dedupe_records(payload, conflict_keys=conflict_keys)
    if not payload:
        return

    parameter_budget = 60000
    parameter_count = max(1, len(payload[0]))
    batch_size = max(1, parameter_budget // parameter_count)

    for index in range(0, len(payload), batch_size):
        batch = payload[index : index + batch_size]
        statement = pg_insert(table).values(batch)
        update_values = {
            column.name: getattr(statement.excluded, column.name)
            for column in table.columns
            if column.name not in conflict_keys and not column.primary_key
        }
        session.execute(
            statement.on_conflict_do_update(
                index_elements=conflict_keys,
                set_=update_values,
            )
        )
