from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session


def _fallback_upsert(
    session: Session,
    model: type[Any],
    records: Sequence[dict[str, Any]],
    *,
    primary_key: str,
) -> None:
    for record in records:
        identifier = record[primary_key]
        existing = session.get(model, identifier)
        if existing is None:
            session.add(model(**record))
            continue

        for key, value in record.items():
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
        _fallback_upsert(session, model, payload, primary_key=primary_key)
        return

    mapper = inspect(model)
    table = mapper.local_table
    conflict_keys = list(conflict_columns or [column.name for column in mapper.primary_key])
    if not conflict_keys:
        _fallback_upsert(session, model, payload, primary_key=primary_key)
        return

    statement = pg_insert(table).values(payload)
    update_values = {
        column.name: getattr(statement.excluded, column.name)
        for column in table.columns
        if column.name not in conflict_keys
    }
    session.execute(
        statement.on_conflict_do_update(
            index_elements=conflict_keys,
            set_=update_values,
        )
    )
