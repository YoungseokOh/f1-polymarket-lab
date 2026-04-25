from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


@cache
def build_engine(database_url: str) -> Engine:
    options: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
    }
    if database_url.startswith("sqlite"):
        options["connect_args"] = {
            "check_same_thread": False,
            "timeout": 30,
        }

    engine = create_engine(database_url, **options)

    if database_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _configure_sqlite_connection(
            dbapi_connection: Any,
            _connection_record: Any,
        ) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("PRAGMA foreign_keys = ON")
                cursor.execute("PRAGMA busy_timeout = 30000")
                try:
                    cursor.execute("PRAGMA journal_mode = WAL")
                    cursor.execute("PRAGMA synchronous = NORMAL")
                except Exception:
                    # In-memory SQLite and some read-only contexts may reject WAL.
                    pass
            finally:
                cursor.close()

    return engine


@cache
def session_factory(database_url: str) -> sessionmaker[Session]:
    return sessionmaker(bind=build_engine(database_url), expire_on_commit=False)


@contextmanager
def db_session(database_url: str) -> Iterator[Session]:
    factory = session_factory(database_url)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
