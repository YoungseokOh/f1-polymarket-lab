from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


def build_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True, pool_pre_ping=True)


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
