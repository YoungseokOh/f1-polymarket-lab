from __future__ import annotations

from collections.abc import Generator
from typing import cast

from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.storage.db import session_factory
from fastapi import Request
from sqlalchemy.orm import Session, sessionmaker


def _get_session_maker(request: Request) -> sessionmaker[Session]:
    session_maker = getattr(request.app.state, "session_maker", None)
    if session_maker is None:
        return cast(sessionmaker[Session], session_factory(get_settings().database_url))
    return cast(sessionmaker[Session], session_maker)


def get_db_session(request: Request) -> Generator[Session, None, None]:
    session_maker = _get_session_maker(request)
    session = session_maker()
    try:
        yield session
    finally:
        session.close()
