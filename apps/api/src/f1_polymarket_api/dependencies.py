from __future__ import annotations

from collections.abc import Generator

from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.storage.db import session_factory
from sqlalchemy.orm import Session

session_maker = session_factory(get_settings().database_url)


def get_db_session() -> Generator[Session, None, None]:
    session = session_maker()
    try:
        yield session
    finally:
        session.close()
