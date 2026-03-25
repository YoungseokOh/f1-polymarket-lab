from __future__ import annotations

from typing import Any

from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    F1Lap,
    F1Meeting,
    F1Session,
    PolymarketEvent,
    PolymarketMarket,
)
from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


def sqlite_engine() -> Engine:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def test_f1_cascade_delete_removes_session_children() -> None:
    engine = sqlite_engine()
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(
            F1Meeting(
                id="meeting-1",
                meeting_key=101,
                season=2026,
                meeting_name="Australian Grand Prix",
            )
        )
        session.add(
            F1Session(
                id="session-1",
                session_key=1001,
                meeting_id="meeting-1",
                session_name="Race",
            )
        )
        session.commit()

        session.add(
            F1Lap(
                id="lap-1",
                session_id="session-1",
                driver_id=None,
                lap_number=1,
            )
        )
        session.commit()

        session.execute(delete(F1Meeting).where(F1Meeting.id == "meeting-1"))
        session.commit()

        assert session.get(F1Session, "session-1") is None
        assert session.get(F1Lap, "lap-1") is None


def test_polymarket_market_requires_existing_event() -> None:
    engine = sqlite_engine()
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(
            PolymarketEvent(
                id="event-1",
                slug="event-1",
                title="Event 1",
            )
        )
        session.commit()

        session.add(
            PolymarketMarket(
                id="market-1",
                event_id="event-1",
                question="Who wins?",
                condition_id="condition-1",
            )
        )
        session.commit()

        session.add(
            PolymarketMarket(
                id="market-2",
                event_id="missing-event",
                question="Broken market",
                condition_id="condition-2",
            )
        )
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
        else:
            raise AssertionError("expected foreign key violation for missing event")

        stored_market_ids = session.scalars(select(PolymarketMarket.id)).all()
        assert stored_market_ids == ["market-1"]
