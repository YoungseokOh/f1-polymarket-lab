from __future__ import annotations

import pytest
from f1_polymarket_lab.storage.models import (
    F1Lap,
    F1Meeting,
    F1Session,
    PolymarketEvent,
    PolymarketMarket,
)
from sqlalchemy import delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres_integration


def test_f1_cascade_delete_removes_session_children_on_postgres(postgres_engine: Engine) -> None:
    with Session(postgres_engine) as session:
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


def test_polymarket_market_requires_existing_event_on_postgres(postgres_engine: Engine) -> None:
    with Session(postgres_engine) as session:
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
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        stored_market_ids = session.scalars(select(PolymarketMarket.id)).all()
        assert stored_market_ids == ["market-1"]
