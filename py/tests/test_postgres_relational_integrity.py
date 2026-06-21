from __future__ import annotations

import pytest
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Lap,
    F1Meeting,
    F1Session,
    MappingCandidate,
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


def test_mapping_ids_accept_composite_keys_on_postgres(postgres_engine: Engine) -> None:
    # reconcile_mappings builds ids of the form "{market_id}:{session_id}" where each
    # component is String(64). These exceed the original UUID-length (36) limit and used
    # to fail with StringDataRightTruncation, rolling back the whole reconcile batch.
    long_market_id = "polymarket-market-" + "9" * 46  # 64 chars
    long_session_id = "session:historical:2026:10:" + "race" * 9 + "x"  # 64 chars
    composite_id = f"{long_market_id}:{long_session_id}"  # 129 chars
    assert len(composite_id) > 36

    with Session(postgres_engine) as session:
        session.add(
            MappingCandidate(
                id=composite_id,
                candidate_type="driver_podium",
                confidence=0.89,
                matched_by="session_code_time_window",
                status="candidate",
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=composite_id,
                mapping_type="driver_podium",
                confidence=0.89,
                matched_by="session_code_time_window",
            )
        )
        session.commit()

        assert session.get(MappingCandidate, composite_id) is not None
        assert session.get(EntityMappingF1ToPolymarket, composite_id) is not None
