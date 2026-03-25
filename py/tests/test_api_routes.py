from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    PolymarketEvent,
    PolymarketMarket,
)
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def build_test_client(tmp_path: Path) -> TestClient:
    database_path = tmp_path / "api-test.sqlite"
    engine = create_engine(f"sqlite+pysqlite:///{database_path}", future=True)
    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine, expire_on_commit=False)

    with Session(engine) as session:
        session.add_all(
            [
                F1Meeting(
                    id="meeting-2026",
                    meeting_key=2026,
                    season=2026,
                    meeting_name="Australian Grand Prix",
                ),
                F1Meeting(
                    id="meeting-2025",
                    meeting_key=2025,
                    season=2025,
                    meeting_name="Bahrain Grand Prix",
                ),
                F1Session(
                    id="session-q",
                    session_key=2001,
                    meeting_id="meeting-2026",
                    session_name="Qualifying",
                    session_code="Q",
                    is_practice=False,
                ),
                F1Session(
                    id="session-fp1",
                    session_key=2002,
                    meeting_id="meeting-2026",
                    session_name="Practice 1",
                    session_code="FP1",
                    is_practice=True,
                ),
                F1Session(
                    id="session-old",
                    session_key=1001,
                    meeting_id="meeting-2025",
                    session_name="Race",
                    session_code="R",
                    is_practice=False,
                ),
                PolymarketEvent(
                    id="event-1",
                    slug="event-1",
                    title="Event 1",
                    active=True,
                    closed=False,
                ),
                PolymarketMarket(
                    id="market-good",
                    event_id="event-1",
                    question="Who gets pole?",
                    slug="pole",
                    taxonomy="driver_pole_position",
                    condition_id="condition-good",
                    active=True,
                    closed=False,
                    end_at_utc=datetime(2026, 3, 14, tzinfo=timezone.utc),
                ),
                PolymarketMarket(
                    id="market-unknown",
                    event_id="event-1",
                    question="Unknown market",
                    slug="unknown",
                    taxonomy="legacy_taxonomy",
                    condition_id="condition-unknown",
                    active=True,
                    closed=False,
                    end_at_utc=datetime(2026, 3, 13, tzinfo=timezone.utc),
                ),
                EntityMappingF1ToPolymarket(
                    id="mapping-high",
                    f1_session_id="session-q",
                    polymarket_market_id="market-good",
                    mapping_type="driver_pole_position",
                    confidence=0.91,
                ),
                EntityMappingF1ToPolymarket(
                    id="mapping-low",
                    f1_session_id="session-fp1",
                    polymarket_market_id="market-unknown",
                    mapping_type="other",
                    confidence=0.4,
                ),
            ]
        )
        session.commit()

    def override_db_session() -> Generator[Session, None, None]:
        session = session_maker()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db_session] = override_db_session
    return TestClient(app)


def test_sessions_endpoint_supports_filters(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/f1/sessions",
            params={"season": 2026, "session_code": "Q", "limit": 5},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload] == ["session-q"]


def test_markets_endpoint_coerces_taxonomy_and_filters(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/polymarket/markets",
            params={"event_id": "event-1", "active": True, "limit": 10},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert {row["taxonomy"] for row in payload} == {
        "driver_pole_position",
        "other",
    }


def test_mappings_endpoint_filters_by_confidence_and_session(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/mappings",
            params={"f1_session_id": "session-q", "min_confidence": 0.8, "limit": 10},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload] == ["mapping-high"]
