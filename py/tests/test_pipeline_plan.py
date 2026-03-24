from __future__ import annotations

from pathlib import Path

import pytest
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import F1Meeting, F1Session, IngestionJobRun
from f1_polymarket_worker.pipeline import (
    MODERN_WEEKEND_SESSION_CODES,
    PipelineContext,
    normalize_float,
    sync_f1_calendar,
    sync_polymarket_catalog,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def _has_fastf1() -> bool:
    try:
        import fastf1  # noqa: F401
        return True
    except ModuleNotFoundError:
        return False


def build_context(tmp_path: Path) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=False, settings=settings)
    return session, context


def test_sync_f1_calendar_plan_mode_creates_planned_run(tmp_path: Path) -> None:
    session, context = build_context(tmp_path)
    try:
        result = sync_f1_calendar(context, season=2024)
        session.commit()

        assert result["status"] == "planned"
        run = session.scalar(
            select(IngestionJobRun).where(IngestionJobRun.job_name == "sync-f1-calendar")
        )
        assert run is not None
        assert run.status == "planned"
        assert run.execute_mode == "plan"
    finally:
        session.close()


def test_sync_polymarket_catalog_plan_mode_creates_planned_run(tmp_path: Path) -> None:
    session, context = build_context(tmp_path)
    try:
        result = sync_polymarket_catalog(context, max_pages=1)
        session.commit()

        assert result["status"] == "planned"
        run = session.scalar(
            select(IngestionJobRun).where(IngestionJobRun.job_name == "sync-polymarket-catalog")
        )
        assert run is not None
        assert run.status == "planned"
        assert run.execute_mode == "plan"
    finally:
        session.close()


def test_normalize_float_ignores_non_numeric_interval_values() -> None:
    assert normalize_float("+1 LAP") is None
    assert normalize_float("DNF") is None
    assert normalize_float("1.25") == 1.25
    assert normalize_float(["", None, "3.5", "+1 LAP", "2.1"]) == 2.1


@pytest.mark.skipif(
    not _has_fastf1(),
    reason="fastf1 not installed",
)
def test_sync_f1_calendar_execute_keeps_only_weekend_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=True, settings=settings)

    sessions = [
        {
            "meeting_key": 1,
            "session_key": 1001,
            "session_name": "Day 1",
            "session_type": "Practice",
            "date_start": "2026-02-26T01:00:00Z",
            "date_end": "2026-02-26T09:00:00Z",
            "country_name": "Bahrain",
            "location": "Sakhir",
            "circuit_short_name": "Bahrain",
            "circuit_key": 10,
        },
        {
            "meeting_key": 2,
            "session_key": 2001,
            "session_name": "Practice 1",
            "session_type": "Practice",
            "date_start": "2026-03-13T03:00:00Z",
            "date_end": "2026-03-13T04:00:00Z",
            "country_name": "China",
            "location": "Shanghai",
            "circuit_short_name": "Shanghai",
            "circuit_key": 20,
        },
        {
            "meeting_key": 2,
            "session_key": 2002,
            "session_name": "Sprint Qualifying",
            "session_type": "Qualifying",
            "date_start": "2026-03-13T07:00:00Z",
            "date_end": "2026-03-13T08:00:00Z",
            "country_name": "China",
            "location": "Shanghai",
            "circuit_short_name": "Shanghai",
            "circuit_key": 20,
        },
        {
            "meeting_key": 2,
            "session_key": 2003,
            "session_name": "Race",
            "session_type": "Race",
            "date_start": "2026-03-15T07:00:00Z",
            "date_end": "2026-03-15T09:00:00Z",
            "country_name": "China",
            "location": "Shanghai",
            "circuit_short_name": "Shanghai",
            "circuit_key": 20,
        },
    ]
    schedule = [
        {
            "Location": "Shanghai",
            "RoundNumber": 2,
            "EventName": "Chinese Grand Prix",
            "OfficialEventName": "Formula 1 Chinese Grand Prix 2026",
        }
    ]
    session.add_all(
        [
            F1Meeting(
                id="meeting:stale",
                source="openf1",
                meeting_key=99,
                season=2026,
                meeting_name="Preseason Testing",
            ),
            F1Session(
                id="session:stale",
                source="openf1",
                session_key=9999,
                meeting_id="meeting:stale",
                session_name="Day 1",
                session_code=None,
                is_practice=False,
            ),
        ]
    )
    session.commit()

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_sessions",
        lambda self, season: sessions,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.fastf1_adapter.FastF1ScheduleConnector.fetch_event_schedule",
        lambda self, season: schedule,
    )

    try:
        result = sync_f1_calendar(context, season=2026)
        session.commit()

        assert result["status"] == "completed"
        stored_sessions = session.scalars(
            select(F1Session).order_by(F1Session.session_key.asc())
        ).all()
        assert [row.session_key for row in stored_sessions] == [2001, 2002, 2003]
        assert {row.session_code for row in stored_sessions} == {"FP1", "SQ", "R"}
        assert {row.session_code for row in stored_sessions}.issubset(MODERN_WEEKEND_SESSION_CODES)
        assert session.get(F1Session, "session:stale") is None
        assert session.get(F1Meeting, "meeting:stale") is None
    finally:
        session.close()
