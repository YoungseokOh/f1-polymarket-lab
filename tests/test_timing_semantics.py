from __future__ import annotations

from pathlib import Path

import pytest
from f1_polymarket_lab.common import parse_gap_value, parse_result_time_value
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import F1Interval, F1Meeting, F1Session, F1SessionResult
from f1_polymarket_worker.pipeline import PipelineContext, hydrate_f1_session
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def build_context(tmp_path: Path) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=True, settings=settings)
    return session, context


def test_parse_gap_value_distinguishes_leader_laps_and_zero_gap() -> None:
    laps_gap = parse_gap_value("+2 LAPS", position=15)
    assert laps_gap.status == "laps_behind"
    assert laps_gap.laps_behind == 2
    assert laps_gap.seconds is None

    leader_gap = parse_gap_value(None, position=1)
    assert leader_gap.status == "leader"

    tied_gap = parse_gap_value("0.0", position=2)
    assert tied_gap.status == "time"
    assert tied_gap.seconds == pytest.approx(0.0)


def test_parse_result_time_value_infers_session_kind() -> None:
    qualifying = parse_result_time_value([90.111, 89.555, 89.234], session_code="Q")
    assert qualifying.kind == "segment_array"
    assert qualifying.segments_json == [90.111, 89.555, 89.234]

    race = parse_result_time_value(5034.543, session_code="R")
    assert race.kind == "total_time"
    assert race.seconds == pytest.approx(5034.543)

    practice = parse_result_time_value(88.321, session_code="FP2")
    assert practice.kind == "best_lap"
    assert practice.seconds == pytest.approx(88.321)


def test_hydrate_f1_session_stores_timing_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:1207",
        source="openf1",
        meeting_key=1207,
        season=2023,
        round_number=4,
        meeting_name="Azerbaijan Grand Prix",
        raw_payload={"meeting_key": 1207},
    )
    race_session = F1Session(
        id="session:9069",
        source="openf1",
        session_key=9069,
        meeting_id=meeting.id,
        session_name="Race",
        session_type="race",
        session_code="R",
        status="complete",
        session_order=51,
        is_practice=False,
        raw_payload={"meeting_key": 1207},
    )
    session.add(meeting)
    session.add(race_session)
    session.commit()

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_drivers",
        lambda self, session_key: [
            {
                "driver_number": 1,
                "broadcast_name": "M VERSTAPPEN",
                "full_name": "Max Verstappen",
                "first_name": "Max",
                "last_name": "Verstappen",
                "name_acronym": "VER",
                "team_name": "Red Bull Racing",
                "team_colour": "3671C6",
                "country_code": "NLD",
                "headshot_url": None,
            }
        ],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_session_results",
        lambda self, session_key: [
            {
                "driver_number": 1,
                "position": 2,
                "duration": 5043.111,
                "gap_to_leader": "+1 LAP",
                "number_of_laps": 56,
                "dnf": True,
                "dns": False,
                "dsq": False,
            }
        ],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_laps",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_stints",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_race_control",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_weather",
        lambda self, meeting_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_positions",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_intervals",
        lambda self, session_key: [
            {
                "driver_number": 1,
                "date": "2023-04-29T13:38:41.364000+00:00",
                "gap_to_leader": "+1 LAP",
                "interval": "+1 LAP",
            }
        ],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_pit",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_team_radio",
        lambda self, session_key: [],
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1.OpenF1Connector.fetch_starting_grid",
        lambda self, session_key: [],
    )

    try:
        result = hydrate_f1_session(context, session_key=9069, include_extended=True)
        session.commit()

        assert result["status"] == "completed"
        stored_result = session.get(F1SessionResult, "9069:1")
        assert stored_result is not None
        assert stored_result.result_time_kind == "total_time"
        assert stored_result.result_time_seconds == pytest.approx(5043.111)
        assert stored_result.gap_to_leader_status == "laps_behind"
        assert stored_result.gap_to_leader_laps_behind == 1
        assert stored_result.dnf is True
        assert stored_result.dns is False
        assert stored_result.dsq is False

        stored_interval = session.get(
            F1Interval,
            "9069:1:2023-04-29T13:38:41.364000+00:00",
        )
        assert stored_interval is not None
        assert stored_interval.gap_to_leader_status == "laps_behind"
        assert stored_interval.gap_to_leader_laps_behind == 1
        assert stored_interval.interval_status == "laps_behind"
        assert stored_interval.interval_laps_behind == 1
    finally:
        session.close()
