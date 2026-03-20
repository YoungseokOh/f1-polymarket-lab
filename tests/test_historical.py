from __future__ import annotations

from pathlib import Path

import pytest
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    F1Lap,
    F1Meeting,
    F1Pit,
    F1Session,
    F1SessionResult,
    F1StartingGrid,
)
from f1_polymarket_worker.historical import (
    bootstrap_f1db_history,
    historical_meeting_id,
    historical_meeting_key,
    historical_session_id,
    historical_session_key,
    sync_jolpica_history,
)
from f1_polymarket_worker.orchestration import backfill_f1_history_all
from f1_polymarket_worker.pipeline import PipelineContext
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def build_context(tmp_path: Path, *, execute: bool = True) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=execute, settings=settings)
    return session, context


def test_bootstrap_f1db_history_normalizes_pre_2023_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)

    def fake_fetch_drivers(self: object) -> list[dict[str, object]]:
        return [
            {
                "id": "max-verstappen",
                "full_name": "Max Verstappen",
                "first_name": "Max",
                "last_name": "Verstappen",
                "abbreviation": "VER",
                "permanent_number": 33,
                "nationality_alpha2_code": "NL",
            }
        ]

    def fake_fetch_constructors(self: object) -> list[dict[str, object]]:
        return [{"id": "red-bull", "full_name": "Red Bull Racing"}]

    def fake_fetch_races(self: object, season: int) -> list[dict[str, object]]:
        assert season == 2022
        return [
            {
                "id": 1058,
                "year": 2022,
                "round": 1,
                "date": "2022-03-20",
                "time": "15:00:00",
                "qualifying_date": "2022-03-19",
                "qualifying_time": "15:00:00",
                "free_practice_2_date": "2022-03-18",
                "free_practice_2_time": "15:00:00",
                "official_name": "Formula 1 Bahrain Grand Prix 2022",
                "grand_prix_name": "Bahrain",
                "grand_prix_full_name": "Bahrain Grand Prix",
                "circuit_name": "Bahrain International Circuit",
                "circuit_full_name": "Bahrain International Circuit",
                "circuit_place_name": "Sakhir",
                "country_name": "Bahrain",
            }
        ]

    def fake_fetch_race_data(self: object, season: int) -> list[dict[str, object]]:
        assert season == 2022
        return [
            {
                "race_id": 1058,
                "type": "FREE_PRACTICE_2_RESULT",
                "position_number": 1,
                "driver_id": "max-verstappen",
                "practice_time_millis": 90123,
                "practice_gap": "+0.000",
                "practice_laps": 26,
            },
            {
                "race_id": 1058,
                "type": "QUALIFYING_RESULT",
                "position_number": 1,
                "driver_id": "max-verstappen",
                "qualifying_time_millis": 89234,
                "qualifying_gap": "+0.000",
                "qualifying_laps": 12,
            },
            {
                "race_id": 1058,
                "type": "STARTING_GRID_POSITION",
                "position_number": 1,
                "driver_id": "max-verstappen",
            },
            {
                "race_id": 1058,
                "type": "RACE_RESULT",
                "position_number": 1,
                "driver_id": "max-verstappen",
                "race_time": "1:23:43.123",
                "race_time_millis": 5023123,
                "race_gap": "+0.000",
                "race_laps": 57,
            },
            {
                "race_id": 1058,
                "type": "FASTEST_LAP",
                "position_number": 1,
                "driver_id": "max-verstappen",
                "fastest_lap_time_millis": 88321,
            },
            {
                "race_id": 1058,
                "type": "PIT_STOP",
                "driver_id": "max-verstappen",
                "pit_stop_stop": 1,
                "pit_stop_lap": 24,
                "pit_stop_time_millis": 23567,
            },
        ]

    monkeypatch.setattr(
        "f1_polymarket_worker.historical.F1DBConnector.fetch_drivers",
        fake_fetch_drivers,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.F1DBConnector.fetch_constructors",
        fake_fetch_constructors,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.F1DBConnector.fetch_races",
        fake_fetch_races,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.F1DBConnector.fetch_race_data",
        fake_fetch_race_data,
    )

    try:
        result = bootstrap_f1db_history(context, season_start=2022, season_end=2022)
        session.commit()

        assert result["status"] == "completed"
        meeting = session.get(F1Meeting, historical_meeting_id(2022, 1))
        assert meeting is not None
        assert meeting.meeting_key == historical_meeting_key(2022, 1)
        assert meeting.source == "f1db"

        fp2_session = session.get(F1Session, historical_session_id(2022, 1, "FP2"))
        qualifying_session = session.get(F1Session, historical_session_id(2022, 1, "Q"))
        race_session = session.get(F1Session, historical_session_id(2022, 1, "R"))
        assert fp2_session is not None
        assert qualifying_session is not None
        assert race_session is not None
        assert race_session.session_key == historical_session_key(2022, 1, "R")

        race_result = session.get(
            F1SessionResult,
            f"{historical_session_id(2022, 1, 'R')}:driver:max-verstappen",
        )
        assert race_result is not None
        assert race_result.result_time_kind == "total_time"
        assert race_result.result_time_seconds == pytest.approx(5023.123)
        assert race_result.gap_to_leader_status == "leader"
        assert race_result.number_of_laps == 57

        grid_row = session.get(
            F1StartingGrid,
            f"{historical_session_id(2022, 1, 'R')}:driver:max-verstappen",
        )
        pit_row = session.get(
            F1Pit,
            f"{historical_session_id(2022, 1, 'R')}:driver:max-verstappen:1",
        )
        assert grid_row is not None
        assert pit_row is not None
        assert pit_row.lap_number == 24
    finally:
        session.close()


def test_sync_jolpica_history_repairs_existing_historical_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    meeting_id = historical_meeting_id(2022, 1)
    race_session_id = historical_session_id(2022, 1, "R")
    session.add(
        F1Meeting(
            id=meeting_id,
            source="f1db",
            meeting_key=historical_meeting_key(2022, 1),
            season=2022,
            round_number=1,
            meeting_name="Bahrain Grand Prix",
        )
    )
    session.add(
        F1Session(
            id=race_session_id,
            source="f1db",
            session_key=historical_session_key(2022, 1, "R"),
            meeting_id=meeting_id,
            session_name="Race",
            session_type="race",
            session_code="R",
            status="complete",
            session_order=51,
            is_practice=False,
        )
    )
    session.add(
        F1SessionResult(
            id=f"{race_session_id}:driver:max-verstappen",
            session_id=race_session_id,
            driver_id="driver:max-verstappen",
            position=1,
            result_time_seconds=None,
            result_time_kind=None,
            result_time_display=None,
            result_time_segments_json=None,
            gap_to_leader_display="+0.000",
            gap_to_leader_seconds=None,
            gap_to_leader_laps_behind=None,
            gap_to_leader_status="leader",
            gap_to_leader_segments_json=None,
            dnf=None,
            dns=None,
            dsq=None,
            number_of_laps=57,
            raw_payload={"source": "f1db"},
        )
    )
    session.commit()

    race_payload: dict[str, object] = {
        "season": "2022",
        "round": "1",
        "raceName": "Bahrain Grand Prix",
        "Circuit": {
            "circuitName": "Bahrain International Circuit",
            "Location": {"locality": "Sakhir", "country": "Bahrain"},
        },
        "date": "2022-03-20",
        "time": "15:00:00Z",
        "FirstPractice": {"date": "2022-03-18", "time": "12:00:00Z"},
        "SecondPractice": {"date": "2022-03-18", "time": "15:00:00Z"},
        "ThirdPractice": {"date": "2022-03-19", "time": "12:00:00Z"},
        "Qualifying": {"date": "2022-03-19", "time": "15:00:00Z"},
    }

    def fake_fetch_races(self: object, season: int) -> list[dict[str, object]]:
        assert season == 2022
        return [race_payload]

    def fake_fetch_results(
        self: object,
        season: int,
        round_number: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        assert season == 2022
        assert round_number == 1
        assert limit == 100
        return [
            {
                **race_payload,
                "Results": [
                    {
                        "position": "1",
                        "laps": "57",
                        "status": "Finished",
                        "Driver": {
                            "driverId": "max_verstappen",
                            "givenName": "Max",
                            "familyName": "Verstappen",
                            "code": "VER",
                            "permanentNumber": "33",
                        },
                        "Constructor": {"constructorId": "red_bull", "name": "Red Bull"},
                        "FastestLap": {"Time": {"time": "1:32.345"}},
                        "Time": {"time": "1:24:03.111"},
                    }
                ],
            }
        ]

    def fake_fetch_qualifying(
        self: object,
        season: int,
        round_number: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        return [
            {
                **race_payload,
                "QualifyingResults": [
                    {
                        "position": "1",
                        "Driver": {
                            "driverId": "max_verstappen",
                            "givenName": "Max",
                            "familyName": "Verstappen",
                            "code": "VER",
                            "permanentNumber": "33",
                        },
                        "Constructor": {"constructorId": "red_bull", "name": "Red Bull"},
                        "Q1": "1:31.111",
                        "Q2": "1:30.555",
                        "Q3": "1:29.234",
                    }
                ],
            }
        ]

    def fake_fetch_sprint(
        self: object,
        season: int,
        round_number: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        return []

    def fake_fetch_pitstops(
        self: object,
        season: int,
        round_number: int,
        *,
        limit: int = 2000,
    ) -> list[dict[str, object]]:
        return [
            {
                **race_payload,
                "PitStops": [
                    {
                        "driverId": "max_verstappen",
                        "stop": "1",
                        "lap": "24",
                        "duration": "23.567",
                    }
                ],
            }
        ]

    def fake_fetch_laps(
        self: object,
        season: int,
        round_number: int,
        *,
        limit: int = 5000,
    ) -> list[dict[str, object]]:
        return [
            {
                **race_payload,
                "Laps": [
                    {
                        "number": "1",
                        "Timings": [
                            {
                                "driverId": "max_verstappen",
                                "position": "1",
                                "time": "1:39.876",
                            }
                        ],
                    }
                ],
            }
        ]

    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_races",
        fake_fetch_races,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_results",
        fake_fetch_results,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_qualifying",
        fake_fetch_qualifying,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_sprint",
        fake_fetch_sprint,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_pitstops",
        fake_fetch_pitstops,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.historical.JolpicaConnector.fetch_laps",
        fake_fetch_laps,
    )

    try:
        result = sync_jolpica_history(
            context,
            season_start=2022,
            season_end=2022,
            resources=("races", "results", "qualifying", "pitstops", "laps"),
        )
        session.commit()

        assert result["status"] == "completed"

        race_session = session.get(F1Session, race_session_id)
        qualifying_session = session.get(F1Session, historical_session_id(2022, 1, "Q"))
        repaired_result = session.get(
            F1SessionResult,
            f"{race_session_id}:driver:max-verstappen",
        )
        qualifying_result = session.get(
            F1SessionResult,
            f"{historical_session_id(2022, 1, 'Q')}:driver:max-verstappen",
        )
        pit_row = session.get(
            F1Pit,
            f"{race_session_id}:driver:max-verstappen:1",
        )
        lap_row = session.get(
            F1Lap,
            f"{race_session_id}:driver:max-verstappen:1",
        )

        assert race_session is not None
        assert race_session.source == "f1db"
        assert qualifying_session is not None
        assert qualifying_session.source == "jolpica"
        assert repaired_result is not None
        assert repaired_result.result_time_kind == "total_time"
        assert repaired_result.result_time_seconds == pytest.approx(5043.111)
        assert repaired_result.gap_to_leader_status == "leader"
        assert qualifying_result is not None
        assert qualifying_result.result_time_kind == "segment_array"
        assert qualifying_result.result_time_segments_json == ["1:31.111", "1:30.555", "1:29.234"]
        assert pit_row is not None
        assert pit_row.pit_duration_seconds == pytest.approx(23.567)
        assert lap_row is not None
        assert lap_row.lap_duration_seconds == pytest.approx(99.876)
    finally:
        session.close()


def test_backfill_f1_history_all_splits_historical_and_openf1_ranges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    calls: list[tuple[str, int, int]] = []

    def fake_bootstrap(
        ctx: PipelineContext,
        *,
        season_start: int,
        season_end: int,
        artifact: str = "sqlite",
    ) -> dict[str, object]:
        assert ctx is context
        assert artifact == "sqlite"
        calls.append(("f1db", season_start, season_end))
        return {"status": "completed", "session_results": 10}

    def fake_jolpica(
        ctx: PipelineContext,
        *,
        season_start: int,
        season_end: int,
        resources: tuple[str, ...],
    ) -> dict[str, object]:
        assert ctx is context
        assert "results" in resources
        calls.append(("jolpica", season_start, season_end))
        return {"status": "completed", "session_results": 5, "lap_rows": 2, "pit_rows": 1}

    def fake_openf1(
        ctx: PipelineContext,
        *,
        season_start: int,
        season_end: int,
        include_extended: bool,
        heavy_mode: str,
    ) -> dict[str, object]:
        assert ctx is context
        assert include_extended is True
        assert heavy_mode == "weekend"
        calls.append(("openf1", season_start, season_end))
        return {"status": "completed", "sessions_hydrated": 3}

    monkeypatch.setattr("f1_polymarket_worker.orchestration.bootstrap_f1db_history", fake_bootstrap)
    monkeypatch.setattr("f1_polymarket_worker.orchestration.sync_jolpica_history", fake_jolpica)
    monkeypatch.setattr("f1_polymarket_worker.orchestration.backfill_f1_history", fake_openf1)

    try:
        result = backfill_f1_history_all(context, season_start=2022, season_end=2024)
        session.commit()

        assert result["status"] == "completed"
        assert calls == [
            ("f1db", 2022, 2022),
            ("jolpica", 2022, 2022),
            ("openf1", 2023, 2024),
        ]
        assert result["records_written"] == 21
    finally:
        session.close()
