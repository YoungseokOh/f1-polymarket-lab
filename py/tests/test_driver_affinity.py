from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.features.driver_profile import compute_driver_sector_profiles
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    F1Driver,
    F1Lap,
    F1Meeting,
    F1Session,
    F1SessionResult,
    F1Team,
    IngestionJobRun,
)
from f1_polymarket_worker.driver_affinity import (
    build_driver_affinity_report,
    get_driver_affinity_refresh_status,
    get_driver_affinity_report,
    refresh_driver_affinity,
)
from f1_polymarket_worker.pipeline import PipelineContext
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_context(tmp_path: Path) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    return session, PipelineContext(db=session, execute=True, settings=settings)


def seed_affinity_fixture(session: Session) -> None:
    meeting_2025 = F1Meeting(
        id="meeting-2025-japan",
        meeting_key=1201,
        season=2025,
        meeting_name="Japanese Grand Prix",
        circuit_short_name="Suzuka",
        start_date_utc=datetime(2025, 4, 4, 2, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2025, 4, 6, 6, 0, tzinfo=timezone.utc),
    )
    meeting_2026 = F1Meeting(
        id="meeting-2026-japan",
        meeting_key=1281,
        season=2026,
        meeting_name="Japanese Grand Prix",
        circuit_short_name="Suzuka",
        start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
    )
    session.add_all([meeting_2025, meeting_2026])

    session.add_all(
        [
            F1Session(
                id="session-2025-q",
                meeting_id=meeting_2025.id,
                session_key=9001,
                session_name="Qualifying",
                session_code="Q",
                session_type="Qualifying",
                date_start_utc=datetime(2025, 4, 5, 6, 0, tzinfo=timezone.utc),
                date_end_utc=datetime(2025, 4, 5, 7, 0, tzinfo=timezone.utc),
                is_practice=False,
            ),
            F1Session(
                id="session-2026-fp2",
                meeting_id=meeting_2026.id,
                session_key=11247,
                session_name="Practice 2",
                session_code="FP2",
                session_type="Practice",
                date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
                date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
                is_practice=True,
            ),
        ]
    )

    session.add_all(
        [
            F1Team(id="team:mclaren", team_name="McLaren"),
            F1Team(id="team:ferrari", team_name="Ferrari"),
            F1Team(id="team:mercedes", team_name="Mercedes"),
        ]
    )

    session.add_all(
        [
            F1Driver(
                id="driver:4",
                driver_number=4,
                full_name="Lando NORRIS",
                first_name="Lando",
                last_name="Norris",
                broadcast_name="L NORRIS",
                team_id="team:mclaren",
            ),
            F1Driver(
                id="driver:1",
                driver_number=1,
                full_name="Lando NORRIS",
                first_name="Lando",
                last_name="Norris",
                broadcast_name="L NORRIS",
                team_id="team:mclaren",
            ),
            F1Driver(
                id="driver:16",
                driver_number=16,
                full_name="Charles LECLERC",
                first_name="Charles",
                last_name="Leclerc",
                broadcast_name="C LECLERC",
                team_id="team:ferrari",
            ),
            F1Driver(
                id="driver:63",
                driver_number=63,
                full_name="George RUSSELL",
                first_name="George",
                last_name="Russell",
                broadcast_name="G RUSSELL",
                team_id="team:mercedes",
            ),
        ]
    )

    lap_rows = [
        ("session-2025-q", "driver:4", 1, 10.0, 20.0, 15.0),
        ("session-2025-q", "driver:4", 2, 10.1, 20.1, 15.1),
        ("session-2025-q", "driver:16", 1, 11.0, 21.0, 16.0),
        ("session-2025-q", "driver:16", 2, 11.1, 21.1, 16.1),
        ("session-2025-q", "driver:63", 1, 12.0, 22.0, 17.0),
        ("session-2025-q", "driver:63", 2, 12.1, 22.1, 17.1),
        ("session-2026-fp2", "driver:1", 1, 10.8, 20.8, 15.8),
        ("session-2026-fp2", "driver:1", 2, 10.9, 20.9, 15.9),
        ("session-2026-fp2", "driver:16", 1, 11.0, 21.0, 16.0),
        ("session-2026-fp2", "driver:16", 2, 11.1, 21.1, 16.1),
        ("session-2026-fp2", "driver:63", 1, 9.0, 19.0, 14.0),
        ("session-2026-fp2", "driver:63", 2, 9.1, 19.1, 14.1),
    ]
    session.add_all(
        [
            F1Lap(
                id=f"{session_id}:{driver_id}:{lap_number}",
                session_id=session_id,
                driver_id=driver_id,
                lap_number=lap_number,
                sector_1_seconds=s1,
                sector_2_seconds=s2,
                sector_3_seconds=s3,
            )
            for session_id, driver_id, lap_number, s1, s2, s3 in lap_rows
        ]
    )
    session.add_all(
        [
            F1SessionResult(
                id=f"{session_id}:{driver_id}",
                session_id=session_id,
                driver_id=driver_id,
                position=index,
            )
            for session_id, driver_id, index in [
                ("session-2025-q", "driver:4", 1),
                ("session-2025-q", "driver:16", 2),
                ("session-2025-q", "driver:63", 3),
                ("session-2026-fp2", "driver:1", 2),
                ("session-2026-fp2", "driver:16", 3),
                ("session-2026-fp2", "driver:63", 1),
            ]
        ]
    )
    session.commit()


def test_driver_affinity_respects_as_of_cutoff_and_merges_driver_identity(
    tmp_path: Path,
) -> None:
    session, _ = build_context(tmp_path)
    try:
        seed_affinity_fixture(session)

        before_fp2 = compute_driver_sector_profiles(
            session,
            circuit_key=39,
            min_season=2024,
            as_of_utc=datetime(2026, 3, 27, 6, 59, tzinfo=timezone.utc),
        )
        after_fp2 = compute_driver_sector_profiles(
            session,
            circuit_key=39,
            min_season=2024,
            as_of_utc=datetime(2026, 3, 27, 7, 1, tzinfo=timezone.utc),
        )

        assert set(before_fp2) == {
            "lando norris",
            "charles leclerc",
            "george russell",
        }
        assert before_fp2["lando norris"]["n_sessions"] == 1
        assert after_fp2["lando norris"]["n_sessions"] == 2
        assert (
            after_fp2["george russell"]["s1_strength"] > before_fp2["george russell"]["s1_strength"]
        )
        assert after_fp2["lando norris"]["s1_strength"] < before_fp2["lando norris"]["s1_strength"]
    finally:
        session.close()


def test_driver_affinity_report_prefers_current_season_display_identity(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        seed_affinity_fixture(session)
        report = build_driver_affinity_report(
            context,
            season=2026,
            meeting_key=1281,
            as_of_utc=datetime(2026, 3, 27, 7, 1, tzinfo=timezone.utc),
        )

        lando = next(
            entry for entry in report["entries"] if entry["canonical_driver_key"] == "lando norris"
        )
        assert lando["display_driver_id"] == "driver:1"
        assert lando["display_name"] == "Lando NORRIS"
        assert lando["team_name"] == "McLaren"
        assert report["latest_ended_relevant_session_code"] == "FP2"
        assert report["source_max_session_end_utc"] == "2026-03-27T07:00:00+00:00"
        assert report["season_weights"] == {"2024": 0.4, "2025": 0.65, "2026": 1.0}
    finally:
        session.close()


def test_driver_affinity_report_defaults_to_current_meeting_when_key_is_omitted(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        seed_affinity_fixture(session)
        report = build_driver_affinity_report(
            context,
            season=2026,
            as_of_utc=datetime(2026, 3, 27, 7, 1, tzinfo=timezone.utc),
        )

        assert report["meeting_key"] == 1281
        assert report["meeting"]["meeting_key"] == 1281
        assert report["entry_count"] == len(report["entries"])
        assert {segment["key"] for segment in report["segments"]} == {
            "current_gp",
            "season_to_date",
            "all_history",
        }
    finally:
        session.close()


def test_driver_affinity_segments_limit_entries_to_current_meeting_drivers(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        seed_affinity_fixture(session)
        prior_meeting = F1Meeting(
            id="meeting-2026-australia",
            meeting_key=1279,
            season=2026,
            meeting_name="Australian Grand Prix",
            circuit_short_name="Melbourne",
            start_date_utc=datetime(2026, 3, 6, 0, 0, tzinfo=timezone.utc),
            end_date_utc=datetime(2026, 3, 8, 6, 0, tzinfo=timezone.utc),
        )
        prior_session = F1Session(
            id="session-2026-australia-fp1",
            meeting_id=prior_meeting.id,
            session_key=11227,
            session_name="Practice 1",
            session_code="FP1",
            session_type="Practice",
            date_start_utc=datetime(2026, 3, 6, 1, 0, tzinfo=timezone.utc),
            date_end_utc=datetime(2026, 3, 6, 2, 0, tzinfo=timezone.utc),
            is_practice=True,
        )
        session.add_all(
            [
                prior_meeting,
                prior_session,
                F1Driver(
                    id="driver:99",
                    driver_number=99,
                    full_name="Reserve DRIVER",
                    broadcast_name="R DRIVER",
                    team_id="team:mclaren",
                ),
            ]
        )
        for driver_id, base in [("driver:1", 10.0), ("driver:16", 11.0), ("driver:99", 9.0)]:
            for lap_number in [1, 2]:
                session.add(
                    F1Lap(
                        id=f"{prior_session.id}:{driver_id}:{lap_number}",
                        session_id=prior_session.id,
                        driver_id=driver_id,
                        lap_number=lap_number,
                        sector_1_seconds=base,
                        sector_2_seconds=base + 10,
                        sector_3_seconds=base + 5,
                    )
                )
            session.add(
                F1SessionResult(
                    id=f"{prior_session.id}:{driver_id}",
                    session_id=prior_session.id,
                    driver_id=driver_id,
                )
            )
        session.commit()

        report = build_driver_affinity_report(
            context,
            season=2026,
            meeting_key=1281,
            as_of_utc=datetime(2026, 3, 27, 7, 1, tzinfo=timezone.utc),
        )

        season_segment = next(
            segment for segment in report["segments"] if segment["key"] == "season_to_date"
        )
        assert "reserve driver" not in {
            entry["canonical_driver_key"] for entry in season_segment["entries"]
        }
        assert season_segment["entry_count"] == 3
    finally:
        session.close()


def test_driver_affinity_report_falls_back_to_identity_ranking_without_report(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        seed_affinity_fixture(session)
        report = get_driver_affinity_report(context, season=2026, meeting_key=1281)

        assert report["season"] == 2026
        assert report["meeting_key"] == 1281
        assert report["entry_count"] == len(report["entries"])
        assert report["entry_count"] == 3
        assert report["entries"][0]["display_name"] == "George RUSSELL"
        assert report["entries"][0]["affinity_score"] == 0.0
        assert report["stale_reason"] is not None
        assert "Missing hydrated data through" in report["stale_reason"]
    finally:
        session.close()


def test_refresh_driver_affinity_blocks_without_credentials_when_fp2_is_missing(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        meeting = F1Meeting(
            id="meeting-2026-japan",
            meeting_key=1281,
            season=2026,
            meeting_name="Japanese Grand Prix",
            circuit_short_name="Suzuka",
            start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
            end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
        )
        fp1 = F1Session(
            id="session-2026-fp1",
            meeting_id=meeting.id,
            session_key=11246,
            session_name="Practice 1",
            session_code="FP1",
            session_type="Practice",
            date_start_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
            date_end_utc=datetime(2026, 3, 27, 3, 30, tzinfo=timezone.utc),
            is_practice=True,
        )
        fp2 = F1Session(
            id="session-2026-fp2",
            meeting_id=meeting.id,
            session_key=11247,
            session_name="Practice 2",
            session_code="FP2",
            session_type="Practice",
            date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
            date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
            is_practice=True,
        )
        session.add_all([meeting, fp1, fp2])
        session.commit()

        result = refresh_driver_affinity(
            context,
            season=2026,
            meeting_key=1281,
        )

        assert result["status"] == "blocked"
        assert result["report"] is None
        assert result["job_run_id"] is not None
        assert result["report_path"] is not None
        assert result["preflight_summary"]["missing_session_keys"] == [11246, 11247]
        assert result["source_max_session_end_utc"] == "2026-03-27T07:00:00+00:00"
    finally:
        session.close()


def test_driver_affinity_refresh_status_marks_missing_credentials_as_blocked(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        meeting = F1Meeting(
            id="meeting-2026-japan",
            meeting_key=1281,
            season=2026,
            meeting_name="Japanese Grand Prix",
            circuit_short_name="Suzuka",
            start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
            end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
        )
        fp2 = F1Session(
            id="session-2026-fp2",
            meeting_id=meeting.id,
            session_key=11247,
            session_name="Practice 2",
            session_code="FP2",
            session_type="Practice",
            date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
            date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
            is_practice=True,
        )
        session.add_all([meeting, fp2])
        session.commit()

        readiness = get_driver_affinity_refresh_status(
            context,
            season=2026,
            meeting_key=1281,
            now=datetime(2026, 3, 27, 8, 0, tzinfo=timezone.utc),
        )

        assert readiness["status"] == "blocked"
        assert readiness["openf1_credentials_configured"] is False
        assert readiness["missing_session_keys"] == [11247]
        assert readiness["latest_ended_session_code"] == "FP2"
    finally:
        session.close()


def test_refresh_driver_affinity_skips_fresh_report_without_writing_lineage_runs(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    try:
        meeting = F1Meeting(
            id="meeting-2026-japan",
            meeting_key=1281,
            season=2026,
            meeting_name="Japanese Grand Prix",
            circuit_short_name="Suzuka",
            start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
            end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
        )
        fp3 = F1Session(
            id="session-2026-fp3",
            meeting_id=meeting.id,
            session_key=11248,
            session_name="Practice 3",
            session_code="FP3",
            session_type="Practice",
            date_start_utc=datetime(2026, 3, 28, 2, 30, tzinfo=timezone.utc),
            date_end_utc=datetime(2026, 3, 28, 3, 30, tzinfo=timezone.utc),
            is_practice=True,
        )
        session.add_all([meeting, fp3])
        session.commit()

        report_path = tmp_path / "reports" / "driver_affinity" / "2026" / "1281" / "latest.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            """
{
  "season": 2026,
  "meeting_key": 1281,
  "computed_at_utc": "2026-03-28T05:03:33.080174Z",
  "as_of_utc": "2026-03-28T05:03:33.080174Z",
  "lookback_start_season": 2024,
  "session_code_weights": {"FP1": 0.4, "FP2": 0.6, "FP3": 0.8, "Q": 1.0},
  "season_weights": {"2024": 0.4, "2025": 0.65, "2026": 1.0},
  "track_weights": {
    "s1_fraction": 0.3333333333,
    "s2_fraction": 0.3333333333,
    "s3_fraction": 0.3333333333
  },
  "source_session_codes_included": ["FP3"],
  "source_max_session_end_utc": "2026-03-28T03:30:00+00:00",
  "latest_ended_relevant_session_code": "FP3",
  "latest_ended_relevant_session_end_utc": "2026-03-28T03:30:00+00:00",
  "entry_count": 0,
  "entries": []
}
            """.strip(),
            encoding="utf-8",
        )

        result = refresh_driver_affinity(
            context,
            season=2026,
            meeting_key=1281,
        )

        runs = list(session.scalars(select(IngestionJobRun)).all())
        assert result["status"] == "skipped"
        assert runs == []
    finally:
        session.close()
