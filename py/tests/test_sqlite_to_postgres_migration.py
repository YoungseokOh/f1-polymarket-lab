from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from f1_polymarket_lab.storage.db import build_engine
from f1_polymarket_lab.storage.migrations import migrate_sqlite_to_postgres
from f1_polymarket_lab.storage.models import F1Meeting, ModelRun
from sqlalchemy.orm import Session

pytestmark = pytest.mark.postgres_integration


def build_source_sqlite_database(path: Path) -> str:
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE f1_meetings (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            meeting_key INTEGER NOT NULL UNIQUE,
            season INTEGER NOT NULL,
            round_number INTEGER,
            meeting_name TEXT NOT NULL,
            meeting_official_name TEXT,
            circuit_short_name TEXT,
            country_name TEXT,
            location TEXT,
            start_date_utc TEXT,
            end_date_utc TEXT,
            raw_payload TEXT
        );

        CREATE TABLE f1_sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            session_key INTEGER NOT NULL UNIQUE,
            meeting_id TEXT,
            session_name TEXT NOT NULL,
            session_type TEXT,
            session_code TEXT,
            date_start_utc TEXT,
            date_end_utc TEXT,
            status TEXT,
            session_order INTEGER,
            is_practice INTEGER NOT NULL,
            raw_payload TEXT
        );

        CREATE TABLE model_runs (
            id TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            model_family TEXT NOT NULL,
            model_name TEXT NOT NULL,
            dataset_version TEXT,
            feature_snapshot_id TEXT,
            train_start TEXT,
            train_end TEXT,
            val_start TEXT,
            val_end TEXT,
            test_start TEXT,
            test_end TEXT,
            config_json TEXT,
            metrics_json TEXT,
            artifact_uri TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    cursor.execute(
        """
        INSERT INTO f1_meetings (
            id,
            source,
            meeting_key,
            season,
            round_number,
            meeting_name,
            meeting_official_name,
            circuit_short_name,
            country_name,
            location,
            start_date_utc,
            end_date_utc,
            raw_payload
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "meeting:historical:2026:1",
            "f1db",
            -202601,
            2026,
            1,
            "British Grand Prix",
            "2026 British Grand Prix",
            "Silverstone",
            "United Kingdom",
            "Silverstone",
            "2026-03-28 00:00:00.000000",
            "2026-03-30 00:00:00.000000",
            json.dumps(
                {
                    "grand_prix_full_name": "British Grand Prix",
                    "qualifying_format": "KNOCKOUT",
                }
            ),
        ),
    )
    cursor.executemany(
        """
        INSERT INTO f1_meetings (
            id,
            source,
            meeting_key,
            season,
            round_number,
            meeting_name,
            meeting_official_name,
            circuit_short_name,
            country_name,
            location,
            start_date_utc,
            end_date_utc,
            raw_payload
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "meeting:historical:2026:2",
                "f1db",
                -202602,
                2026,
                2,
                "Bahrain",
                None,
                "Bahrain International Circuit",
                "Bahrain",
                "Sakhir",
                "2026-04-10 00:00:00.000000",
                "2026-04-12 00:00:00.000000",
                json.dumps({}),
            ),
            (
                "meeting:historical:2026:3",
                "f1db",
                -202603,
                2026,
                3,
                "Saudi Arabia",
                None,
                "Jeddah Corniche Circuit",
                "Saudi Arabia",
                "Jeddah",
                "2026-04-17 00:00:00.000000",
                "2026-04-19 00:00:00.000000",
                json.dumps({}),
            ),
            (
                "meeting:historical:2026:4",
                "f1db",
                -202604,
                2026,
                4,
                "Japanese Grand Prix",
                "FORMULA 1 ARAMCO JAPANESE GRAND PRIX 2026",
                "Suzuka",
                "Japan",
                "Suzuka",
                "2026-04-24 00:00:00.000000",
                "2026-04-26 00:00:00.000000",
                json.dumps({}),
            ),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO f1_sessions (
            id,
            source,
            session_key,
            meeting_id,
            session_name,
            session_type,
            session_code,
            date_start_utc,
            date_end_utc,
            status,
            session_order,
            is_practice,
            raw_payload
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "session:fp1",
                "openf1",
                101,
                "meeting:historical:2026:1",
                "Practice 1",
                "Practice",
                "FP1",
                "2026-03-28 10:00:00.000000",
                "2026-03-28 11:00:00.000000",
                None,
                1,
                1,
                None,
            ),
            (
                "session:fp2",
                "openf1",
                102,
                "meeting:historical:2026:1",
                "Practice 2",
                "Practice",
                "FP2",
                "2026-03-28 14:00:00.000000",
                "2026-03-28 15:00:00.000000",
                None,
                2,
                1,
                None,
            ),
            (
                "session:fp3",
                "openf1",
                103,
                "meeting:historical:2026:1",
                "Practice 3",
                "Practice",
                "FP3",
                "2026-03-29 10:00:00.000000",
                "2026-03-29 11:00:00.000000",
                None,
                3,
                1,
                None,
            ),
            (
                "session:q",
                "openf1",
                104,
                "meeting:historical:2026:1",
                "Qualifying",
                "Qualifying",
                "Q",
                "2026-03-29 14:00:00.000000",
                "2026-03-29 15:00:00.000000",
                None,
                4,
                0,
                None,
            ),
            (
                "session:r",
                "openf1",
                105,
                "meeting:historical:2026:1",
                "Race",
                "Race",
                "R",
                "2026-03-30 14:00:00.000000",
                "2026-03-30 16:00:00.000000",
                None,
                5,
                0,
                None,
            ),
        ],
    )
    cursor.execute(
        """
        INSERT INTO model_runs (
            id,
            stage,
            model_family,
            model_name,
            dataset_version,
            feature_snapshot_id,
            config_json,
            metrics_json,
            artifact_uri,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "model-run-1",
            "multitask_qr",
            "torch_multitask",
            "shared_encoder_multitask_v2",
            "dataset-v1",
            "snapshot-1",
            json.dumps({"seed": 7}),
            json.dumps({"roi_pct": 12.5}),
            "/tmp/model-run-1",
            "2026-04-01 00:00:00.000000",
        ),
    )
    connection.commit()
    connection.close()
    return f"sqlite+pysqlite:///{path}"


def test_migrate_sqlite_to_postgres_derives_missing_columns(
    tmp_path: Path,
    postgres_db_url: str,
) -> None:
    sqlite_url = build_source_sqlite_database(tmp_path / "source.sqlite")

    plan = migrate_sqlite_to_postgres(
        sqlite_url=sqlite_url,
        postgres_url=postgres_db_url,
        batch_size=2,
        execute=False,
    )
    assert plan["target_nonempty_tables"] == [
        {
            "table_name": "f1_calendar_overrides",
            "target_count": 2,
        }
    ]
    assert plan["unexpected_target_nonempty_tables"] == []
    assert plan["source_schema_drift"]["f1_meetings"] == ["meeting_slug", "event_format"]
    assert plan["source_schema_drift"]["model_runs"] == ["registry_run_id"]

    result = migrate_sqlite_to_postgres(
        sqlite_url=sqlite_url,
        postgres_url=postgres_db_url,
        batch_size=2,
        execute=True,
    )

    assert result["unexpected_target_nonempty_tables"] == []
    assert result["row_count_mismatches"] == []

    engine = build_engine(postgres_db_url)
    with Session(engine) as session:
        meeting = session.get(F1Meeting, "meeting:historical:2026:1")
        bahrain = session.get(F1Meeting, "meeting:historical:2026:2")
        saudi = session.get(F1Meeting, "meeting:historical:2026:3")
        sponsored = session.get(F1Meeting, "meeting:historical:2026:4")
        model_run = session.get(ModelRun, "model-run-1")

        assert meeting is not None
        assert meeting.meeting_slug == "british-grand-prix"
        assert meeting.event_format == "conventional"

        assert bahrain is not None
        assert bahrain.meeting_slug == "bahrain-grand-prix"

        assert saudi is not None
        assert saudi.meeting_slug == "saudi-arabian-grand-prix"

        assert sponsored is not None
        assert sponsored.meeting_slug == "japanese-grand-prix"

        assert model_run is not None
        assert model_run.registry_run_id is None
