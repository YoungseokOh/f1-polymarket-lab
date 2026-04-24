from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import db_session
from f1_polymarket_lab.storage.migrations import ensure_database_schema
from f1_polymarket_lab.storage.models import IngestionJobRun
from f1_polymarket_worker import cli, job_queue
from f1_polymarket_worker.lineage import ensure_job_definition
from f1_polymarket_worker.pipeline import PipelineContext


@contextmanager
def fake_db_session(_: str) -> Iterator[object]:
    yield object()


def test_sync_polymarket_catalog_command_routes_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "get_settings", lambda: Settings())
    monkeypatch.setattr(cli, "db_session", fake_db_session)

    def fake_sync_polymarket_catalog(
        context: PipelineContext,
        *,
        max_pages: int,
        batch_size: int,
        active: bool | None,
        closed: bool | None,
        archived: bool | None,
    ) -> dict[str, str]:
        captured.update(
            {
                "execute": context.execute,
                "max_pages": max_pages,
                "batch_size": batch_size,
                "active": active,
                "closed": closed,
                "archived": archived,
            }
        )
        return {"status": "completed"}

    monkeypatch.setattr(cli, "sync_polymarket_catalog", fake_sync_polymarket_catalog)

    cli.sync_polymarket_catalog_command(
        max_pages=3,
        batch_size=25,
        execute=True,
        active=True,
        closed=False,
        archived=False,
    )

    assert captured == {
        "execute": True,
        "max_pages": 3,
        "batch_size": 25,
        "active": True,
        "closed": False,
        "archived": False,
    }


def test_set_f1_calendar_override_command_routes_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "get_settings", lambda: Settings())
    monkeypatch.setattr(cli, "db_session", fake_db_session)

    class Override:
        season = 2026
        meeting_slug = "miami-grand-prix"
        status = "postponed"
        ops_slug = "miami"
        source_url = "https://example.com"

    def fake_set_calendar_override(session: object, **kwargs: object) -> Override:
        captured.update(kwargs)
        return Override()

    monkeypatch.setattr(
        "f1_polymarket_worker.ops_calendar.set_calendar_override",
        fake_set_calendar_override,
    )

    cli.set_f1_calendar_override_command(
        season=2026,
        meeting_slug="miami-grand-prix",
        status="postponed",
        ops_slug="miami",
        effective_round_number=None,
        effective_start_date_utc=None,
        effective_end_date_utc=None,
        effective_meeting_name=None,
        effective_country_name=None,
        effective_location=None,
        source_label=None,
        source_url="https://example.com",
        note=None,
        execute=True,
    )

    assert captured["season"] == 2026
    assert captured["meeting_slug"] == "miami-grand-prix"
    assert captured["status"] == "postponed"
    assert captured["ops_slug"] == "miami"
    assert captured["source_url"] == "https://example.com"


def test_clear_f1_calendar_override_command_routes_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "get_settings", lambda: Settings())
    monkeypatch.setattr(cli, "db_session", fake_db_session)

    class Override:
        season = 2026
        meeting_slug = "miami-grand-prix"
        is_active = False

    def fake_clear_calendar_override(session: object, **kwargs: object) -> Override:
        captured.update(kwargs)
        return Override()

    monkeypatch.setattr(
        "f1_polymarket_worker.ops_calendar.clear_calendar_override",
        fake_clear_calendar_override,
    )

    cli.clear_f1_calendar_override_command(
        season=2026,
        meeting_slug="miami-grand-prix",
        execute=True,
    )

    assert captured == {
        "season": 2026,
        "meeting_slug": "miami-grand-prix",
    }


def test_worker_command_processes_queued_ingest_demo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'worker.db'}"
    ensure_database_schema(database_url)
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(database_url_override=database_url),
    )

    def fake_ingest_demo(
        _session: object,
        _settings: object,
        inputs: dict[str, object],
    ) -> dict[str, int]:
        assert inputs["season"] == 2026
        assert inputs["weekends"] == 1
        assert inputs["market_batches"] == 2
        return {"records_written": 7}

    monkeypatch.setitem(
        job_queue.QUEUE_JOB_SPECS,
        "ingest-demo",
        replace(job_queue.QUEUE_JOB_SPECS["ingest-demo"], handler=fake_ingest_demo),
    )

    with db_session(database_url) as session:
        definition = ensure_job_definition(
            session,
            job_name="ingest-demo",
            source="demo",
            dataset="demo_ingest",
            description="Seed a lightweight demo ingestion run for the dashboard.",
            schedule_hint="manual",
        )
        run = IngestionJobRun(
            job_definition_id=definition.id,
            job_name=definition.job_name,
            source=definition.source,
            dataset=definition.dataset,
            status="queued",
            execute_mode="queued",
            planned_inputs={"season": 2026, "weekends": 1, "market_batches": 2},
        )
        session.add(run)
        session.flush()
        run_id = run.id

    cli.worker(
        once=True,
        poll_interval=0.1,
        max_jobs=1,
        job_name=None,
        stale_after_seconds=7200,
    )

    with db_session(database_url) as session:
        stored = session.get(IngestionJobRun, run_id)
        assert stored is not None
        assert stored.status == "completed"
        assert stored.records_written == 7
        assert stored.cursor_after == {"records_written": 7}


def test_bootstrap_db_creates_lineage_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "bootstrap.db"
    repo_root = Path(__file__).resolve().parents[2]
    pythonpath_entries = [
        "apps/worker/src",
        "py/common/src",
        "py/connectors/src",
        "py/storage/src",
        "py/features/src",
        "py/models/src",
        "py/experiments/src",
        "py/agent/src",
    ]
    env = os.environ.copy()
    env["DATABASE_URL_OVERRIDE"] = f"sqlite+pysqlite:///{db_path}"
    env["PYTHONPATH"] = os.pathsep.join(
        pythonpath_entries + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )

    result = subprocess.run(
        [sys.executable, "-m", "f1_polymarket_worker.cli", "bootstrap-db"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ingestion_job_runs'"
    )
    assert cur.fetchone() == ("ingestion_job_runs",)
    cur.execute("SELECT version_num FROM alembic_version")
    assert cur.fetchone() == ("20260425_0014",)
