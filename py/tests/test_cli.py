from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
import typer
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_worker import cli
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


def test_worker_command_fails_fast() -> None:
    with pytest.raises(typer.Exit) as exc_info:
        cli.worker()

    assert exc_info.value.exit_code == 2


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
