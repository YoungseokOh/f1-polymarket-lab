from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import db_session
from f1_polymarket_lab.storage.migrations import ensure_database_schema
from f1_polymarket_lab.storage.models import IngestionJobRun
from f1_polymarket_worker import job_queue


def test_queued_job_retries_then_completes(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'worker.db'}"
    ensure_database_schema(database_url)
    settings = Settings(database_url_override=database_url)
    calls = 0

    def flaky_handler(_session: object, _settings: object, _inputs: dict[str, object]) -> dict:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient failure")
        return {"records_written": 9}

    monkeypatch.setitem(
        job_queue.QUEUE_JOB_SPECS,
        "ingest-demo",
        replace(job_queue.QUEUE_JOB_SPECS["ingest-demo"], handler=flaky_handler),
    )

    with db_session(database_url) as session:
        run = job_queue.enqueue_job(
            session,
            job_name="ingest-demo",
            planned_inputs={"season": 2026},
            max_attempts=2,
        )
        run_id = run.id

    first = job_queue.run_worker_once(settings, worker_id="test-worker")
    with db_session(database_url) as session:
        stored = session.get(IngestionJobRun, run_id)
        assert stored is not None
        assert stored.status == "queued"
        assert stored.attempt_count == 1
        assert stored.max_attempts == 2
        assert stored.error_message == "transient failure"
        stored.available_at = utc_now()

    second = job_queue.run_worker_once(settings, worker_id="test-worker")

    assert first["status"] == "retrying"
    assert second["status"] == "completed"
    with db_session(database_url) as session:
        stored = session.get(IngestionJobRun, run_id)
        assert stored is not None
        assert stored.status == "completed"
        assert stored.records_written == 9
        assert stored.attempt_count == 2
        assert stored.locked_by is None
        assert stored.locked_at is None


def test_worker_dispatches_queued_paper_trade(tmp_path: Path, monkeypatch) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'worker.db'}"
    ensure_database_schema(database_url)
    settings = Settings(database_url_override=database_url)
    calls: dict[str, object] = {}

    def fake_resolve_gp_config(short_code: str, *, db: object) -> SimpleNamespace:
        calls["resolved_short_code"] = short_code
        calls["resolved_db"] = db
        return SimpleNamespace(short_code=short_code)

    def fake_run_gp_paper_trade_pipeline(*args, **kwargs) -> dict[str, object]:
        calls["context"] = args[0]
        calls["config_short_code"] = kwargs["config"].short_code
        calls["snapshot_id"] = kwargs["snapshot_id"]
        calls["baseline"] = kwargs["baseline"]
        calls["min_edge"] = kwargs["min_edge"]
        calls["bet_size"] = kwargs["bet_size"]
        return {"trades_executed": 2, "total_pnl": 1.25}

    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.resolve_gp_config",
        fake_resolve_gp_config,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.run_gp_paper_trade_pipeline",
        fake_run_gp_paper_trade_pipeline,
    )

    with db_session(database_url) as session:
        run = job_queue.enqueue_job(
            session,
            job_name="run-paper-trade",
            planned_inputs={
                "gp_short_code": "miami_fp1_q",
                "snapshot_id": "snapshot-1",
                "baseline": "hybrid",
                "min_edge": 0.07,
                "bet_size": 15.0,
            },
        )
        run_id = run.id

    result = job_queue.run_worker_once(
        settings,
        worker_id="test-worker",
        job_names={"run-paper-trade"},
    )

    assert result["status"] == "completed"
    assert calls["resolved_short_code"] == "miami_fp1_q"
    assert calls["config_short_code"] == "miami_fp1_q"
    assert calls["snapshot_id"] == "snapshot-1"
    assert calls["baseline"] == "hybrid"
    assert calls["min_edge"] == 0.07
    assert calls["bet_size"] == 15.0
    with db_session(database_url) as session:
        stored = session.get(IngestionJobRun, run_id)
        assert stored is not None
        assert stored.status == "completed"
        assert stored.records_written == 2
        assert stored.cursor_after == {
            "gp_short_code": "miami_fp1_q",
            "trades_executed": 2,
            "total_pnl": 1.25,
        }
