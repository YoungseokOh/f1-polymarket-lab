from __future__ import annotations

from pathlib import Path

from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import IngestionJobRun
from f1_polymarket_worker.pipeline import PipelineContext, sync_f1_calendar, sync_polymarket_catalog
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


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
