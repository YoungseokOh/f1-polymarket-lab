from __future__ import annotations

import time

import typer
from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.storage.db import Base, build_engine, db_session

from f1_polymarket_worker.demo_ingest import ingest_demo
from f1_polymarket_worker.historical import bootstrap_f1db_history, sync_jolpica_history
from f1_polymarket_worker.orchestration import (
    backfill_f1_history,
    backfill_f1_history_all,
    capture_live_weekend,
    discover_session_polymarket,
    hydrate_polymarket_f1_history,
    sync_polymarket_f1_catalog,
    validate_f1_weekend_subset,
)
from f1_polymarket_worker.pipeline import (
    PipelineContext,
    ensure_default_feature_registry,
    hydrate_f1_session,
    hydrate_polymarket_market,
    reconcile_mappings,
    run_data_quality_checks,
    sync_f1_calendar,
)

app = typer.Typer(no_args_is_help=True)


@app.command("bootstrap-db")
def bootstrap_db() -> None:
    settings = get_settings()
    engine = build_engine(settings.database_url)
    Base.metadata.create_all(engine)
    typer.echo("Database tables ensured.")


@app.command("ingest-demo")
def ingest_demo_command(season: int = 2024, weekends: int = 2, market_batches: int = 3) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        ingest_demo(session, season=season, weekends=weekends, market_batches=market_batches)
    typer.echo("Demo ingestion complete.")


@app.command("sync-f1-calendar")
def sync_f1_calendar_command(
    season: int = 2024,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sync_f1_calendar(context, season=season)
    typer.echo(result)


@app.command("hydrate-f1-session")
def hydrate_f1_session_command(
    session_key: int,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
    include_extended: bool = typer.Option(False, "--extended/--core-only"),
    include_heavy: bool = typer.Option(False, "--include-heavy/--skip-heavy"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        ensure_default_feature_registry(context)
        result = hydrate_f1_session(
            context,
            session_key=session_key,
            include_extended=include_extended,
            include_heavy=include_heavy,
        )
    typer.echo(result)


@app.command("sync-polymarket-catalog")
def sync_polymarket_catalog_command(
    max_pages: int = 1,
    batch_size: int = 100,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
    active: bool | None = None,
    closed: bool | None = None,
    archived: bool | None = None,
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sync_polymarket_f1_catalog(
            context,
            max_pages=max_pages,
            batch_size=batch_size,
        )
    typer.echo(result)


@app.command("sync-polymarket-f1-catalog")
def sync_polymarket_f1_catalog_command(
    max_pages: int = 20,
    batch_size: int = 100,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
    search_fallback: bool = True,
    start_year: int = 2022,
    end_year: int | None = None,
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sync_polymarket_f1_catalog(
            context,
            max_pages=max_pages,
            batch_size=batch_size,
            search_fallback=search_fallback,
            start_year=start_year,
            end_year=end_year,
        )
    typer.echo(result)


@app.command("hydrate-polymarket-market")
def hydrate_polymarket_market_command(
    market_id: str,
    fidelity: int = 60,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = hydrate_polymarket_market(context, market_id=market_id, fidelity=fidelity)
    typer.echo(result)


@app.command("hydrate-polymarket-f1-history")
def hydrate_polymarket_f1_history_command(
    fidelity: int = 60,
    active_only: bool = typer.Option(False, "--active-only/--all"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = hydrate_polymarket_f1_history(
            context,
            fidelity=fidelity,
            active_only=active_only,
        )
    typer.echo(result)


@app.command("discover-session-polymarket")
def discover_session_polymarket_command(
    session_key: int,
    batch_size: int = 100,
    max_pages: int = 5,
    search_fallback: bool = True,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = discover_session_polymarket(
            context,
            session_key=session_key,
            batch_size=batch_size,
            max_pages=max_pages,
            search_fallback=search_fallback,
        )
    typer.echo(result)


@app.command("backfill-f1-history")
def backfill_f1_history_command(
    season_start: int = 2023,
    season_end: int | None = None,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
    include_extended: bool = typer.Option(True, "--extended/--core-only"),
    heavy_mode: str = typer.Option("weekend", "--heavy-mode"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = backfill_f1_history(
            context,
            season_start=season_start,
            season_end=season_end,
            include_extended=include_extended,
            heavy_mode=heavy_mode,
        )
    typer.echo(result)


@app.command("bootstrap-f1db-history")
def bootstrap_f1db_history_command(
    season_start: int = 1950,
    season_end: int = 2022,
    artifact: str = "sqlite",
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = bootstrap_f1db_history(
            context,
            season_start=season_start,
            season_end=season_end,
            artifact=artifact,
        )
    typer.echo(result)


@app.command("sync-jolpica-history")
def sync_jolpica_history_command(
    season_start: int = 1950,
    season_end: int = 2022,
    resources: list[str] = typer.Option(
        ["races", "results", "qualifying", "sprint", "pitstops", "laps"],
        "--resource",
    ),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sync_jolpica_history(
            context,
            season_start=season_start,
            season_end=season_end,
            resources=tuple(resources),
        )
    typer.echo(result)


@app.command("backfill-f1-history-all")
def backfill_f1_history_all_command(
    season_start: int = 1950,
    season_end: int | None = None,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
    include_extended: bool = typer.Option(True, "--extended/--core-only"),
    heavy_mode: str = typer.Option("weekend", "--heavy-mode"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = backfill_f1_history_all(
            context,
            season_start=season_start,
            season_end=season_end,
            include_extended=include_extended,
            heavy_mode=heavy_mode,
        )
    typer.echo(result)


@app.command("capture-live-weekend")
def capture_live_weekend_command(
    session_key: int,
    market_ids: list[str] | None = typer.Option(None, "--market-id"),
    start_buffer_min: int = 15,
    stop_buffer_min: int = 15,
    message_limit: int | None = None,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = capture_live_weekend(
            context,
            session_key=session_key,
            market_ids=market_ids,
            start_buffer_min=start_buffer_min,
            stop_buffer_min=stop_buffer_min,
            message_limit=message_limit,
        )
    typer.echo(result)


@app.command("reconcile-mappings")
def reconcile_mappings_command(min_confidence: float = 0.65) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=True)
        result = reconcile_mappings(context, min_confidence=min_confidence)
    typer.echo(result)


@app.command("dq-run")
def data_quality_command() -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=True)
        result = run_data_quality_checks(context)
    typer.echo(result)


@app.command("validate-f1-weekend-subset")
def validate_f1_weekend_subset_command(
    meeting_key: int = typer.Option(..., "--meeting-key"),
    season: int | None = typer.Option(None, "--season"),
    report_slug: str | None = typer.Option(None, "--report-slug"),
    validation_mode: str = typer.Option("smoke", "--validation-mode"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = validate_f1_weekend_subset(
            context,
            meeting_key=meeting_key,
            season=season,
            report_slug=report_slug,
            validation_mode=validation_mode,
        )
    typer.echo(result)


@app.command("worker")
def worker() -> None:
    typer.echo("Worker heartbeat loop started. Use the CLI to trigger ingestion jobs.")
    while True:
        time.sleep(30)


if __name__ == "__main__":
    app()
