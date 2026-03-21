from __future__ import annotations

import time

import typer
from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.storage.db import Base, build_engine, db_session

from f1_polymarket_worker.backtest import (
    collect_resolutions,
    run_walk_forward_backtest,
    save_backtest_report,
    settle_backtest,
    settle_single_gp,
)
from f1_polymarket_worker.demo_ingest import ingest_demo
from f1_polymarket_worker.historical import (
    bootstrap_f1db_history,
    sweep_polymarket_historical_poles,
    sync_jolpica_history,
    sync_openf1_season_range,
)
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
from f1_polymarket_worker.quicktest import (
    build_aus_fp1_to_q_snapshot,
    build_china_fp1_to_sq_snapshot,
    build_japan_fp1_to_q_snapshot,
    build_japan_pre_weekend_snapshot,
    report_aus_q_pole_quicktest,
    report_china_sq_pole_quicktest,
    report_japan_fp1_q_pole_quicktest,
    report_japan_q_pole_quicktest,
    run_aus_q_pole_baseline,
    run_china_sq_pole_baseline,
    run_japan_fp1_q_pole_baseline,
    run_japan_q_pole_baseline,
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


@app.command("sync-openf1-season-range")
def sync_openf1_season_range_command(
    season_start: int = 2023,
    season_end: int = 2025,
    force_rehydrate: bool = typer.Option(False, "--force-rehydrate/--skip-hydrated"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Bulk-sync OpenF1 FP1/Q/SQ sessions for a range of seasons."""
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sync_openf1_season_range(
            context,
            season_start=season_start,
            season_end=season_end,
            force_rehydrate=force_rehydrate,
        )
    typer.echo(result)


@app.command("sweep-polymarket-historical-poles")
def sweep_polymarket_historical_poles_command(
    max_pages: int = 300,
    batch_size: int = 100,
    fidelity: int = 60,
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Sweep closed Polymarket F1 pole markets and hydrate price/resolution history."""
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = sweep_polymarket_historical_poles(
            context,
            max_pages=max_pages,
            batch_size=batch_size,
            fidelity=fidelity,
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


@app.command("build-china-fp1-to-sq-snapshot")
def build_china_fp1_to_sq_snapshot_command(
    meeting_key: int = typer.Option(1280, "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    entry_offset_min: int = typer.Option(10, "--entry-offset-min"),
    fidelity: int = typer.Option(60, "--fidelity"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = build_china_fp1_to_sq_snapshot(
            context,
            meeting_key=meeting_key,
            season=season,
            entry_offset_min=entry_offset_min,
            fidelity=fidelity,
        )
    typer.echo(result)


@app.command("run-china-sq-pole-baseline")
def run_china_sq_pole_baseline_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_china_sq_pole_baseline(
            context,
            snapshot_id=snapshot_id,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("report-china-sq-pole-quicktest")
def report_china_sq_pole_quicktest_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    report_slug: str | None = typer.Option(None, "--report-slug"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = report_china_sq_pole_quicktest(
            context,
            snapshot_id=snapshot_id,
            report_slug=report_slug,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("build-aus-fp1-to-q-snapshot")
def build_aus_fp1_to_q_snapshot_command(
    meeting_key: int = typer.Option(1279, "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    entry_offset_min: int = typer.Option(10, "--entry-offset-min"),
    fidelity: int = typer.Option(60, "--fidelity"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = build_aus_fp1_to_q_snapshot(
            context,
            meeting_key=meeting_key,
            season=season,
            entry_offset_min=entry_offset_min,
            fidelity=fidelity,
        )
    typer.echo(result)


@app.command("run-aus-q-pole-baseline")
def run_aus_q_pole_baseline_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_aus_q_pole_baseline(
            context,
            snapshot_id=snapshot_id,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("report-aus-q-pole-quicktest")
def report_aus_q_pole_quicktest_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    report_slug: str | None = typer.Option(None, "--report-slug"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = report_aus_q_pole_quicktest(
            context,
            snapshot_id=snapshot_id,
            report_slug=report_slug,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("build-japan-pre-weekend-snapshot")
def build_japan_pre_weekend_snapshot_command(
    meeting_key: int = typer.Option(1281, "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = build_japan_pre_weekend_snapshot(
            context,
            meeting_key=meeting_key,
            season=season,
        )
    typer.echo(result)


@app.command("run-japan-q-pole-baseline")
def run_japan_q_pole_baseline_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_japan_q_pole_baseline(
            context,
            snapshot_id=snapshot_id,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("report-japan-q-pole-quicktest")
def report_japan_q_pole_quicktest_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    report_slug: str | None = typer.Option(None, "--report-slug"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = report_japan_q_pole_quicktest(
            context,
            snapshot_id=snapshot_id,
            report_slug=report_slug,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("build-japan-fp1-to-q-snapshot")
def build_japan_fp1_to_q_snapshot_command(
    meeting_key: int = typer.Option(1281, "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    entry_offset_min: int = typer.Option(10, "--entry-offset-min"),
    fidelity: int = typer.Option(60, "--fidelity"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = build_japan_fp1_to_q_snapshot(
            context,
            meeting_key=meeting_key,
            season=season,
            entry_offset_min=entry_offset_min,
            fidelity=fidelity,
        )
    typer.echo(result)


@app.command("run-japan-fp1-q-pole-baseline")
def run_japan_fp1_q_pole_baseline_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_japan_fp1_q_pole_baseline(
            context,
            snapshot_id=snapshot_id,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("report-japan-fp1-q-pole-quicktest")
def report_japan_fp1_q_pole_quicktest_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    report_slug: str | None = typer.Option(None, "--report-slug"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = report_japan_fp1_q_pole_quicktest(
            context,
            snapshot_id=snapshot_id,
            report_slug=report_slug,
            min_edge=min_edge,
        )
    typer.echo(result)


@app.command("save-backtest-report")
def save_backtest_report_command(
    snapshot_ids: str = typer.Option(..., "--snapshot-ids", help="Comma-separated snapshot IDs"),
    meeting_keys: str = typer.Option(..., "--meeting-keys", help="Comma-separated meeting keys"),
    season: int = typer.Option(2026, "--season"),
    strategy_name: str = typer.Option("hybrid_flat_bet", "--strategy"),
    model_name: str = typer.Option("hybrid", "--model"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    bet_size: float = typer.Option(10.0, "--bet-size"),
    slug: str | None = typer.Option(None, "--slug"),
    title: str | None = typer.Option(None, "--title"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Run walk-forward backtest and save a report to disk."""
    sids = [s.strip() for s in snapshot_ids.split(",")]
    mkeys = [int(k.strip()) for k in meeting_keys.split(",")]
    if len(sids) != len(mkeys):
        typer.echo("Error: snapshot-ids and meeting-keys must have the same count.", err=True)
        raise typer.Exit(1)
    gp_configs = [
        {"meeting_key": mk, "season": season, "snapshot_id": sid}
        for mk, sid in zip(mkeys, sids, strict=True)
    ]
    report_slug = slug or f"{season}-season-backtest"
    report_title = title or f"{season} Season Backtest (R1\u2013R{len(gp_configs)})"
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_walk_forward_backtest(
            context,
            gp_configs=gp_configs,
            strategy_name=strategy_name,
            model_name=model_name,
            min_edge=min_edge,
            bet_size=bet_size,
        )
        if execute:
            report_path = save_backtest_report(
                context,
                result,
                slug=report_slug,
                title=report_title,
            )
            typer.echo(f"Report saved: {report_path}")
        else:
            typer.echo(result)


@app.command("collect-meeting-data")
def collect_meeting_data_command(
    meeting_key: int = typer.Argument(..., help="OpenF1 meeting key"),
    year: int = typer.Option(2026, "--year"),
    include_laps: bool = typer.Option(False, "--include-laps/--skip-laps"),
    include_weather: bool = typer.Option(False, "--include-weather/--skip-weather"),
) -> None:
    from f1_polymarket_lab.connectors import DataCollector

    collector = DataCollector()
    data = collector.collect_meeting_data(
        meeting_key,
        year,
        include_laps=include_laps,
        include_weather=include_weather,
    )
    typer.echo(f"Meeting {meeting_key}: {len(data.sessions)} sessions, {len(data.drivers)} drivers")
    for sk, sd in data.session_data.items():
        name = sd.session_info.get("session_name", "?")
        typer.echo(f"  session {sk} ({name}): {len(sd.results)} results")


@app.command("collect-resolutions")
def collect_resolutions_command(
    meeting_key: int = typer.Option(..., "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = collect_resolutions(context, meeting_key=meeting_key, season=season)
    typer.echo(result)


@app.command("settle-backtest")
def settle_backtest_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    strategy_name: str = typer.Option("hybrid_flat_bet", "--strategy"),
    model_name: str = typer.Option("hybrid", "--model"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    bet_size: float = typer.Option(10.0, "--bet-size"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = settle_backtest(
            context,
            snapshot_id=snapshot_id,
            strategy_name=strategy_name,
            model_name=model_name,
            min_edge=min_edge,
            bet_size=bet_size,
        )
    typer.echo(result)


@app.command("settle-single-gp")
def settle_single_gp_command(
    meeting_key: int = typer.Option(..., "--meeting-key"),
    season: int = typer.Option(2026, "--season"),
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    strategy_name: str = typer.Option("hybrid_flat_bet", "--strategy"),
    model_name: str = typer.Option("hybrid", "--model"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    bet_size: float = typer.Option(10.0, "--bet-size"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = settle_single_gp(
            context,
            meeting_key=meeting_key,
            season=season,
            snapshot_id=snapshot_id,
            strategy_name=strategy_name,
            model_name=model_name,
            min_edge=min_edge,
            bet_size=bet_size,
        )
    typer.echo(result)


@app.command("run-walk-forward-backtest")
def run_walk_forward_backtest_command(
    snapshot_ids: str = typer.Option(..., "--snapshot-ids", help="Comma-separated snapshot IDs"),
    meeting_keys: str = typer.Option(..., "--meeting-keys", help="Comma-separated meeting keys"),
    season: int = typer.Option(2026, "--season"),
    strategy_name: str = typer.Option("hybrid_flat_bet", "--strategy"),
    model_name: str = typer.Option("hybrid", "--model"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    bet_size: float = typer.Option(10.0, "--bet-size"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    sids = [s.strip() for s in snapshot_ids.split(",")]
    mkeys = [int(k.strip()) for k in meeting_keys.split(",")]
    if len(sids) != len(mkeys):
        typer.echo("Error: snapshot-ids and meeting-keys must have the same count.", err=True)
        raise typer.Exit(1)

    gp_configs = [
        {"meeting_key": mk, "season": season, "snapshot_id": sid}
        for mk, sid in zip(mkeys, sids, strict=True)
    ]

    settings = get_settings()
    with db_session(settings.database_url) as session:
        context = PipelineContext(db=session, execute=execute)
        result = run_walk_forward_backtest(
            context,
            gp_configs=gp_configs,
            strategy_name=strategy_name,
            model_name=model_name,
            min_edge=min_edge,
            bet_size=bet_size,
        )

        if execute:
            report_path = save_backtest_report(
                context,
                result,
                slug=f"{season}-season-backtest",
                title=f"{season} Season Backtest (R1–R{len(gp_configs)})",
            )
            typer.echo(f"Report saved: {report_path}")
        else:
            typer.echo(result)


@app.command("worker")
def worker() -> None:
    typer.echo("Worker heartbeat loop started. Use the CLI to trigger ingestion jobs.")
    while True:
        time.sleep(30)


if __name__ == "__main__":
    app()
