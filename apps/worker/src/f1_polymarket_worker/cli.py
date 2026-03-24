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
from f1_polymarket_worker.gp_registry import (
    GP_REGISTRY,
    build_snapshot,
    generate_report,
    run_baseline,
)
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




# ---------------------------------------------------------------------------
# Dynamic GP quicktest commands — auto-registered from GP_REGISTRY
# ---------------------------------------------------------------------------
def _register_gp_commands() -> None:
    """Register build/run/report commands for every GP in the registry."""
    for gp in GP_REGISTRY:
        code = gp.short_code.replace("_", "-")

        # -- build --
        def _make_build(cfg=gp):  # noqa: B006
            def _cmd(
                meeting_key: int = typer.Option(cfg.meeting_key, "--meeting-key"),
                season: int = typer.Option(cfg.season, "--season"),
                entry_offset_min: int = typer.Option(cfg.entry_offset_min, "--entry-offset-min"),
                fidelity: int = typer.Option(cfg.fidelity, "--fidelity"),
                execute: bool = typer.Option(False, "--execute/--plan-only"),
            ) -> None:
                settings = get_settings()
                with db_session(settings.database_url) as session:
                    context = PipelineContext(db=session, execute=execute)
                    result = build_snapshot(
                        context,
                        cfg,
                        meeting_key=meeting_key,
                        season=season,
                        entry_offset_min=entry_offset_min,
                        fidelity=fidelity,
                    )
                typer.echo(result)
            return _cmd

        app.command(f"build-{code}-snapshot")(_make_build())

        # -- run baseline --
        def _make_run(cfg=gp):  # noqa: B006
            def _cmd(
                snapshot_id: str = typer.Option(..., "--snapshot-id"),
                min_edge: float = typer.Option(cfg.min_edge, "--min-edge"),
                execute: bool = typer.Option(False, "--execute/--plan-only"),
            ) -> None:
                settings = get_settings()
                with db_session(settings.database_url) as session:
                    context = PipelineContext(db=session, execute=execute)
                    result = run_baseline(
                        context, cfg, snapshot_id=snapshot_id, min_edge=min_edge
                    )
                typer.echo(result)
            return _cmd

        app.command(f"run-{code}-baseline")(_make_run())

        # -- report --
        def _make_report(cfg=gp):  # noqa: B006
            def _cmd(
                snapshot_id: str = typer.Option(..., "--snapshot-id"),
                report_slug: str | None = typer.Option(None, "--report-slug"),
                min_edge: float = typer.Option(cfg.min_edge, "--min-edge"),
                execute: bool = typer.Option(False, "--execute/--plan-only"),
            ) -> None:
                settings = get_settings()
                with db_session(settings.database_url) as session:
                    context = PipelineContext(db=session, execute=execute)
                    result = generate_report(
                        context,
                        cfg,
                        snapshot_id=snapshot_id,
                        report_slug=report_slug,
                        min_edge=min_edge,
                    )
                typer.echo(result)
            return _cmd

        app.command(f"report-{code}-quicktest")(_make_report())

        # -- paper trade (build + baseline + paper trade in one shot) --
        def _make_paper_trade(cfg=gp):  # noqa: B006
            def _cmd(
                snapshot_id: str | None = typer.Option(
                    None,
                    "--snapshot-id",
                    help="Use existing snapshot; omit to build fresh from FP1 data.",
                ),
                baseline: str = typer.Option(
                    "hybrid",
                    "--baseline",
                    help="Which baseline to trade: market_implied | fp1_pace | hybrid",
                ),
                min_edge: float = typer.Option(cfg.min_edge, "--min-edge"),
                bet_size: float = typer.Option(10.0, "--bet-size"),
                max_daily_loss: float = typer.Option(100.0, "--max-daily-loss"),
                execute: bool = typer.Option(False, "--execute/--plan-only"),
            ) -> None:
                """One-shot pipeline: build snapshot → run baselines → paper trade."""
                import polars as pl_module

                from f1_polymarket_worker.paper_trading import (  # noqa: PLC0415
                    PaperTradeConfig,
                    PaperTradingEngine,
                )

                settings = get_settings()
                with db_session(settings.database_url) as session:
                    context = PipelineContext(db=session, execute=execute)

                    # Step 1: build snapshot if not provided
                    if snapshot_id is None:
                        typer.echo(f"[1/3] Building {cfg.short_code} snapshot...")
                        snap_result = build_snapshot(
                            context,
                            cfg,
                            meeting_key=cfg.meeting_key,
                            season=cfg.season,
                            entry_offset_min=cfg.entry_offset_min,
                            fidelity=cfg.fidelity,
                        )
                        used_snapshot_id = snap_result.get("snapshot_id")
                        if not used_snapshot_id:
                            typer.echo(f"Snapshot build result: {snap_result}")
                            raise typer.Exit(1)
                        typer.echo(f"  snapshot_id={used_snapshot_id}")
                    else:
                        used_snapshot_id = snapshot_id
                        typer.echo(f"[1/3] Using existing snapshot_id={used_snapshot_id}")

                    # Step 2: run baselines
                    typer.echo(f"[2/3] Running baselines for {cfg.short_code}...")
                    baseline_result = run_baseline(
                        context, cfg, snapshot_id=used_snapshot_id, min_edge=min_edge
                    )
                    model_run_ids: list[str] = baseline_result.get("model_runs", [])
                    if not model_run_ids:
                        typer.echo(f"Baseline result: {baseline_result}")
                        raise typer.Exit(1)

                    # Pick the requested baseline by name
                    baseline_idx = {"market_implied": 0, "fp1_pace": 1, "hybrid": 2}.get(
                        baseline, 2
                    )
                    used_model_run_id = model_run_ids[min(baseline_idx, len(model_run_ids) - 1)]
                    typer.echo(f"  baseline={baseline}, model_run_id={used_model_run_id}")

                    if not execute:
                        typer.echo(
                            f"[plan] Would paper-trade with model_run_id={used_model_run_id}"
                        )
                        return

                    # Step 3: paper trade
                    typer.echo("[3/3] Running paper trade...")
                    from f1_polymarket_lab.storage.models import FeatureSnapshot, ModelPrediction
                    from sqlalchemy import select

                    preds = session.scalars(
                        select(ModelPrediction).where(
                            ModelPrediction.model_run_id == used_model_run_id
                        )
                    ).all()
                    snap = session.get(FeatureSnapshot, used_snapshot_id)

                    if not preds:
                        typer.echo("No predictions found for this model run.")
                        raise typer.Exit(1)

                    snapshot_df = (
                        pl_module.read_parquet(snap.storage_path)
                        if snap and snap.storage_path
                        else None
                    )
                    price_lookup: dict[str, float] = {}
                    label_lookup: dict[str, bool] = {}
                    if snapshot_df is not None:
                        for row in snapshot_df.to_dicts():
                            mid = row.get("market_id")
                            if mid:
                                price_lookup[mid] = float(row.get("entry_yes_price", 0.5))
                                label = row.get("label_yes")
                                if label is not None:
                                    label_lookup[mid] = bool(int(label))

                    engine = PaperTradingEngine(
                        config=PaperTradeConfig(
                            min_edge=min_edge,
                            bet_size=bet_size,
                            max_daily_loss=max_daily_loss,
                        )
                    )
                    for pred in preds:
                        market_price = price_lookup.get(pred.market_id or "", 0.5)
                        engine.evaluate_signal(
                            market_id=pred.market_id or "",
                            token_id=pred.token_id,
                            model_prob=pred.probability_yes or 0.5,
                            market_price=market_price,
                        )
                    for mid, outcome in label_lookup.items():
                        engine.settle_position(mid, outcome)

                    summary = engine.summary()
                    log_path = (
                        settings.data_root
                        / "reports"
                        / "paper_trading"
                        / f"{cfg.short_code}_{used_model_run_id}.json"
                    )
                    engine.save_log(log_path)
                    pt_session_id = engine.persist(
                        session,
                        gp_slug=cfg.short_code,
                        snapshot_id=used_snapshot_id,
                        model_run_id=used_model_run_id,
                        log_path=log_path,
                    )
                    session.commit()

                    typer.echo(
                        f"Paper trading complete: {summary['trades_executed']} trades, "
                        f"PnL: ${summary['total_pnl']:.2f}, "
                        f"win rate: {summary.get('win_rate') or 0:.1%}"
                    )
                    typer.echo(f"  Session ID: {pt_session_id}")
                    typer.echo(f"  Log: {log_path}")

            return _cmd

        app.command(f"run-{code}-paper-trade")(_make_paper_trade())


_register_gp_commands()


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


@app.command("train-xgb-walk-forward")
def train_xgb_walk_forward_command(
    snapshot_ids: str = typer.Option(
        ..., "--snapshot-ids", help="Comma-separated snapshot IDs",
    ),
    meeting_keys: str = typer.Option(
        ..., "--meeting-keys", help="Comma-separated meeting keys",
    ),
    stage: str = typer.Option("xgb_pole_quicktest", "--stage"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    min_train_gps: int = typer.Option(
        2, "--min-train-gps", help="Min training GPs",
    ),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Train XGBoost walk-forward across GP snapshots."""
    from f1_polymarket_lab.common import stable_uuid
    from f1_polymarket_lab.models import build_walk_forward_splits, train_one_split
    from f1_polymarket_lab.storage.models import ModelPrediction, ModelRun
    from f1_polymarket_lab.storage.repository import upsert_records

    sids = [s.strip() for s in snapshot_ids.split(",")]
    mkeys = [int(k.strip()) for k in meeting_keys.split(",")]
    if len(sids) != len(mkeys):
        typer.echo("Error: snapshot-ids and meeting-keys must have the same count.", err=True)
        raise typer.Exit(1)

    splits = build_walk_forward_splits(mkeys, min_train=min_train_gps)
    if not splits:
        typer.echo(f"Need at least {min_train_gps + 1} GPs for walk-forward training.")
        raise typer.Exit(1)

    key_to_sid = dict(zip(mkeys, sids, strict=True))

    if not execute:
        for sp in splits:
            typer.echo(f"[plan] train on {sp.train_meeting_keys} → test {sp.test_meeting_key}")
        return

    settings = get_settings()
    with db_session(settings.database_url) as session:
        from f1_polymarket_lab.storage.models import FeatureSnapshot

        all_results = []
        for sp in splits:
            train_paths = []
            for mk in sp.train_meeting_keys:
                snap = session.get(FeatureSnapshot, key_to_sid[mk])
                if snap and snap.storage_path:
                    train_paths.append(snap.storage_path)
            test_snap = session.get(FeatureSnapshot, key_to_sid[sp.test_meeting_key])
            if not test_snap or not test_snap.storage_path:
                typer.echo(f"Skipping test meeting_key={sp.test_meeting_key}: snapshot not found")
                continue
            if not train_paths:
                typer.echo(f"Skipping test meeting_key={sp.test_meeting_key}: no training data")
                continue

            import polars as pl_module

            train_df = pl_module.concat([pl_module.read_parquet(p) for p in train_paths])
            test_df = pl_module.read_parquet(test_snap.storage_path)

            model_run_id = stable_uuid("xgb-run", key_to_sid[sp.test_meeting_key], stage)
            result = train_one_split(
                train_df, test_df,
                model_run_id=model_run_id,
                stage=stage,
                min_edge=min_edge,
            )

            run_record = ModelRun(
                id=result.model_run_id,
                stage=stage,
                model_family="xgboost",
                model_name="xgb_walk_forward",
                dataset_version=test_snap.feature_version,
                feature_snapshot_id=test_snap.id,
                test_start=test_snap.as_of_ts,
                test_end=test_snap.as_of_ts,
                config_json=result.config,
                metrics_json=result.metrics,
            )
            upsert_records(session, ModelRun, [run_record], key_columns=["id"])

            pred_records = [ModelPrediction(**p) for p in result.predictions]
            upsert_records(
                session, ModelPrediction, pred_records,
                key_columns=["model_run_id", "market_id"],
            )

            session.commit()
            all_results.append(result)
            typer.echo(
                f"GP {sp.test_meeting_key}: brier={result.metrics['brier_score']:.4f} "
                f"log_loss={result.metrics['log_loss']:.4f} bets={result.metrics['bet_count']}"
            )

        typer.echo(f"Walk-forward training complete: {len(all_results)} folds evaluated.")


@app.command("train-lgbm-walk-forward")
def train_lgbm_walk_forward_command(
    snapshot_ids: str = typer.Option(
        ..., "--snapshot-ids", help="Comma-separated snapshot IDs",
    ),
    meeting_keys: str = typer.Option(
        ..., "--meeting-keys", help="Comma-separated meeting keys",
    ),
    stage: str = typer.Option("lgbm_pole_quicktest", "--stage"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    min_train_gps: int = typer.Option(
        2, "--min-train-gps", help="Min training GPs",
    ),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Train LightGBM walk-forward across GP snapshots."""
    from f1_polymarket_lab.common import stable_uuid
    from f1_polymarket_lab.models import build_walk_forward_splits, train_one_split_lgbm
    from f1_polymarket_lab.storage.models import FeatureSnapshot, ModelPrediction, ModelRun
    from f1_polymarket_lab.storage.repository import upsert_records

    sids = [s.strip() for s in snapshot_ids.split(",")]
    mkeys = [int(k.strip()) for k in meeting_keys.split(",")]
    if len(sids) != len(mkeys):
        typer.echo("Error: snapshot-ids and meeting-keys must have the same count.", err=True)
        raise typer.Exit(1)

    splits = build_walk_forward_splits(mkeys, min_train=min_train_gps)
    if not splits:
        typer.echo(f"Need at least {min_train_gps + 1} GPs for walk-forward training.")
        raise typer.Exit(1)

    key_to_sid = dict(zip(mkeys, sids, strict=True))

    if not execute:
        for sp in splits:
            typer.echo(f"[plan] train on {sp.train_meeting_keys} → test {sp.test_meeting_key}")
        return

    settings = get_settings()
    with db_session(settings.database_url) as session:
        import polars as pl_module

        all_results = []
        for sp in splits:
            train_paths = []
            for mk in sp.train_meeting_keys:
                snap = session.get(FeatureSnapshot, key_to_sid[mk])
                if snap and snap.storage_path:
                    train_paths.append(snap.storage_path)
            test_snap = session.get(FeatureSnapshot, key_to_sid[sp.test_meeting_key])
            if not test_snap or not test_snap.storage_path:
                typer.echo(f"Skipping test meeting_key={sp.test_meeting_key}: snapshot not found")
                continue
            if not train_paths:
                typer.echo(f"Skipping test meeting_key={sp.test_meeting_key}: no training data")
                continue

            train_df = pl_module.concat([pl_module.read_parquet(p) for p in train_paths])
            test_df = pl_module.read_parquet(test_snap.storage_path)

            model_run_id = stable_uuid("lgbm-run", key_to_sid[sp.test_meeting_key], stage)
            result = train_one_split_lgbm(
                train_df, test_df,
                model_run_id=model_run_id,
                stage=stage,
                min_edge=min_edge,
            )

            run_record = ModelRun(
                id=result.model_run_id,
                stage=stage,
                model_family="lightgbm",
                model_name="lgbm_walk_forward",
                dataset_version=test_snap.feature_version,
                feature_snapshot_id=test_snap.id,
                test_start=test_snap.as_of_ts,
                test_end=test_snap.as_of_ts,
                config_json=result.config,
                metrics_json=result.metrics,
            )
            upsert_records(session, ModelRun, [run_record], key_columns=["id"])

            pred_records = [ModelPrediction(**p) for p in result.predictions]
            upsert_records(
                session, ModelPrediction, pred_records,
                key_columns=["model_run_id", "market_id"],
            )

            session.commit()
            all_results.append(result)
            typer.echo(
                f"GP {sp.test_meeting_key}: brier={result.metrics['brier_score']:.4f} "
                f"log_loss={result.metrics['log_loss']:.4f} bets={result.metrics['bet_count']}"
            )

        typer.echo(f"LightGBM walk-forward training complete: {len(all_results)} folds evaluated.")


@app.command("tune-xgb-optuna")
def tune_xgb_optuna_command(
    snapshot_ids: str = typer.Option(
        ..., "--snapshot-ids", help="Comma-separated snapshot IDs",
    ),
    meeting_keys: str = typer.Option(
        ..., "--meeting-keys", help="Comma-separated meeting keys",
    ),
    stage: str = typer.Option("xgb_pole_quicktest", "--stage"),
    n_trials: int = typer.Option(50, "--n-trials"),
    min_train_gps: int = typer.Option(
        2, "--min-train-gps", help="Min training GPs",
    ),
) -> None:
    """Run Optuna hyperparameter search for XGBoost."""
    from f1_polymarket_lab.models import tune_xgb
    from f1_polymarket_lab.storage.models import FeatureSnapshot

    sids = [s.strip() for s in snapshot_ids.split(",")]
    mkeys = [int(k.strip()) for k in meeting_keys.split(",")]
    if len(sids) != len(mkeys):
        typer.echo("Error: snapshot-ids and meeting-keys must have the same count.", err=True)
        raise typer.Exit(1)

    settings = get_settings()
    with db_session(settings.database_url) as session:
        import polars as pl_module

        dataframes: dict[int, pl_module.DataFrame] = {}
        key_to_sid = dict(zip(mkeys, sids, strict=True))
        for mk in mkeys:
            snap = session.get(FeatureSnapshot, key_to_sid[mk])
            if snap and snap.storage_path:
                dataframes[mk] = pl_module.read_parquet(snap.storage_path)

    result = tune_xgb(
        dataframes,
        mkeys,
        stage=stage,
        n_trials=n_trials,
        min_train_gps=min_train_gps,
    )
    typer.echo(f"Best log_loss: {result.get('best_log_loss', 'N/A')}")
    typer.echo(f"Best params: {result.get('best_params', {})}")


@app.command("paper-trade")
def paper_trade_command(
    snapshot_id: str = typer.Option(..., "--snapshot-id"),
    model_run_id: str = typer.Option(..., "--model-run-id"),
    gp_slug: str = typer.Option("unknown", "--gp-slug"),
    min_edge: float = typer.Option(0.05, "--min-edge"),
    bet_size: float = typer.Option(10.0, "--bet-size"),
    max_daily_loss: float = typer.Option(100.0, "--max-daily-loss"),
    execute: bool = typer.Option(False, "--execute/--plan-only"),
) -> None:
    """Run paper trading simulation using model predictions."""
    from f1_polymarket_lab.storage.models import FeatureSnapshot, ModelPrediction

    from f1_polymarket_worker.paper_trading import PaperTradeConfig, PaperTradingEngine

    settings = get_settings()
    with db_session(settings.database_url) as session:
        from sqlalchemy import select

        preds = session.scalars(
            select(ModelPrediction).where(
                ModelPrediction.model_run_id == model_run_id
            )
        ).all()

        snap = session.get(FeatureSnapshot, snapshot_id)
        if not preds:
            typer.echo("No predictions found for this model run.")
            raise typer.Exit(1)

        if not execute:
            typer.echo(f"[plan] Would paper-trade {len(preds)} predictions")
            return

        import polars as pl_module

        snapshot_df = (
            pl_module.read_parquet(snap.storage_path) if snap and snap.storage_path else None
        )
        price_lookup: dict[str, float] = {}
        label_lookup: dict[str, bool] = {}
        if snapshot_df is not None:
            for row in snapshot_df.to_dicts():
                mid = row.get("market_id")
                if mid:
                    price_lookup[mid] = float(row.get("entry_yes_price", 0.5))
                    label = row.get("label_yes")
                    if label is not None:
                        label_lookup[mid] = bool(int(label))

        engine = PaperTradingEngine(
            config=PaperTradeConfig(
                min_edge=min_edge,
                bet_size=bet_size,
                max_daily_loss=max_daily_loss,
            )
        )

        for pred in preds:
            market_price = price_lookup.get(pred.market_id or "", 0.5)
            engine.evaluate_signal(
                market_id=pred.market_id or "",
                token_id=pred.token_id,
                model_prob=pred.probability_yes or 0.5,
                market_price=market_price,
            )

        # Settle against known outcomes
        for mid, outcome in label_lookup.items():
            engine.settle_position(mid, outcome)

        summary = engine.summary()
        log_path = settings.data_root / "reports" / "paper_trading" / f"{model_run_id}.json"
        engine.save_log(log_path)

        pt_session_id = engine.persist(
            session,
            gp_slug=gp_slug,
            snapshot_id=snapshot_id,
            model_run_id=model_run_id,
            log_path=log_path,
        )
        session.commit()

        typer.echo(f"Paper trading complete: {summary['trades_executed']} trades, "
                   f"PnL: ${summary['total_pnl']:.2f}")
        typer.echo(f"Session ID: {pt_session_id}")
        typer.echo(f"Log saved: {log_path}")


@app.command("worker")
def worker() -> None:
    typer.echo("Worker heartbeat loop started. Use the CLI to trigger ingestion jobs.")
    while True:
        time.sleep(30)


if __name__ == "__main__":
    app()
