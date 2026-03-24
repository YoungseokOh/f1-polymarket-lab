# f1-polymarket-lab

Research platform for Formula 1 prediction-market data ingestion, feature engineering, modeling, backtesting, and paper trading.

## Quick start

```bash
cp .env.example .env
# set a local-only POSTGRES_PASSWORD in .env before starting infra
make bootstrap
make infra-up
make db-upgrade
make ingest-demo
make api
make web
```

Override the demo backfill scope if needed:

```bash
make ingest-demo DEMO_WEEKENDS=2 DEMO_MARKET_BATCHES=3
```

## Stack

- **Ingestion:** OpenF1, F1DB (Jolpica), Polymarket REST + WebSocket
- **Storage:** SQLite (local) / PostgreSQL (prod), Alembic migrations, Bronze/Silver data lake
- **Features:** Snapshot-based feature registry with walk-forward splits
- **Models:** XGBoost / LightGBM with Optuna tuning and isotonic calibration
- **Backtesting:** Walk-forward backtest engine with PnL and calibration metrics
- **Paper trading:** Signal engine that evaluates YES/NO edges against live Polymarket prices
- **API:** FastAPI with typed Pydantic schemas
- **Dashboard:** Next.js with TypeScript SDK and shared UI components

## Collection workflow

For the F1-focused ingestion path:

```bash
make backfill-f1-history-all
make sync-polymarket-f1-catalog
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

For a post-session weekend update (≥45 min after session end):

```bash
SESSION=<SESSION_KEY>
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-f1-session --session-key $SESSION --extended --include-heavy --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli discover-session-polymarket --session-key $SESSION --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-polymarket-f1-history --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

For a weekend validation pass:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli validate-f1-weekend-subset --meeting-key <MEETING_KEY> --season <SEASON> --execute
```

`--validation-mode smoke` (default) runs heavy OpenF1 hydration for Q/SQ/R only. Use `--validation-mode full` for all sessions.

For live event capture:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli capture-live-weekend --session-key <SESSION_KEY> --execute
```

## GP paper trading workflow

After FP1 ends, build a snapshot and run paper trade signals:

```bash
make db-upgrade
uv run python -m f1_polymarket_worker.cli run-<gp-code>-fp1-paper-trade --execute
```

## Worker package layout

| Module | Purpose |
|---|---|
| `pipeline/` | Core ETL context, F1 sync, Polymarket sync, mappings, DQ |
| `orchestration.py` | Re-export shim (see sub-modules below) |
| `market_discovery.py` | Polymarket catalog sync and session discovery scoring |
| `f1_backfill.py` | Multi-season F1 history and Polymarket history backfill |
| `weekend_ops.py` | Weekend validation reports and live capture |
| `historical.py` | Jolpica and OpenF1 season-range historical backfill |
| `gp_registry.py` | GP configurations, pre-weekend snapshots, baseline models |
| `backtest.py` | Walk-forward backtest engine |
| `paper_trading.py` | Paper trading signal engine and position tracking |

## Agent setup

- Codex repo instructions: `AGENTS.md`
- Claude repo instructions: `CLAUDE.md`
- Shared portable skills: `skills/`
- Commit convention: `docs/engineering/commit-convention.md`
