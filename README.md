# f1-polymarket-lab

Research platform for Formula 1 prediction-market data ingestion, feature engineering, modeling, backtesting, paper trading, and operator-assisted live trading.

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

`make bootstrap` installs the shared workspace packages, base dev tooling, and
the modeling stack so the full Python test suite, including multitask trainer
coverage, can run locally. On macOS it also verifies the `libomp` runtime that
LightGBM needs. Sonoma-or-newer machines use the Homebrew bottle path; older
Intel macOS installs fall back to a local source build so `lightgbm` imports
without manual runtime fixes.

To migrate an existing local SQLite lab database into Postgres:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli migrate-sqlite-to-postgres --plan-only
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli migrate-sqlite-to-postgres --execute
```

Override the demo backfill scope if needed:

```bash
make ingest-demo DEMO_WEEKENDS=2 DEMO_MARKET_BATCHES=3
```

## Stack

- **Ingestion:** OpenF1, F1DB (Jolpica), Polymarket REST + WebSocket
- **Storage:** PostgreSQL-first relational storage, Alembic migrations, Bronze/Silver data lake, SQLite source artifacts where required
- **Features:** Snapshot-based feature registry with walk-forward splits
- **Models:** XGBoost / LightGBM with Optuna tuning and isotonic calibration
- **Backtesting:** Walk-forward backtest engine with PnL and calibration metrics
- **Paper trading:** Signal engine that evaluates YES/NO edges against live Polymarket prices
- **Live trading:** Operator ticket flow for manual browser execution with recorded fills and risk blocks
- **API:** FastAPI with typed Pydantic schemas
- **Dashboard:** Next.js with TypeScript SDK and shared UI components

There is no queue-backed background worker in the current slice. Collection and hydration run through
explicit CLI commands.

## Ops calendar authority

Weekend ops no longer rely on a static per-GP calendar. `f1_meetings` carries the synced base
schedule, while `f1_calendar_overrides` is the final authority layer for official changes such as
cancelled or postponed Grands Prix. Ops stages are generated dynamically from the effective
calendar, so sprint weekends like Miami can activate without a hardcoded registry edit.

Useful commands:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli set-f1-calendar-override \
  --meeting-slug saudi-arabian-grand-prix \
  --status cancelled \
  --source-label "Formula 1 official" \
  --source-url "<OFFICIAL_URL>" \
  --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli clear-f1-calendar-override \
  --meeting-slug saudi-arabian-grand-prix \
  --execute
```

The API exposes the same authority layer through:

```text
GET  /api/v1/ops-calendar
POST /api/v1/actions/set-calendar-override
POST /api/v1/actions/clear-calendar-override
```

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

Qualifying and race weekend ops now require an active promoted `multitask_qr` champion. Train the
walk-forward model, promote the selected run, then score a live snapshot:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli train-multitask-walk-forward \
  --manifest data/feature_snapshots/multitask/2026/manifest.json \
  --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli promote-model-run \
  --model-run-id <MODEL_RUN_ID> \
  --stage multitask_qr \
  --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli score-multitask-snapshot \
  --snapshot-id <SNAPSHOT_ID> \
  --stage multitask_qr \
  --execute
```

`train-multitask-walk-forward` logs runs to MLflow experiment `weekend_ops.multitask_qr`,
persists the MLflow run id on `model_runs.registry_run_id`, and weekend paper trading will hard-fail
when no active promotion exists for the required stage.

## Miami live trading workflow

Miami live trading is configured as four session-specific stages in `gp_registry.py`:

- `miami_fp1_sq` -> requires promoted `sq_pole_live_v1`
- `miami_sq_sprint` -> requires promoted `sprint_winner_live_v1`
- `miami_fp1_q` -> requires promoted `multitask_qr`
- `miami_q_r` -> requires promoted `multitask_qr`

The current live path is operator-assisted:

1. Run `make db-upgrade` so `live_trade_tickets` and `live_trade_executions` exist.
2. Start `make api` and `make web`.
3. Use the weekend cockpit to capture live quotes and create a ticket for the active stage.
4. Submit the actual order in the Polymarket browser UI.
5. Record the fill back into the cockpit.

The API also exposes the live flow directly:

```text
GET  /api/v1/live-trading/signal-board
GET  /api/v1/live-trading/tickets
GET  /api/v1/live-trading/executions
POST /api/v1/actions/create-live-trade-ticket
POST /api/v1/actions/record-live-trade-fill
POST /api/v1/actions/cancel-live-trade-ticket
```

Ticket creation will block when the required promoted champion is missing, the live quote spread is
too wide, the daily loss budget is exhausted, or another open live ticket already exists for the
same market.

## Multitask Q/R research workflow

Build checkpoint-aware snapshots:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli build-multitask-qr-snapshots \
  --meeting-key 1281 \
  --season 2026 \
  --execute
```

Train the shared-encoder model with walk-forward splits:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli train-multitask-walk-forward \
  --manifest data/feature_snapshots/multitask/2026/manifest.json \
  --execute
```

Run the experiment loop:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli run-multitask-autoresearch \
  --output-dir data/experiments/autoresearch/multitask_qr \
  --iterations 20
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
