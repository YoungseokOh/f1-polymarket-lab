# Operational Workflows

## Initial Setup

```bash
cp .env.example .env
make bootstrap
make infra-up
make db-upgrade
make ingest-demo
```

`make bootstrap` installs the workspace Python packages plus the optional
modeling dependencies needed for the full local test suite. On macOS it also
verifies the `libomp` runtime that LightGBM needs, using Homebrew bottles when
available and a source-build fallback on older Intel macOS installs.

If you already have local data in `data/lab.db`, migrate it into Postgres before
continuing:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli migrate-sqlite-to-postgres --plan-only
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli migrate-sqlite-to-postgres --execute
```

## Full Historical Backfill

```bash
# 1950–2022: F1DB + Jolpica
make bootstrap-f1db-history
make sync-jolpica-history

# 2023+: OpenF1
make backfill-f1-history

# Or all-in-one:
make backfill-f1-history-all

# Polymarket catalog
make sync-polymarket-f1-catalog
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

## Post-Session Update

Run ≥45 min after a session ends:

```bash
SESSION=<SESSION_KEY>
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-f1-session \
  --session-key $SESSION --extended --include-heavy --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli discover-session-polymarket \
  --session-key $SESSION --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-polymarket-f1-history --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

To rebuild labeled snapshots, baseline runs, and settled backtests for the
matching GP stage after results arrive:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli backfill-backtests

# Use this only when you explicitly want to reuse stored snapshots as-is.
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli backfill-backtests --stored-only
```

## Weekend Validation

```bash
# Smoke mode (Q/SQ/R only get heavy OpenF1 hydration — default)
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli validate-f1-weekend-subset \
  --meeting-key <MEETING_KEY> --season <SEASON> --execute

# Full mode (heavy hydration for every session)
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli validate-f1-weekend-subset \
  --meeting-key <MEETING_KEY> --season <SEASON> --validation-mode full --execute
```

Reports are written to `data/reports/validation/<season>/<slug>/summary.{json,md}`.

## GP Paper Trading

After FP1 ends, build a snapshot and run the paper trading signal engine:

```bash
make db-upgrade   # run once to ensure tables exist
uv run python -m f1_polymarket_worker.cli run-<gp-code>-fp1-paper-trade --execute
```

Available GP codes: `japan`, `china`, `australia` (and others registered in `gp_registry.py`).

To check paper trading results via the API:

```bash
make api
# GET /api/v1/paper-trading/sessions
# GET /api/v1/paper-trading/sessions/{id}/positions
```

## Miami Live Trading

Miami sprint-weekend live trading is split into four GP registry entries:

- `miami_fp1_sq` for Sprint Qualifying pole tickets
- `miami_sq_sprint` for Sprint winner tickets
- `miami_fp1_q` for Qualifying pole tickets
- `miami_q_r` for Race winner tickets

Promoted champions are required per session stage:

- `sq_pole_live_v1` for `miami_fp1_sq`
- `sprint_winner_live_v1` for `miami_sq_sprint`
- `multitask_qr` for `miami_fp1_q` and `miami_q_r`

The live trading path is operator-assisted. Create the ticket in the cockpit, place the order in the
Polymarket browser, then record the actual fill back into the system.

```bash
make db-upgrade
make api
make web
```

Keep the live path disabled until the manual go-live checklist is complete:

```bash
LIVE_TRADING_ENABLED=false
LIVE_TRADING_READINESS_CONFIRMED=false
LIVE_QUOTE_MAX_AGE_SEC=90
```

Flip `LIVE_TRADING_ENABLED=true` only when you want manual operator tickets to be creatable, and
flip `LIVE_TRADING_READINESS_CONFIRMED=true` only after jurisdiction, account, and Miami rehearsal
checks are complete. Setting `LIVE_TRADING_ENABLED=false` is also the emergency stop path.

The API routes behind the cockpit are:

```text
GET  /api/v1/live-trading/signal-board
GET  /api/v1/live-trading/tickets
GET  /api/v1/live-trading/executions
POST /api/v1/actions/create-live-trade-ticket
POST /api/v1/actions/record-live-trade-fill
POST /api/v1/actions/cancel-live-trade-ticket
```

Miami v1 live-ticket creation will block when any of the following are true:

- live operator tickets are disabled or the readiness flag is still off
- the session's required promoted champion is missing
- the selected snapshot or artifact version is incompatible
- no live quote is available
- the live quote is stale
- the observed spread is wider than the configured maximum
- the requested ticket size exceeds the configured conservative cap
- the requested min edge or spread override is looser than the configured limits
- the daily loss budget has already been consumed
- the target session is outside the live trading window
- another open live ticket already exists for the same market

## Live Event Capture

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli capture-live-weekend \
  --session-key <SESSION_KEY> --execute
```

## Modeling

```bash
# Build feature snapshot
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli build-<gp>-fp1-to-sq-snapshot \
  --meeting-key <MEETING_KEY> --season <SEASON> --execute

# Run baseline model
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli run-<gp>-sq-pole-baseline \
  --snapshot-id <SNAPSHOT_ID> --execute
```

For multitask Q/R research, treat the commands as two separate layers:

```bash
# Real snapshot + walk-forward training path
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli build-multitask-qr-snapshots \
  --meeting-key <MEETING_KEY> --season <SEASON> --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli train-multitask-walk-forward \
  --manifest data/feature_snapshots/multitask/<SEASON>/manifest.json --execute

# Manifest-based candidate search with walk-forward evaluation
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli run-multitask-autoresearch \
  --output-dir data/experiments/autoresearch/multitask_qr --iterations 20
```

## Data Quality

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

Checks marked with warning severity record `warning` rather than `fail` when optional datasets such
as live websocket manifests are absent. Error-severity checks still fail the result.

## Dev Commands

```bash
make api       # FastAPI on :8000
make web       # Next.js on :3000
make worker    # DB-backed queued job worker
make lint      # ruff + biome
make test      # pytest + vitest
make typecheck # mypy + tsc
make format    # ruff format + biome format
```

Queued API actions and `queue-job` commands write `ingestion_job_runs` rows with `status=queued`.
The worker records attempt counts, locks the claimed row, retries with backoff when configured, and
marks stale worker locks recoverable after the configured timeout. Use lineage to follow queued,
running, completed, and failed jobs.

The generic queue dispatcher covers ingestion, calendar and market syncs, backtests, paper trading,
weekend cockpit refreshes, latest-session refreshes, driver-affinity report refreshes, live weekend
capture, and data-quality checks. Live ticket create/fill/cancel routes intentionally stay
synchronous because the operator must receive an immediate write result before taking the next
manual trading step.
