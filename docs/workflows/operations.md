# Operational Workflows

## Initial Setup

```bash
cp .env.example .env
make bootstrap
make infra-up
make db-upgrade
make ingest-demo
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

## Data Quality

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

## Dev Commands

```bash
make api       # FastAPI on :8000
make web       # Next.js on :3000
make lint      # ruff + biome
make test      # pytest + vitest
make typecheck # mypy + tsc
make format    # ruff format + biome format
```
