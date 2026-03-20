# f1-polymarket-lab

Research platform for Formula 1 prediction-market data ingestion, feature snapshots, modeling, backtesting, and analyst-facing exploration.

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

## Current slice

- Phase 0: monorepo, tooling, local infra, CI, base docs.
- Thin Phase 1: OpenF1 and Polymarket connectors, Bronze/Silver persistence, normalized schema, API browsing endpoints, and a seeded local dashboard.

## Collection workflow

For the F1-focused ingestion path, use the staged commands below:

```bash
make backfill-f1-history-all
make sync-polymarket-f1-catalog
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

If you want the historical layers separately, run:

```bash
make bootstrap-f1db-history
make sync-jolpica-history
make backfill-f1-history
make sync-polymarket-f1-catalog
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli discover-session-polymarket --session-key <SESSION_KEY> --execute
make hydrate-polymarket-f1-history
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

`backfill-f1-history-all` treats `1950-2022` as `F1DB + Jolpica` history and `2023+` as `OpenF1` canonical history. The current pre-2023 scope is results, qualifying, sprint, laps, pit stops, and schedule metadata; richer telemetry stays on the `2023+ OpenF1` path.

For `2023+`, the modern OpenF1 backfill now keeps race-weekend sessions only: `FP1/FP2/FP3/Q/SQ/S/R`. Pre-season testing days are excluded from the normalized session catalog, and `backfill-f1-history` defaults to `--heavy-mode weekend`, which means all retained weekend sessions collect heavy `car_data` and `location`.

F1 timing normalization keeps source display values and typed semantics separately. `f1_intervals` now records explicit `laps_behind` states for values like `+1 LAP`, and `f1_session_results` stores generic `result_time_*` fields plus explicit `dnf/dns/dsq` instead of overloading everything into `fastest_lap_seconds`.

`sync-polymarket-f1-catalog` uses F1 tags first and then public search plus slug hydration to cover historical F1 markets that are missing current tag metadata. The practical historical Polymarket sweep starts at `2022`.

For a free post-session weekend workflow, wait until a target session is at least 45 minutes past end time, then run:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-f1-session --session-key <SESSION_KEY> --extended --include-heavy --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli discover-session-polymarket --session-key <SESSION_KEY> --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-polymarket-f1-history --execute
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli reconcile-mappings
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli dq-run
```

For a focused sprint-weekend validation pass such as the `2026 Chinese Grand Prix`, run:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli validate-f1-weekend-subset --meeting-key 1280 --season 2026 --validation-mode smoke --execute
```

`validate-f1-weekend-subset` now defaults to `--validation-mode smoke`, which only runs heavy OpenF1 hydration for `Q/SQ/R`. Use `--validation-mode full` if you want heavy telemetry for every retained weekend session. OpenF1 free-tier throttling is also configurable through `.env` via `OPENF1_MAX_REQUESTS_PER_MINUTE` and `OPENF1_MAX_REQUESTS_PER_SECOND`; the repo defaults are conservative (`24/min`, `2/sec`) so validation runs are less likely to stall on rate-limit retries.

This writes a reusable subset report under `data/reports/validation/<season>/<report-slug>/summary.{json,md}` with per-session F1 counts, Polymarket discovery/mapping summaries, representative market probes, and a research-readiness verdict.

For event-weekend live capture, run:

```bash
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli capture-live-weekend --session-key <SESSION_KEY> --execute
```

## Agent setup

- Codex repo instructions: `AGENTS.md`
- Claude repo instructions: `CLAUDE.md`
- Shared portable skills: `skills/`
- Commit convention: `docs/engineering/commit-convention.md`
