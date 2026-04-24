# Architecture Overview

- `apps/web`: analyst-facing dashboard and explorer UI.
- `apps/api`: FastAPI service exposing normalized entities, freshness, and lineage.
- `apps/worker`: explicit one-shot ingestion/orchestration CLI entrypoints plus the DB-backed worker loop for queued jobs.
- `apps/worker/src/f1_polymarket_worker/live_trading.py`: operator ticket creation, fill logging, and live risk checks for Miami-style manual execution.
- `apps/worker/src/f1_polymarket_worker/multitask_snapshot.py`: checkpoint-aware qualifying/race snapshot generation.
- `apps/worker/src/f1_polymarket_worker/model_registry.py`: MLflow logging, promotion gates, champion lookup, and promoted-snapshot scoring.
- `apps/worker/src/f1_polymarket_worker/ops_calendar.py`: effective ops calendar resolution, schedule overrides, and dynamic weekend stage generation.
- `py/connectors`: source adapters for OpenF1, FastF1, and Polymarket.
- `py/storage`: lake and relational persistence.
- `py/models/src/f1_polymarket_lab/models/multitask_model.py`: shared encoder with family-specific heads.
- `py/experiments/src/f1_polymarket_lab/experiments/autoresearch.py`: screening and promotion loop for fixed-evaluator research.
- `py/features`, `py/models`, `py/experiments`, `py/agent`: downstream modeling and assistant layers.

The worker loop claims `queued` rows from `ingestion_job_runs`, records attempts and worker locks,
backs off retryable failures, and recovers stale queued-job locks. The generic dispatcher currently
handles `ingest-demo`, `sync-f1-calendar`, `sync-f1-markets`, `run-backtest`,
`backfill-backtests`, `run-paper-trade`, `run-weekend-cockpit`, `refresh-latest-session`,
`refresh-driver-affinity`, `capture-live-weekend`, and `dq-run`. Long-running collection still
supports explicit CLI commands for operator-controlled runs, and immediate live-ticket writes remain
synchronous so operators get a direct accept/reject result.

## Storage tiers

- Bronze: raw API and websocket payloads with fetch metadata.
- Silver: normalized canonical tables in Postgres and partitioned Parquet.
- Gold: feature snapshots, predictions, metrics, backtests, and analyst aggregates.
- Silver ops authority: `f1_calendar_overrides` overlays official schedule corrections on top of synced meeting data.
- Gold live operations: `live_trade_tickets` stores operator-facing tickets, while
  `live_trade_executions` stores the recorded browser fills tied back to those tickets.
- Gold model governance: `model_runs` stores registry metadata and artifacts, while
  `model_run_promotions` tracks the active promoted champion per stage for weekend paper trading.
  Operators can promote a known run id or let `promote-best-model-run` choose the highest-ranked
  eligible candidate for a stage.
