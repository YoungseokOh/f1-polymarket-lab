# Architecture Overview

- `apps/web`: analyst-facing dashboard and explorer UI.
- `apps/api`: FastAPI service exposing normalized entities, freshness, and lineage.
- `apps/worker`: explicit one-shot ingestion and orchestration CLI entrypoints.
- `py/connectors`: source adapters for OpenF1, FastF1, and Polymarket.
- `py/storage`: lake and relational persistence.
- `py/features`, `py/models`, `py/experiments`, `py/agent`: downstream modeling and assistant layers.

The current repo does not ship a queue-backed background worker service. Long-running collection still
uses explicit CLI commands.

## Storage tiers

- Bronze: raw API and websocket payloads with fetch metadata.
- Silver: normalized canonical tables in Postgres and partitioned Parquet.
- Gold: feature snapshots, predictions, metrics, backtests, and analyst aggregates.
