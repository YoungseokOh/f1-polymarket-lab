# Architecture Overview

- `apps/web`: analyst-facing dashboard and explorer UI.
- `apps/api`: FastAPI service exposing normalized entities, freshness, and lineage.
- `apps/worker`: explicit one-shot ingestion and orchestration CLI entrypoints.
- `apps/worker/src/f1_polymarket_worker/multitask_snapshot.py`: checkpoint-aware qualifying/race snapshot generation.
- `apps/worker/src/f1_polymarket_worker/model_registry.py`: MLflow logging, promotion gates, champion lookup, and promoted-snapshot scoring.
- `py/connectors`: source adapters for OpenF1, FastF1, and Polymarket.
- `py/storage`: lake and relational persistence.
- `py/models/src/f1_polymarket_lab/models/multitask_model.py`: shared encoder with family-specific heads.
- `py/experiments/src/f1_polymarket_lab/experiments/autoresearch.py`: screening and promotion loop for fixed-evaluator research.
- `py/features`, `py/models`, `py/experiments`, `py/agent`: downstream modeling and assistant layers.

The current repo does not ship a queue-backed background worker service. Long-running collection still
uses explicit CLI commands.

## Storage tiers

- Bronze: raw API and websocket payloads with fetch metadata.
- Silver: normalized canonical tables in Postgres and partitioned Parquet.
- Gold: feature snapshots, predictions, metrics, backtests, and analyst aggregates.
- Gold model governance: `model_runs` stores registry metadata and artifacts, while `model_run_promotions`
  tracks the active promoted champion per stage for weekend paper trading.
