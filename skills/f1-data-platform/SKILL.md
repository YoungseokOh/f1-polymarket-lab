---
name: f1-data-platform
description: Use when working on connectors, ingestion jobs, Bronze/Silver/Gold storage, schema migrations, lineage, entity mapping, or data quality for F1 and Polymarket data.
---

# f1-data-platform

Use this skill for storage and ingestion changes.

## Start here

- Read `README.md`, `docs/data-dictionary/core-tables.md`, and `db/ddl/schema-overview.md`.
- Inspect the current SQLAlchemy models, Alembic migrations, worker pipeline, and connector code
  before proposing changes.

## Workflow

1. Confirm the source contract from official docs before hardcoding endpoints, parameters, or
   websocket payload assumptions.
2. Keep ingestion split by concern: discovery, hydrate, normalize, reconcile, and quality checks.
3. Preserve idempotency with cursor state, fetch logs, and object manifests.
4. Store raw source payloads in Bronze as close to source format as practical.
5. Keep Silver typed and normalized; do not silently coerce whole records to strings.
6. When schema changes, update SQLAlchemy models, Alembic, docs, and tests together.
7. Preserve Polymarket official API and documented websocket usage only; do not scrape the DOM.

## Validation

- `uv run ruff check .`
- `uv run mypy apps py tests`
- `uv run pytest`
- `uv run alembic upgrade head` when models or migrations change
