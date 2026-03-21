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

## Polymarket market discovery

- **주 이벤트 마켓**(레이스 우승자, 폴 포지션 등 고유동성)은 Gamma API의 `/markets` 또는 `/events` 엔드포인트로는 조회되지 않을 수 있다.
- 항상 `https://polymarket.com/sports/f1/props` 웹페이지를 직접 조회하여 열린 마켓 전체를 먼저 파악하라.
- 이벤트 슬러그 패턴 예시: `f1-japanese-grand-prix-winner-2026-03-29`, `f1-japanese-grand-prix-driver-pole-position-2026-03-28`
- 슬러그를 확인한 뒤 Gamma API `GET /markets?slug=<slug>` 또는 `GET /events?slug=<slug>`로 토큰 ID와 가격을 조회한다.

## Validation

- `uv run ruff check .`
- `uv run mypy apps py tests`
- `uv run pytest`
- `uv run alembic upgrade head` when models or migrations change
