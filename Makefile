COMPOSE_FILE := infra/compose/local.yml
DEMO_SEASON ?= 2026
DEMO_WEEKENDS ?= 3
DEMO_MARKET_BATCHES ?= 1
SESSION_KEY ?=

.PHONY: bootstrap infra-up infra-down api web worker dev db-upgrade ingest-demo lint test typecheck format sync-f1-calendar sync-polymarket-catalog sync-polymarket-f1-catalog backfill-f1-history backfill-f1-history-all bootstrap-f1db-history sync-jolpica-history hydrate-polymarket-f1-history discover-session-polymarket

bootstrap:
	corepack enable
	corepack prepare pnpm@10.6.3 --activate
	pnpm install
	uv lock
	uv sync --group dev

infra-up:
	docker compose -f $(COMPOSE_FILE) up -d

infra-down:
	docker compose -f $(COMPOSE_FILE) down

api:
	uv run --package f1-polymarket-api uvicorn f1_polymarket_api.main:app --reload --host 0.0.0.0 --port 8000

web:
	pnpm --filter @f1/web dev

worker:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli worker

dev:
	pnpm dev

db-upgrade:
	uv run alembic upgrade head

ingest-demo:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli ingest-demo --season $(DEMO_SEASON) --weekends $(DEMO_WEEKENDS) --market-batches $(DEMO_MARKET_BATCHES)

sync-f1-calendar:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli sync-f1-calendar --season $(DEMO_SEASON)

sync-polymarket-catalog:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli sync-polymarket-catalog --max-pages $(DEMO_MARKET_BATCHES)

sync-polymarket-f1-catalog:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli sync-polymarket-f1-catalog --max-pages $(DEMO_MARKET_BATCHES) --execute

backfill-f1-history:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli backfill-f1-history --season-start 2023 --season-end $(DEMO_SEASON) --execute

bootstrap-f1db-history:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli bootstrap-f1db-history --season-start 1950 --season-end 2022 --execute

sync-jolpica-history:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli sync-jolpica-history --season-start 1950 --season-end 2022 --execute

backfill-f1-history-all:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli backfill-f1-history-all --season-start 1950 --season-end $(DEMO_SEASON) --execute

hydrate-polymarket-f1-history:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli hydrate-polymarket-f1-history --execute

discover-session-polymarket:
	uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli discover-session-polymarket --session-key $(SESSION_KEY) --execute

lint:
	pnpm lint
	uv run ruff check .

test:
	pnpm test
	uv run pytest

typecheck:
	pnpm typecheck
	uv run mypy apps py py/tests

format:
	pnpm format
	uv run ruff format .
