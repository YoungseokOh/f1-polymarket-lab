COMPOSE_FILE := infra/compose/local.yml
DEMO_SEASON ?= 2024
DEMO_WEEKENDS ?= 1
DEMO_MARKET_BATCHES ?= 1

.PHONY: bootstrap infra-up infra-down api web worker dev db-upgrade ingest-demo lint test typecheck format sync-f1-calendar sync-polymarket-catalog

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

lint:
	pnpm lint
	uv run ruff check .

test:
	pnpm test
	uv run pytest

typecheck:
	pnpm typecheck
	uv run mypy apps py tests

format:
	pnpm format
	uv run ruff format .
