# f1-polymarket-lab

Research platform for Formula 1 prediction-market data ingestion, feature snapshots, modeling, backtesting, and analyst-facing exploration.

## Quick start

```bash
cp .env.example .env
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

## Agent setup

- Codex repo instructions: `AGENTS.md`
- Claude repo instructions: `CLAUDE.md`
- Shared portable skills: `skills/`
- Commit convention: `docs/engineering/commit-convention.md`
