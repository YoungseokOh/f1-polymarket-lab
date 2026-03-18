# f1-polymarket-lab Agent Guide

Use the shared project skills in `./skills/`. The repository keeps the same skill content for Codex
and Claude, and the tool-specific directories should point to that shared source of truth.

## Working rules

- Inspect the current code, docs, and tests before changing behavior.
- Keep durable logic in packages/modules, not notebooks.
- If you change storage or ingestion behavior, update SQLAlchemy models, Alembic, docs, and tests
  in the same change.
- If you change API or UI behavior, update the FastAPI schema, frontend callers, and validation
  together.
- Prefer the narrowest relevant checks first, then broader repo checks if the change crosses
  subsystems.

## Shared skills

- `f1-data-platform`: connectors, schema, ingestion, lineage, mappings, data quality
- `f1-modeling-backtest`: features, snapshots, modeling order, evaluation, backtests
- `f1-web-api-lab`: FastAPI, Next.js, TS contracts, dashboard UX
- `research-reporting`: EDA, research notes, model explanations, report generation
- `change-finalizer`: validation, release notes, commit message drafting

## Common commands

- Bootstrap: `make bootstrap`
- Infra: `make infra-up`, `make infra-down`
- DB migration: `make db-upgrade`
- Demo ingest: `make ingest-demo`
- API: `make api`
- Web: `make web`
- Python checks: `uv run ruff check .`, `uv run mypy apps py tests`, `uv run pytest`
- JS/TS checks: `pnpm lint`, `pnpm typecheck`, `pnpm test`

## Git and commits

- Commit format: `<type>: <subject>`
- Do not use scopes.
- Preferred types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `build`, `perf`,
  `data`
- Use an imperative subject, keep it concise, and do not end it with a period.
- See `docs/engineering/commit-convention.md` for examples.
