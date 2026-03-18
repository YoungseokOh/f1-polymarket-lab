# f1-polymarket-lab

Use the shared project skills in `./skills/`. The `.claude/skills` and `.codex/skills`
directories are expected to reference that shared source of truth.

- @docs/engineering/commit-convention.md

## Working rules

- Inspect existing code, docs, and tests before changing behavior.
- Keep durable logic in packages/modules, not notebooks.
- Update schema, migration, docs, and tests together when storage or ingestion changes.
- Update API contracts and frontend callers together when API/UI behavior changes.

## Shared skills

- `/f1-data-platform`
- `/f1-modeling-backtest`
- `/f1-web-api-lab`
- `/research-reporting`
- `/change-finalizer`

## Common commands

- `make bootstrap`
- `make infra-up`
- `make db-upgrade`
- `make ingest-demo`
- `make api`
- `make web`
- `uv run ruff check .`
- `uv run mypy apps py tests`
- `uv run pytest`
- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
