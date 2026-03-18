---
name: f1-web-api-lab
description: Use when changing the FastAPI service, Pydantic schemas, TypeScript contracts, Next.js dashboard, or analyst-facing exploration workflows.
---

# f1-web-api-lab

Use this skill for API and dashboard work.

## Goals

- Keep the product research-first, not demo-first.
- Surface provenance, freshness, timestamps, model version, and executable prices where relevant.
- Keep API and frontend contracts strongly typed and synchronized.

## Workflow

1. Inspect FastAPI routes/schemas and the frontend pages that consume them before editing.
2. When API shape changes, update the backend schema and every direct consumer in the same change.
3. Preserve strong filtering and drill-down UX for season, circuit, weekend, session, team, driver,
   and market type.
4. Avoid placeholder UI that hides missing lineage or low-confidence data.
5. Prefer small, composable changes that keep `apps/api`, `apps/web`, and shared TS packages in sync.

## Validation

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
- Relevant `uv run pytest` coverage if backend behavior changes
