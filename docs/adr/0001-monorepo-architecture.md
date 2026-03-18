# ADR 0001: Monorepo and Data Platform Baseline

## Decision

- Use `pnpm` + Turborepo for JS/TS workspaces.
- Use `uv` for Python environment and workspace management.
- Serve normalized entities and metadata from Postgres.
- Store analytical datasets in filesystem-backed Bronze/Silver/Gold partitions under `data/`.
- Use DuckDB + Polars for analytical reads and Parquet materialization.

## Rationale

- The platform needs a clean split between serving workloads, analytical workloads, and experiment artifacts.
- Local-first development matters more than distributed infrastructure in v1.
- Separate Python packages keep ingestion, storage, features, models, experiments, and agent concerns modular.

