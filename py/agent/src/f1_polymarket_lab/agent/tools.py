from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AgentToolSpec:
    name: str
    description: str


def default_agent_tools() -> list[AgentToolSpec]:
    return [
        AgentToolSpec(
            "warehouse_query", "Query normalized warehouse tables with explicit SQL provenance."
        ),
        AgentToolSpec(
            "duckdb_query", "Run DuckDB analytics against Parquet-backed Silver and Gold tables."
        ),
        AgentToolSpec(
            "model_explanation", "Fetch saved metrics, calibrations, and feature attributions."
        ),
    ]
