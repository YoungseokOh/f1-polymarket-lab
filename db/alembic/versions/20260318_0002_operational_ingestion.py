"""operational ingestion schema"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from f1_polymarket_lab.storage import models as storage_models  # noqa: F401
from f1_polymarket_lab.storage.db import Base
from sqlalchemy import inspect

revision = "20260318_0002"
down_revision = "20260318_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    Base.metadata.create_all(bind=bind, checkfirst=True)

    source_fetch_columns = {column["name"] for column in inspector.get_columns("source_fetch_log")}
    if "job_run_id" not in source_fetch_columns:
        op.add_column(
            "source_fetch_log",
            sa.Column("job_run_id", sa.String(length=36), nullable=True),
        )
        op.create_index(
            op.f("ix_source_fetch_log_job_run_id"),
            "source_fetch_log",
            ["job_run_id"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables_to_drop = [
        "snapshot_run_manifest",
        "dataset_version_manifest",
        "manual_mapping_overrides",
        "mapping_candidates",
        "market_taxonomy_labels",
        "market_taxonomy_versions",
        "polymarket_ws_message_manifest",
        "polymarket_orderbook_levels",
        "polymarket_open_interest_history",
        "polymarket_market_status_history",
        "f1_starting_grid",
        "f1_team_radio_metadata",
        "data_quality_results",
        "data_quality_checks",
        "schema_registry",
        "bronze_object_manifest",
        "source_cursor_state",
        "ingestion_job_runs",
        "ingestion_job_definitions",
    ]
    for table_name in tables_to_drop:
        if inspector.has_table(table_name):
            op.drop_table(table_name)

    source_fetch_columns = {column["name"] for column in inspector.get_columns("source_fetch_log")}
    if "job_run_id" in source_fetch_columns:
        op.drop_index(op.f("ix_source_fetch_log_job_run_id"), table_name="source_fetch_log")
        op.drop_column("source_fetch_log", "job_run_id")
