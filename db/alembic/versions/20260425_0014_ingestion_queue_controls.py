"""add ingestion queue controls"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260425_0014"
down_revision = "20260408_0013"
branch_labels = None
depends_on = None


def _add_column_if_missing(
    existing_columns: set[str],
    table_name: str,
    column: sa.Column[object],
) -> None:
    if column.name not in existing_columns:
        op.add_column(table_name, column)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("ingestion_job_runs")}

    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
    )
    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="1"),
    )
    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("locked_by", sa.String(length=128), nullable=True),
    )
    _add_column_if_missing(
        columns,
        "ingestion_job_runs",
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
    )

    indexes = {index["name"] for index in inspector.get_indexes("ingestion_job_runs")}
    if "ix_ingestion_job_runs_locked_by" not in indexes:
        op.create_index(
            "ix_ingestion_job_runs_locked_by",
            "ingestion_job_runs",
            ["locked_by"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("ingestion_job_runs")}
    indexes = {index["name"] for index in inspector.get_indexes("ingestion_job_runs")}

    if "ix_ingestion_job_runs_locked_by" in indexes:
        op.drop_index("ix_ingestion_job_runs_locked_by", table_name="ingestion_job_runs")
    for column_name in (
        "locked_at",
        "locked_by",
        "max_attempts",
        "attempt_count",
        "available_at",
        "queued_at",
    ):
        if column_name in columns:
            op.drop_column("ingestion_job_runs", column_name)
