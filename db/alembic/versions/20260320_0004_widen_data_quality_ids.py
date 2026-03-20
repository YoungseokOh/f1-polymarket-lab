"""widen data quality ids"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260320_0004"
down_revision = "20260320_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if inspector.has_table("data_quality_checks"):
        columns = {
            column["name"]: column for column in inspector.get_columns("data_quality_checks")
        }
        if columns.get("id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_checks") as batch_op:
                batch_op.alter_column(
                    "id",
                    existing_type=columns["id"]["type"],
                    type_=sa.String(length=128),
                )

    if inspector.has_table("data_quality_results"):
        columns = {
            column["name"]: column for column in inspector.get_columns("data_quality_results")
        }
        if columns.get("id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_results") as batch_op:
                batch_op.alter_column(
                    "id",
                    existing_type=columns["id"]["type"],
                    type_=sa.String(length=128),
                )
        if columns.get("check_id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_results") as batch_op:
                batch_op.alter_column(
                    "check_id",
                    existing_type=columns["check_id"]["type"],
                    type_=sa.String(length=128),
                )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if inspector.has_table("data_quality_results"):
        columns = {
            column["name"]: column for column in inspector.get_columns("data_quality_results")
        }
        if columns.get("id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_results") as batch_op:
                batch_op.alter_column(
                    "id",
                    existing_type=columns["id"]["type"],
                    type_=sa.String(length=36),
                )
        if columns.get("check_id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_results") as batch_op:
                batch_op.alter_column(
                    "check_id",
                    existing_type=columns["check_id"]["type"],
                    type_=sa.String(length=36),
                )

    if inspector.has_table("data_quality_checks"):
        columns = {
            column["name"]: column for column in inspector.get_columns("data_quality_checks")
        }
        if columns.get("id", {}).get("type") is not None:
            with op.batch_alter_table("data_quality_checks") as batch_op:
                batch_op.alter_column(
                    "id",
                    existing_type=columns["id"]["type"],
                    type_=sa.String(length=36),
                )
