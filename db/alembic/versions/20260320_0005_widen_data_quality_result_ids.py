"""widen data quality result ids"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260320_0005"
down_revision = "20260320_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
