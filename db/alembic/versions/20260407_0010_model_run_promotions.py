"""add model run registry id and promotion table"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260407_0010"
down_revision = "20260325_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "model_runs",
        sa.Column("registry_run_id", sa.String(length=128), nullable=True),
    )
    op.create_index(
        "ix_model_runs_registry_run_id",
        "model_runs",
        ["registry_run_id"],
        unique=False,
    )

    op.create_table(
        "model_run_promotions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=False),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("gate_metrics_json", sa.JSON, nullable=True),
        sa.Column(
            "promoted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "model_run_id",
            "stage",
            name="uq_model_run_promotions_run_stage",
        ),
    )
    op.create_index(
        "ix_model_run_promotions_model_run_id",
        "model_run_promotions",
        ["model_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_model_run_promotions_stage",
        "model_run_promotions",
        ["stage"],
        unique=False,
    )
    op.create_index(
        "ix_model_run_promotions_status",
        "model_run_promotions",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_model_run_promotions_status", table_name="model_run_promotions")
    op.drop_index("ix_model_run_promotions_stage", table_name="model_run_promotions")
    op.drop_index("ix_model_run_promotions_model_run_id", table_name="model_run_promotions")
    op.drop_table("model_run_promotions")
    op.drop_index("ix_model_runs_registry_run_id", table_name="model_runs")
    op.drop_column("model_runs", "registry_run_id")
