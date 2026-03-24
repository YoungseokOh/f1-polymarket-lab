"""force widen data quality result id"""

from __future__ import annotations

from alembic import op

revision = "20260320_0006"
down_revision = "20260320_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE data_quality_results "
            "ALTER COLUMN id TYPE VARCHAR(128)"
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE data_quality_results "
            "ALTER COLUMN id TYPE VARCHAR(36)"
        )
