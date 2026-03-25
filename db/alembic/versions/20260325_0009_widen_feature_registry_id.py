"""widen feature registry id to fit stable feature keys"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260325_0009"
down_revision = "20260325_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "feature_registry",
        "id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=512),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "feature_registry",
        "id",
        existing_type=sa.String(length=512),
        type_=sa.String(length=36),
        existing_nullable=False,
    )
