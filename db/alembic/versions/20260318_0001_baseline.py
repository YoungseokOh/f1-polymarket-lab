"""baseline schema"""

from __future__ import annotations

from alembic import op
from f1_polymarket_lab.storage import models as storage_models  # noqa: F401
from f1_polymarket_lab.storage.db import Base

revision = "20260318_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
