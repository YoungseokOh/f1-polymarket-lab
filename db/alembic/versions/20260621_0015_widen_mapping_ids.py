"""widen mapping candidate and entity mapping ids"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260621_0015"
down_revision = "20260425_0014"
branch_labels = None
depends_on = None

_TABLES = ("mapping_candidates", "entity_mapping_f1_to_polymarket")


def _alter_id(table: str, length: int) -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if not inspector.has_table(table):
        return
    columns = {column["name"]: column for column in inspector.get_columns(table)}
    if columns.get("id", {}).get("type") is None:
        return
    with op.batch_alter_table(table) as batch_op:
        batch_op.alter_column(
            "id",
            existing_type=columns["id"]["type"],
            type_=sa.String(length=length),
        )


def upgrade() -> None:
    # Mapping ids are composite "{market_id}:{session_id}" keys that exceed the
    # original UUID-length (36) limit. Widen to 160 for headroom.
    for table in _TABLES:
        _alter_id(table, 160)


def downgrade() -> None:
    for table in _TABLES:
        _alter_id(table, 36)
