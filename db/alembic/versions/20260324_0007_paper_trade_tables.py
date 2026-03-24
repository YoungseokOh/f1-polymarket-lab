"""add paper_trade_sessions and paper_trade_positions tables"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260324_0007"
down_revision = "20260320_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from alembic import op as _op
    bind = _op.get_bind()
    existing = bind.dialect.get_table_names(bind)
    if "paper_trade_sessions" in existing:
        return

    op.create_table(
        "paper_trade_sessions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("gp_slug", sa.String(128), nullable=False, index=True),
        sa.Column("snapshot_id", sa.String(36), nullable=True, index=True),
        sa.Column("model_run_id", sa.String(36), nullable=True, index=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="open", index=True),
        sa.Column("config_json", sa.JSON, nullable=True),
        sa.Column("summary_json", sa.JSON, nullable=True),
        sa.Column("log_path", sa.String(512), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "paper_trade_positions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("session_id", sa.String(36), nullable=False, index=True),
        sa.Column("market_id", sa.String(64), nullable=False, index=True),
        sa.Column("token_id", sa.String(128), nullable=True),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("entry_price", sa.Float, nullable=False),
        sa.Column("entry_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model_prob", sa.Float, nullable=False),
        sa.Column("market_prob", sa.Float, nullable=False),
        sa.Column("edge", sa.Float, nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="open", index=True),
        sa.Column("exit_price", sa.Float, nullable=True),
        sa.Column("exit_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("realized_pnl", sa.Float, nullable=True),
    )


def downgrade() -> None:
    op.drop_table("paper_trade_positions")
    op.drop_table("paper_trade_sessions")
