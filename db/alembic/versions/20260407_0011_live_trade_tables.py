"""add live trade ticket and execution tables"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260407_0011"
down_revision = "20260407_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_trade_tickets",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("gp_slug", sa.String(length=128), nullable=False),
        sa.Column("session_code", sa.String(length=16), nullable=False),
        sa.Column("market_id", sa.String(length=64), nullable=False),
        sa.Column("token_id", sa.String(length=128), nullable=True),
        sa.Column("snapshot_id", sa.String(length=36), nullable=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=True),
        sa.Column("promotion_stage", sa.String(length=64), nullable=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("signal_action", sa.String(length=16), nullable=False),
        sa.Column("side_label", sa.String(length=8), nullable=False),
        sa.Column("model_prob", sa.Float(), nullable=False),
        sa.Column("market_price", sa.Float(), nullable=False),
        sa.Column("edge", sa.Float(), nullable=False),
        sa.Column("recommended_size", sa.Float(), nullable=False),
        sa.Column("observed_spread", sa.Float(), nullable=True),
        sa.Column("max_spread", sa.Float(), nullable=True),
        sa.Column("observed_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_event_type", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="open"),
        sa.Column("rationale_json", sa.JSON(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    for index_name, columns in (
        ("ix_live_trade_tickets_gp_slug", ["gp_slug"]),
        ("ix_live_trade_tickets_session_code", ["session_code"]),
        ("ix_live_trade_tickets_market_id", ["market_id"]),
        ("ix_live_trade_tickets_token_id", ["token_id"]),
        ("ix_live_trade_tickets_snapshot_id", ["snapshot_id"]),
        ("ix_live_trade_tickets_model_run_id", ["model_run_id"]),
        ("ix_live_trade_tickets_promotion_stage", ["promotion_stage"]),
        ("ix_live_trade_tickets_observed_at_utc", ["observed_at_utc"]),
        ("ix_live_trade_tickets_status", ["status"]),
    ):
        op.create_index(index_name, "live_trade_tickets", columns, unique=False)

    op.create_table(
        "live_trade_executions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("ticket_id", sa.String(length=36), nullable=False),
        sa.Column("market_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("submitted_size", sa.Float(), nullable=False),
        sa.Column("actual_fill_size", sa.Float(), nullable=True),
        sa.Column("actual_fill_price", sa.Float(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("operator_note", sa.Text(), nullable=True),
        sa.Column("external_reference", sa.String(length=128), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="submitted",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("ticket_id", name="uq_live_trade_executions_ticket"),
    )
    for index_name, columns in (
        ("ix_live_trade_executions_ticket_id", ["ticket_id"]),
        ("ix_live_trade_executions_market_id", ["market_id"]),
        ("ix_live_trade_executions_submitted_at", ["submitted_at"]),
        ("ix_live_trade_executions_status", ["status"]),
    ):
        op.create_index(index_name, "live_trade_executions", columns, unique=False)


def downgrade() -> None:
    op.drop_index("ix_live_trade_executions_status", table_name="live_trade_executions")
    op.drop_index("ix_live_trade_executions_submitted_at", table_name="live_trade_executions")
    op.drop_index("ix_live_trade_executions_market_id", table_name="live_trade_executions")
    op.drop_index("ix_live_trade_executions_ticket_id", table_name="live_trade_executions")
    op.drop_table("live_trade_executions")

    op.drop_index("ix_live_trade_tickets_status", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_observed_at_utc", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_promotion_stage", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_model_run_id", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_snapshot_id", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_token_id", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_market_id", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_session_code", table_name="live_trade_tickets")
    op.drop_index("ix_live_trade_tickets_gp_slug", table_name="live_trade_tickets")
    op.drop_table("live_trade_tickets")
