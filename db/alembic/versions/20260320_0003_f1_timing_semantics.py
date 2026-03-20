"""f1 timing semantics"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260320_0003"
down_revision = "20260318_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if inspector.has_table("f1_session_results"):
        columns = {column["name"] for column in inspector.get_columns("f1_session_results")}
        with op.batch_alter_table("f1_session_results") as batch_op:
            if "fastest_lap_seconds" in columns and "result_time_seconds" not in columns:
                batch_op.alter_column("fastest_lap_seconds", new_column_name="result_time_seconds")
            if "gap_to_leader" in columns and "gap_to_leader_display" not in columns:
                batch_op.alter_column("gap_to_leader", new_column_name="gap_to_leader_display")
            if "result_time_kind" not in columns:
                batch_op.add_column(
                    sa.Column("result_time_kind", sa.String(length=24), nullable=True)
                )
            if "result_time_display" not in columns:
                batch_op.add_column(
                    sa.Column("result_time_display", sa.String(length=128), nullable=True)
                )
            if "result_time_segments_json" not in columns:
                batch_op.add_column(
                    sa.Column("result_time_segments_json", sa.JSON(), nullable=True)
                )
            if "gap_to_leader_seconds" not in columns:
                batch_op.add_column(sa.Column("gap_to_leader_seconds", sa.Float(), nullable=True))
            if "gap_to_leader_laps_behind" not in columns:
                batch_op.add_column(
                    sa.Column("gap_to_leader_laps_behind", sa.Integer(), nullable=True)
                )
            if "gap_to_leader_status" not in columns:
                batch_op.add_column(
                    sa.Column("gap_to_leader_status", sa.String(length=24), nullable=True)
                )
            if "gap_to_leader_segments_json" not in columns:
                batch_op.add_column(
                    sa.Column("gap_to_leader_segments_json", sa.JSON(), nullable=True)
                )
            if "dnf" not in columns:
                batch_op.add_column(sa.Column("dnf", sa.Boolean(), nullable=True))
            if "dns" not in columns:
                batch_op.add_column(sa.Column("dns", sa.Boolean(), nullable=True))
            if "dsq" not in columns:
                batch_op.add_column(sa.Column("dsq", sa.Boolean(), nullable=True))

    if inspector.has_table("f1_intervals"):
        columns = {column["name"] for column in inspector.get_columns("f1_intervals")}
        with op.batch_alter_table("f1_intervals") as batch_op:
            if "gap_to_leader" in columns and "gap_to_leader_display" not in columns:
                batch_op.alter_column("gap_to_leader", new_column_name="gap_to_leader_display")
            if "interval" in columns and "interval_seconds" not in columns:
                batch_op.alter_column("interval", new_column_name="interval_seconds")
            if "gap_to_leader_seconds" not in columns:
                batch_op.add_column(sa.Column("gap_to_leader_seconds", sa.Float(), nullable=True))
            if "gap_to_leader_laps_behind" not in columns:
                batch_op.add_column(
                    sa.Column("gap_to_leader_laps_behind", sa.Integer(), nullable=True)
                )
            if "gap_to_leader_status" not in columns:
                batch_op.add_column(
                    sa.Column("gap_to_leader_status", sa.String(length=16), nullable=True)
                )
            if "interval_display" not in columns:
                batch_op.add_column(
                    sa.Column("interval_display", sa.String(length=64), nullable=True)
                )
            if "interval_laps_behind" not in columns:
                batch_op.add_column(sa.Column("interval_laps_behind", sa.Integer(), nullable=True))
            if "interval_status" not in columns:
                batch_op.add_column(
                    sa.Column("interval_status", sa.String(length=16), nullable=True)
                )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if inspector.has_table("f1_session_results"):
        columns = {column["name"] for column in inspector.get_columns("f1_session_results")}
        with op.batch_alter_table("f1_session_results") as batch_op:
            for column_name in (
                "dsq",
                "dns",
                "dnf",
                "gap_to_leader_segments_json",
                "gap_to_leader_status",
                "gap_to_leader_laps_behind",
                "gap_to_leader_seconds",
                "result_time_segments_json",
                "result_time_display",
                "result_time_kind",
            ):
                if column_name in columns:
                    batch_op.drop_column(column_name)
            if "gap_to_leader_display" in columns and "gap_to_leader" not in columns:
                batch_op.alter_column("gap_to_leader_display", new_column_name="gap_to_leader")
            if "result_time_seconds" in columns and "fastest_lap_seconds" not in columns:
                batch_op.alter_column("result_time_seconds", new_column_name="fastest_lap_seconds")

    if inspector.has_table("f1_intervals"):
        columns = {column["name"] for column in inspector.get_columns("f1_intervals")}
        with op.batch_alter_table("f1_intervals") as batch_op:
            for column_name in (
                "interval_status",
                "interval_laps_behind",
                "interval_display",
                "gap_to_leader_status",
                "gap_to_leader_laps_behind",
                "gap_to_leader_seconds",
            ):
                if column_name in columns:
                    batch_op.drop_column(column_name)
            if "interval_seconds" in columns and "interval" not in columns:
                batch_op.alter_column("interval_seconds", new_column_name="interval")
            if "gap_to_leader_display" in columns and "gap_to_leader" not in columns:
                batch_op.alter_column("gap_to_leader_display", new_column_name="gap_to_leader")
