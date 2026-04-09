"""add ops calendar override authority layer"""

from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision = "20260408_0012"
down_revision = "20260407_0011"
branch_labels = None
depends_on = None

F1_OFFICIAL_OVERRIDE_URL = (
    "https://www.formula1.com/en/latest/article/"
    "bahrain-and-saudi-arabian-grands-prix-will-not-take-place-in-april.1hnqllVG85RSt8pbFc5Ivx/"
)


def _has_column(bind: sa.Connection, table_name: str, column_name: str) -> bool:
    inspector = sa.inspect(bind)
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def _has_index(bind: sa.Connection, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(bind)
    return index_name in {index["name"] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    seed_timestamp = datetime.now(timezone.utc)

    if _has_column(bind, "f1_meetings", "meeting_slug") is False:
        op.add_column(
            "f1_meetings",
            sa.Column("meeting_slug", sa.String(length=255), nullable=True),
        )
    if _has_column(bind, "f1_meetings", "event_format") is False:
        op.add_column(
            "f1_meetings",
            sa.Column("event_format", sa.String(length=64), nullable=True),
        )
    if _has_index(bind, "f1_meetings", "ix_f1_meetings_meeting_slug") is False:
        op.create_index(
            "ix_f1_meetings_meeting_slug",
            "f1_meetings",
            ["meeting_slug"],
            unique=False,
        )

    if "f1_calendar_overrides" not in existing_tables:
        op.create_table(
            "f1_calendar_overrides",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("season", sa.Integer(), nullable=False),
            sa.Column("meeting_slug", sa.String(length=255), nullable=False),
            sa.Column("ops_slug", sa.String(length=255), nullable=True),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("effective_round_number", sa.Integer(), nullable=True),
            sa.Column("effective_start_date_utc", sa.DateTime(timezone=True), nullable=True),
            sa.Column("effective_end_date_utc", sa.DateTime(timezone=True), nullable=True),
            sa.Column("effective_meeting_name", sa.String(length=255), nullable=True),
            sa.Column("effective_country_name", sa.String(length=255), nullable=True),
            sa.Column("effective_location", sa.String(length=255), nullable=True),
            sa.Column("source_label", sa.String(length=255), nullable=True),
            sa.Column("source_url", sa.String(length=512), nullable=True),
            sa.Column("note", sa.Text(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
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
            sa.UniqueConstraint(
                "season",
                "meeting_slug",
                name="uq_f1_calendar_overrides_season_meeting_slug",
            ),
        )
    for index_name, columns in (
        ("ix_f1_calendar_overrides_season", ["season"]),
        ("ix_f1_calendar_overrides_meeting_slug", ["meeting_slug"]),
        ("ix_f1_calendar_overrides_status", ["status"]),
        ("ix_f1_calendar_overrides_is_active", ["is_active"]),
    ):
        if _has_index(bind, "f1_calendar_overrides", index_name) is False:
            op.create_index(index_name, "f1_calendar_overrides", columns, unique=False)

    rows = [
        {
            "id": "calendar-override-2026-bahrain",
            "season": 2026,
            "meeting_slug": "bahrain-grand-prix",
            "ops_slug": "bahrain",
            "status": "cancelled",
            "source_label": "Formula 1 official",
            "source_url": F1_OFFICIAL_OVERRIDE_URL,
            "note": "Official 2026 schedule update cancelled the April Bahrain GP slot.",
            "is_active": True,
            "created_at": seed_timestamp,
            "updated_at": seed_timestamp,
        },
        {
            "id": "calendar-override-2026-saudi",
            "season": 2026,
            "meeting_slug": "saudi-arabian-grand-prix",
            "ops_slug": "saudi",
            "status": "cancelled",
            "source_label": "Formula 1 official",
            "source_url": F1_OFFICIAL_OVERRIDE_URL,
            "note": "Official 2026 schedule update cancelled the April Saudi GP slot.",
            "is_active": True,
            "created_at": seed_timestamp,
            "updated_at": seed_timestamp,
        },
    ]
    for row in rows:
        bind.execute(
            sa.text(
                """
                INSERT INTO f1_calendar_overrides (
                    id,
                    season,
                    meeting_slug,
                    ops_slug,
                    status,
                    source_label,
                    source_url,
                    note,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (
                    :id,
                    :season,
                    :meeting_slug,
                    :ops_slug,
                    :status,
                    :source_label,
                    :source_url,
                    :note,
                    :is_active,
                    :created_at,
                    :updated_at
                )
                ON CONFLICT (id) DO NOTHING
                """
            ),
            row,
        )


def downgrade() -> None:
    op.drop_index("ix_f1_calendar_overrides_is_active", table_name="f1_calendar_overrides")
    op.drop_index("ix_f1_calendar_overrides_status", table_name="f1_calendar_overrides")
    op.drop_index("ix_f1_calendar_overrides_meeting_slug", table_name="f1_calendar_overrides")
    op.drop_index("ix_f1_calendar_overrides_season", table_name="f1_calendar_overrides")
    op.drop_table("f1_calendar_overrides")
    op.drop_index("ix_f1_meetings_meeting_slug", table_name="f1_meetings")
    op.drop_column("f1_meetings", "event_format")
    op.drop_column("f1_meetings", "meeting_slug")
