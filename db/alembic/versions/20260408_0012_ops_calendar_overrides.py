"""add ops calendar override authority layer"""

from __future__ import annotations

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


def upgrade() -> None:
    op.add_column(
        "f1_meetings",
        sa.Column("meeting_slug", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "f1_meetings",
        sa.Column("event_format", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_f1_meetings_meeting_slug",
        "f1_meetings",
        ["meeting_slug"],
        unique=False,
    )

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
    op.create_index(
        "ix_f1_calendar_overrides_season",
        "f1_calendar_overrides",
        ["season"],
        unique=False,
    )
    op.create_index(
        "ix_f1_calendar_overrides_meeting_slug",
        "f1_calendar_overrides",
        ["meeting_slug"],
        unique=False,
    )
    op.create_index(
        "ix_f1_calendar_overrides_status",
        "f1_calendar_overrides",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_f1_calendar_overrides_is_active",
        "f1_calendar_overrides",
        ["is_active"],
        unique=False,
    )

    calendar_table = sa.table(
        "f1_calendar_overrides",
        sa.column("id", sa.String(length=36)),
        sa.column("season", sa.Integer()),
        sa.column("meeting_slug", sa.String(length=255)),
        sa.column("ops_slug", sa.String(length=255)),
        sa.column("status", sa.String(length=32)),
        sa.column("source_label", sa.String(length=255)),
        sa.column("source_url", sa.String(length=512)),
        sa.column("note", sa.Text()),
        sa.column("is_active", sa.Boolean()),
    )
    op.bulk_insert(
        calendar_table,
        [
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
            },
        ],
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
