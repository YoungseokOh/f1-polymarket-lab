from __future__ import annotations

from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import F1Team, FeatureRegistry
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session


def test_upsert_records_updates_existing_row_on_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        upsert_records(
            session,
            FeatureRegistry,
            [
                {
                    "id": "feature:pace",
                    "feature_name": "pace",
                    "feature_group": "historical",
                    "description": "initial",
                    "data_type": "float",
                    "version": "v1",
                    "owner": "test",
                }
            ],
        )
        session.commit()

        upsert_records(
            session,
            FeatureRegistry,
            [
                {
                    "id": "feature:pace",
                    "feature_name": "pace",
                    "feature_group": "historical",
                    "description": "updated",
                    "data_type": "float",
                    "version": "v2",
                    "owner": "test",
                }
            ],
        )
        session.commit()

        record = session.scalar(
            select(FeatureRegistry).where(FeatureRegistry.feature_name == "pace")
        )
        assert record is not None
        assert record.description == "updated"
        assert record.version == "v2"


def test_upsert_records_coalesces_duplicate_keys_within_single_call() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        upsert_records(
            session,
            FeatureRegistry,
            [
                {
                    "id": "feature:pace",
                    "feature_name": "pace",
                    "feature_group": "historical",
                    "description": "initial",
                    "data_type": "float",
                    "version": "v1",
                    "owner": "test",
                },
                {
                    "id": "feature:pace",
                    "feature_name": "pace",
                    "feature_group": "historical",
                    "description": "final",
                    "data_type": "float",
                    "version": "v2",
                    "owner": "test",
                },
            ],
        )
        session.commit()

        record = session.scalar(
            select(FeatureRegistry).where(FeatureRegistry.feature_name == "pace")
        )
        count = session.scalar(select(func.count()).select_from(FeatureRegistry))
        assert record is not None
        assert count == 1
        assert record.description == "final"
        assert record.version == "v2"


def test_upsert_records_honors_conflict_columns_on_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        upsert_records(
            session,
            F1Team,
            [
                {
                    "id": "team:alphatauri",
                    "source": "f1db",
                    "team_name": "Racing Bulls",
                    "team_color": "#1e5bc6",
                    "raw_payload": {"season": 2024},
                },
                {
                    "id": "team:rb",
                    "source": "f1db",
                    "team_name": "Racing Bulls",
                    "team_color": "#6692ff",
                    "raw_payload": {"season": 2025},
                },
            ],
            conflict_columns=["team_name"],
        )
        session.commit()

        record = session.scalar(select(F1Team).where(F1Team.team_name == "Racing Bulls"))
        count = session.scalar(select(func.count()).select_from(F1Team))
        assert record is not None
        assert count == 1
        assert record.id == "team:rb"
        assert record.team_color == "#6692ff"
