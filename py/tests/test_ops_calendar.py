from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import F1Meeting, F1Session
from f1_polymarket_worker.live_trading import build_live_signal_board
from f1_polymarket_worker.ops_calendar import (
    get_ops_stage_config,
    list_ops_stage_configs,
    normalize_ops_short_code,
)
from f1_polymarket_worker.pipeline import PipelineContext
from f1_polymarket_worker.weekend_ops import get_current_weekend_operations_readiness
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def _seed_meeting(
    session: Session,
    *,
    meeting_key: int = 1285,
    meeting_name: str = "Canadian Grand Prix",
    meeting_slug: str = "canadian-grand-prix",
    event_format: str = "sprint",
) -> None:
    session.add(
        F1Meeting(
            id=f"meeting:{meeting_key}",
            meeting_key=meeting_key,
            season=2026,
            round_number=5,
            meeting_name=meeting_name,
            meeting_slug=meeting_slug,
            event_format=event_format,
            start_date_utc=datetime(2026, 5, 22, 16, 30, tzinfo=timezone.utc),
            end_date_utc=datetime(2026, 5, 24, 21, 0, tzinfo=timezone.utc),
        )
    )
    session_codes = (
        ("FP1", "Practice 1"),
        ("SQ", "Sprint Qualifying"),
        ("S", "Sprint"),
        ("Q", "Qualifying"),
        ("R", "Race"),
    )
    if event_format == "conventional":
        session_codes = (
            ("FP1", "Practice 1"),
            ("FP2", "Practice 2"),
            ("FP3", "Practice 3"),
            ("Q", "Qualifying"),
            ("R", "Race"),
        )
    for index, (session_code, session_name) in enumerate(session_codes, start=1):
        session.add(
            F1Session(
                id=f"session:{meeting_key}:{session_code}",
                session_key=meeting_key * 100 + index,
                meeting_id=f"meeting:{meeting_key}",
                session_name=session_name,
                session_code=session_code,
                date_start_utc=datetime(2026, 5, 22, 16, 30, tzinfo=timezone.utc),
                date_end_utc=datetime(2026, 5, 22, 17, 30, tzinfo=timezone.utc),
                is_practice=session_code.startswith("FP"),
            )
        )


def _session(tmp_path: Path) -> Session:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'ops-calendar.sqlite'}")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_normalize_ops_short_code_accepts_hyphen_and_underscore_aliases() -> None:
    assert normalize_ops_short_code("canadian-grand-prix_fp1_sq") == (
        "canadian_grand_prix_fp1_sq"
    )
    assert normalize_ops_short_code("canadian_grand_prix_fp1_sq") == (
        "canadian_grand_prix_fp1_sq"
    )


def test_dynamic_ops_stage_uses_canonical_short_codes_for_future_sprint_gp(
    tmp_path: Path,
) -> None:
    with _session(tmp_path) as session:
        _seed_meeting(session)
        session.commit()

        configs = [
            config
            for _, config in list_ops_stage_configs(session, season=2026)
            if config.meeting_key == 1285
        ]

    assert [config.short_code for config in configs] == [
        "canadian_grand_prix_fp1_sq",
        "canadian_grand_prix_sq_sprint",
        "canadian_grand_prix_fp1_q",
        "canadian_grand_prix_q_r",
    ]
    assert all("-" not in config.short_code for config in configs)
    assert configs[0].snapshot_dataset == "canadian_grand_prix_fp1_to_sq_pole_live_snapshot"


def test_dynamic_ops_stage_accepts_meeting_slug_alias(tmp_path: Path) -> None:
    with _session(tmp_path) as session:
        _seed_meeting(session)
        session.commit()

        meeting, config = get_ops_stage_config(
            session,
            short_code="canadian-grand-prix_fp1_sq",
            now=datetime(2026, 5, 22, 18, 0, tzinfo=timezone.utc),
        )

    assert meeting.meeting_slug == "canadian-grand-prix"
    assert config.short_code == "canadian_grand_prix_fp1_sq"
    assert config.snapshot_dataset == "canadian_grand_prix_fp1_to_sq_pole_live_snapshot"


def test_dynamic_ops_stage_alias_feeds_readiness_and_signal_board(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 5, 22, 18, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("f1_polymarket_worker.weekend_ops.utc_now", lambda: now)
    monkeypatch.setattr("f1_polymarket_worker.live_trading.utc_now", lambda: now)

    with _session(tmp_path) as session:
        _seed_meeting(session)
        session.commit()
        ctx = PipelineContext(db=session, execute=False)

        readiness = get_current_weekend_operations_readiness(
            ctx,
            gp_short_code="canadian-grand-prix_fp1_sq",
        )
        signal_board = build_live_signal_board(
            ctx,
            gp_short_code="canadian-grand-prix_fp1_sq",
        )

    assert readiness["selected_gp_short_code"] == "canadian_grand_prix_fp1_sq"
    assert signal_board["gp_short_code"] == "canadian_grand_prix_fp1_sq"
