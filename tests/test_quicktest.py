from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Lap,
    F1Meeting,
    F1Session,
    F1SessionResult,
    F1Stint,
    FeatureSnapshot,
    ModelPrediction,
    ModelRun,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
)
from f1_polymarket_worker.pipeline import PipelineContext
from f1_polymarket_worker.quicktest import (
    build_aus_fp1_to_q_snapshot,
    build_china_fp1_to_sq_snapshot,
    report_aus_q_pole_quicktest,
    report_china_sq_pole_quicktest,
    run_aus_q_pole_baseline,
    run_china_sq_pole_baseline,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_context(tmp_path: Path, *, execute: bool = True) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=execute, settings=settings)
    return session, context


def seed_china_quicktest_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting-1280",
        meeting_key=1280,
        season=2026,
        round_number=2,
        meeting_name="Chinese Grand Prix",
        country_name="China",
        location="Shanghai",
        start_date_utc=datetime(2026, 3, 13, 2, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 15, 9, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Chinese Grand Prix"},
    )
    session.add(meeting)

    fp1 = F1Session(
        id="session-fp1",
        meeting_id=meeting.id,
        session_key=11235,
        session_name="Practice 1",
        session_type="Practice",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 13, 3, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 13, 4, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Chinese Grand Prix"},
    )
    sq = F1Session(
        id="session-sq",
        meeting_id=meeting.id,
        session_key=11236,
        session_name="Sprint Qualifying",
        session_type="Qualifying",
        session_code="SQ",
        date_start_utc=datetime(2026, 3, 13, 7, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 13, 8, 15, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Chinese Grand Prix"},
    )
    session.add_all([fp1, sq])

    drivers = [
        F1Driver(
            id="driver-max",
            driver_number=1,
            full_name="Max Verstappen",
            first_name="Max",
            last_name="Verstappen",
            broadcast_name="M Verstappen",
            team_id="team-redbull",
            raw_payload={},
        ),
        F1Driver(
            id="driver-lewis",
            driver_number=44,
            full_name="Lewis Hamilton",
            first_name="Lewis",
            last_name="Hamilton",
            broadcast_name="L Hamilton",
            team_id="team-ferrari",
            raw_payload={},
        ),
        F1Driver(
            id="driver-charles",
            driver_number=16,
            full_name="Charles Leclerc",
            first_name="Charles",
            last_name="Leclerc",
            broadcast_name="C Leclerc",
            team_id="team-ferrari",
            raw_payload={},
        ),
    ]
    session.add_all(drivers)

    fp1_results = [
        ("driver-lewis", 1, 89.90, "leader", 0.0, 25),
        ("driver-max", 2, 89.98, "time", 0.08, 24),
        ("driver-charles", 3, 90.05, "time", 0.15, 23),
    ]
    sq_results = [
        ("driver-lewis", 1, 88.10),
        ("driver-max", 2, 88.22),
        ("driver-charles", 3, 88.35),
    ]
    for driver_id, position, lap_time, gap_status, gap_seconds, laps in fp1_results:
        session.add(
            F1SessionResult(
                id=f"fp1-result-{driver_id}",
                session_id=fp1.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_display="0" if position == 1 else f"+{gap_seconds:.3f}",
                gap_to_leader_seconds=None if position == 1 else gap_seconds,
                gap_to_leader_status=gap_status,
                number_of_laps=laps,
                raw_payload={},
            )
        )
        for lap_number in range(1, laps + 1):
            session.add(
                F1Lap(
                    id=f"fp1-lap-{driver_id}-{lap_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    lap_number=lap_number,
                    lap_duration_seconds=90.0,
                    raw_payload={},
                )
            )
        stint_total = 2 if driver_id != "driver-charles" else 3
        for stint_number in range(1, stint_total + 1):
            session.add(
                F1Stint(
                    id=f"fp1-stint-{driver_id}-{stint_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    stint_number=stint_number,
                    raw_payload={},
                )
            )
    for driver_id, position, lap_time in sq_results:
        session.add(
            F1SessionResult(
                id=f"sq-result-{driver_id}",
                session_id=sq.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_status="leader" if position == 1 else "time",
                raw_payload={},
            )
        )

    event = PolymarketEvent(
        id="event-sq",
        slug="f1-chinese-grand-prix-sprint-qualifying-pole-winner-2026-03-13",
        ticker="f1-chinese-sq-pole-2026-03-13",
        title="Chinese Grand Prix: Sprint Qualifying Pole Winner",
        description="Sprint Qualifying pole market",
        start_at_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 13, 7, 30, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        raw_payload={},
    )
    session.add(event)

    market_specs = [
        ("market-max", "Max Verstappen", "Verstappen", 0.55),
        ("market-lewis", "Lewis Hamilton", "Hamilton", 0.25),
        ("market-charles", "Charles Leclerc", "Leclerc", 0.20),
    ]
    for index, (market_id, full_name, last_name, entry_price) in enumerate(market_specs):
        session.add(
            PolymarketMarket(
                id=market_id,
                event_id=event.id,
                question=(
                    f"Will {full_name} achieve pole position in Sprint Qualifying "
                    "at the 2026 F1 Chinese Grand Prix?"
                ),
                slug=f"china-sq-pole-{last_name.lower()}",
                condition_id=f"condition-{market_id}",
                question_id=f"question-{market_id}",
                taxonomy="driver_pole_position",
                taxonomy_confidence=0.95,
                target_session_code="SQ",
                driver_a=last_name,
                description="Chinese Grand Prix: Sprint Qualifying Pole Winner",
                start_at_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 13, 7, 30, tzinfo=timezone.utc),
                active=False,
                closed=True,
                archived=False,
                enable_order_book=True,
                best_bid=max(entry_price - 0.02, 0.01),
                best_ask=min(entry_price + 0.02, 0.99),
                spread=0.04,
                last_trade_price=entry_price,
                clob_token_ids=[f"token-{market_id}-yes", f"token-{market_id}-no"],
                raw_payload={"groupItemTitle": full_name},
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=f"mapping-{market_id}",
                f1_meeting_id=meeting.id,
                f1_session_id=sq.id,
                polymarket_event_id=event.id,
                polymarket_market_id=market_id,
                mapping_type="driver_pole_position",
                confidence=0.95 - (index * 0.01),
                matched_by="test_fixture",
                override_flag=False,
            )
        )
        session.add(
            PolymarketToken(
                id=f"token-{market_id}-yes",
                market_id=market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=entry_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketToken(
                id=f"token-{market_id}-no",
                market_id=market_id,
                outcome="No",
                outcome_index=1,
                latest_price=1.0 - entry_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketPriceHistory(
                id=f"price-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                observed_at_utc=datetime(2026, 3, 13, 4, 45, tzinfo=timezone.utc),
                price=entry_price,
                midpoint=entry_price,
                best_bid=max(entry_price - 0.02, 0.01),
                best_ask=min(entry_price + 0.02, 0.99),
                source_kind="clob",
                raw_payload={},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"trade-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                condition_id=f"condition-{market_id}",
                trade_timestamp_utc=datetime(2026, 3, 13, 4, 44, tzinfo=timezone.utc),
                side="buy",
                price=entry_price,
                size=10.0,
                raw_payload={},
            )
        )

    session.commit()


def test_build_china_fp1_to_sq_snapshot_creates_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_china_quicktest_fixture(session)

    def fail_hydrate(*args: object, **kwargs: object) -> None:
        raise AssertionError("hydrate_polymarket_market should not be called when history exists")

    monkeypatch.setattr("f1_polymarket_worker.quicktest.hydrate_polymarket_market", fail_hydrate)

    result = build_china_fp1_to_sq_snapshot(
        context,
        meeting_key=1280,
        season=2026,
        entry_offset_min=10,
    )

    assert result["status"] == "completed"
    assert result["row_count"] == 3
    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 3
    assert sum(int(row["label_yes"]) for row in rows) == 1
    assert all(row["entry_yes_price"] is not None for row in rows)
    assert all(row["entry_selection_rule"] == "first_observation_in_window" for row in rows)


def test_run_china_sq_pole_baseline_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_china_quicktest_fixture(session)

    monkeypatch.setattr(
        "f1_polymarket_worker.quicktest.hydrate_polymarket_market",
        lambda *args, **kwargs: None,
    )

    snapshot_result = build_china_fp1_to_sq_snapshot(context, meeting_key=1280, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]

    baseline_result = run_china_sq_pole_baseline(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert baseline_result["status"] == "completed"
    model_runs = session.scalars(
        select(ModelRun).where(ModelRun.feature_snapshot_id == snapshot_id)
    ).all()
    predictions = session.scalars(select(ModelPrediction)).all()
    assert len(model_runs) == 3
    assert len(predictions) == 9
    assert {row.model_name for row in model_runs} == {"market_implied", "fp1_pace", "hybrid"}

    report_result = report_china_sq_pole_quicktest(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert report_result["status"] == "completed"
    report_dir = (
        tmp_path / "reports" / "research" / "2026" / "2026-chinese-grand-prix-sq-pole-quicktest"
    )
    report_json = report_dir / "summary.json"
    assert report_json.exists()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["snapshot_id"] == snapshot_id
    assert report["market_count"] == 3
    assert set(report["baselines"]) == {"market_implied", "fp1_pace", "hybrid"}
    assert report["top_hybrid_predictions"][0]["driver_name"] == "Lewis Hamilton"


# ---------------------------------------------------------------------------
# Australian Grand Prix FP1→Q pole quicktest fixtures and tests
# ---------------------------------------------------------------------------


def seed_aus_quicktest_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting-1273",
        meeting_key=1273,
        season=2026,
        round_number=1,
        meeting_name="Australian Grand Prix",
        country_name="Australia",
        location="Melbourne",
        start_date_utc=datetime(2026, 3, 13, 1, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 15, 8, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    session.add(meeting)

    fp1 = F1Session(
        id="aus-session-fp1",
        meeting_id=meeting.id,
        session_key=11220,
        session_name="Practice 1",
        session_type="Practice",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 13, 2, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 13, 3, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    q = F1Session(
        id="aus-session-q",
        meeting_id=meeting.id,
        session_key=11223,
        session_name="Qualifying",
        session_type="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 14, 7, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 14, 8, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    session.add_all([fp1, q])

    drivers = [
        F1Driver(
            id="aus-driver-max",
            driver_number=1,
            full_name="Max Verstappen",
            first_name="Max",
            last_name="Verstappen",
            broadcast_name="M Verstappen",
            team_id="team-redbull",
            raw_payload={},
        ),
        F1Driver(
            id="aus-driver-lewis",
            driver_number=44,
            full_name="Lewis Hamilton",
            first_name="Lewis",
            last_name="Hamilton",
            broadcast_name="L Hamilton",
            team_id="team-ferrari",
            raw_payload={},
        ),
        F1Driver(
            id="aus-driver-charles",
            driver_number=16,
            full_name="Charles Leclerc",
            first_name="Charles",
            last_name="Leclerc",
            broadcast_name="C Leclerc",
            team_id="team-ferrari",
            raw_payload={},
        ),
    ]
    session.add_all(drivers)

    # FP1 results: Hamilton P1 (fastest), Verstappen P2, Leclerc P3
    fp1_results = [
        ("aus-driver-lewis", 1, 90.10, "leader", 0.0, 22),
        ("aus-driver-max", 2, 90.25, "time", 0.15, 23),
        ("aus-driver-charles", 3, 90.40, "time", 0.30, 21),
    ]
    # Q results: Verstappen P1 (pole winner), Hamilton P2, Leclerc P3
    q_results = [
        ("aus-driver-max", 1, 87.50),
        ("aus-driver-lewis", 2, 87.65),
        ("aus-driver-charles", 3, 87.80),
    ]
    for driver_id, position, lap_time, gap_status, gap_seconds, laps in fp1_results:
        session.add(
            F1SessionResult(
                id=f"aus-fp1-result-{driver_id}",
                session_id=fp1.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_display="0" if position == 1 else f"+{gap_seconds:.3f}",
                gap_to_leader_seconds=None if position == 1 else gap_seconds,
                gap_to_leader_status=gap_status,
                number_of_laps=laps,
                raw_payload={},
            )
        )
        for lap_number in range(1, laps + 1):
            session.add(
                F1Lap(
                    id=f"aus-fp1-lap-{driver_id}-{lap_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    lap_number=lap_number,
                    lap_duration_seconds=91.0,
                    raw_payload={},
                )
            )
        stint_total = 2 if driver_id != "aus-driver-charles" else 3
        for stint_number in range(1, stint_total + 1):
            session.add(
                F1Stint(
                    id=f"aus-fp1-stint-{driver_id}-{stint_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    stint_number=stint_number,
                    raw_payload={},
                )
            )
    for driver_id, position, lap_time in q_results:
        session.add(
            F1SessionResult(
                id=f"aus-q-result-{driver_id}",
                session_id=q.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_status="leader" if position == 1 else "time",
                raw_payload={},
            )
        )

    event = PolymarketEvent(
        id="aus-event-q",
        slug="f1-australian-grand-prix-qualifying-pole-winner-2026-03-14",
        ticker="f1-aus-q-pole-2026-03-14",
        title="Australian Grand Prix: Qualifying Pole Winner",
        description="Qualifying pole market",
        start_at_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 14, 7, 0, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        raw_payload={},
    )
    session.add(event)

    market_specs = [
        ("aus-market-max", "Max Verstappen", "Verstappen", 0.50),
        ("aus-market-lewis", "Lewis Hamilton", "Hamilton", 0.30),
        ("aus-market-charles", "Charles Leclerc", "Leclerc", 0.20),
    ]
    for index, (market_id, full_name, last_name, entry_price) in enumerate(market_specs):
        session.add(
            PolymarketMarket(
                id=market_id,
                event_id=event.id,
                question=(
                    f"Will {full_name} achieve pole position in Qualifying "
                    "at the 2026 F1 Australian Grand Prix?"
                ),
                slug=f"aus-q-pole-{last_name.lower()}",
                condition_id=f"aus-condition-{market_id}",
                question_id=f"aus-question-{market_id}",
                taxonomy="driver_pole_position",
                taxonomy_confidence=0.95,
                target_session_code="Q",
                driver_a=last_name,
                description="Australian Grand Prix: Qualifying Pole Winner",
                start_at_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 14, 7, 0, tzinfo=timezone.utc),
                active=False,
                closed=True,
                archived=False,
                enable_order_book=True,
                best_bid=max(entry_price - 0.02, 0.01),
                best_ask=min(entry_price + 0.02, 0.99),
                spread=0.04,
                last_trade_price=entry_price,
                clob_token_ids=[f"aus-token-{market_id}-yes", f"aus-token-{market_id}-no"],
                raw_payload={"groupItemTitle": full_name},
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=f"aus-mapping-{market_id}",
                f1_meeting_id=meeting.id,
                f1_session_id=q.id,
                polymarket_event_id=event.id,
                polymarket_market_id=market_id,
                mapping_type="driver_pole_position",
                confidence=0.95 - (index * 0.01),
                matched_by="test_fixture",
                override_flag=False,
            )
        )
        session.add(
            PolymarketToken(
                id=f"aus-token-{market_id}-yes",
                market_id=market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=entry_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketToken(
                id=f"aus-token-{market_id}-no",
                market_id=market_id,
                outcome="No",
                outcome_index=1,
                latest_price=1.0 - entry_price,
                raw_payload={},
            )
        )
        # Price history observation at FP1 end + 15 min → inside entry window
        session.add(
            PolymarketPriceHistory(
                id=f"aus-price-{market_id}",
                market_id=market_id,
                token_id=f"aus-token-{market_id}-yes",
                observed_at_utc=datetime(2026, 3, 13, 3, 45, tzinfo=timezone.utc),
                price=entry_price,
                midpoint=entry_price,
                best_bid=max(entry_price - 0.02, 0.01),
                best_ask=min(entry_price + 0.02, 0.99),
                source_kind="clob",
                raw_payload={},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"aus-trade-{market_id}",
                market_id=market_id,
                token_id=f"aus-token-{market_id}-yes",
                condition_id=f"aus-condition-{market_id}",
                trade_timestamp_utc=datetime(2026, 3, 13, 3, 44, tzinfo=timezone.utc),
                side="buy",
                price=entry_price,
                size=10.0,
                raw_payload={},
            )
        )

    session.commit()


def test_build_aus_fp1_to_q_snapshot_creates_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_aus_quicktest_fixture(session)

    def fail_hydrate(*args: object, **kwargs: object) -> None:
        raise AssertionError("hydrate_polymarket_market should not be called when history exists")

    monkeypatch.setattr("f1_polymarket_worker.quicktest.hydrate_polymarket_market", fail_hydrate)

    result = build_aus_fp1_to_q_snapshot(
        context,
        meeting_key=1273,
        season=2026,
        entry_offset_min=10,
    )

    assert result["status"] == "completed"
    assert result["row_count"] == 3
    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 3
    assert sum(int(row["label_yes"]) for row in rows) == 1
    assert all(row["entry_yes_price"] is not None for row in rows)
    assert all(row["entry_selection_rule"] == "first_observation_in_window" for row in rows)


def test_run_aus_q_pole_baseline_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_aus_quicktest_fixture(session)

    monkeypatch.setattr(
        "f1_polymarket_worker.quicktest.hydrate_polymarket_market",
        lambda *args, **kwargs: None,
    )

    snapshot_result = build_aus_fp1_to_q_snapshot(context, meeting_key=1273, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]

    baseline_result = run_aus_q_pole_baseline(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert baseline_result["status"] == "completed"
    model_runs = session.scalars(
        select(ModelRun).where(ModelRun.feature_snapshot_id == snapshot_id)
    ).all()
    predictions = session.scalars(select(ModelPrediction)).all()
    assert len(model_runs) == 3
    assert len(predictions) == 9
    assert {row.model_name for row in model_runs} == {"market_implied", "fp1_pace", "hybrid"}

    report_result = report_aus_q_pole_quicktest(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert report_result["status"] == "completed"
    report_dir = (
        tmp_path / "reports" / "research" / "2026" / "2026-australian-grand-prix-q-pole-quicktest"
    )
    report_json = report_dir / "summary.json"
    assert report_json.exists()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["snapshot_id"] == snapshot_id
    assert report["market_count"] == 3
    assert set(report["baselines"]) == {"market_implied", "fp1_pace", "hybrid"}
    # Verstappen is Q pole winner (label_yes=1); market price 0.50 (highest) → top hybrid pick
    assert report["top_hybrid_predictions"][0]["driver_name"] == "Max Verstappen"
