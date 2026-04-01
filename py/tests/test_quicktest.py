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
    build_japan_fp1_to_q_snapshot,
    build_japan_pre_weekend_snapshot,
    report_aus_q_pole_quicktest,
    report_china_sq_pole_quicktest,
    report_japan_fp1_q_pole_quicktest,
    report_japan_q_pole_quicktest,
    run_aus_q_pole_baseline,
    run_china_sq_pole_baseline,
    run_japan_fp1_q_pole_baseline,
    run_japan_q_pole_baseline,
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

    # All 22 drivers on the 2026 F1 grid — real data from OpenF1 API
    drivers = [
        F1Driver(id="driver-norris", driver_number=1, full_name="Lando NORRIS",
                 first_name="Lando", last_name="Norris", broadcast_name="L NORRIS",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="driver-max", driver_number=3, full_name="Max VERSTAPPEN",
                 first_name="Max", last_name="Verstappen", broadcast_name="M VERSTAPPEN",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="driver-bortoleto", driver_number=5, full_name="Gabriel BORTOLETO",
                 first_name="Gabriel", last_name="Bortoleto", broadcast_name="G BORTOLETO",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="driver-hadjar", driver_number=6, full_name="Isack HADJAR",
                 first_name="Isack", last_name="Hadjar", broadcast_name="I HADJAR",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="driver-gasly", driver_number=10, full_name="Pierre GASLY",
                 first_name="Pierre", last_name="Gasly", broadcast_name="P GASLY",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="driver-perez", driver_number=11, full_name="Sergio PEREZ",
                 first_name="Sergio", last_name="Perez", broadcast_name="S PEREZ",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="driver-antonelli", driver_number=12, full_name="Kimi ANTONELLI",
                 first_name="Kimi", last_name="Antonelli", broadcast_name="K ANTONELLI",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="driver-alonso", driver_number=14, full_name="Fernando ALONSO",
                 first_name="Fernando", last_name="Alonso", broadcast_name="F ALONSO",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="driver-charles", driver_number=16, full_name="Charles LECLERC",
                 first_name="Charles", last_name="Leclerc", broadcast_name="C LECLERC",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="driver-stroll", driver_number=18, full_name="Lance STROLL",
                 first_name="Lance", last_name="Stroll", broadcast_name="L STROLL",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="driver-albon", driver_number=23, full_name="Alexander ALBON",
                 first_name="Alexander", last_name="Albon", broadcast_name="A ALBON",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="driver-hulkenberg", driver_number=27, full_name="Nico HULKENBERG",
                 first_name="Nico", last_name="Hulkenberg", broadcast_name="N HULKENBERG",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="driver-lawson", driver_number=30, full_name="Liam LAWSON",
                 first_name="Liam", last_name="Lawson", broadcast_name="L LAWSON",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="driver-ocon", driver_number=31, full_name="Esteban OCON",
                 first_name="Esteban", last_name="Ocon", broadcast_name="E OCON",
                 team_id="team-haas", raw_payload={}),
        F1Driver(id="driver-lindblad", driver_number=41, full_name="Arvid LINDBLAD",
                 first_name="Arvid", last_name="Lindblad", broadcast_name="A LINDBLAD",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="driver-colapinto", driver_number=43, full_name="Franco COLAPINTO",
                 first_name="Franco", last_name="Colapinto", broadcast_name="F COLAPINTO",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="driver-lewis", driver_number=44, full_name="Lewis HAMILTON",
                 first_name="Lewis", last_name="Hamilton", broadcast_name="L HAMILTON",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="driver-sainz", driver_number=55, full_name="Carlos SAINZ",
                 first_name="Carlos", last_name="Sainz", broadcast_name="C SAINZ",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="driver-russell", driver_number=63, full_name="George RUSSELL",
                 first_name="George", last_name="Russell", broadcast_name="G RUSSELL",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="driver-bottas", driver_number=77, full_name="Valtteri BOTTAS",
                 first_name="Valtteri", last_name="Bottas", broadcast_name="V BOTTAS",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="driver-piastri", driver_number=81, full_name="Oscar PIASTRI",
                 first_name="Oscar", last_name="Piastri", broadcast_name="O PIASTRI",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="driver-bearman", driver_number=87, full_name="Oliver BEARMAN",
                 first_name="Oliver", last_name="Bearman", broadcast_name="O BEARMAN",
                 team_id="team-haas", raw_payload={}),
    ]
    session.add_all(drivers)

    # Real 2026 China GP FP1 results from OpenF1 API (all 22 drivers)
    fp1_results = [
        ("driver-russell",    1,  92.741, "leader", 0.0,   29),
        ("driver-antonelli",  2,  92.861, "time",   0.120, 30),
        ("driver-norris",     3,  93.296, "time",   0.555, 29),
        ("driver-piastri",    4,  93.472, "time",   0.731, 28),
        ("driver-charles",    5,  93.599, "time",   0.858, 28),
        ("driver-lewis",      6,  94.129, "time",   1.388, 26),
        ("driver-bearman",    7,  94.426, "time",   1.685, 28),
        ("driver-max",        8,  94.541, "time",   1.800, 24),
        ("driver-hulkenberg", 9,  94.639, "time",   1.898, 27),
        ("driver-gasly",      10, 94.676, "time",   1.935, 28),
        ("driver-lawson",     11, 94.773, "time",   2.032, 29),
        ("driver-bortoleto",  12, 94.828, "time",   2.087, 26),
        ("driver-hadjar",     13, 94.856, "time",   2.115, 26),
        ("driver-ocon",       14, 94.877, "time",   2.136, 25),
        ("driver-colapinto",  15, 94.947, "time",   2.206, 26),
        ("driver-albon",      16, 95.480, "time",   2.739, 31),
        ("driver-sainz",      17, 95.679, "time",   2.938, 18),
        ("driver-alonso",     18, 95.856, "time",   3.115, 18),
        ("driver-bottas",     19, 96.057, "time",   3.316, 25),
        ("driver-stroll",     20, 97.224, "time",   4.483, 20),
        ("driver-lindblad",   21, 97.896, "time",   5.155, 6),
        ("driver-perez",      22, 99.200, "time",   6.459, 13),
    ]
    # Real 2026 China GP Sprint Qualifying results from OpenF1 API (21 drivers)
    # Russell P1 (SQ pole, 91.520s); Bottas DNS as P22 placeholder
    sq_results = [
        ("driver-russell",    1,  91.520),
        ("driver-antonelli",  2,  91.809),
        ("driver-norris",     3,  92.141),
        ("driver-lewis",      4,  92.161),
        ("driver-piastri",    5,  92.224),
        ("driver-charles",    6,  92.528),
        ("driver-gasly",      7,  92.888),
        ("driver-max",        8,  93.254),
        ("driver-bearman",    9,  93.409),
        ("driver-hadjar",     10, 93.723),
        ("driver-hulkenberg", 11, 93.635),
        ("driver-ocon",       12, 93.639),
        ("driver-lawson",     13, 93.714),
        ("driver-bortoleto",  14, 93.774),
        ("driver-lindblad",   15, 94.048),
        ("driver-colapinto",  16, 94.327),
        ("driver-sainz",      17, 94.761),
        ("driver-albon",      18, 95.305),
        ("driver-alonso",     19, 95.581),
        ("driver-stroll",     20, 96.151),
        ("driver-bottas",     21, 97.378),
        ("driver-perez",      22, 99.500),
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
        for lap_number in range(1, min(laps, 5) + 1):
            session.add(
                F1Lap(
                    id=f"fp1-lap-{driver_id}-{lap_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    lap_number=lap_number,
                    lap_duration_seconds=lap_time + 1.0,
                    raw_payload={},
                )
            )
        stint_total = 2 if position <= 10 else 3
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

    # Synthetic Polymarket prices reflecting post-FP1 odds for all 22 drivers
    # Russell FP1 P1 → market favorite at 0.18; Antonelli FP1 P2 → 0.14
    market_specs = [
        ("market-russell",    "George RUSSELL",     "Russell",    0.18),
        ("market-antonelli",  "Kimi ANTONELLI",     "Antonelli",  0.14),
        ("market-norris",     "Lando NORRIS",       "Norris",     0.12),
        ("market-piastri",    "Oscar PIASTRI",      "Piastri",    0.08),
        ("market-charles",    "Charles LECLERC",    "Leclerc",    0.07),
        ("market-lewis",      "Lewis HAMILTON",     "Hamilton",   0.06),
        ("market-max",        "Max VERSTAPPEN",     "Verstappen", 0.05),
        ("market-bearman",    "Oliver BEARMAN",     "Bearman",    0.04),
        ("market-hulkenberg", "Nico HULKENBERG",    "Hulkenberg", 0.03),
        ("market-gasly",      "Pierre GASLY",       "Gasly",      0.03),
        ("market-lawson",     "Liam LAWSON",        "Lawson",     0.02),
        ("market-hadjar",     "Isack HADJAR",       "Hadjar",     0.02),
        ("market-bortoleto",  "Gabriel BORTOLETO",  "Bortoleto",  0.02),
        ("market-ocon",       "Esteban OCON",       "Ocon",       0.02),
        ("market-colapinto",  "Franco COLAPINTO",   "Colapinto",  0.01),
        ("market-albon",      "Alexander ALBON",    "Albon",      0.01),
        ("market-sainz",      "Carlos SAINZ",       "Sainz",      0.01),
        ("market-alonso",     "Fernando ALONSO",    "Alonso",     0.01),
        ("market-lindblad",   "Arvid LINDBLAD",     "Lindblad",   0.01),
        ("market-bottas",     "Valtteri BOTTAS",    "Bottas",     0.01),
        ("market-stroll",     "Lance STROLL",       "Stroll",     0.01),
        ("market-perez",      "Sergio PEREZ",       "Perez",      0.01),
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

    monkeypatch.setattr("f1_polymarket_worker.gp_registry.hydrate_polymarket_market", fail_hydrate)

    result = build_china_fp1_to_sq_snapshot(
        context,
        meeting_key=1280,
        season=2026,
        entry_offset_min=10,
    )

    assert result["status"] == "completed"
    assert result["row_count"] == 22
    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 22
    assert sum(int(row["label_yes"]) for row in rows) == 1
    assert all(row["entry_yes_price"] is not None for row in rows)
    assert all(row["entry_selection_rule"] == "first_observation_in_window" for row in rows)


def test_run_china_sq_pole_baseline_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_china_quicktest_fixture(session)

    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market",
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
    assert len(predictions) == 66
    assert {row.model_name for row in model_runs} == {"market_implied", "fp1_pace", "hybrid"}
    assert all("calibration_buckets" in (row.metrics_json or {}) for row in model_runs)

    report_result = report_china_sq_pole_quicktest(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert report_result["status"] == "completed"
    report_dir = (
        tmp_path / "reports" / "research" / "2026" / "2026-chinese-grand-prix-sq-pole-quicktest"
    )
    report_json = report_dir / "summary.json"
    assert report_json.exists()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["snapshot_id"] == snapshot_id
    assert report["market_count"] == 22
    assert set(report["baselines"]) == {"market_implied", "fp1_pace", "hybrid"}
    # Russell is FP1 P1 + highest market price (0.18) → top hybrid pick
    assert report["top_hybrid_predictions"][0]["driver_name"] == "George RUSSELL"


# ---------------------------------------------------------------------------
# Australian Grand Prix FP1→Q pole quicktest fixtures and tests
# ---------------------------------------------------------------------------


def seed_aus_quicktest_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting-1279",
        meeting_key=1279,
        season=2026,
        round_number=1,
        meeting_name="Australian Grand Prix",
        country_name="Australia",
        location="Melbourne",
        start_date_utc=datetime(2026, 3, 6, 1, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 8, 8, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    session.add(meeting)

    fp1 = F1Session(
        id="aus-session-fp1",
        meeting_id=meeting.id,
        session_key=11227,
        session_name="Practice 1",
        session_type="Practice",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 6, 1, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 6, 2, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    q = F1Session(
        id="aus-session-q",
        meeting_id=meeting.id,
        session_key=11230,
        session_name="Qualifying",
        session_type="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 7, 5, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 7, 6, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Australian Grand Prix"},
    )
    session.add_all([fp1, q])

    # All 22 drivers on the 2026 F1 grid — real data from OpenF1 API
    drivers = [
        F1Driver(id="aus-driver-norris", driver_number=1, full_name="Lando NORRIS",
                 first_name="Lando", last_name="Norris", broadcast_name="L NORRIS",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="aus-driver-verstappen", driver_number=3, full_name="Max VERSTAPPEN",
                 first_name="Max", last_name="Verstappen", broadcast_name="M VERSTAPPEN",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="aus-driver-bortoleto", driver_number=5, full_name="Gabriel BORTOLETO",
                 first_name="Gabriel", last_name="Bortoleto", broadcast_name="G BORTOLETO",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="aus-driver-hadjar", driver_number=6, full_name="Isack HADJAR",
                 first_name="Isack", last_name="Hadjar", broadcast_name="I HADJAR",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="aus-driver-gasly", driver_number=10, full_name="Pierre GASLY",
                 first_name="Pierre", last_name="Gasly", broadcast_name="P GASLY",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="aus-driver-perez", driver_number=11, full_name="Sergio PEREZ",
                 first_name="Sergio", last_name="Perez", broadcast_name="S PEREZ",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="aus-driver-antonelli", driver_number=12, full_name="Kimi ANTONELLI",
                 first_name="Kimi", last_name="Antonelli", broadcast_name="K ANTONELLI",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="aus-driver-alonso", driver_number=14, full_name="Fernando ALONSO",
                 first_name="Fernando", last_name="Alonso", broadcast_name="F ALONSO",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="aus-driver-leclerc", driver_number=16, full_name="Charles LECLERC",
                 first_name="Charles", last_name="Leclerc", broadcast_name="C LECLERC",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="aus-driver-stroll", driver_number=18, full_name="Lance STROLL",
                 first_name="Lance", last_name="Stroll", broadcast_name="L STROLL",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="aus-driver-albon", driver_number=23, full_name="Alexander ALBON",
                 first_name="Alexander", last_name="Albon", broadcast_name="A ALBON",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="aus-driver-hulkenberg", driver_number=27, full_name="Nico HULKENBERG",
                 first_name="Nico", last_name="Hulkenberg", broadcast_name="N HULKENBERG",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="aus-driver-lawson", driver_number=30, full_name="Liam LAWSON",
                 first_name="Liam", last_name="Lawson", broadcast_name="L LAWSON",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="aus-driver-ocon", driver_number=31, full_name="Esteban OCON",
                 first_name="Esteban", last_name="Ocon", broadcast_name="E OCON",
                 team_id="team-haas", raw_payload={}),
        F1Driver(id="aus-driver-lindblad", driver_number=41, full_name="Arvid LINDBLAD",
                 first_name="Arvid", last_name="Lindblad", broadcast_name="A LINDBLAD",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="aus-driver-colapinto", driver_number=43, full_name="Franco COLAPINTO",
                 first_name="Franco", last_name="Colapinto", broadcast_name="F COLAPINTO",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="aus-driver-hamilton", driver_number=44, full_name="Lewis HAMILTON",
                 first_name="Lewis", last_name="Hamilton", broadcast_name="L HAMILTON",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="aus-driver-sainz", driver_number=55, full_name="Carlos SAINZ",
                 first_name="Carlos", last_name="Sainz", broadcast_name="C SAINZ",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="aus-driver-russell", driver_number=63, full_name="George RUSSELL",
                 first_name="George", last_name="Russell", broadcast_name="G RUSSELL",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="aus-driver-bottas", driver_number=77, full_name="Valtteri BOTTAS",
                 first_name="Valtteri", last_name="Bottas", broadcast_name="V BOTTAS",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="aus-driver-piastri", driver_number=81, full_name="Oscar PIASTRI",
                 first_name="Oscar", last_name="Piastri", broadcast_name="O PIASTRI",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="aus-driver-bearman", driver_number=87, full_name="Oliver BEARMAN",
                 first_name="Oliver", last_name="Bearman", broadcast_name="O BEARMAN",
                 team_id="team-haas", raw_payload={}),
    ]
    session.add_all(drivers)

    # Real 2026 AUS GP FP1 results from OpenF1 API (21 drivers; Alonso DNS)
    # Alonso did not set a time in FP1 — assigned P22 placeholder for fixture completeness
    fp1_results = [
        ("aus-driver-leclerc",    1, 80.267, "leader", 0.0,    33),
        ("aus-driver-hamilton",   2, 80.736, "time",   0.469,  30),
        ("aus-driver-verstappen", 3, 80.789, "time",   0.522,  27),
        ("aus-driver-hadjar",     4, 81.087, "time",   0.820,  24),
        ("aus-driver-lindblad",   5, 81.313, "time",   1.046,  22),
        ("aus-driver-piastri",    6, 81.342, "time",   1.075,  21),
        ("aus-driver-russell",    7, 81.371, "time",   1.104,  26),
        ("aus-driver-antonelli",  8, 81.376, "time",   1.109,  24),
        ("aus-driver-bortoleto",  9, 81.696, "time",   1.429,  23),
        ("aus-driver-hulkenberg", 10, 81.969, "time",  1.702,  21),
        ("aus-driver-ocon",       11, 82.161, "time",  1.894,  28),
        ("aus-driver-sainz",      12, 82.323, "time",  2.056,  30),
        ("aus-driver-lawson",     13, 82.613, "time",  2.346,  28),
        ("aus-driver-bearman",    14, 82.682, "time",  2.415,  25),
        ("aus-driver-albon",      15, 83.130, "time",  2.863,  24),
        ("aus-driver-colapinto",  16, 83.325, "time",  3.058,  26),
        ("aus-driver-bottas",     17, 84.022, "time",  3.755,  24),
        ("aus-driver-gasly",      18, 84.035, "time",  3.768,  27),
        ("aus-driver-norris",     19, 84.391, "time",  4.124,  7),
        ("aus-driver-perez",      20, 84.620, "time",  4.353,  14),
        ("aus-driver-stroll",     21, 110.334, "time", 30.067, 3),
        ("aus-driver-alonso",     22, 82.500, "time",  2.233,  10),
    ]
    # Real 2026 AUS GP Q results from OpenF1 API (19 drivers)
    # Russell P1 (pole, 78.518s); Verstappen, Sainz, Stroll did not set Q times
    q_results = [
        ("aus-driver-russell",    1,  78.518),
        ("aus-driver-antonelli",  2,  78.811),
        ("aus-driver-hadjar",     3,  79.303),
        ("aus-driver-leclerc",    4,  79.327),
        ("aus-driver-piastri",    5,  79.380),
        ("aus-driver-norris",     6,  79.475),
        ("aus-driver-hamilton",   7,  79.478),
        ("aus-driver-lawson",     8,  79.994),
        ("aus-driver-lindblad",   9,  81.247),
        ("aus-driver-bortoleto",  10, 80.221),
        ("aus-driver-hulkenberg", 11, 80.303),
        ("aus-driver-bearman",    12, 80.311),
        ("aus-driver-ocon",       13, 80.491),
        ("aus-driver-gasly",      14, 80.501),
        ("aus-driver-albon",      15, 80.941),
        ("aus-driver-colapinto",  16, 81.270),
        ("aus-driver-alonso",     17, 81.969),
        ("aus-driver-perez",      18, 82.605),
        ("aus-driver-bottas",     19, 83.244),
        ("aus-driver-verstappen", 20, 85.000),
        ("aus-driver-sainz",      21, 85.500),
        ("aus-driver-stroll",     22, 112.000),
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
        for lap_number in range(1, min(laps, 5) + 1):
            session.add(
                F1Lap(
                    id=f"aus-fp1-lap-{driver_id}-{lap_number}",
                    session_id=fp1.id,
                    driver_id=driver_id,
                    lap_number=lap_number,
                    lap_duration_seconds=lap_time + 1.0,
                    raw_payload={},
                )
            )
        stint_count = 2 if position <= 10 else 3
        for stint_number in range(1, stint_count + 1):
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
        slug="f1-australian-grand-prix-qualifying-pole-winner-2026-03-07",
        ticker="f1-aus-q-pole-2026-03-07",
        title="Australian Grand Prix: Qualifying Pole Winner",
        description="Qualifying pole market",
        start_at_utc=datetime(2026, 3, 4, 12, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 7, 5, 0, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        raw_payload={},
    )
    session.add(event)

    # Synthetic Polymarket prices reflecting post-FP1 implied odds for all 22 drivers
    # Leclerc FP1 P1 → market favorite at 0.20; Russell FP1 P7 → underpriced at 0.06
    market_specs = [
        ("aus-market-leclerc",    "Charles LECLERC",    "Leclerc",    0.20),
        ("aus-market-hamilton",   "Lewis HAMILTON",      "Hamilton",   0.12),
        ("aus-market-verstappen", "Max VERSTAPPEN",      "Verstappen", 0.12),
        ("aus-market-hadjar",     "Isack HADJAR",        "Hadjar",     0.08),
        ("aus-market-piastri",    "Oscar PIASTRI",       "Piastri",    0.07),
        ("aus-market-russell",    "George RUSSELL",      "Russell",    0.06),
        ("aus-market-antonelli",  "Kimi ANTONELLI",      "Antonelli",  0.06),
        ("aus-market-norris",     "Lando NORRIS",        "Norris",     0.05),
        ("aus-market-lindblad",   "Arvid LINDBLAD",      "Lindblad",   0.04),
        ("aus-market-bortoleto",  "Gabriel BORTOLETO",   "Bortoleto",  0.03),
        ("aus-market-hulkenberg", "Nico HULKENBERG",     "Hulkenberg", 0.02),
        ("aus-market-ocon",       "Esteban OCON",        "Ocon",       0.02),
        ("aus-market-sainz",      "Carlos SAINZ",        "Sainz",      0.02),
        ("aus-market-lawson",     "Liam LAWSON",         "Lawson",     0.02),
        ("aus-market-bearman",    "Oliver BEARMAN",      "Bearman",    0.01),
        ("aus-market-albon",      "Alexander ALBON",     "Albon",      0.01),
        ("aus-market-gasly",      "Pierre GASLY",        "Gasly",      0.01),
        ("aus-market-colapinto",  "Franco COLAPINTO",    "Colapinto",  0.01),
        ("aus-market-alonso",     "Fernando ALONSO",     "Alonso",     0.01),
        ("aus-market-bottas",     "Valtteri BOTTAS",     "Bottas",     0.01),
        ("aus-market-perez",      "Sergio PEREZ",        "Perez",      0.01),
        ("aus-market-stroll",     "Lance STROLL",        "Stroll",     0.01),
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
                start_at_utc=datetime(2026, 3, 4, 12, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 7, 5, 0, tzinfo=timezone.utc),
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
        session.add(
            PolymarketPriceHistory(
                id=f"aus-price-{market_id}",
                market_id=market_id,
                token_id=f"aus-token-{market_id}-yes",
                observed_at_utc=datetime(2026, 3, 6, 2, 45, tzinfo=timezone.utc),
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
                trade_timestamp_utc=datetime(2026, 3, 6, 2, 44, tzinfo=timezone.utc),
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

    monkeypatch.setattr("f1_polymarket_worker.gp_registry.hydrate_polymarket_market", fail_hydrate)

    result = build_aus_fp1_to_q_snapshot(
        context,
        meeting_key=1279,
        season=2026,
        entry_offset_min=10,
    )

    assert result["status"] == "completed"
    assert result["row_count"] == 22
    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 22
    assert sum(int(row["label_yes"]) for row in rows) == 1
    assert all(row["entry_yes_price"] is not None for row in rows)
    assert all(row["entry_selection_rule"] == "first_observation_in_window" for row in rows)


def test_run_aus_q_pole_baseline_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_aus_quicktest_fixture(session)

    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market",
        lambda *args, **kwargs: None,
    )

    snapshot_result = build_aus_fp1_to_q_snapshot(context, meeting_key=1279, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]

    baseline_result = run_aus_q_pole_baseline(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert baseline_result["status"] == "completed"
    model_runs = session.scalars(
        select(ModelRun).where(ModelRun.feature_snapshot_id == snapshot_id)
    ).all()
    predictions = session.scalars(select(ModelPrediction)).all()
    assert len(model_runs) == 3
    assert len(predictions) == 66
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
    assert report["market_count"] == 22
    assert set(report["baselines"]) == {"market_implied", "fp1_pace", "hybrid"}
    # Leclerc is FP1 P1 + highest market price (0.20) → top hybrid pick
    # Russell (actual pole winner) is FP1 P7 with market price 0.06
    assert report["top_hybrid_predictions"][0]["driver_name"] == "Charles LECLERC"


# ---------------------------------------------------------------------------
# Japanese Grand Prix pre-weekend Q pole quicktest fixtures and tests
# ---------------------------------------------------------------------------


def seed_japan_quicktest_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting-1281",
        meeting_key=1281,
        season=2026,
        round_number=3,
        meeting_name="Japanese Grand Prix",
        country_name="Japan",
        location="Suzuka",
        start_date_utc=datetime(2026, 3, 27, 2, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 29, 8, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    session.add(meeting)

    # FP1 session stores pre-computed historical form (avg AUS+China FP1
    # positions) rather than live FP1 results — Japan FP1 hasn't happened.
    fp1 = F1Session(
        id="japan-session-fp1",
        meeting_id=meeting.id,
        session_key=11246,
        session_name="Practice 1",
        session_type="Practice",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 3, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    q = F1Session(
        id="japan-session-q",
        meeting_id=meeting.id,
        session_key=11249,
        session_name="Qualifying",
        session_type="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    session.add_all([fp1, q])

    # All 22 drivers on the 2026 F1 grid
    drivers = [
        F1Driver(id="japan-driver-norris", driver_number=1, full_name="Lando NORRIS",
                 first_name="Lando", last_name="Norris", broadcast_name="L NORRIS",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="japan-driver-verstappen", driver_number=3, full_name="Max VERSTAPPEN",
                 first_name="Max", last_name="Verstappen", broadcast_name="M VERSTAPPEN",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="japan-driver-bortoleto", driver_number=5, full_name="Gabriel BORTOLETO",
                 first_name="Gabriel", last_name="Bortoleto", broadcast_name="G BORTOLETO",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="japan-driver-hadjar", driver_number=6, full_name="Isack HADJAR",
                 first_name="Isack", last_name="Hadjar", broadcast_name="I HADJAR",
                 team_id="team-redbull", raw_payload={}),
        F1Driver(id="japan-driver-gasly", driver_number=10, full_name="Pierre GASLY",
                 first_name="Pierre", last_name="Gasly", broadcast_name="P GASLY",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="japan-driver-perez", driver_number=11, full_name="Sergio PEREZ",
                 first_name="Sergio", last_name="Perez", broadcast_name="S PEREZ",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="japan-driver-antonelli", driver_number=12, full_name="Kimi ANTONELLI",
                 first_name="Kimi", last_name="Antonelli", broadcast_name="K ANTONELLI",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="japan-driver-alonso", driver_number=14, full_name="Fernando ALONSO",
                 first_name="Fernando", last_name="Alonso", broadcast_name="F ALONSO",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="japan-driver-leclerc", driver_number=16, full_name="Charles LECLERC",
                 first_name="Charles", last_name="Leclerc", broadcast_name="C LECLERC",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="japan-driver-stroll", driver_number=18, full_name="Lance STROLL",
                 first_name="Lance", last_name="Stroll", broadcast_name="L STROLL",
                 team_id="team-astonmartin", raw_payload={}),
        F1Driver(id="japan-driver-albon", driver_number=23, full_name="Alexander ALBON",
                 first_name="Alexander", last_name="Albon", broadcast_name="A ALBON",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="japan-driver-hulkenberg", driver_number=27, full_name="Nico HULKENBERG",
                 first_name="Nico", last_name="Hulkenberg", broadcast_name="N HULKENBERG",
                 team_id="team-audi", raw_payload={}),
        F1Driver(id="japan-driver-lawson", driver_number=30, full_name="Liam LAWSON",
                 first_name="Liam", last_name="Lawson", broadcast_name="L LAWSON",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="japan-driver-ocon", driver_number=31, full_name="Esteban OCON",
                 first_name="Esteban", last_name="Ocon", broadcast_name="E OCON",
                 team_id="team-haas", raw_payload={}),
        F1Driver(id="japan-driver-lindblad", driver_number=41, full_name="Arvid LINDBLAD",
                 first_name="Arvid", last_name="Lindblad", broadcast_name="A LINDBLAD",
                 team_id="team-racingbulls", raw_payload={}),
        F1Driver(id="japan-driver-colapinto", driver_number=43, full_name="Franco COLAPINTO",
                 first_name="Franco", last_name="Colapinto", broadcast_name="F COLAPINTO",
                 team_id="team-alpine", raw_payload={}),
        F1Driver(id="japan-driver-hamilton", driver_number=44, full_name="Lewis HAMILTON",
                 first_name="Lewis", last_name="Hamilton", broadcast_name="L HAMILTON",
                 team_id="team-ferrari", raw_payload={}),
        F1Driver(id="japan-driver-sainz", driver_number=55, full_name="Carlos SAINZ",
                 first_name="Carlos", last_name="Sainz", broadcast_name="C SAINZ",
                 team_id="team-williams", raw_payload={}),
        F1Driver(id="japan-driver-russell", driver_number=63, full_name="George RUSSELL",
                 first_name="George", last_name="Russell", broadcast_name="G RUSSELL",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="japan-driver-bottas", driver_number=77, full_name="Valtteri BOTTAS",
                 first_name="Valtteri", last_name="Bottas", broadcast_name="V BOTTAS",
                 team_id="team-cadillac", raw_payload={}),
        F1Driver(id="japan-driver-piastri", driver_number=81, full_name="Oscar PIASTRI",
                 first_name="Oscar", last_name="Piastri", broadcast_name="O PIASTRI",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="japan-driver-bearman", driver_number=87, full_name="Oliver BEARMAN",
                 first_name="Oliver", last_name="Bearman", broadcast_name="O BEARMAN",
                 team_id="team-haas", raw_payload={}),
    ]
    session.add_all(drivers)

    # Pre-computed historical form data (avg of AUS FP1 + China FP1 positions)
    # stored as FP1 session results for the snapshot builder to consume.
    # Russell: avg(AUS P7, China P1) = 4.0; Leclerc: avg(AUS P1, China P5) = 3.0
    form_results = [
        # (driver_id, position, avg_time, gap_to_leader, avg_laps)
        ("japan-driver-leclerc",    1,  86.933, 0.0,   30),
        ("japan-driver-russell",    2,  87.056, 0.123, 27),
        ("japan-driver-antonelli",  3,  87.118, 0.185, 27),
        ("japan-driver-hamilton",   4,  87.432, 0.499, 28),
        ("japan-driver-norris",     5,  88.843, 1.910, 18),
        ("japan-driver-piastri",    6,  87.407, 0.474, 24),
        ("japan-driver-hadjar",     7,  87.971, 1.038, 25),
        ("japan-driver-verstappen", 8,  87.665, 0.732, 25),
        ("japan-driver-bearman",    9,  88.554, 1.621, 26),
        ("japan-driver-lindblad",   10, 89.604, 2.671, 14),
        ("japan-driver-bortoleto",  11, 88.262, 1.329, 24),
        ("japan-driver-hulkenberg", 12, 88.304, 1.371, 24),
        ("japan-driver-ocon",       13, 88.519, 1.586, 26),
        ("japan-driver-lawson",     14, 88.693, 1.760, 28),
        ("japan-driver-gasly",      15, 89.355, 2.422, 27),
        ("japan-driver-colapinto",  16, 89.136, 2.203, 26),
        ("japan-driver-sainz",      17, 89.001, 2.068, 24),
        ("japan-driver-albon",      18, 89.305, 2.372, 27),
        ("japan-driver-alonso",     19, 89.178, 2.245, 14),
        ("japan-driver-bottas",     20, 90.039, 3.106, 24),
        ("japan-driver-stroll",     21, 103.779, 16.846, 11),
        ("japan-driver-perez",      22, 91.910, 4.977, 13),
    ]
    for driver_id, position, avg_time, gap, avg_laps in form_results:
        session.add(
            F1SessionResult(
                id=f"japan-form-{driver_id}",
                session_id=fp1.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=avg_time,
                result_time_kind="best_lap",
                gap_to_leader_display="0" if position == 1 else f"+{gap:.3f}",
                gap_to_leader_seconds=None if position == 1 else gap,
                gap_to_leader_status="leader" if position == 1 else "time",
                number_of_laps=avg_laps,
                raw_payload={},
            )
        )

    # Q results — Russell P1 (predicted pole based on strongest historical form)
    q_results = [
        ("japan-driver-russell",    1,  88.500),
        ("japan-driver-leclerc",    2,  88.600),
        ("japan-driver-antonelli",  3,  88.700),
        ("japan-driver-norris",     4,  88.800),
        ("japan-driver-piastri",    5,  88.900),
        ("japan-driver-hamilton",   6,  89.000),
        ("japan-driver-verstappen", 7,  89.100),
        ("japan-driver-hadjar",     8,  89.200),
        ("japan-driver-bearman",    9,  89.300),
        ("japan-driver-hulkenberg", 10, 89.400),
        ("japan-driver-bortoleto",  11, 89.500),
        ("japan-driver-lindblad",   12, 89.600),
        ("japan-driver-ocon",       13, 89.700),
        ("japan-driver-lawson",     14, 89.800),
        ("japan-driver-gasly",      15, 89.900),
        ("japan-driver-colapinto",  16, 90.000),
        ("japan-driver-albon",      17, 90.100),
        ("japan-driver-sainz",      18, 90.200),
        ("japan-driver-alonso",     19, 90.300),
        ("japan-driver-bottas",     20, 90.400),
        ("japan-driver-stroll",     21, 90.500),
        ("japan-driver-perez",      22, 90.600),
    ]
    for driver_id, position, lap_time in q_results:
        session.add(
            F1SessionResult(
                id=f"japan-q-result-{driver_id}",
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
        id="japan-event-q",
        slug="f1-japanese-grand-prix-qualifying-pole-winner-2026-03-28",
        ticker="f1-japan-q-pole-2026-03-28",
        title="Japanese Grand Prix: Qualifying Pole Winner",
        description="Qualifying pole market",
        start_at_utc=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        raw_payload={},
    )
    session.add(event)

    # Synthetic Polymarket prices — pre-weekend odds reflecting historical form
    # Leclerc best historical form (form P1) → market favorite at 0.16
    market_specs = [
        ("japan-market-leclerc",    "Charles LECLERC",    "Leclerc",    0.16),
        ("japan-market-russell",    "George RUSSELL",     "Russell",    0.14),
        ("japan-market-antonelli",  "Kimi ANTONELLI",     "Antonelli",  0.10),
        ("japan-market-norris",     "Lando NORRIS",       "Norris",     0.08),
        ("japan-market-hamilton",   "Lewis HAMILTON",      "Hamilton",   0.07),
        ("japan-market-piastri",    "Oscar PIASTRI",      "Piastri",    0.07),
        ("japan-market-verstappen", "Max VERSTAPPEN",      "Verstappen", 0.06),
        ("japan-market-hadjar",     "Isack HADJAR",        "Hadjar",     0.05),
        ("japan-market-bearman",    "Oliver BEARMAN",      "Bearman",    0.03),
        ("japan-market-lindblad",   "Arvid LINDBLAD",      "Lindblad",   0.03),
        ("japan-market-bortoleto",  "Gabriel BORTOLETO",   "Bortoleto",  0.02),
        ("japan-market-hulkenberg", "Nico HULKENBERG",     "Hulkenberg", 0.02),
        ("japan-market-ocon",       "Esteban OCON",        "Ocon",       0.02),
        ("japan-market-lawson",     "Liam LAWSON",         "Lawson",     0.02),
        ("japan-market-gasly",      "Pierre GASLY",        "Gasly",      0.02),
        ("japan-market-colapinto",  "Franco COLAPINTO",    "Colapinto",  0.01),
        ("japan-market-albon",      "Alexander ALBON",     "Albon",      0.01),
        ("japan-market-sainz",      "Carlos SAINZ",        "Sainz",      0.01),
        ("japan-market-alonso",     "Fernando ALONSO",     "Alonso",     0.01),
        ("japan-market-bottas",     "Valtteri BOTTAS",     "Bottas",     0.01),
        ("japan-market-stroll",     "Lance STROLL",        "Stroll",     0.01),
        ("japan-market-perez",      "Sergio PEREZ",        "Perez",      0.01),
    ]
    for index, (market_id, full_name, last_name, entry_price) in enumerate(market_specs):
        session.add(
            PolymarketMarket(
                id=market_id,
                event_id=event.id,
                question=(
                    f"Will {full_name} achieve pole position in Qualifying "
                    "at the 2026 F1 Japanese Grand Prix?"
                ),
                slug=f"japan-q-pole-{last_name.lower()}",
                condition_id=f"japan-condition-{market_id}",
                question_id=f"japan-question-{market_id}",
                taxonomy="driver_pole_position",
                taxonomy_confidence=0.95,
                target_session_code="Q",
                driver_a=last_name,
                description="Japanese Grand Prix: Qualifying Pole Winner",
                start_at_utc=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
                active=False,
                closed=True,
                archived=False,
                enable_order_book=True,
                best_bid=max(entry_price - 0.02, 0.01),
                best_ask=min(entry_price + 0.02, 0.99),
                spread=0.04,
                last_trade_price=entry_price,
                clob_token_ids=[f"japan-token-{market_id}-yes", f"japan-token-{market_id}-no"],
                raw_payload={"groupItemTitle": full_name},
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=f"japan-mapping-{market_id}",
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
                id=f"japan-token-{market_id}-yes",
                market_id=market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=entry_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketToken(
                id=f"japan-token-{market_id}-no",
                market_id=market_id,
                outcome="No",
                outcome_index=1,
                latest_price=1.0 - entry_price,
                raw_payload={},
            )
        )
        # Price history 3 days before Q (pre-weekend entry point)
        session.add(
            PolymarketPriceHistory(
                id=f"japan-price-{market_id}",
                market_id=market_id,
                token_id=f"japan-token-{market_id}-yes",
                observed_at_utc=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
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
                id=f"japan-trade-{market_id}",
                market_id=market_id,
                token_id=f"japan-token-{market_id}-yes",
                condition_id=f"japan-condition-{market_id}",
                trade_timestamp_utc=datetime(2026, 3, 25, 11, 55, tzinfo=timezone.utc),
                side="buy",
                price=entry_price,
                size=10.0,
                raw_payload={},
            )
        )

    session.commit()


def test_build_japan_pre_weekend_snapshot_creates_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_quicktest_fixture(session)

    def fail_hydrate(*args: object, **kwargs: object) -> None:
        raise AssertionError("hydrate_polymarket_market should not be called when history exists")

    monkeypatch.setattr("f1_polymarket_worker.gp_registry.hydrate_polymarket_market", fail_hydrate)

    result = build_japan_pre_weekend_snapshot(
        context,
        meeting_key=1281,
        season=2026,
    )

    assert result["status"] == "completed"
    assert result["row_count"] == 22
    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 22
    assert sum(int(row["label_yes"]) for row in rows) == 1
    assert all(row["entry_yes_price"] is not None for row in rows)


def test_run_japan_q_pole_baseline_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_quicktest_fixture(session)

    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market",
        lambda *args, **kwargs: None,
    )

    snapshot_result = build_japan_pre_weekend_snapshot(context, meeting_key=1281, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]

    baseline_result = run_japan_q_pole_baseline(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert baseline_result["status"] == "completed"
    model_runs = session.scalars(
        select(ModelRun).where(ModelRun.feature_snapshot_id == snapshot_id)
    ).all()
    predictions = session.scalars(select(ModelPrediction)).all()
    assert len(model_runs) == 3
    assert len(predictions) == 66
    assert {row.model_name for row in model_runs} == {"market_implied", "form_pace", "hybrid"}

    report_result = report_japan_q_pole_quicktest(context, snapshot_id=snapshot_id, min_edge=0.05)
    assert report_result["status"] == "completed"
    report_dir = (
        tmp_path / "reports" / "research" / "2026" / "2026-japanese-grand-prix-q-pole-quicktest"
    )
    report_json = report_dir / "summary.json"
    assert report_json.exists()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["snapshot_id"] == snapshot_id
    assert report["market_count"] == 22
    assert set(report["baselines"]) == {"market_implied", "form_pace", "hybrid"}
    # Leclerc is form P1 + highest market price (0.16) → top hybrid pick
    assert report["top_hybrid_predictions"][0]["driver_name"] == "Charles LECLERC"


# ---------------------------------------------------------------------------
# Japan FP1-to-Q snapshot builder
# ---------------------------------------------------------------------------


def test_build_japan_fp1_to_q_snapshot_plan_only(tmp_path: Path) -> None:
    session, context = build_context(tmp_path, execute=False)
    result = build_japan_fp1_to_q_snapshot(context, meeting_key=1281, season=2026)
    assert result["status"] == "planned"


def test_build_japan_fp1_to_q_snapshot_creates_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_quicktest_fixture(session)
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market", lambda *a, **kw: None
    )
    result = build_japan_fp1_to_q_snapshot(context, meeting_key=1281, season=2026)
    assert result["status"] == "completed"
    assert result["row_count"] == 22

    snapshot = session.get(FeatureSnapshot, result["snapshot_id"])
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert len(rows) == 22
    assert sum(int(r["label_yes"]) for r in rows) == 1  # Russell P1
    assert all(r["fp1_session_id"] is not None for r in rows)
    assert all(r["q_session_id"] is not None for r in rows)


def test_run_japan_fp1_q_pole_baseline_plan_only(tmp_path: Path) -> None:
    session, context = build_context(tmp_path, execute=False)
    result = run_japan_fp1_q_pole_baseline(context, snapshot_id="any-id")
    assert result["status"] == "planned"


def test_run_japan_fp1_q_pole_baseline_creates_model_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_quicktest_fixture(session)
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market", lambda *a, **kw: None
    )
    snapshot_result = build_japan_fp1_to_q_snapshot(context, meeting_key=1281, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]

    baseline_result = run_japan_fp1_q_pole_baseline(
        context, snapshot_id=snapshot_id, min_edge=0.05
    )
    assert baseline_result["status"] == "completed"

    model_runs = session.scalars(
        select(ModelRun).where(ModelRun.feature_snapshot_id == snapshot_id)
    ).all()
    predictions = session.scalars(select(ModelPrediction)).all()
    assert len(model_runs) == 3
    assert len(predictions) == 66
    assert {row.model_name for row in model_runs} == {"market_implied", "fp1_pace", "hybrid"}


def test_report_japan_fp1_q_pole_quicktest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_quicktest_fixture(session)
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.hydrate_polymarket_market", lambda *a, **kw: None
    )
    snapshot_result = build_japan_fp1_to_q_snapshot(context, meeting_key=1281, season=2026)
    snapshot_id = snapshot_result["snapshot_id"]
    run_japan_fp1_q_pole_baseline(context, snapshot_id=snapshot_id, min_edge=0.05)

    report_result = report_japan_fp1_q_pole_quicktest(
        context, snapshot_id=snapshot_id, min_edge=0.05
    )
    assert report_result["status"] == "completed"
    report_dir = (
        tmp_path / "reports" / "research" / "2026"
        / "2026-japanese-grand-prix-fp1-q-pole-quicktest"
    )
    report_json = report_dir / "summary.json"
    assert report_json.exists()
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["snapshot_id"] == snapshot_id
    assert report["market_count"] == 22
    assert set(report["baselines"]) == {"market_implied", "fp1_pace", "hybrid"}
