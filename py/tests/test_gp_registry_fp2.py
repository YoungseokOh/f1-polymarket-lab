from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Meeting,
    F1Session,
    F1SessionResult,
    FeatureSnapshot,
    ModelRun,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
)
from f1_polymarket_worker.gp_registry import build_snapshot, get_gp_config, run_baseline
from f1_polymarket_worker.pipeline import PipelineContext
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_context(tmp_path: Path, *, execute: bool = True) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=execute, settings=settings)
    return session, context


def seed_japan_fp2_fixture(session: Session) -> None:
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

    fp1 = F1Session(
        id="session-fp1",
        meeting_id=meeting.id,
        session_key=11246,
        session_name="Practice 1",
        session_type="Practice",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 3, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    fp2 = F1Session(
        id="session-fp2",
        meeting_id=meeting.id,
        session_key=11247,
        session_name="Practice 2",
        session_type="Practice",
        session_code="FP2",
        date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    q = F1Session(
        id="session-q",
        meeting_id=meeting.id,
        session_key=11249,
        session_name="Qualifying",
        session_type="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    session.add_all([fp1, fp2, q])

    drivers = [
        F1Driver(
            id="driver-leclerc",
            driver_number=16,
            full_name="Charles LECLERC",
            first_name="Charles",
            last_name="Leclerc",
            broadcast_name="C LECLERC",
            team_id="team-ferrari",
            raw_payload={},
        ),
        F1Driver(
            id="driver-russell",
            driver_number=63,
            full_name="George RUSSELL",
            first_name="George",
            last_name="Russell",
            broadcast_name="G RUSSELL",
            team_id="team-mercedes",
            raw_payload={},
        ),
        F1Driver(
            id="driver-norris",
            driver_number=4,
            full_name="Lando NORRIS",
            first_name="Lando",
            last_name="Norris",
            broadcast_name="L NORRIS",
            team_id="team-mclaren",
            raw_payload={},
        ),
    ]
    session.add_all(drivers)

    fp1_results = [
        ("driver-leclerc", 1, 88.100, 0.0),
        ("driver-russell", 2, 88.240, 0.140),
        ("driver-norris", 3, 88.510, 0.410),
    ]
    for driver_id, position, lap_time, gap_seconds in fp1_results:
        session.add(
            F1SessionResult(
                id=f"fp1-{driver_id}",
                session_id=fp1.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_seconds=None if position == 1 else gap_seconds,
                gap_to_leader_status="leader" if position == 1 else "time",
                number_of_laps=20,
                raw_payload={},
            )
        )

    fp2_results = [
        ("driver-russell", 1, 87.900, 0.0),
        ("driver-leclerc", 2, 88.020, 0.120),
        ("driver-norris", 3, 88.180, 0.280),
    ]
    for driver_id, position, lap_time, gap_seconds in fp2_results:
        session.add(
            F1SessionResult(
                id=f"fp2-{driver_id}",
                session_id=fp2.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_seconds=None if position == 1 else gap_seconds,
                gap_to_leader_status="leader" if position == 1 else "time",
                number_of_laps=24,
                raw_payload={},
            )
        )

    q_results = [
        ("driver-russell", 1, 87.400),
        ("driver-leclerc", 2, 87.510),
        ("driver-norris", 3, 87.700),
    ]
    for driver_id, position, lap_time in q_results:
        session.add(
            F1SessionResult(
                id=f"q-{driver_id}",
                session_id=q.id,
                driver_id=driver_id,
                position=position,
                result_time_seconds=lap_time,
                result_time_kind="best_lap",
                gap_to_leader_status="leader" if position == 1 else "time",
                raw_payload={},
            )
        )

    fp2_event = PolymarketEvent(
        id="event-fp2",
        slug="japan-practice-2-fastest-lap",
        ticker="japan-fp2-fastest-lap",
        title="Japanese Grand Prix: Practice 2 Fastest Lap",
        description="FP2 fastest lap market",
        start_at_utc=datetime(2026, 3, 27, 4, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
        active=True,
        closed=False,
        archived=False,
        raw_payload={},
    )
    q_event = PolymarketEvent(
        id="event-q",
        slug="japan-qualifying-pole",
        ticker="japan-q-pole",
        title="Japanese Grand Prix: Qualifying Pole Winner",
        description="Qualifying pole market",
        start_at_utc=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        active=True,
        closed=False,
        archived=False,
        raw_payload={},
    )
    session.add_all([fp2_event, q_event])

    market_specs = [
        ("driver-leclerc", "Leclerc", 0.34, 0.28),
        ("driver-russell", "Russell", 0.31, 0.36),
        ("driver-norris", "Norris", 0.22, 0.20),
    ]
    for _driver_id, last_name, fp2_price, q_price in market_specs:
        fp2_market_id = f"market-fp2-{last_name.lower()}"
        q_market_id = f"market-q-{last_name.lower()}"

        session.add(
            PolymarketMarket(
                id=fp2_market_id,
                event_id=fp2_event.id,
                question=f"Will {last_name} set the fastest lap in Practice 2?",
                slug=f"japan-fp2-fastest-lap-{last_name.lower()}",
                condition_id=f"condition-fp2-{last_name.lower()}",
                question_id=f"question-fp2-{last_name.lower()}",
                taxonomy="driver_fastest_lap_practice",
                taxonomy_confidence=0.95,
                target_session_code="FP2",
                driver_a=last_name,
                description="Japanese Grand Prix: Practice 2 Fastest Lap",
                start_at_utc=datetime(2026, 3, 27, 4, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=max(fp2_price - 0.02, 0.01),
                best_ask=min(fp2_price + 0.02, 0.99),
                spread=0.04,
                last_trade_price=fp2_price,
                clob_token_ids=[f"token-fp2-{last_name.lower()}-yes"],
                raw_payload={"groupItemTitle": last_name},
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=f"mapping-fp2-{last_name.lower()}",
                f1_meeting_id=meeting.id,
                f1_session_id=fp2.id,
                polymarket_event_id=fp2_event.id,
                polymarket_market_id=fp2_market_id,
                mapping_type="driver_fastest_lap_practice",
                confidence=0.95,
                matched_by="test_fixture",
                override_flag=False,
            )
        )
        session.add(
            PolymarketToken(
                id=f"token-fp2-{last_name.lower()}-yes",
                market_id=fp2_market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=fp2_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketPriceHistory(
                id=f"price-fp2-{last_name.lower()}",
                market_id=fp2_market_id,
                token_id=f"token-fp2-{last_name.lower()}-yes",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=fp2_price,
                midpoint=fp2_price,
                best_bid=max(fp2_price - 0.02, 0.01),
                best_ask=min(fp2_price + 0.02, 0.99),
                source_kind="clob",
                raw_payload={},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"trade-fp2-{last_name.lower()}",
                market_id=fp2_market_id,
                token_id=f"token-fp2-{last_name.lower()}-yes",
                condition_id=f"condition-fp2-{last_name.lower()}",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 49, tzinfo=timezone.utc),
                side="buy",
                price=fp2_price,
                size=10.0,
                raw_payload={},
            )
        )

        session.add(
            PolymarketMarket(
                id=q_market_id,
                event_id=q_event.id,
                question=f"Will {last_name} achieve pole position in Qualifying?",
                slug=f"japan-q-pole-{last_name.lower()}",
                condition_id=f"condition-q-{last_name.lower()}",
                question_id=f"question-q-{last_name.lower()}",
                taxonomy="driver_pole_position",
                taxonomy_confidence=0.95,
                target_session_code="Q",
                driver_a=last_name,
                description="Japanese Grand Prix: Qualifying Pole Winner",
                start_at_utc=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=max(q_price - 0.02, 0.01),
                best_ask=min(q_price + 0.02, 0.99),
                spread=0.04,
                last_trade_price=q_price,
                clob_token_ids=[f"token-q-{last_name.lower()}-yes"],
                raw_payload={"groupItemTitle": last_name},
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id=f"mapping-q-{last_name.lower()}",
                f1_meeting_id=meeting.id,
                f1_session_id=q.id,
                polymarket_event_id=q_event.id,
                polymarket_market_id=q_market_id,
                mapping_type="driver_pole_position",
                confidence=0.95,
                matched_by="test_fixture",
                override_flag=False,
            )
        )
        session.add(
            PolymarketToken(
                id=f"token-q-{last_name.lower()}-yes",
                market_id=q_market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=q_price,
                raw_payload={},
            )
        )
        session.add(
            PolymarketPriceHistory(
                id=f"price-q-{last_name.lower()}",
                market_id=q_market_id,
                token_id=f"token-q-{last_name.lower()}-yes",
                observed_at_utc=datetime(2026, 3, 27, 7, 10, tzinfo=timezone.utc),
                price=q_price,
                midpoint=q_price,
                best_bid=max(q_price - 0.02, 0.01),
                best_ask=min(q_price + 0.02, 0.99),
                source_kind="clob",
                raw_payload={},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"trade-q-{last_name.lower()}",
                market_id=q_market_id,
                token_id=f"token-q-{last_name.lower()}-yes",
                condition_id=f"condition-q-{last_name.lower()}",
                trade_timestamp_utc=datetime(2026, 3, 27, 7, 5, tzinfo=timezone.utc),
                side="buy",
                price=q_price,
                size=10.0,
                raw_payload={},
            )
        )

    session.commit()


def test_build_japan_fp1_to_fp2_snapshot_and_run_baseline_without_labels(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_fp2_fixture(session)

    config = get_gp_config("japan_fp1_fp2")
    snapshot_result = build_snapshot(context, config, meeting_key=1281, season=2026)

    snapshot_id = snapshot_result["snapshot_id"]
    snapshot = session.get(FeatureSnapshot, snapshot_id)
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert {row["market_taxonomy"] for row in rows} == {"driver_fastest_lap_practice"}
    assert {row["label_yes"] for row in rows} == {None}

    baseline_result = run_baseline(context, config, snapshot_id=snapshot_id, min_edge=0.05)

    model_runs = session.scalars(
        select(ModelRun)
        .where(ModelRun.feature_snapshot_id == snapshot_id)
        .order_by(ModelRun.model_name.asc())
    ).all()
    assert baseline_result["status"] == "completed"
    assert [run.model_name for run in model_runs] == ["fp1_pace", "hybrid", "market_implied"]
    assert model_runs[0].metrics_json["row_count"] == 3
    assert model_runs[0].metrics_json["brier_score"] is None


def test_build_japan_fp2_to_q_snapshot_uses_fp2_baseline(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    seed_japan_fp2_fixture(session)

    config = get_gp_config("japan_fp2_q")
    snapshot_result = build_snapshot(context, config, meeting_key=1281, season=2026)

    snapshot_id = snapshot_result["snapshot_id"]
    snapshot = session.get(FeatureSnapshot, snapshot_id)
    assert snapshot is not None
    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    assert {row["market_taxonomy"] for row in rows} == {"driver_pole_position"}
    assert {row["latest_fp_number"] for row in rows} == {2}

    baseline_result = run_baseline(context, config, snapshot_id=snapshot_id, min_edge=0.05)

    model_runs = session.scalars(
        select(ModelRun)
        .where(ModelRun.feature_snapshot_id == snapshot_id)
        .order_by(ModelRun.model_name.asc())
    ).all()
    assert baseline_result["status"] == "completed"
    assert [run.model_name for run in model_runs] == ["fp2_pace", "hybrid", "market_implied"]
    assert model_runs[0].metrics_json["brier_score"] is not None
