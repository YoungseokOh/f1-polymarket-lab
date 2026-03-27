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
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
)
from f1_polymarket_worker.multitask_snapshot import (
    build_multitask_checkpoint_rows,
    build_multitask_feature_snapshots,
)
from f1_polymarket_worker.pipeline import PipelineContext
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from f1_polymarket_lab.features import compute_features, default_feature_registry


def build_context(tmp_path: Path) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    context = PipelineContext(db=session, execute=True, settings=Settings(data_root=tmp_path))
    return session, context


def seed_multitask_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting-1281",
        meeting_key=1281,
        season=2026,
        round_number=3,
        meeting_name="Japanese Grand Prix",
        country_name="Japan",
        location="Suzuka",
        start_date_utc=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
        end_date_utc=datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc),
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
    fp3 = F1Session(
        id="session-fp3",
        meeting_id=meeting.id,
        session_key=11248,
        session_name="Practice 3",
        session_type="Practice",
        session_code="FP3",
        date_start_utc=datetime(2026, 3, 27, 9, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 10, 30, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    q = F1Session(
        id="session-q",
        meeting_id=meeting.id,
        session_key=11249,
        session_name="Qualifying",
        session_type="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 13, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    r = F1Session(
        id="session-r",
        meeting_id=meeting.id,
        session_key=11250,
        session_name="Grand Prix",
        session_type="Race",
        session_code="R",
        date_start_utc=datetime(2026, 3, 29, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 29, 8, 0, tzinfo=timezone.utc),
        raw_payload={"meeting_name": "Japanese Grand Prix"},
    )
    session.add_all([fp1, fp2, fp3, q, r])

    leclerc = F1Driver(
        id="driver-leclerc",
        driver_number=16,
        full_name="Charles LECLERC",
        first_name="Charles",
        last_name="Leclerc",
        broadcast_name="C LECLERC",
        team_id="team-ferrari",
        raw_payload={},
    )
    russell = F1Driver(
        id="driver-russell",
        driver_number=63,
        full_name="George RUSSELL",
        first_name="George",
        last_name="Russell",
        broadcast_name="G RUSSELL",
        team_id="team-mercedes",
        raw_payload={},
    )
    session.add_all([leclerc, russell])

    session.add_all(
        [
            F1SessionResult(
                id="fp1-leclerc",
                session_id=fp1.id,
                driver_id=leclerc.id,
                position=1,
                result_time_seconds=88.100,
                result_time_kind="best_lap",
                gap_to_leader_status="leader",
                number_of_laps=20,
                raw_payload={},
            ),
            F1SessionResult(
                id="fp1-russell",
                session_id=fp1.id,
                driver_id=russell.id,
                position=2,
                result_time_seconds=88.240,
                result_time_kind="best_lap",
                gap_to_leader_seconds=0.140,
                gap_to_leader_status="time",
                number_of_laps=20,
                raw_payload={},
            ),
            F1SessionResult(
                id="fp2-russell",
                session_id=fp2.id,
                driver_id=russell.id,
                position=1,
                result_time_seconds=87.900,
                result_time_kind="best_lap",
                gap_to_leader_status="leader",
                number_of_laps=22,
                raw_payload={},
            ),
            F1SessionResult(
                id="fp2-leclerc",
                session_id=fp2.id,
                driver_id=leclerc.id,
                position=2,
                result_time_seconds=88.020,
                result_time_kind="best_lap",
                gap_to_leader_seconds=0.120,
                gap_to_leader_status="time",
                number_of_laps=22,
                raw_payload={},
            ),
            F1SessionResult(
                id="fp3-leclerc",
                session_id=fp3.id,
                driver_id=leclerc.id,
                position=1,
                result_time_seconds=87.700,
                result_time_kind="best_lap",
                gap_to_leader_status="leader",
                number_of_laps=18,
                raw_payload={},
            ),
            F1SessionResult(
                id="fp3-russell",
                session_id=fp3.id,
                driver_id=russell.id,
                position=2,
                result_time_seconds=87.860,
                result_time_kind="best_lap",
                gap_to_leader_seconds=0.160,
                gap_to_leader_status="time",
                number_of_laps=18,
                raw_payload={},
            ),
            F1SessionResult(
                id="q-leclerc",
                session_id=q.id,
                driver_id=leclerc.id,
                position=1,
                result_time_seconds=87.400,
                result_time_kind="best_lap",
                gap_to_leader_status="leader",
                raw_payload={},
            ),
            F1SessionResult(
                id="q-russell",
                session_id=q.id,
                driver_id=russell.id,
                position=2,
                result_time_seconds=87.540,
                result_time_kind="best_lap",
                gap_to_leader_seconds=0.140,
                gap_to_leader_status="time",
                raw_payload={},
            ),
            F1SessionResult(
                id="r-leclerc",
                session_id=r.id,
                driver_id=leclerc.id,
                position=1,
                result_time_seconds=5400.0,
                result_time_kind="race_time",
                gap_to_leader_status="leader",
                raw_payload={},
            ),
            F1SessionResult(
                id="r-russell",
                session_id=r.id,
                driver_id=russell.id,
                position=2,
                result_time_seconds=5402.1,
                result_time_kind="race_time",
                gap_to_leader_seconds=2.100,
                gap_to_leader_status="time",
                raw_payload={},
            ),
        ]
    )

    q_event = PolymarketEvent(
        id="event-q",
        slug="japan-qualifying",
        ticker="japan-q",
        title="Japanese Grand Prix Qualifying",
        description="Qualifying markets",
        start_at_utc=datetime(2026, 3, 27, 11, 30, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 27, 13, 30, tzinfo=timezone.utc),
        active=True,
        closed=False,
        archived=False,
        raw_payload={},
    )
    r_event = PolymarketEvent(
        id="event-r",
        slug="japan-race",
        ticker="japan-r",
        title="Japanese Grand Prix Race",
        description="Race markets",
        start_at_utc=datetime(2026, 3, 29, 5, 30, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 29, 8, 30, tzinfo=timezone.utc),
        active=True,
        closed=False,
        archived=False,
        raw_payload={},
    )
    session.add_all([q_event, r_event])

    session.add_all(
        [
            PolymarketMarket(
                id="market-pole-leclerc",
                event_id=q_event.id,
                question="Will Charles Leclerc take pole position?",
                slug="japan-q-pole-leclerc",
                condition_id="condition-q-pole-leclerc",
                question_id="question-q-pole-leclerc",
                taxonomy="driver_pole_position",
                taxonomy_confidence=0.97,
                target_session_code="Q",
                driver_a="Leclerc",
                description="Pole position market",
                start_at_utc=datetime(2026, 3, 27, 11, 30, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 27, 13, 30, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=0.28,
                best_ask=0.32,
                spread=0.04,
                last_trade_price=0.30,
                clob_token_ids=["token-pole-leclerc-yes"],
                raw_payload={"groupItemTitle": "Leclerc"},
            ),
            PolymarketMarket(
                id="market-constructor-ferrari",
                event_id=q_event.id,
                question="Will Ferrari secure pole position?",
                slug="japan-q-constructor-pole-ferrari",
                condition_id="condition-q-constructor-ferrari",
                question_id="question-q-constructor-ferrari",
                taxonomy="constructor_pole_position",
                taxonomy_confidence=0.97,
                target_session_code="Q",
                driver_a="Leclerc",
                team_name="Ferrari",
                description="Constructor pole market",
                start_at_utc=datetime(2026, 3, 27, 11, 30, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 27, 13, 30, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=0.40,
                best_ask=0.44,
                spread=0.04,
                last_trade_price=0.42,
                clob_token_ids=["token-constructor-ferrari-yes"],
                raw_payload={"groupItemTitle": "Ferrari"},
            ),
            PolymarketMarket(
                id="market-winner-leclerc",
                event_id=r_event.id,
                question="Will Charles Leclerc win the race?",
                slug="japan-r-winner-leclerc",
                condition_id="condition-r-winner-leclerc",
                question_id="question-r-winner-leclerc",
                taxonomy="race_winner",
                taxonomy_confidence=0.98,
                target_session_code="R",
                driver_a="Leclerc",
                description="Race winner market",
                start_at_utc=datetime(2026, 3, 29, 5, 30, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 29, 8, 30, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=0.44,
                best_ask=0.48,
                spread=0.04,
                last_trade_price=0.46,
                clob_token_ids=["token-winner-leclerc-yes"],
                raw_payload={"groupItemTitle": "Leclerc"},
            ),
            PolymarketMarket(
                id="market-h2h-leclerc-russell",
                event_id=r_event.id,
                question="Who will finish higher: Leclerc or Russell?",
                slug="japan-r-h2h-leclerc-russell",
                condition_id="condition-r-h2h-leclerc-russell",
                question_id="question-r-h2h-leclerc-russell",
                taxonomy="head_to_head_session",
                taxonomy_confidence=0.98,
                target_session_code="R",
                driver_a="Leclerc",
                driver_b="Russell",
                description="Head-to-head market",
                start_at_utc=datetime(2026, 3, 29, 5, 30, tzinfo=timezone.utc),
                end_at_utc=datetime(2026, 3, 29, 8, 30, tzinfo=timezone.utc),
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                best_bid=0.51,
                best_ask=0.55,
                spread=0.04,
                last_trade_price=0.53,
                clob_token_ids=["token-h2h-leclerc", "token-h2h-russell"],
                raw_payload={"groupItemTitle": "Leclerc vs Russell"},
            ),
        ]
    )

    session.add_all(
        [
            EntityMappingF1ToPolymarket(
                id="mapping-pole-leclerc",
                f1_meeting_id=meeting.id,
                f1_session_id=q.id,
                polymarket_event_id=q_event.id,
                polymarket_market_id="market-pole-leclerc",
                mapping_type="driver_pole_position",
                confidence=0.97,
                matched_by="test_fixture",
                override_flag=False,
            ),
            EntityMappingF1ToPolymarket(
                id="mapping-constructor-ferrari",
                f1_meeting_id=meeting.id,
                f1_session_id=q.id,
                polymarket_event_id=q_event.id,
                polymarket_market_id="market-constructor-ferrari",
                mapping_type="constructor_pole_position",
                confidence=0.97,
                matched_by="test_fixture",
                override_flag=False,
            ),
            EntityMappingF1ToPolymarket(
                id="mapping-winner-leclerc",
                f1_meeting_id=meeting.id,
                f1_session_id=r.id,
                polymarket_event_id=r_event.id,
                polymarket_market_id="market-winner-leclerc",
                mapping_type="race_winner",
                confidence=0.98,
                matched_by="test_fixture",
                override_flag=False,
            ),
            EntityMappingF1ToPolymarket(
                id="mapping-h2h-leclerc-russell",
                f1_meeting_id=meeting.id,
                f1_session_id=r.id,
                polymarket_event_id=r_event.id,
                polymarket_market_id="market-h2h-leclerc-russell",
                mapping_type="head_to_head_session",
                confidence=0.98,
                matched_by="test_fixture",
                override_flag=False,
            ),
        ]
    )

    session.add_all(
        [
            PolymarketToken(
                id="token-pole-leclerc-yes",
                market_id="market-pole-leclerc",
                outcome="Yes",
                outcome_index=0,
                latest_price=0.30,
                raw_payload={},
            ),
            PolymarketToken(
                id="token-constructor-ferrari-yes",
                market_id="market-constructor-ferrari",
                outcome="Yes",
                outcome_index=0,
                latest_price=0.42,
                raw_payload={},
            ),
            PolymarketToken(
                id="token-winner-leclerc-yes",
                market_id="market-winner-leclerc",
                outcome="Yes",
                outcome_index=0,
                latest_price=0.46,
                raw_payload={},
            ),
            PolymarketToken(
                id="token-h2h-leclerc",
                market_id="market-h2h-leclerc-russell",
                outcome="Leclerc",
                outcome_index=0,
                latest_price=0.53,
                raw_payload={},
            ),
            PolymarketToken(
                id="token-h2h-russell",
                market_id="market-h2h-leclerc-russell",
                outcome="Russell",
                outcome_index=1,
                latest_price=0.47,
                raw_payload={},
            ),
        ]
    )

    session.add_all(
        [
            PolymarketPriceHistory(
                id="price-pole-leclerc",
                market_id="market-pole-leclerc",
                token_id="token-pole-leclerc-yes",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=0.30,
                midpoint=0.30,
                best_bid=0.28,
                best_ask=0.32,
                source_kind="clob",
                raw_payload={},
            ),
            PolymarketPriceHistory(
                id="price-constructor-ferrari",
                market_id="market-constructor-ferrari",
                token_id="token-constructor-ferrari-yes",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=0.42,
                midpoint=0.42,
                best_bid=0.40,
                best_ask=0.44,
                source_kind="clob",
                raw_payload={},
            ),
            PolymarketPriceHistory(
                id="price-winner-leclerc",
                market_id="market-winner-leclerc",
                token_id="token-winner-leclerc-yes",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=0.46,
                midpoint=0.46,
                best_bid=0.44,
                best_ask=0.48,
                source_kind="clob",
                raw_payload={},
            ),
            PolymarketPriceHistory(
                id="price-h2h-leclerc",
                market_id="market-h2h-leclerc-russell",
                token_id="token-h2h-leclerc",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=0.53,
                midpoint=0.53,
                best_bid=0.51,
                best_ask=0.55,
                source_kind="clob",
                raw_payload={},
            ),
            PolymarketPriceHistory(
                id="price-h2h-russell",
                market_id="market-h2h-leclerc-russell",
                token_id="token-h2h-russell",
                observed_at_utc=datetime(2026, 3, 27, 5, 50, tzinfo=timezone.utc),
                price=0.47,
                midpoint=0.47,
                best_bid=0.45,
                best_ask=0.49,
                source_kind="clob",
                raw_payload={},
            ),
        ]
    )

    session.add_all(
        [
            PolymarketTrade(
                id="trade-pole-leclerc",
                market_id="market-pole-leclerc",
                token_id="token-pole-leclerc-yes",
                condition_id="condition-q-pole-leclerc",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 45, tzinfo=timezone.utc),
                side="buy",
                price=0.30,
                size=10.0,
                raw_payload={},
            ),
            PolymarketTrade(
                id="trade-constructor-ferrari",
                market_id="market-constructor-ferrari",
                token_id="token-constructor-ferrari-yes",
                condition_id="condition-q-constructor-ferrari",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 45, tzinfo=timezone.utc),
                side="buy",
                price=0.42,
                size=10.0,
                raw_payload={},
            ),
            PolymarketTrade(
                id="trade-winner-leclerc",
                market_id="market-winner-leclerc",
                token_id="token-winner-leclerc-yes",
                condition_id="condition-r-winner-leclerc",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 45, tzinfo=timezone.utc),
                side="buy",
                price=0.46,
                size=10.0,
                raw_payload={},
            ),
            PolymarketTrade(
                id="trade-h2h-leclerc",
                market_id="market-h2h-leclerc-russell",
                token_id="token-h2h-leclerc",
                condition_id="condition-r-h2h-leclerc-russell",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 45, tzinfo=timezone.utc),
                side="buy",
                price=0.53,
                size=10.0,
                raw_payload={},
            ),
            PolymarketTrade(
                id="trade-h2h-russell",
                market_id="market-h2h-leclerc-russell",
                token_id="token-h2h-russell",
                condition_id="condition-r-h2h-leclerc-russell",
                trade_timestamp_utc=datetime(2026, 3, 27, 5, 45, tzinfo=timezone.utc),
                side="buy",
                price=0.47,
                size=10.0,
                raw_payload={},
            ),
        ]
    )

    session.commit()


def test_default_feature_registry_includes_multitask_contract_features() -> None:
    expected_names = {
        "has_fp1",
        "has_fp2",
        "has_fp3",
        "has_q",
        "checkpoint_ordinal",
        "market_family_is_pole",
        "market_family_is_constructor_pole",
        "market_family_is_winner",
        "market_family_is_h2h",
        "qualifying_position",
        "qualifying_gap_to_pole_seconds",
    }

    actual_names = {definition.feature_name for definition in default_feature_registry()}

    assert expected_names <= actual_names


def test_compute_features_adds_multitask_checkpoint_contract_columns() -> None:
    df = pl.DataFrame(
        {
            "event_id": [101, 101, 101, 101],
            "driver_id": ["driver-1", "driver-2", "driver-3", "driver-4"],
            "meeting_key": [2026, 2026, 2026, 2026],
            "as_of_checkpoint": ["FP1", "FP2", "FP3", "Q"],
            "target_market_family": ["winner", "winner", "winner", "h2h"],
            "has_fp1": [True, True, True, True],
            "has_fp2": [False, True, True, True],
            "has_fp3": [False, False, True, True],
            "has_q": [False, False, False, True],
            "fp1_position": [3, 7, 9, 11],
            "fp2_position": [None, 5, 8, 10],
            "fp3_position": [None, None, 6, 12],
            "qualifying_position": [None, None, None, 2],
            "qualifying_gap_to_pole_seconds": [None, None, None, 0.123],
        }
    )

    result = compute_features(df, zscore=False, log=False, interactions=False, cross_gp=False)

    assert {
        "checkpoint_ordinal",
        "market_family_is_winner",
        "market_family_is_h2h",
        "availability_sum",
        "pace_x_checkpoint",
    } <= set(result.columns)
    assert result["checkpoint_ordinal"].to_list() == [1, 2, 3, 4]
    assert result["availability_sum"].to_list() == [1, 2, 3, 4]
    assert result["pace_x_checkpoint"].to_list() == [3.0, 10.0, 18.0, 8.0]


def test_build_multitask_checkpoint_rows_respects_visibility(tmp_path: Path) -> None:
    session, context = build_context(tmp_path)
    seed_multitask_fixture(session)

    fp1_rows = build_multitask_checkpoint_rows(
        context,
        meeting_key=1281,
        season=2026,
        checkpoint="FP1",
    )
    q_rows = build_multitask_checkpoint_rows(
        context,
        meeting_key=1281,
        season=2026,
        checkpoint="Q",
    )

    fp1_winner = next(
        row
        for row in fp1_rows
        if row["target_market_family"] == "winner" and row["driver_id"] == "driver-leclerc"
    )
    q_winner = next(
        row for row in q_rows if row["target_market_family"] == "winner" and row["driver_id"] == "driver-leclerc"
    )

    assert fp1_winner["has_q"] == 0
    assert fp1_winner["qualifying_position"] is None
    assert q_winner["has_q"] == 1
    assert q_winner["qualifying_position"] == 1


def test_build_multitask_checkpoint_rows_emits_all_supported_families(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    seed_multitask_fixture(session)

    rows = build_multitask_checkpoint_rows(context, meeting_key=1281, season=2026, checkpoint="Q")
    families = {row["target_market_family"] for row in rows if row["as_of_checkpoint"] == "Q"}

    assert families == {"constructor_pole", "h2h", "pole", "winner"}


def test_build_multitask_feature_snapshots_persists_manifest_and_rows(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    seed_multitask_fixture(session)

    result = build_multitask_feature_snapshots(
        context,
        meeting_key=1281,
        season=2026,
        checkpoints=("FP1", "FP2", "FP3", "Q"),
    )

    snapshots = session.scalars(select(FeatureSnapshot)).all()
    assert len(snapshots) == 4
    assert set(result["snapshot_ids"]) == {row.id for row in snapshots}
    assert result["manifest_path"].endswith("manifest.json")
