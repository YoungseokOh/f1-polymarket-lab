"""Tests for the backtest module.

Covers:
- _get_executable_entry_price with various bid/ask/midpoint combinations
- _resolve_market_outcome with label_yes and resolution fallback
- _compute_backtest_metrics with known inputs/outputs
- settle_backtest end-to-end with fixture data
- collect_resolutions with closed-market resolution extraction
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketResolution,
    PolymarketToken,
    PolymarketTrade,
)
from f1_polymarket_worker.backtest import (
    _compute_backtest_metrics,
    _get_executable_entry_price,
    _resolve_market_outcome,
    backfill_backtests,
    collect_resolutions,
    settle_backtest,
)
from f1_polymarket_worker.gp_registry import GPConfig, get_gp_config, resolve_gp_config
from f1_polymarket_worker.pipeline import PipelineContext
from f1_polymarket_worker.quicktest import (
    _enrich_snapshot_probabilities,
    build_china_fp1_to_sq_snapshot,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def build_context(tmp_path: Path, *, execute: bool = True) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=execute, settings=settings)
    return session, context


# ---------------------------------------------------------------------------
# _get_executable_entry_price
# ---------------------------------------------------------------------------


class TestGetExecutableEntryPrice:
    def test_uses_best_ask_when_available(self) -> None:
        row = {
            "entry_yes_price": 0.20, "entry_best_ask": 0.22,
            "entry_best_bid": 0.18, "entry_spread": 0.04,
        }
        price, slippage = _get_executable_entry_price(row)
        assert price == 0.22
        assert slippage == pytest.approx(0.02)

    def test_skips_spread_only_rows_when_no_ask(self) -> None:
        row = {
            "entry_yes_price": 0.20, "entry_best_ask": None,
            "entry_best_bid": None, "entry_spread": 0.06,
        }
        price, slippage = _get_executable_entry_price(row)
        assert price is None
        assert slippage is None

    def test_skips_midpoint_only_rows_when_no_ask(self) -> None:
        row = {
            "entry_yes_price": 0.20, "entry_best_ask": None,
            "entry_best_bid": None, "entry_spread": None,
        }
        price, slippage = _get_executable_entry_price(row)
        assert price is None
        assert slippage is None

    def test_zero_ask_is_not_executable(self) -> None:
        row = {
            "entry_yes_price": 0.15, "entry_best_ask": 0.0,
            "entry_best_bid": 0.0, "entry_spread": None,
        }
        price, slippage = _get_executable_entry_price(row)
        assert price is None
        assert slippage is None

    def test_zero_spread_without_ask_is_not_executable(self) -> None:
        row = {
            "entry_yes_price": 0.10, "entry_best_ask": None,
            "entry_best_bid": None, "entry_spread": 0.0,
        }
        price, slippage = _get_executable_entry_price(row)
        assert price is None
        assert slippage is None


# ---------------------------------------------------------------------------
# _resolve_market_outcome
# ---------------------------------------------------------------------------


class TestResolveMarketOutcome:
    def test_label_yes_takes_priority(self) -> None:
        row = {"label_yes": 1}
        assert _resolve_market_outcome(None, row) == 1

    def test_label_no_takes_priority(self) -> None:
        row = {"label_yes": 0}
        assert _resolve_market_outcome(None, row) == 0

    def test_resolution_fallback_yes(self) -> None:
        resolution = PolymarketResolution(
            id="res-1", market_id="m-1", result="YES", raw_payload={}
        )
        row: dict[str, object] = {}
        assert _resolve_market_outcome(resolution, row) == 1

    def test_resolution_fallback_no(self) -> None:
        resolution = PolymarketResolution(
            id="res-1", market_id="m-1", result="no", raw_payload={}
        )
        row: dict[str, object] = {}
        assert _resolve_market_outcome(resolution, row) == 0

    def test_returns_none_when_no_data(self) -> None:
        assert _resolve_market_outcome(None, {}) is None


# ---------------------------------------------------------------------------
# _compute_backtest_metrics
# ---------------------------------------------------------------------------


class TestComputeBacktestMetrics:
    def test_empty_rows(self) -> None:
        metrics = _compute_backtest_metrics([])
        assert metrics["bet_count"] == 0
        assert metrics["total_pnl"] == 0.0
        assert metrics["brier_score"] is None

    def test_single_win(self) -> None:
        rows = [
            {
                "market_id": "m1",
                "driver_name": "Driver A",
                "model_probability": 0.30,
                "entry_price": 0.20,
                "edge": 0.10,
                "outcome": 1,
                "quantity": 50.0,
                "pnl": 40.0,
                "slippage": 0.0,
            }
        ]
        metrics = _compute_backtest_metrics(rows, bet_size=10.0)
        assert metrics["bet_count"] == 1
        assert metrics["wins"] == 1
        assert metrics["losses"] == 0
        assert metrics["hit_rate"] == 1.0
        assert metrics["total_wagered"] == 10.0
        assert metrics["total_pnl"] == 40.0
        assert metrics["brier_score"] == pytest.approx((0.30 - 1) ** 2, abs=1e-4)

    def test_mixed_results(self) -> None:
        rows = [
            {
                "market_id": "m1", "driver_name": "A",
                "model_probability": 0.30, "entry_price": 0.20, "edge": 0.10,
                "outcome": 1, "quantity": 50.0, "pnl": 40.0, "slippage": 0.0,
            },
            {
                "market_id": "m2", "driver_name": "B",
                "model_probability": 0.25, "entry_price": 0.15, "edge": 0.10,
                "outcome": 0, "quantity": 66.67, "pnl": -10.0, "slippage": 0.0,
            },
        ]
        metrics = _compute_backtest_metrics(rows, bet_size=10.0)
        assert metrics["bet_count"] == 2
        assert metrics["wins"] == 1
        assert metrics["losses"] == 1
        assert metrics["hit_rate"] == 0.5
        assert metrics["total_pnl"] == pytest.approx(30.0)
        assert metrics["roi_pct"] == pytest.approx(150.0)
        assert metrics["sharpe"] != 0.0
        assert "calibration_buckets" in metrics


# ---------------------------------------------------------------------------
# collect_resolutions (integration)
# ---------------------------------------------------------------------------


def _seed_minimal_closed_market(session: Session) -> None:
    """Seed a meeting with one closed market for resolution collection."""
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
        raw_payload={},
    )
    session.add(meeting)

    sq_session = F1Session(
        id="session-sq",
        meeting_id=meeting.id,
        session_key=11236,
        session_name="Sprint Qualifying",
        session_type="Qualifying",
        session_code="SQ",
        date_start_utc=datetime(2026, 3, 13, 7, 30, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 13, 8, 15, tzinfo=timezone.utc),
        raw_payload={},
    )
    session.add(sq_session)

    event = PolymarketEvent(
        id="event-sq",
        slug="china-sq-pole",
        ticker="china-sq-pole",
        title="China SQ Pole",
        description="desc",
        start_at_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 13, 8, 0, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        raw_payload={},
    )
    session.add(event)

    market = PolymarketMarket(
        id="market-russell",
        event_id=event.id,
        question="Will Russell win SQ pole?",
        slug="china-sq-pole-russell",
        condition_id="cond-russell",
        question_id="qid-russell",
        taxonomy="driver_pole_position",
        taxonomy_confidence=0.95,
        target_session_code="SQ",
        driver_a="Russell",
        description="desc",
        start_at_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 13, 8, 0, tzinfo=timezone.utc),
        active=False,
        closed=True,
        archived=False,
        enable_order_book=True,
        best_bid=0.60,
        best_ask=0.65,
        spread=0.05,
        last_trade_price=0.63,
        clob_token_ids=["token-russell-yes"],
        raw_payload={
            "result": "YES",
            "outcome": "Russell",
            "resolveDate": "2026-03-13T08:30:00Z",
            "groupItemTitle": "George RUSSELL",
        },
    )
    session.add(market)

    mapping = EntityMappingF1ToPolymarket(
        id="mapping-russell",
        f1_meeting_id=meeting.id,
        f1_session_id=sq_session.id,
        polymarket_event_id=event.id,
        polymarket_market_id=market.id,
        mapping_type="driver_pole_position",
        confidence=0.95,
        matched_by="manual",
        override_flag=False,
    )
    session.add(mapping)
    session.commit()


class TestCollectResolutions:
    def test_collects_resolution_from_closed_market(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path)
        _seed_minimal_closed_market(session)

        result = collect_resolutions(ctx, meeting_key=1280, season=2026)

        assert result["status"] == "completed"
        assert result["resolutions_written"] == 1

        resolution = session.query(PolymarketResolution).first()
        assert resolution is not None
        assert resolution.market_id == "market-russell"
        assert resolution.result == "YES"

    def test_skips_existing_resolutions(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path)
        _seed_minimal_closed_market(session)

        # First run
        collect_resolutions(ctx, meeting_key=1280, season=2026)

        # Second run should skip
        result = collect_resolutions(ctx, meeting_key=1280, season=2026)
        assert result["resolutions_written"] == 0

    def test_plan_only_mode(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path, execute=False)
        _seed_minimal_closed_market(session)

        result = collect_resolutions(ctx, meeting_key=1280, season=2026)
        assert result["status"] == "planned"

    def test_raises_for_missing_meeting(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path)
        with pytest.raises(ValueError, match="meeting_key=9999 not found"):
            collect_resolutions(ctx, meeting_key=9999, season=2026)


# ---------------------------------------------------------------------------
# settle_backtest (integration with China GP fixture)
# ---------------------------------------------------------------------------


def _seed_china_backtest_fixture(session: Session) -> None:
    """Seed the China GP data needed for a full backtest settlement.

    Reuses the test_quicktest fixture pattern but adds closed market + resolution.
    """
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
        raw_payload={},
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
        raw_payload={},
    )
    session.add_all([fp1, sq])

    # 5 drivers for minimal but meaningful fixture
    drivers = [
        F1Driver(id="driver-russell", driver_number=63, full_name="George RUSSELL",
                 first_name="George", last_name="Russell", broadcast_name="G RUSSELL",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="driver-antonelli", driver_number=12, full_name="Kimi ANTONELLI",
                 first_name="Kimi", last_name="Antonelli", broadcast_name="K ANTONELLI",
                 team_id="team-mercedes", raw_payload={}),
        F1Driver(id="driver-norris", driver_number=1, full_name="Lando NORRIS",
                 first_name="Lando", last_name="Norris", broadcast_name="L NORRIS",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="driver-piastri", driver_number=81, full_name="Oscar PIASTRI",
                 first_name="Oscar", last_name="Piastri", broadcast_name="O PIASTRI",
                 team_id="team-mclaren", raw_payload={}),
        F1Driver(id="driver-charles", driver_number=16, full_name="Charles LECLERC",
                 first_name="Charles", last_name="Leclerc", broadcast_name="C LECLERC",
                 team_id="team-ferrari", raw_payload={}),
    ]
    session.add_all(drivers)

    # FP1 results
    fp1_data = [
        ("driver-russell", 1, 92.741, "leader", None),
        ("driver-antonelli", 2, 92.861, "time", 0.120),
        ("driver-norris", 3, 93.296, "time", 0.555),
        ("driver-piastri", 4, 93.472, "time", 0.731),
        ("driver-charles", 5, 93.599, "time", 0.858),
    ]
    for driver_id, pos, time_s, gap_status, gap_s in fp1_data:
        session.add(F1SessionResult(
            id=f"fp1-result-{driver_id}",
            session_id=fp1.id, driver_id=driver_id, position=pos,
            result_time_seconds=time_s, result_time_kind="best_lap",
            gap_to_leader_display="0" if pos == 1 else f"+{gap_s:.3f}",
            gap_to_leader_seconds=gap_s, gap_to_leader_status=gap_status,
            number_of_laps=25, raw_payload={},
        ))
        for lap_num in range(1, 4):
            session.add(F1Lap(
                id=f"fp1-lap-{driver_id}-{lap_num}",
                session_id=fp1.id, driver_id=driver_id,
                lap_number=lap_num, lap_duration_seconds=time_s + 1.0,
                raw_payload={},
            ))
        for stint_num in range(1, 3):
            session.add(F1Stint(
                id=f"fp1-stint-{driver_id}-{stint_num}",
                session_id=fp1.id, driver_id=driver_id,
                stint_number=stint_num, raw_payload={},
            ))

    # SQ results — Russell wins pole
    sq_data = [
        ("driver-russell", 1, 91.520),
        ("driver-antonelli", 2, 91.809),
        ("driver-norris", 3, 92.141),
        ("driver-piastri", 4, 92.224),
        ("driver-charles", 5, 92.528),
    ]
    for driver_id, pos, time_s in sq_data:
        session.add(F1SessionResult(
            id=f"sq-result-{driver_id}",
            session_id=sq.id, driver_id=driver_id, position=pos,
            result_time_seconds=time_s, result_time_kind="best_lap",
            gap_to_leader_status="leader" if pos == 1 else "time",
            raw_payload={},
        ))

    # Event + markets
    event = PolymarketEvent(
        id="event-sq", slug="china-sq-pole", ticker="china-sq-pole",
        title="China SQ Pole", description="desc",
        start_at_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        end_at_utc=datetime(2026, 3, 13, 8, 0, tzinfo=timezone.utc),
        active=False, closed=True, archived=False, raw_payload={},
    )
    session.add(event)

    market_specs = [
        ("market-russell", "George RUSSELL", "Russell", 0.18),
        ("market-antonelli", "Kimi ANTONELLI", "Antonelli", 0.14),
        ("market-norris", "Lando NORRIS", "Norris", 0.12),
        ("market-piastri", "Oscar PIASTRI", "Piastri", 0.08),
        ("market-charles", "Charles LECLERC", "Leclerc", 0.07),
    ]
    for idx, (market_id, full_name, last_name, price) in enumerate(market_specs):
        session.add(PolymarketMarket(
            id=market_id, event_id=event.id,
            question=f"Will {full_name} win SQ pole?",
            slug=f"china-sq-pole-{last_name.lower()}",
            condition_id=f"cond-{last_name.lower()}", question_id=f"qid-{last_name.lower()}",
            taxonomy="driver_pole_position", taxonomy_confidence=0.95,
            target_session_code="SQ", driver_a=last_name,
            description="desc",
            start_at_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
            end_at_utc=datetime(2026, 3, 13, 8, 0, tzinfo=timezone.utc),
            active=False, closed=True, archived=False, enable_order_book=True,
            best_bid=round(max(price - 0.02, 0.005), 4),
            best_ask=round(min(price + 0.02, 0.995), 4),
            spread=0.04, last_trade_price=price,
            clob_token_ids=[f"token-{last_name.lower()}-yes", f"token-{last_name.lower()}-no"],
            raw_payload={"groupItemTitle": full_name},
        ))
        session.add(EntityMappingF1ToPolymarket(
            id=f"mapping-{last_name.lower()}",
            f1_meeting_id=meeting.id, f1_session_id=sq.id,
            polymarket_event_id=event.id, polymarket_market_id=market_id,
            mapping_type="driver_pole_position", confidence=0.95 - (idx * 0.01),
            matched_by="manual", override_flag=False,
        ))
        session.add(PolymarketToken(
            id=f"token-{last_name.lower()}-yes", market_id=market_id,
            outcome="Yes", outcome_index=0, latest_price=price, raw_payload={},
        ))
        session.add(PolymarketToken(
            id=f"token-{last_name.lower()}-no", market_id=market_id,
            outcome="No", outcome_index=1, latest_price=round(1.0 - price, 4), raw_payload={},
        ))
        session.add(PolymarketPriceHistory(
            id=f"price-{last_name.lower()}",
            market_id=market_id, token_id=f"token-{last_name.lower()}-yes",
            observed_at_utc=datetime(2026, 3, 13, 4, 45, tzinfo=timezone.utc),
            price=price, midpoint=price,
            best_bid=round(max(price - 0.02, 0.005), 4),
            best_ask=round(min(price + 0.02, 0.995), 4),
            source_kind="clob", raw_payload={},
        ))
        session.add(PolymarketTrade(
            id=f"trade-{last_name.lower()}",
            market_id=market_id, token_id=f"token-{last_name.lower()}-yes",
            condition_id=f"cond-{last_name.lower()}",
            trade_timestamp_utc=datetime(2026, 3, 13, 4, 40, tzinfo=timezone.utc),
            side="buy", price=price, size=50.0, raw_payload={},
        ))

    # Add resolution for Russell (the winner)
    session.add(PolymarketResolution(
        id="resolution-russell", market_id="market-russell",
        resolved_at_utc=datetime(2026, 3, 13, 8, 30, tzinfo=timezone.utc),
        result="YES", outcome="Russell", raw_payload={"result": "YES"},
    ))

    session.commit()


class TestSettleBacktest:
    def test_settle_with_snapshot(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path)
        _seed_china_backtest_fixture(session)

        # Build snapshot first
        snap = build_china_fp1_to_sq_snapshot(ctx, meeting_key=1280, season=2026)
        snapshot_id = snap["snapshot_id"]
        assert snap["row_count"] == 5

        # Now settle
        result = settle_backtest(
            ctx,
            snapshot_id=snapshot_id,
            min_edge=0.05,
            bet_size=10.0,
        )

        assert result["status"] == "completed"
        assert result["bets_placed"] >= 0
        assert "metrics" in result
        metrics = result["metrics"]
        assert "bet_count" in metrics
        assert "total_pnl" in metrics
        assert "brier_score" in metrics

    def test_plan_only_mode(self, tmp_path: Path) -> None:
        session, ctx = build_context(tmp_path, execute=False)
        _seed_china_backtest_fixture(session)

        result = settle_backtest(ctx, snapshot_id="nonexistent")
        assert result["status"] == "planned"


# ---------------------------------------------------------------------------
# _enrich_snapshot_probabilities — market_normalized_prob
# ---------------------------------------------------------------------------


def _make_enrich_rows(
    prices: list[float], event_id: str = "event-1"
) -> list[dict[str, Any]]:
    rows = []
    for i, price in enumerate(prices):
        rows.append({
            "row_id": f"row-{i}",
            "event_id": event_id,
            "entry_yes_price": price,
            "fp1_position": i + 1,
            "fp1_gap_to_leader_seconds": 0.0 if i == 0 else i * 0.3,
            "fp1_teammate_gap_seconds": None,
            "fp1_lap_count": 25,
            "fp1_stint_count": 2,
        })
    return rows


class TestEnrichSnapshotProbabilities:
    """Verify _enrich_snapshot_probabilities adds market_normalized_prob."""

    def test_market_normalized_prob_present(self) -> None:
        rows = _make_enrich_rows([0.30, 0.25, 0.20, 0.15, 0.10])
        enriched = _enrich_snapshot_probabilities(rows)
        assert len(enriched) == 5
        for row in enriched:
            assert "market_normalized_prob" in row

    def test_market_normalized_prob_sums_to_one(self) -> None:
        rows = _make_enrich_rows([0.30, 0.25, 0.20, 0.15, 0.10])
        enriched = _enrich_snapshot_probabilities(rows)
        total = sum(row["market_normalized_prob"] for row in enriched)
        assert total == pytest.approx(1.0)

    def test_inflated_prices_still_normalize(self) -> None:
        """negRisk scenario: YES prices sum > 1."""
        rows = _make_enrich_rows([0.35, 0.30, 0.25, 0.20, 0.12])
        enriched = _enrich_snapshot_probabilities(rows)
        total = sum(row["market_normalized_prob"] for row in enriched)
        assert total == pytest.approx(1.0)

    def test_hybrid_probability_present(self) -> None:
        rows = _make_enrich_rows([0.30, 0.25, 0.20, 0.15, 0.10])
        enriched = _enrich_snapshot_probabilities(rows)
        for row in enriched:
            assert "hybrid_probability" in row
            assert 0.0 <= row["hybrid_probability"] <= 1.0

    def test_multi_event_groups(self) -> None:
        """Rows from different events should be normalized independently."""
        rows_a = _make_enrich_rows([0.60, 0.40], event_id="event-a")
        rows_b = _make_enrich_rows([0.30, 0.30, 0.40], event_id="event-b")
        enriched = _enrich_snapshot_probabilities(rows_a + rows_b)
        event_a = [r for r in enriched if r["event_id"] == "event-a"]
        event_b = [r for r in enriched if r["event_id"] == "event-b"]
        assert sum(r["market_normalized_prob"] for r in event_a) == pytest.approx(1.0)
        assert sum(r["market_normalized_prob"] for r in event_b) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Calibration bucket clamping
# ---------------------------------------------------------------------------


class TestCalibrationBucketClamp:
    def test_prob_one_does_not_overflow(self) -> None:
        """prob=1.0 should land in 90-100%, not 100-110%."""
        settled = [
            {"pnl": 5.0, "outcome": 1, "model_probability": 1.0, "edge": 0.1},
        ]
        metrics = _compute_backtest_metrics(settled, bet_size=10.0)
        buckets = metrics["calibration_buckets"]
        assert "100-110%" not in buckets
        assert "90-100%" in buckets

    def test_prob_zero_bucket(self) -> None:
        settled = [
            {"pnl": -5.0, "outcome": 0, "model_probability": 0.005, "edge": 0.1},
        ]
        metrics = _compute_backtest_metrics(settled, bet_size=10.0)
        buckets = metrics["calibration_buckets"]
        assert "0-10%" in buckets


def _seed_snapshot_for_backfill(
    session: Session,
    tmp_path: Path,
    *,
    gp_short_code: str | None = None,
    config: GPConfig | None = None,
    labels: list[int | None],
    snapshot_id: str | None = None,
    seed_target_results: bool = False,
) -> FeatureSnapshot:
    if config is None:
        if gp_short_code is None:
            raise ValueError("gp_short_code or config is required")
        config = get_gp_config(gp_short_code)
    meeting = session.get(F1Meeting, f"meeting:{config.meeting_key}") or F1Meeting(
        id=f"meeting:{config.meeting_key}",
        meeting_key=config.meeting_key,
        season=config.season,
        meeting_name=config.name,
        raw_payload={},
    )
    session_record = session.get(
        F1Session,
        f"session:{config.meeting_key}:{config.short_code}",
    ) or F1Session(
        id=f"session:{config.meeting_key}:{config.short_code}",
        meeting_id=meeting.id,
        session_key=config.meeting_key,
        session_name=config.target_session_code,
        session_code=config.target_session_code,
        raw_payload={},
    )
    session.merge(meeting)
    session.merge(session_record)
    if seed_target_results:
        session.merge(
            F1SessionResult(
                id=f"result:{config.short_code}",
                session_id=session_record.id,
                driver_id="driver:test",
                position=1,
                raw_payload={},
            )
        )

    snapshot_name = snapshot_id or f"snapshot-{config.short_code}"
    snapshot_path = tmp_path / f"{snapshot_name}.parquet"
    pl.DataFrame(
        {
            "meeting_key": [config.meeting_key] * len(labels),
            "market_id": [f"market-{idx}" for idx in range(len(labels))],
            "label_yes": labels,
        }
    ).write_parquet(snapshot_path)

    snapshot = FeatureSnapshot(
        id=snapshot_name,
        session_id=session_record.id,
        as_of_ts=datetime(2026, 3, 28, 3, 40, tzinfo=timezone.utc),
        snapshot_type=config.snapshot_type,
        feature_version="v1",
        storage_path=str(snapshot_path),
        row_count=len(labels),
    )
    session.add(snapshot)
    session.commit()
    return snapshot


class TestBackfillBacktests:
    def test_backfills_dynamic_ops_stage_snapshot(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        session, ctx = build_context(tmp_path)
        session.merge(
            F1Meeting(
                id="meeting:1284",
                meeting_key=1284,
                season=2026,
                round_number=4,
                meeting_name="Miami Grand Prix",
                meeting_slug="miami-grand-prix",
                event_format="sprint",
                raw_payload={},
            )
        )
        session.commit()

        config = resolve_gp_config("miami_fp1_q", db=session)
        snapshot = _seed_snapshot_for_backfill(
            session,
            tmp_path,
            config=config,
            labels=[1, 0],
        )

        baseline_calls: list[str] = []
        settle_calls: list[str] = []

        monkeypatch.setattr(
            "f1_polymarket_worker.gp_registry.run_baseline",
            lambda *_args, snapshot_id, **_kwargs: baseline_calls.append(snapshot_id) or {},
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.backtest.settle_single_gp",
            lambda *_args, snapshot_id, **_kwargs: settle_calls.append(snapshot_id)
            or {
                "backtest": {
                    "backtest_run_id": "bt-miami",
                    "metrics": {"bet_count": 1, "total_pnl": 2.5},
                }
            },
        )

        result = backfill_backtests(ctx, gp_short_code="miami_fp1_q")

        assert result["processed_count"] == 1
        assert result["skipped_count"] == 0
        assert baseline_calls == [snapshot.id]
        assert settle_calls == [snapshot.id]
        assert result["processed"][0]["gp_short_code"] == "miami_fp1_q"

    def test_backfills_labeled_snapshot(self, tmp_path: Path, monkeypatch) -> None:
        session, ctx = build_context(tmp_path)
        snapshot = _seed_snapshot_for_backfill(
            session,
            tmp_path,
            gp_short_code="japan_fp3",
            labels=[1, 0, 0],
        )

        baseline_calls: list[str] = []
        settle_calls: list[str] = []

        monkeypatch.setattr(
            "f1_polymarket_worker.gp_registry.run_baseline",
            lambda *_args, snapshot_id, **_kwargs: baseline_calls.append(snapshot_id) or {},
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.backtest.settle_single_gp",
            lambda *_args, snapshot_id, **_kwargs: settle_calls.append(snapshot_id)
            or {
                "backtest": {
                    "backtest_run_id": "bt-1",
                    "metrics": {"bet_count": 2, "total_pnl": 4.5},
                }
            },
        )

        result = backfill_backtests(ctx, gp_short_code="japan_fp3")

        assert result["processed_count"] == 1
        assert result["skipped_count"] == 0
        assert baseline_calls == [snapshot.id]
        assert settle_calls == [snapshot.id]
        assert result["processed"][0]["labeled_row_count"] == 3
        assert result["processed"][0]["bet_count"] == 2

    def test_skips_unlabeled_snapshot(self, tmp_path: Path, monkeypatch) -> None:
        session, ctx = build_context(tmp_path)
        _seed_snapshot_for_backfill(
            session,
            tmp_path,
            gp_short_code="japan_q_race",
            labels=[None, None],
        )

        monkeypatch.setattr(
            "f1_polymarket_worker.gp_registry.run_baseline",
            lambda *_args, **_kwargs: pytest.fail("run_baseline should not be called"),
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.backtest.settle_single_gp",
            lambda *_args, **_kwargs: pytest.fail("settle_single_gp should not be called"),
        )

        result = backfill_backtests(ctx, gp_short_code="japan_q_race")

        assert result["processed_count"] == 0
        assert result["skipped_count"] == 1
        assert result["skipped"][0]["reason"] == "target_results_unavailable"

    def test_rebuilds_stale_snapshot_when_target_results_exist(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        session, ctx = build_context(tmp_path)
        stale = _seed_snapshot_for_backfill(
            session,
            tmp_path,
            gp_short_code="japan_q_race",
            labels=[None, None],
            snapshot_id="snapshot-stale",
            seed_target_results=True,
        )
        rebuilt = _seed_snapshot_for_backfill(
            session,
            tmp_path,
            gp_short_code="japan_q_race",
            labels=[1, 0],
            snapshot_id="snapshot-rebuilt",
            seed_target_results=True,
        )

        build_calls: list[str] = []
        baseline_calls: list[str] = []
        settle_calls: list[str] = []

        monkeypatch.setattr(
            "f1_polymarket_worker.gp_registry.build_snapshot",
            lambda _ctx, config, **_kwargs: build_calls.append(config.short_code)
            or {"snapshot_id": rebuilt.id},
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.gp_registry.run_baseline",
            lambda *_args, snapshot_id, **_kwargs: baseline_calls.append(snapshot_id) or {},
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.backtest.settle_single_gp",
            lambda *_args, snapshot_id, **_kwargs: settle_calls.append(snapshot_id)
            or {
                "backtest": {
                    "backtest_run_id": "bt-race",
                    "metrics": {"bet_count": 1, "total_pnl": 3.25},
                }
            },
        )
        monkeypatch.setattr(
            "f1_polymarket_worker.backtest._latest_snapshot_for_config",
            lambda *_args, **_kwargs: stale,
        )

        result = backfill_backtests(ctx, gp_short_code="japan_q_race")

        assert result["processed_count"] == 1
        assert result["skipped_count"] == 0
        assert build_calls == ["japan_q_race"]
        assert baseline_calls == [rebuilt.id]
        assert settle_calls == [rebuilt.id]
        assert result["processed"][0]["rebuilt_snapshot"] is True
