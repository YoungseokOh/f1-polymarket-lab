from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from f1_polymarket_lab.common import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    LiveTradeExecution,
    LiveTradeTicket,
    PolymarketMarket,
)
from f1_polymarket_worker.gp_registry import resolve_gp_config
from f1_polymarket_worker.live_trading import (
    create_live_trade_ticket,
    record_live_trade_fill,
    summarize_live_trading,
)
from f1_polymarket_worker.pipeline.context import PipelineContext
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_context(tmp_path: Path) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(
        data_root=tmp_path,
        live_trading_enabled=True,
        live_trading_readiness_confirmed=True,
    )
    return session, PipelineContext(db=session, execute=True, settings=settings)


def seed_miami_live_fixture(session: Session) -> None:
    meeting = F1Meeting(
        id="meeting:1284",
        meeting_key=1284,
        season=2026,
        meeting_name="Miami Grand Prix",
    )
    sq_session = F1Session(
        id="session:sq",
        session_key=128401,
        meeting_id=meeting.id,
        session_name="Sprint Qualifying",
        session_code="SQ",
        session_type="Qualifying",
        date_start_utc=datetime(2026, 5, 2, 20, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 5, 2, 21, 0, tzinfo=timezone.utc),
        is_practice=False,
    )
    market = PolymarketMarket(
        id="market:miami:sq",
        question="Will Oscar Piastri win Sprint Qualifying pole?",
        slug="miami-sq-piastri",
        condition_id="condition:miami:sq",
        taxonomy="driver_pole_position",
        target_session_code="SQ",
        best_bid=0.40,
        best_ask=0.42,
        last_trade_price=0.41,
        active=True,
        closed=False,
    )
    mapping = EntityMappingF1ToPolymarket(
        id="mapping:miami:sq",
        f1_session_id=sq_session.id,
        polymarket_market_id=market.id,
        mapping_type="driver_pole_position",
        confidence=0.95,
    )
    session.add_all([meeting, sq_session, market, mapping])
    session.commit()


def stub_miami_ops_config(monkeypatch: pytest.MonkeyPatch) -> None:
    config = resolve_gp_config("miami_fp1_sq")
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.get_ops_stage_config",
        lambda *_args, **_kwargs: (None, config),
    )


def test_create_live_trade_ticket_persists_ticket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    seed_miami_live_fixture(session)
    stub_miami_ops_config(monkeypatch)
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.utc_now",
        lambda: datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.build_live_signal_board",
        lambda *_args, **_kwargs: {
            "gp_short_code": "miami_fp1_sq",
            "required_stage": "sq_pole_live_v1",
            "active_model_run_id": "champion-sq",
            "model_run_id": "scored-sq",
            "snapshot_id": "snapshot-sq",
            "blockers": [],
            "rows": [
                {
                    "market_id": "market:miami:sq",
                    "token_id": "token:yes",
                    "question": "Will Oscar Piastri win Sprint Qualifying pole?",
                    "session_code": "SQ",
                    "promotion_stage": "sq_pole_live_v1",
                    "model_run_id": "scored-sq",
                    "snapshot_id": "snapshot-sq",
                    "model_prob": 0.65,
                    "market_price": 0.41,
                    "edge": 0.24,
                    "spread": 0.02,
                    "signal_action": "buy_yes",
                    "side_label": "YES",
                    "recommended_size": 10.0,
                    "max_spread": 0.03,
                    "observed_at_utc": "2026-05-02T20:15:00+00:00",
                    "event_type": "best_bid_ask",
                }
            ],
        },
    )

    try:
        result = create_live_trade_ticket(
            context,
            gp_short_code="miami_fp1_sq",
            market_id="market:miami:sq",
            observed_market_price=0.41,
            observed_spread=0.02,
            observed_at_utc=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
            source_event_type="best_bid_ask",
        )
        session.commit()

        ticket = session.get(LiveTradeTicket, result["ticket_id"])
        assert ticket is not None
        assert ticket.status == "open"
        assert ticket.model_run_id == "scored-sq"
        assert ticket.snapshot_id == "snapshot-sq"
        assert ticket.promotion_stage == "sq_pole_live_v1"
        assert ticket.side_label == "YES"
        assert ticket.recommended_size == 10.0
    finally:
        session.close()


def test_record_live_trade_fill_persists_execution_and_updates_ticket(
    tmp_path: Path,
) -> None:
    session, context = build_context(tmp_path)
    seed_miami_live_fixture(session)
    ticket = LiveTradeTicket(
        id="ticket-1",
        gp_slug="miami_fp1_sq",
        session_code="SQ",
        market_id="market:miami:sq",
        token_id="token:yes",
        snapshot_id="snapshot-sq",
        model_run_id="scored-sq",
        promotion_stage="sq_pole_live_v1",
        question="Will Oscar Piastri win Sprint Qualifying pole?",
        signal_action="buy_yes",
        side_label="YES",
        model_prob=0.65,
        market_price=0.41,
        edge=0.24,
        recommended_size=10.0,
        observed_spread=0.02,
        max_spread=0.03,
        observed_at_utc=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
        source_event_type="best_bid_ask",
        status="open",
        rationale_json={"entry_price": 0.41},
        created_at=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
    )
    session.add(ticket)
    session.commit()

    try:
        result = record_live_trade_fill(
            context,
            ticket_id="ticket-1",
            submitted_size=10.0,
            actual_fill_size=10.0,
            actual_fill_price=0.415,
            submitted_at=datetime(2026, 5, 2, 20, 16, tzinfo=timezone.utc),
            filled_at=datetime(2026, 5, 2, 20, 16, tzinfo=timezone.utc),
            operator_note="filled in browser",
            status="filled",
        )
        session.commit()

        execution = session.scalar(
            select(LiveTradeExecution).where(LiveTradeExecution.ticket_id == "ticket-1")
        )
        refreshed_ticket = session.get(LiveTradeTicket, "ticket-1")
        assert execution is not None
        assert execution.actual_fill_price == pytest.approx(0.415)
        assert execution.status == "filled"
        assert refreshed_ticket is not None
        assert refreshed_ticket.status == "filled"
        assert result["execution_status"] == "filled"
    finally:
        session.close()


def test_create_live_trade_ticket_blocks_duplicate_open_market(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    seed_miami_live_fixture(session)
    stub_miami_ops_config(monkeypatch)
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.utc_now",
        lambda: datetime(2026, 5, 2, 20, 20, tzinfo=timezone.utc),
    )
    session.add(
        LiveTradeTicket(
            id="ticket-open",
            gp_slug="miami_fp1_sq",
            session_code="SQ",
            market_id="market:miami:sq",
            token_id="token:yes",
            snapshot_id="snapshot-sq",
            model_run_id="scored-sq",
            promotion_stage="sq_pole_live_v1",
            question="Will Oscar Piastri win Sprint Qualifying pole?",
            signal_action="buy_yes",
            side_label="YES",
            model_prob=0.65,
            market_price=0.41,
            edge=0.24,
            recommended_size=10.0,
            observed_spread=0.02,
            max_spread=0.03,
            observed_at_utc=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
            source_event_type="best_bid_ask",
            status="open",
            rationale_json={"entry_price": 0.41},
            created_at=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
        )
    )
    session.commit()
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.build_live_signal_board",
        lambda *_args, **_kwargs: {
            "gp_short_code": "miami_fp1_sq",
            "required_stage": "sq_pole_live_v1",
            "active_model_run_id": "champion-sq",
            "model_run_id": "scored-sq",
            "snapshot_id": "snapshot-sq",
            "blockers": [],
            "rows": [
                {
                    "market_id": "market:miami:sq",
                    "token_id": "token:yes",
                    "question": "Will Oscar Piastri win Sprint Qualifying pole?",
                    "session_code": "SQ",
                    "promotion_stage": "sq_pole_live_v1",
                    "model_run_id": "scored-sq",
                    "snapshot_id": "snapshot-sq",
                    "model_prob": 0.65,
                    "market_price": 0.41,
                    "edge": 0.24,
                    "spread": 0.02,
                    "signal_action": "buy_yes",
                    "side_label": "YES",
                    "recommended_size": 10.0,
                    "max_spread": 0.03,
                    "observed_at_utc": "2026-05-02T20:15:00+00:00",
                    "event_type": "best_bid_ask",
                }
            ],
        },
    )

    try:
        with pytest.raises(ValueError, match="already has an open live ticket"):
            create_live_trade_ticket(
                context,
                gp_short_code="miami_fp1_sq",
                market_id="market:miami:sq",
                observed_market_price=0.41,
                observed_spread=0.02,
                observed_at_utc=datetime(2026, 5, 2, 20, 20, tzinfo=timezone.utc),
            )

        summary = summarize_live_trading(context, gp_slug="miami_fp1_sq")
        assert summary.ticket_count == 1
        assert summary.open_ticket_count == 1
    finally:
        session.close()


def test_create_live_trade_ticket_blocks_when_live_trading_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(data_root=tmp_path)
    context = PipelineContext(db=session, execute=True, settings=settings)
    seed_miami_live_fixture(session)
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.build_live_signal_board",
        lambda *_args, **_kwargs: {
            "gp_short_code": "miami_fp1_sq",
            "required_stage": "sq_pole_live_v1",
            "active_model_run_id": "champion-sq",
            "model_run_id": "scored-sq",
            "snapshot_id": "snapshot-sq",
            "blockers": [],
            "rows": [
                {
                    "market_id": "market:miami:sq",
                    "token_id": "token:yes",
                    "question": "Will Oscar Piastri win Sprint Qualifying pole?",
                    "session_code": "SQ",
                    "promotion_stage": "sq_pole_live_v1",
                    "model_run_id": "scored-sq",
                    "snapshot_id": "snapshot-sq",
                    "model_prob": 0.65,
                    "market_price": 0.41,
                    "edge": 0.24,
                    "spread": 0.02,
                    "signal_action": "buy_yes",
                    "side_label": "YES",
                    "recommended_size": 10.0,
                    "max_spread": 0.03,
                    "observed_at_utc": "2026-05-02T20:15:00+00:00",
                    "event_type": "best_bid_ask",
                }
            ],
        },
    )

    try:
        with pytest.raises(ValueError, match="Live operator tickets are disabled"):
            create_live_trade_ticket(
                context,
                gp_short_code="miami_fp1_sq",
                market_id="market:miami:sq",
                observed_market_price=0.41,
                observed_spread=0.02,
                observed_at_utc=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
            )
    finally:
        session.close()


def test_create_live_trade_ticket_rejects_stale_quote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    seed_miami_live_fixture(session)
    stub_miami_ops_config(monkeypatch)
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.utc_now",
        lambda: datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.build_live_signal_board",
        lambda *_args, **_kwargs: {
            "gp_short_code": "miami_fp1_sq",
            "required_stage": "sq_pole_live_v1",
            "active_model_run_id": "champion-sq",
            "model_run_id": "scored-sq",
            "snapshot_id": "snapshot-sq",
            "blockers": [],
            "rows": [
                {
                    "market_id": "market:miami:sq",
                    "token_id": "token:yes",
                    "question": "Will Oscar Piastri win Sprint Qualifying pole?",
                    "session_code": "SQ",
                    "promotion_stage": "sq_pole_live_v1",
                    "model_run_id": "scored-sq",
                    "snapshot_id": "snapshot-sq",
                    "model_prob": 0.65,
                    "market_price": 0.41,
                    "edge": 0.24,
                    "spread": 0.02,
                    "signal_action": "buy_yes",
                    "side_label": "YES",
                    "recommended_size": 10.0,
                    "max_spread": 0.03,
                    "observed_at_utc": "2026-05-02T20:13:00+00:00",
                    "event_type": "best_bid_ask",
                }
            ],
        },
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.utc_now",
        lambda: datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
    )

    try:
        with pytest.raises(ValueError, match="Live quote is stale"):
            create_live_trade_ticket(
                context,
                gp_short_code="miami_fp1_sq",
                market_id="market:miami:sq",
                observed_market_price=0.41,
                observed_spread=0.02,
                observed_at_utc=datetime(2026, 5, 2, 20, 13, tzinfo=timezone.utc),
            )
    finally:
        session.close()


def test_create_live_trade_ticket_rejects_requested_size_above_configured_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    seed_miami_live_fixture(session)
    stub_miami_ops_config(monkeypatch)
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.utc_now",
        lambda: datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.live_trading.build_live_signal_board",
        lambda *_args, **_kwargs: {
            "gp_short_code": "miami_fp1_sq",
            "required_stage": "sq_pole_live_v1",
            "active_model_run_id": "champion-sq",
            "model_run_id": "scored-sq",
            "snapshot_id": "snapshot-sq",
            "blockers": [],
            "rows": [
                {
                    "market_id": "market:miami:sq",
                    "token_id": "token:yes",
                    "question": "Will Oscar Piastri win Sprint Qualifying pole?",
                    "session_code": "SQ",
                    "promotion_stage": "sq_pole_live_v1",
                    "model_run_id": "scored-sq",
                    "snapshot_id": "snapshot-sq",
                    "model_prob": 0.65,
                    "market_price": 0.41,
                    "edge": 0.24,
                    "spread": 0.02,
                    "signal_action": "buy_yes",
                    "side_label": "YES",
                    "recommended_size": 10.0,
                    "max_spread": 0.03,
                    "observed_at_utc": "2026-05-02T20:15:00+00:00",
                    "event_type": "best_bid_ask",
                }
            ],
        },
    )

    try:
        with pytest.raises(ValueError, match="exceeds the configured max"):
            create_live_trade_ticket(
                context,
                gp_short_code="miami_fp1_sq",
                market_id="market:miami:sq",
                observed_market_price=0.41,
                observed_spread=0.02,
                observed_at_utc=datetime(2026, 5, 2, 20, 15, tzinfo=timezone.utc),
                bet_size=12.0,
            )
    finally:
        session.close()
