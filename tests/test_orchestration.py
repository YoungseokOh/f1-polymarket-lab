from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Lap,
    F1Meeting,
    F1Session,
    F1SessionResult,
    F1TelemetryIndex,
    MappingCandidate,
    MarketTaxonomyLabel,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
    PolymarketWsMessageManifest,
)
from f1_polymarket_worker.orchestration import (
    backfill_f1_history,
    capture_live_weekend,
    discover_session_polymarket,
    sync_polymarket_f1_catalog,
    validate_f1_weekend_subset,
)
from f1_polymarket_worker.pipeline import PipelineContext, reconcile_mappings
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_context(tmp_path: Path, *, execute: bool = True) -> tuple[Session, PipelineContext]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    settings = Settings(
        data_root=tmp_path,
        openf1_username="user@example.com",
        openf1_password="secret",
    )
    context = PipelineContext(db=session, execute=execute, settings=settings)
    return session, context


def make_f1_event(event_id: str, market_id: str, question: str) -> dict[str, object]:
    return {
        "id": event_id,
        "slug": f"event-{event_id}",
        "title": "Qatar Grand Prix: Driver Winner",
        "description": "Formula 1 market",
        "ticker": f"f1-{event_id}",
        "active": True,
        "closed": False,
        "archived": False,
        "startDate": "2025-11-24T13:36:21.307145Z",
        "endDate": "2025-12-07T16:00:00Z",
        "tags": [
            {"label": "Formula 1", "slug": "formula1"},
            {"label": "f1", "slug": "f1"},
        ],
        "markets": [
            {
                "id": market_id,
                "question": question,
                "conditionId": f"condition-{market_id}",
                "questionID": f"question-{market_id}",
                "slug": f"market-{market_id}",
                "description": "Formula 1 winner market",
                "startDate": "2025-11-24T13:32:51.133598Z",
                "endDate": "2025-12-07T16:00:00Z",
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "bestBid": 0.48,
                "bestAsk": 0.49,
                "lastTradePrice": 0.485,
                "volumeNum": 123.0,
                "liquidityNum": 456.0,
                "clobTokenIds": "[\"token-1\", \"token-2\"]",
                "outcomes": "[\"Yes\", \"No\"]",
                "outcomePrices": "[\"0.48\", \"0.52\"]",
            }
        ],
    }


def make_session_market_event(
    *,
    event_id: str,
    market_id: str,
    slug: str,
    title: str,
    question: str,
    description: str,
) -> dict[str, object]:
    return {
        "id": event_id,
        "slug": slug,
        "title": title,
        "description": description,
        "ticker": slug,
        "active": True,
        "closed": False,
        "archived": False,
        "startDate": "2026-03-11T11:53:48.271929Z",
        "endDate": "2026-04-03T06:00:00Z",
        "tags": [
            {"label": "Formula 1", "slug": "formula1"},
            {"label": "f1", "slug": "f1"},
        ],
        "markets": [
            {
                "id": market_id,
                "question": question,
                "conditionId": f"condition-{market_id}",
                "questionID": f"question-{market_id}",
                "slug": slug,
                "description": description,
                "startDate": "2026-03-11T11:53:48.271929Z",
                "endDate": "2026-04-03T06:00:00Z",
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "bestBid": 0.48,
                "bestAsk": 0.49,
                "lastTradePrice": 0.485,
                "volumeNum": 123.0,
                "liquidityNum": 456.0,
                "clobTokenIds": "[\"token-1\", \"token-2\"]",
                "outcomes": "[\"Yes\", \"No\"]",
                "outcomePrices": "[\"0.48\", \"0.52\"]",
            }
        ],
    }


def seed_validation_weekend_fixture(
    session: Session,
    *,
    invalid_q_taxonomy: str | None = None,
    telemetry_session_codes: set[str] | None = None,
) -> dict[str, Any]:
    telemetry_codes = telemetry_session_codes or {"FP1", "SQ", "S", "Q", "R"}
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

    session_rows = [
        ("session-fp1", 11235, "Practice 1", "Practice", "FP1", datetime(2026, 3, 13, 3, 30)),
        (
            "session-sq",
            11236,
            "Sprint Qualifying",
            "Qualifying",
            "SQ",
            datetime(2026, 3, 13, 7, 30),
        ),
        ("session-s", 11240, "Sprint", "Race", "S", datetime(2026, 3, 14, 3, 0)),
        ("session-q", 11241, "Qualifying", "Qualifying", "Q", datetime(2026, 3, 14, 7, 0)),
        ("session-r", 11245, "Race", "Race", "R", datetime(2026, 3, 15, 7, 0)),
    ]
    sessions_by_code: dict[str, F1Session] = {}
    for session_id, session_key, name, session_type, code, start_naive in session_rows:
        start_at = start_naive.replace(tzinfo=timezone.utc)
        end_at = start_at.replace(hour=start_at.hour + 1)
        row = F1Session(
            id=session_id,
            meeting_id=meeting.id,
            session_key=session_key,
            session_name=name,
            session_type=session_type,
            session_code=code,
            date_start_utc=start_at,
            date_end_utc=end_at,
            raw_payload={"meeting_name": "Chinese Grand Prix", "gmt_offset": "+08:00"},
        )
        sessions_by_code[code] = row
        session.add(row)
        session.add(
            F1SessionResult(
                id=f"result-{code}",
                session_id=session_id,
                driver_id="driver-1",
                position=1,
                result_time_seconds=90.0,
                result_time_kind="best_lap" if code in {"FP1", "SQ", "Q"} else "total_time",
                gap_to_leader_status="leader",
                raw_payload={"session_code": code},
            )
        )
        session.add(
            F1Lap(
                id=f"lap-{code}",
                session_id=session_id,
                driver_id="driver-1",
                lap_number=1,
                lap_duration_seconds=90.0,
                raw_payload={"session_code": code},
            )
        )
        if code in telemetry_codes:
            session.add(
                F1TelemetryIndex(
                    id=f"telemetry-{code}",
                    session_id=session_id,
                    driver_id="driver-1",
                    dataset_name="car_data",
                    storage_path=f"bronze/{code}/car_data.json",
                    sample_count=10,
                    started_at_utc=start_at,
                    ended_at_utc=end_at,
                    raw_payload={"session_code": code},
                )
            )

    market_defs = [
        (
            "event-fp1",
            "market-fp1",
            "f1-chinese-grand-prix-practice-1-fastest-lap-2026-03-13",
            "Chinese Grand Prix: Practice 1 Fastest Lap",
            "Chinese Grand Prix: Practice 1 Fastest Lap",
            "driver_fastest_lap_practice",
            "FP1",
            "FP1",
            False,
        ),
        (
            "event-sq",
            "market-sq",
            "f1-chinese-grand-prix-sprint-qualifying-pole-winner-2026-03-13",
            "Chinese Grand Prix: Sprint Qualifying Pole Winner",
            "Chinese Grand Prix: Sprint Qualifying Pole Winner",
            "qualifying_winner",
            "SQ",
            "SQ",
            True,
        ),
        (
            "event-s",
            "market-s",
            "f1-chinese-grand-prix-sprint-winner-2026-03-14",
            "Chinese Grand Prix: Sprint Winner",
            "Chinese Grand Prix: Sprint Winner",
            "sprint_winner",
            "S",
            "S",
            False,
        ),
        (
            "event-q",
            "market-q",
            "f1-chinese-grand-prix-driver-pole-position-2026-03-14",
            "Chinese Grand Prix: Driver Pole Position",
            "Chinese Grand Prix: Driver Pole Position",
            "driver_pole_position",
            "Q",
            "Q",
            True,
        ),
        (
            "event-r-h2h",
            "market-r-h2h",
            "f1-chinese-grand-prix-head-to-head-matchups",
            "Chinese Grand Prix: Head-to-Head",
            "Leclerc vs Piastri",
            "head_to_head_session",
            "R",
            "R",
            True,
        ),
        (
            "event-r-winner",
            "market-r-winner",
            "f1-chinese-grand-prix-winner-2026-03-15",
            "Chinese Grand Prix: Winner",
            "Chinese Grand Prix: Winner",
            "race_winner",
            "R",
            "R",
            True,
        ),
    ]
    for index, (
        event_id,
        market_id,
        slug,
        title,
        question,
        taxonomy,
        session_code,
        target_session_code,
        create_mapping,
    ) in enumerate(market_defs):
        event_start = sessions_by_code[session_code].date_start_utc
        event = PolymarketEvent(
            id=event_id,
            slug=slug,
            ticker=slug,
            title=title,
            description=f"{title} Formula 1 market",
            start_at_utc=event_start,
            end_at_utc=event_start,
            active=True,
            closed=False,
            archived=False,
            raw_payload={"title": title, "slug": slug},
        )
        market = PolymarketMarket(
            id=market_id,
            event_id=event_id,
            question=question,
            slug=slug,
            condition_id=f"condition-{market_id}",
            question_id=f"question-{market_id}",
            taxonomy=taxonomy,
            taxonomy_confidence=0.95,
            target_session_code=target_session_code,
            description=title,
            start_at_utc=event_start,
            end_at_utc=event_start,
            active=True,
            closed=False,
            archived=False,
            enable_order_book=True,
            clob_token_ids=[f"token-{market_id}-yes", f"token-{market_id}-no"],
            raw_payload={"question": question, "slug": slug},
        )
        session.add(event)
        session.add(market)
        session.add(
            PolymarketToken(
                id=f"token-{market_id}-yes",
                market_id=market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=0.55,
                raw_payload={"market_id": market_id},
            )
        )
        candidate = MappingCandidate(
            id=f"candidate-{market_id}",
            f1_meeting_id=meeting.id,
            f1_session_id=sessions_by_code[session_code].id,
            polymarket_event_id=event_id,
            polymarket_market_id=market_id,
            candidate_type=taxonomy,
            confidence=0.95 - (index * 0.01),
            matched_by="test_fixture",
            rationale_json={"market_id": market_id},
            status="candidate",
        )
        session.add(candidate)
        if create_mapping:
            session.add(
                EntityMappingF1ToPolymarket(
                    id=f"mapping-{market_id}",
                    f1_meeting_id=meeting.id,
                    f1_session_id=sessions_by_code[session_code].id,
                    polymarket_event_id=event_id,
                    polymarket_market_id=market_id,
                    mapping_type=taxonomy,
                    confidence=0.95 - (index * 0.01),
                    matched_by="test_fixture",
                    notes="fixture",
                    override_flag=False,
                )
            )

    if invalid_q_taxonomy is not None:
        bad_event_id = "event-q-bad"
        bad_market_id = "market-q-bad"
        bad_start = sessions_by_code["Q"].date_start_utc
        session.add(
            PolymarketEvent(
                id=bad_event_id,
                slug="f1-chinese-grand-prix-winner-2026-03-14",
                ticker="f1-china-bad-q",
                title="Chinese Grand Prix: Winner",
                description="Wrongly linked race market",
                start_at_utc=bad_start,
                end_at_utc=bad_start,
                active=True,
                closed=False,
                archived=False,
                raw_payload={"title": "Chinese Grand Prix: Winner"},
            )
        )
        session.add(
            PolymarketMarket(
                id=bad_market_id,
                event_id=bad_event_id,
                question="Chinese Grand Prix: Winner",
                slug="f1-chinese-grand-prix-winner-2026-03-14",
                condition_id=f"condition-{bad_market_id}",
                taxonomy=invalid_q_taxonomy,
                taxonomy_confidence=0.9,
                target_session_code="Q",
                description="Wrongly classified race market",
                start_at_utc=bad_start,
                end_at_utc=bad_start,
                active=True,
                closed=False,
                archived=False,
                enable_order_book=True,
                clob_token_ids=[f"token-{bad_market_id}-yes"],
                raw_payload={"question": "Chinese Grand Prix: Winner"},
            )
        )
        session.add(
            MappingCandidate(
                id="candidate-q-bad",
                f1_meeting_id=meeting.id,
                f1_session_id=sessions_by_code["Q"].id,
                polymarket_event_id=bad_event_id,
                polymarket_market_id=bad_market_id,
                candidate_type=invalid_q_taxonomy,
                confidence=0.9,
                matched_by="test_fixture",
                rationale_json={"bad": True},
                status="candidate",
            )
        )
        session.add(
            EntityMappingF1ToPolymarket(
                id="mapping-q-bad",
                f1_meeting_id=meeting.id,
                f1_session_id=sessions_by_code["Q"].id,
                polymarket_event_id=bad_event_id,
                polymarket_market_id=bad_market_id,
                mapping_type=invalid_q_taxonomy,
                confidence=0.9,
                matched_by="test_fixture",
                notes="bad fixture",
                override_flag=False,
            )
        )

    session.commit()
    return {"meeting": meeting, "sessions_by_code": sessions_by_code}


def test_validate_f1_weekend_subset_writes_report_and_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    fixture = seed_validation_weekend_fixture(session, telemetry_session_codes={"SQ", "Q", "R"})
    heavy_calls: dict[int, bool] = {}

    def fake_sync_f1_calendar(_ctx: PipelineContext, season: int) -> dict[str, Any]:
        assert season == 2026
        return {"status": "completed", "season": season}

    def fake_hydrate_f1_session(
        _ctx: PipelineContext,
        *,
        session_key: int,
        include_extended: bool,
        include_heavy: bool,
    ) -> dict[str, Any]:
        assert include_extended is True
        heavy_calls[session_key] = include_heavy
        return {"status": "completed", "session_key": session_key, "records_written": 3}

    def fake_discover_session_polymarket(
        _ctx: PipelineContext,
        *,
        session_key: int,
        batch_size: int = 100,
        max_pages: int = 5,
        search_fallback: bool = True,
    ) -> dict[str, Any]:
        return {
            "status": "completed",
            "session_key": session_key,
            "batch_size": batch_size,
            "max_pages": max_pages,
            "search_fallback": search_fallback,
        }

    def fake_reconcile_mappings(_ctx: PipelineContext) -> dict[str, Any]:
        return {"status": "completed", "candidate_rows": 0, "mapping_rows": 0}

    def fake_run_data_quality_checks(_ctx: PipelineContext) -> dict[str, Any]:
        return {"job_run_id": "dq-run-1", "status": "completed"}

    def fake_hydrate_polymarket_market(
        _ctx: PipelineContext,
        *,
        market_id: str,
        fidelity: int = 60,
    ) -> dict[str, Any]:
        market = session.get(PolymarketMarket, market_id)
        assert market is not None
        observed_at = utc_now()
        session.add(
            PolymarketPriceHistory(
                id=f"price-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                observed_at_utc=observed_at,
                price=0.55,
                midpoint=0.56,
                best_bid=0.54,
                best_ask=0.57,
                raw_payload={"market_id": market_id},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"trade-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                condition_id=market.condition_id,
                trade_timestamp_utc=observed_at,
                side="buy",
                price=0.55,
                size=10.0,
                raw_payload={"market_id": market_id},
            )
        )
        session.commit()
        return {"status": "completed", "market_id": market_id, "fidelity": fidelity}

    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.sync_f1_calendar",
        fake_sync_f1_calendar,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.ensure_default_feature_registry",
        lambda _ctx: None,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_f1_session",
        fake_hydrate_f1_session,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.discover_session_polymarket",
        fake_discover_session_polymarket,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.reconcile_mappings",
        fake_reconcile_mappings,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.run_data_quality_checks",
        fake_run_data_quality_checks,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_polymarket_market",
        fake_hydrate_polymarket_market,
    )

    result = validate_f1_weekend_subset(context, meeting_key=fixture["meeting"].meeting_key)

    assert result["status"] == "completed"
    assert result["market_probes"] == 3
    assert heavy_calls == {
        11235: False,
        11236: True,
        11240: False,
        11241: True,
        11245: True,
    }

    report_dir = tmp_path / "reports" / "validation" / "2026" / "2026-chinese-grand-prix"
    report_json = report_dir / "summary.json"
    report_md = report_dir / "summary.md"
    assert report_json.exists()
    assert report_md.exists()

    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["overall_status"] == "completed"
    assert report["validation_mode"] == "smoke"
    assert report["heavy_session_codes"] == ["Q", "R", "SQ"]
    assert set(report["session_pattern"]) == {"FP1", "SQ", "S", "Q", "R"}
    assert report["research_readiness"]["f1_subset_data"] == "ready"
    assert report["research_readiness"]["session_market_mapping"] == "ready"
    assert report["research_readiness"]["market_history_probe"] == "ready"
    assert report["research_readiness"]["analysis_joinability"] == "ready"
    assert report["mapping_summary"]["11241"]["mapping_count"] == 1
    assert {probe["probe_key"] for probe in report["market_probes"]} == {
        "pole",
        "race_head_to_head",
        "race_outcome",
    }
    assert report["failures"] == []
    assert report["warnings"] == []


def test_validate_f1_weekend_subset_fails_on_invalid_q_taxonomy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    fixture = seed_validation_weekend_fixture(session, invalid_q_taxonomy="race_winner")

    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.sync_f1_calendar",
        lambda _ctx, season: {"status": "completed", "season": season},
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.ensure_default_feature_registry",
        lambda _ctx: None,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_f1_session",
        lambda _ctx, *, session_key, include_extended, include_heavy: {
            "status": "completed",
            "session_key": session_key,
        },
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.discover_session_polymarket",
        lambda _ctx, *, session_key, batch_size=100, max_pages=5, search_fallback=True: {
            "status": "completed",
            "session_key": session_key,
        },
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.reconcile_mappings",
        lambda _ctx: {"status": "completed"},
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.run_data_quality_checks",
        lambda _ctx: {"job_run_id": "dq-run-2", "status": "completed"},
    )

    def fake_hydrate_market(
        _ctx: PipelineContext,
        *,
        market_id: str,
        fidelity: int = 60,
    ) -> dict[str, Any]:
        market = session.get(PolymarketMarket, market_id)
        assert market is not None
        observed_at = utc_now()
        session.add(
            PolymarketPriceHistory(
                id=f"price-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                observed_at_utc=observed_at,
                price=0.5,
                raw_payload={"market_id": market_id},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"trade-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                condition_id=market.condition_id,
                trade_timestamp_utc=observed_at,
                side="buy",
                price=0.5,
                size=5.0,
                raw_payload={"market_id": market_id},
            )
        )
        session.commit()
        return {"status": "completed", "market_id": market_id, "fidelity": fidelity}

    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_polymarket_market",
        fake_hydrate_market,
    )

    result = validate_f1_weekend_subset(context, meeting_key=fixture["meeting"].meeting_key)

    assert result["status"] == "failed"

    report_json = (
        tmp_path / "reports" / "validation" / "2026" / "2026-chinese-grand-prix" / "summary.json"
    )
    report = json.loads(report_json.read_text(encoding="utf-8"))
    assert report["overall_status"] == "failed"
    assert any("invalid candidate taxonomies" in item for item in report["failures"])
    assert any("invalid mapped taxonomies" in item for item in report["failures"])


def seed_australia_standard_weekend_fixture(session: Session) -> dict[str, Any]:
    """Seed a 2026 Australian GP standard weekend (FP1/FP2/FP3/Q/R) for validation tests."""
    meeting = F1Meeting(
        id="meeting-aus-2026",
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

    # Standard weekend: FP1 Friday, FP2 Friday, FP3 Saturday, Q Saturday, R Sunday
    session_rows = [
        ("session-aus-fp1", 11227, "Practice 1", "Practice", "FP1", datetime(2026, 3, 6, 1, 30)),
        ("session-aus-fp2", 11228, "Practice 2", "Practice", "FP2", datetime(2026, 3, 6, 5, 0)),
        ("session-aus-fp3", 11229, "Practice 3", "Practice", "FP3", datetime(2026, 3, 7, 2, 30)),
        ("session-aus-q", 11230, "Qualifying", "Qualifying", "Q", datetime(2026, 3, 7, 5, 0)),
        ("session-aus-r", 11234, "Race", "Race", "R", datetime(2026, 3, 8, 4, 0)),
    ]
    # Smoke-mode heavy sessions for standard weekend: Q and R only (no SQ)
    heavy_telemetry_codes = {"Q", "R"}
    sessions_by_code: dict[str, F1Session] = {}
    for session_id, session_key, name, session_type, code, start_naive in session_rows:
        start_at = start_naive.replace(tzinfo=timezone.utc)
        end_at = start_at.replace(hour=start_at.hour + 1)
        row = F1Session(
            id=session_id,
            meeting_id=meeting.id,
            session_key=session_key,
            session_name=name,
            session_type=session_type,
            session_code=code,
            date_start_utc=start_at,
            date_end_utc=end_at,
            raw_payload={"meeting_name": "Australian Grand Prix", "gmt_offset": "+11:00"},
        )
        sessions_by_code[code] = row
        session.add(row)
        session.add(
            F1SessionResult(
                id=f"aus-result-{code}",
                session_id=session_id,
                driver_id="driver-1",
                position=1,
                result_time_seconds=90.0,
                result_time_kind="best_lap" if code in {"FP1", "FP2", "FP3", "Q"} else "total_time",
                gap_to_leader_status="leader",
                raw_payload={"session_code": code},
            )
        )
        session.add(
            F1Lap(
                id=f"aus-lap-{code}",
                session_id=session_id,
                driver_id="driver-1",
                lap_number=1,
                lap_duration_seconds=90.0,
                raw_payload={"session_code": code},
            )
        )
        if code in heavy_telemetry_codes:
            session.add(
                F1TelemetryIndex(
                    id=f"aus-telemetry-{code}",
                    session_id=session_id,
                    driver_id="driver-1",
                    dataset_name="car_data",
                    storage_path=f"bronze/aus/{code}/car_data.json",
                    sample_count=10,
                    started_at_utc=start_at,
                    ended_at_utc=end_at,
                    raw_payload={"session_code": code},
                )
            )

    # Markets: practice sessions get candidates only; Q and R get full mappings
    market_defs = [
        (
            "event-aus-fp1",
            "market-aus-fp1",
            "f1-australian-grand-prix-practice-1-fastest-lap-2026-03-06",
            "Australian Grand Prix: Practice 1 Fastest Lap",
            "Australian Grand Prix: Practice 1 Fastest Lap",
            "driver_fastest_lap_practice",
            "FP1",
            "FP1",
            False,
        ),
        (
            "event-aus-fp2",
            "market-aus-fp2",
            "f1-australian-grand-prix-practice-2-fastest-lap-2026-03-06",
            "Australian Grand Prix: Practice 2 Fastest Lap",
            "Australian Grand Prix: Practice 2 Fastest Lap",
            "driver_fastest_lap_practice",
            "FP2",
            "FP2",
            False,
        ),
        (
            "event-aus-fp3",
            "market-aus-fp3",
            "f1-australian-grand-prix-practice-3-fastest-lap-2026-03-07",
            "Australian Grand Prix: Practice 3 Fastest Lap",
            "Australian Grand Prix: Practice 3 Fastest Lap",
            "driver_fastest_lap_practice",
            "FP3",
            "FP3",
            False,
        ),
        (
            "event-aus-q",
            "market-aus-q",
            "f1-australian-grand-prix-driver-pole-position-2026-03-07",
            "Australian Grand Prix: Driver Pole Position",
            "Australian Grand Prix: Driver Pole Position",
            "driver_pole_position",
            "Q",
            "Q",
            True,
        ),
        (
            "event-aus-r-h2h",
            "market-aus-r-h2h",
            "f1-australian-grand-prix-head-to-head-matchups",
            "Australian Grand Prix: Head-to-Head",
            "Norris vs Piastri",
            "head_to_head_session",
            "R",
            "R",
            True,
        ),
        (
            "event-aus-r-winner",
            "market-aus-r-winner",
            "f1-australian-grand-prix-winner-2026-03-08",
            "Australian Grand Prix: Winner",
            "Australian Grand Prix: Winner",
            "race_winner",
            "R",
            "R",
            True,
        ),
    ]

    for index, (
        event_id,
        market_id,
        slug,
        title,
        question,
        taxonomy,
        session_code,
        target_session_code,
        create_mapping,
    ) in enumerate(market_defs):
        event_start = sessions_by_code[session_code].date_start_utc
        event = PolymarketEvent(
            id=event_id,
            slug=slug,
            ticker=slug,
            title=title,
            description=f"{title} Formula 1 market",
            start_at_utc=event_start,
            end_at_utc=event_start,
            active=True,
            closed=False,
            archived=False,
            raw_payload={"title": title, "slug": slug},
        )
        market = PolymarketMarket(
            id=market_id,
            event_id=event_id,
            question=question,
            slug=slug,
            condition_id=f"condition-{market_id}",
            question_id=f"question-{market_id}",
            taxonomy=taxonomy,
            taxonomy_confidence=0.95,
            target_session_code=target_session_code,
            description=title,
            start_at_utc=event_start,
            end_at_utc=event_start,
            active=True,
            closed=False,
            archived=False,
            enable_order_book=True,
            clob_token_ids=[f"token-{market_id}-yes", f"token-{market_id}-no"],
            raw_payload={"question": question, "slug": slug},
        )
        session.add(event)
        session.add(market)
        session.add(
            PolymarketToken(
                id=f"token-{market_id}-yes",
                market_id=market_id,
                outcome="Yes",
                outcome_index=0,
                latest_price=0.55,
                raw_payload={"market_id": market_id},
            )
        )
        session.add(
            MappingCandidate(
                id=f"candidate-{market_id}",
                f1_meeting_id=meeting.id,
                f1_session_id=sessions_by_code[session_code].id,
                polymarket_event_id=event_id,
                polymarket_market_id=market_id,
                candidate_type=taxonomy,
                confidence=0.95 - (index * 0.01),
                matched_by="test_fixture",
                rationale_json={"market_id": market_id},
                status="candidate",
            )
        )
        if create_mapping:
            session.add(
                EntityMappingF1ToPolymarket(
                    id=f"mapping-{market_id}",
                    f1_meeting_id=meeting.id,
                    f1_session_id=sessions_by_code[session_code].id,
                    polymarket_event_id=event_id,
                    polymarket_market_id=market_id,
                    mapping_type=taxonomy,
                    confidence=0.95 - (index * 0.01),
                    matched_by="test_fixture",
                    notes="fixture",
                    override_flag=False,
                )
            )

    session.commit()
    return {"meeting": meeting, "sessions_by_code": sessions_by_code}


def test_validate_f1_weekend_subset_standard_weekend_australia(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate_f1_weekend_subset handles a standard (FP1/FP2/FP3/Q/R) weekend correctly.

    Validates that:
    - REGULAR_WEEKEND_SESSION_PATTERN is accepted without failures
    - Only Q and R sessions receive heavy telemetry hydration (no SQ in standard weekend)
    - All three market probes (pole, race_head_to_head, race_outcome) are selected from Q/R
    - The validation report is written to the correct path
    """
    session, context = build_context(tmp_path)
    fixture = seed_australia_standard_weekend_fixture(session)
    heavy_calls: dict[int, bool] = {}

    def fake_sync_f1_calendar(_ctx: PipelineContext, season: int) -> dict[str, Any]:
        assert season == 2026
        return {"status": "completed", "season": season}

    def fake_hydrate_f1_session(
        _ctx: PipelineContext,
        *,
        session_key: int,
        include_extended: bool,
        include_heavy: bool,
    ) -> dict[str, Any]:
        assert include_extended is True
        heavy_calls[session_key] = include_heavy
        return {"status": "completed", "session_key": session_key, "records_written": 3}

    def fake_discover_session_polymarket(
        _ctx: PipelineContext,
        *,
        session_key: int,
        batch_size: int = 100,
        max_pages: int = 5,
        search_fallback: bool = True,
    ) -> dict[str, Any]:
        return {"status": "completed", "session_key": session_key}

    def fake_reconcile_mappings(_ctx: PipelineContext) -> dict[str, Any]:
        return {"status": "completed", "candidate_rows": 0, "mapping_rows": 0}

    def fake_run_data_quality_checks(_ctx: PipelineContext) -> dict[str, Any]:
        return {"job_run_id": "dq-aus-1", "status": "completed"}

    def fake_hydrate_polymarket_market(
        _ctx: PipelineContext,
        *,
        market_id: str,
        fidelity: int = 60,
    ) -> dict[str, Any]:
        market = session.get(PolymarketMarket, market_id)
        assert market is not None, f"probe market {market_id} not found in fixture"
        observed_at = utc_now()
        session.add(
            PolymarketPriceHistory(
                id=f"aus-price-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                observed_at_utc=observed_at,
                price=0.55,
                midpoint=0.56,
                best_bid=0.54,
                best_ask=0.57,
                raw_payload={"market_id": market_id},
            )
        )
        session.add(
            PolymarketTrade(
                id=f"aus-trade-{market_id}",
                market_id=market_id,
                token_id=f"token-{market_id}-yes",
                condition_id=f"condition-{market_id}",
                trade_timestamp_utc=observed_at,
                side="buy",
                price=0.55,
                size=10.0,
                raw_payload={"market_id": market_id},
            )
        )
        session.commit()
        return {"status": "completed", "market_id": market_id, "fidelity": fidelity}

    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.sync_f1_calendar",
        fake_sync_f1_calendar,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.ensure_default_feature_registry",
        lambda _ctx: None,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_f1_session",
        fake_hydrate_f1_session,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.discover_session_polymarket",
        fake_discover_session_polymarket,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.reconcile_mappings",
        fake_reconcile_mappings,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.run_data_quality_checks",
        fake_run_data_quality_checks,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_polymarket_market",
        fake_hydrate_polymarket_market,
    )

    try:
        result = validate_f1_weekend_subset(context, meeting_key=fixture["meeting"].meeting_key)
        session.commit()

        # Top-level result
        assert result["status"] == "completed"
        assert result["market_probes"] == 3
        assert result["sessions"] == 5

        # Heavy hydration only applies to Q and R for standard weekend (no SQ)
        assert heavy_calls == {
            11227: False,  # FP1 — not heavy
            11228: False,  # FP2 — not heavy
            11229: False,  # FP3 — not heavy
            11230: True,   # Q  — heavy
            11234: True,   # R  — heavy
        }

        # Report files written
        report_dir = tmp_path / "reports" / "validation" / "2026" / "2026-australian-grand-prix"
        report_json = report_dir / "summary.json"
        report_md = report_dir / "summary.md"
        assert report_json.exists()
        assert report_md.exists()

        report = json.loads(report_json.read_text(encoding="utf-8"))
        assert report["overall_status"] == "completed"
        assert report["validation_mode"] == "smoke"

        # Standard weekend has Q and R as heavy sessions (no SQ)
        assert report["heavy_session_codes"] == ["Q", "R"]
        assert set(report["session_pattern"]) == {"FP1", "FP2", "FP3", "Q", "R"}

        # Research readiness
        assert report["research_readiness"]["f1_subset_data"] == "ready"
        assert report["research_readiness"]["session_market_mapping"] == "ready"
        assert report["research_readiness"]["market_history_probe"] == "ready"
        assert report["research_readiness"]["analysis_joinability"] == "ready"

        # Q session should have exactly 1 mapping (pole position)
        assert report["mapping_summary"]["11230"]["mapping_count"] == 1

        # All three probes must be present: pole (Q), race H2H (R), race outcome (R)
        assert {probe["probe_key"] for probe in report["market_probes"]} == {
            "pole",
            "race_head_to_head",
            "race_outcome",
        }
        # Pole probe must come from Q session (no SQ in standard weekend)
        pole_probe = next(p for p in report["market_probes"] if p["probe_key"] == "pole")
        assert pole_probe["session_code"] == "Q"

        assert report["failures"] == []
        assert report["warnings"] == []
    finally:
        session.close()


def test_sync_polymarket_f1_catalog_filters_to_f1_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    f1_event = make_f1_event("event-1", "market-1", "Will Oscar Piastri win Qualifying?")
    search_event = make_f1_event("event-2", "market-2", "Will another driver win the Grand Prix?")
    non_f1_event = {
        "id": "event-3",
        "slug": "bitcoin-event",
        "title": "Bitcoin Event",
        "description": "Not F1",
        "ticker": "btc",
        "active": True,
        "closed": False,
        "archived": False,
        "startDate": "2025-01-01T00:00:00Z",
        "endDate": "2025-01-02T00:00:00Z",
        "tags": [{"label": "Crypto", "slug": "crypto"}],
        "markets": [],
    }

    def fake_iterate_events(
        self: object,
        *,
        batch_size: int,
        max_pages: int | None,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        tag_id: int | str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> object:
        assert batch_size == 10
        yield 0, [f1_event, non_f1_event]

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        return {"events": [search_event if query == "formula 1" else f1_event]}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = sync_polymarket_f1_catalog(
            context,
            batch_size=10,
            max_pages=1,
            search_fallback=True,
        )
        session.commit()

        assert result["status"] == "completed"
        assert result["events"] == 2
        assert (
            session.scalar(select(PolymarketEvent).where(PolymarketEvent.id == "event-1"))
            is not None
        )
        assert (
            session.scalar(select(PolymarketEvent).where(PolymarketEvent.id == "event-2"))
            is not None
        )
        assert (
            session.scalar(select(PolymarketEvent).where(PolymarketEvent.id == "event-3"))
            is None
        )
        market = session.get(PolymarketMarket, "market-1")
        assert market is not None
        assert market.taxonomy == "qualifying_winner"
        label = session.scalar(
            select(MarketTaxonomyLabel).where(MarketTaxonomyLabel.market_id == "market-1")
        )
        assert label is not None
        assert len(label.id) == 36
    finally:
        session.close()


def test_sync_polymarket_f1_catalog_search_canonicalizes_historical_slug(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    search_event = {
        "id": "search-2022",
        "slug": "F1-frenchgp2022",
        "title": "Formula 1 - 2022 French Grand Prix",
        "description": "Historical F1 market",
        "ticker": "f1-france-2022",
        "startDate": "2022-07-22T00:00:00Z",
        "endDate": "2022-07-24T00:00:00Z",
        "tags": [],
    }
    canonical_event = make_f1_event(
        "5781",
        "market-2022",
        "Will Max Verstappen win the 2022 French Grand Prix?",
    )
    canonical_event["slug"] = "F1-frenchgp2022"
    canonical_event["title"] = "Formula 1 - 2022 French Grand Prix"
    canonical_event["startDate"] = "2022-07-22T00:00:00Z"
    canonical_event["endDate"] = "2022-07-24T00:00:00Z"

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        if query == "formula 1 2022":
            return {"events": [search_event]}
        return {"events": []}

    def fake_list_events(
        self: object,
        *,
        limit: int = 100,
        offset: int = 0,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        tag_id: int | str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
        slug: str | None = None,
    ) -> list[dict[str, object]]:
        assert slug == "F1-frenchgp2022"
        return [canonical_event]

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )

    try:
        result = sync_polymarket_f1_catalog(
            context,
            batch_size=10,
            max_pages=1,
            search_fallback=True,
            start_year=2022,
            end_year=2022,
        )
        session.commit()

        assert result["status"] == "completed"
        assert result["events"] == 1
        event = session.get(PolymarketEvent, "5781")
        assert event is not None
        assert event.slug == "F1-frenchgp2022"
        market = session.get(PolymarketMarket, "market-2022")
        assert market is not None
        assert market.taxonomy == "race_winner"
    finally:
        session.close()


def test_discover_session_polymarket_uses_exact_slug_and_auto_maps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:jp",
        meeting_key=1200,
        season=2026,
        meeting_name="Japanese Grand Prix",
        country_name="Japan",
        location="Suzuka",
    )
    session_row = F1Session(
        id="session:fp2",
        session_key=2202,
        meeting_id=meeting.id,
        session_name="Practice 2",
        session_code="FP2",
        date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
        is_practice=True,
        raw_payload={"gmt_offset": "+09:00:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    exact_event = make_session_market_event(
        event_id="event-fp2",
        market_id="market-fp2",
        slug="f1-japanese-grand-prix-practice-2-fastest-lap-2026-03-27",
        title="Japanese Grand Prix: Practice 2 Fastest Lap",
        question=(
            "Will Oscar Piastri set the fastest lap in Practice 2 "
            "at the 2026 Japanese Grand Prix?"
        ),
        description=(
            "This market will resolve according to the fastest lap in Practice 2 at the 2026 "
            "Japanese Grand Prix, scheduled for Mar 27, 2026."
        ),
    )

    def fake_list_events(
        self: object,
        *,
        limit: int = 100,
        offset: int = 0,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        tag_id: int | str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
        slug: str | None = None,
    ) -> list[dict[str, object]]:
        if slug == "f1-japanese-grand-prix-practice-2-fastest-lap-2026-03-27":
            return [exact_event]
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2202)
        session.commit()

        assert result["status"] == "completed"
        assert result["events"] == 1
        assert result["mapping_candidates"] == 1
        assert result["auto_mappings"] == 1
        candidate = session.get(MappingCandidate, "market-fp2:session:fp2")
        assert candidate is not None
        assert candidate.matched_by == "session_discovery_exact_slug"
        mapping = session.get(EntityMappingF1ToPolymarket, "market-fp2:session:fp2")
        assert mapping is not None
        assert mapping.matched_by == "session_discovery_exact_slug"
        label = session.scalar(
            select(MarketTaxonomyLabel).where(MarketTaxonomyLabel.market_id == "market-fp2")
        )
        assert label is not None
        assert len(label.id) == 36
    finally:
        session.close()


def test_backfill_f1_history_skips_future_sessions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:future-check",
        meeting_key=3200,
        season=2026,
        meeting_name="Future Check Grand Prix",
    )
    completed_session = F1Session(
        id="session:completed",
        session_key=5001,
        meeting_id=meeting.id,
        session_name="Practice 1",
        session_code="FP1",
        date_start_utc=datetime(2026, 3, 1, 1, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 1, 2, 0, tzinfo=timezone.utc),
        is_practice=True,
    )
    future_session = F1Session(
        id="session:future",
        session_key=5002,
        meeting_id=meeting.id,
        session_name="Race",
        session_code="R",
        date_start_utc=datetime(2026, 12, 1, 1, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 12, 1, 3, 0, tzinfo=timezone.utc),
        is_practice=False,
    )
    session.add_all([meeting, completed_session, future_session])
    session.commit()

    hydrated: list[int] = []

    def fake_sync_f1_calendar(ctx: PipelineContext, *, season: int) -> dict[str, object]:
        assert season == 2026
        return {"status": "completed"}

    def fake_hydrate_f1_session(
        ctx: PipelineContext,
        *,
        session_key: int,
        include_extended: bool = False,
        include_heavy: bool = False,
    ) -> dict[str, object]:
        hydrated.append(session_key)
        return {
            "status": "completed",
            "session_key": session_key,
            "include_extended": include_extended,
            "include_heavy": include_heavy,
        }

    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.sync_f1_calendar",
        fake_sync_f1_calendar,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.orchestration.hydrate_f1_session",
        fake_hydrate_f1_session,
    )

    try:
        result = backfill_f1_history(
            context,
            season_start=2026,
            season_end=2026,
            include_extended=True,
            heavy_mode="none",
        )
        session.commit()

        assert result["status"] == "completed"
        assert hydrated == [5001]
        assert result["sessions_hydrated"] == 1
        assert result["sessions_skipped"] == 1
    finally:
        session.close()


def test_discover_session_polymarket_search_fallback_preserves_mapping_over_reconcile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:q",
        meeting_key=1201,
        season=2026,
        meeting_name="Japanese Grand Prix",
        country_name="Japan",
        location="Suzuka",
    )
    session_row = F1Session(
        id="session:q",
        session_key=2203,
        meeting_id=meeting.id,
        session_name="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
        is_practice=False,
        raw_payload={"gmt_offset": "+09:00:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    search_event = make_session_market_event(
        event_id="event-q",
        market_id="market-q",
        slug="f1-japanese-grand-prix-driver-pole-position-2026-03-28",
        title="Japanese Grand Prix: Driver Pole Position",
        question="Will Max Verstappen get pole position at the 2026 Japanese Grand Prix?",
        description=(
            "This market resolves to the driver on pole position at the 2026 Japanese Grand Prix, "
            "scheduled for Mar 28, 2026."
        ),
    )

    def fake_list_events(self: object, **_: object) -> list[dict[str, object]]:
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        if "Pole Position" in query or "Qualifying" in query:
            return {"events": [search_event]}
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2203)
        session.commit()

        assert result["auto_mappings"] == 1
        mapping_before = session.get(EntityMappingF1ToPolymarket, "market-q:session:q")
        assert mapping_before is not None
        assert mapping_before.matched_by == "session_discovery_public_search"

        reconcile_mappings(context)
        session.commit()

        mapping_after = session.get(EntityMappingF1ToPolymarket, "market-q:session:q")
        assert mapping_after is not None
        assert mapping_after.matched_by == "session_discovery_public_search"
    finally:
        session.close()


def test_backfill_f1_history_enables_heavy_for_weekend_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    calls: list[tuple[int, bool, bool]] = []

    def fake_sync(ctx: PipelineContext, *, season: int) -> dict[str, object]:
        meeting = F1Meeting(
            id=f"meeting:{season}",
            meeting_key=season,
            season=season,
            meeting_name=f"Season {season}",
        )
        practice = F1Session(
            id=f"session:{season}:practice",
            session_key=season * 10 + 1,
            meeting_id=meeting.id,
            session_name="Practice 1",
            session_code="FP1",
            date_start_utc=utc_now(),
            date_end_utc=datetime(2025, 3, 1, 2, 0, tzinfo=timezone.utc),
            is_practice=True,
            raw_payload={"meeting_key": season},
        )
        race = F1Session(
            id=f"session:{season}:race",
            session_key=season * 10 + 2,
            meeting_id=meeting.id,
            session_name="Race",
            session_code="R",
            date_start_utc=utc_now(),
            date_end_utc=datetime(2025, 3, 1, 4, 0, tzinfo=timezone.utc),
            is_practice=False,
            raw_payload={"meeting_key": season},
        )
        testing = F1Session(
            id=f"session:{season}:testing",
            session_key=season * 10 + 3,
            meeting_id=meeting.id,
            session_name="Day 1",
            session_code=None,
            date_start_utc=utc_now(),
            date_end_utc=datetime(2025, 2, 27, 4, 0, tzinfo=timezone.utc),
            is_practice=False,
            raw_payload={"meeting_key": season},
        )
        session.merge(meeting)
        session.merge(practice)
        session.merge(race)
        session.merge(testing)
        session.flush()
        return {"status": "completed"}

    def fake_hydrate(
        ctx: PipelineContext,
        *,
        session_key: int,
        include_extended: bool,
        include_heavy: bool,
    ) -> dict[str, object]:
        calls.append((session_key, include_extended, include_heavy))
        return {"status": "completed", "records_written": 1}

    monkeypatch.setattr("f1_polymarket_worker.orchestration.sync_f1_calendar", fake_sync)
    monkeypatch.setattr("f1_polymarket_worker.orchestration.hydrate_f1_session", fake_hydrate)

    try:
        result = backfill_f1_history(
            context,
            season_start=2025,
            season_end=2025,
            include_extended=True,
            heavy_mode="weekend",
        )
        session.commit()

        assert result["status"] == "completed"
        assert calls == [
            (20251, True, True),
            (20252, True, True),
        ]
    finally:
        session.close()


def test_discover_session_polymarket_search_fallback_maps_race_head_to_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:r",
        meeting_key=1301,
        season=2025,
        meeting_name="Monaco Grand Prix",
        country_name="Monaco",
        location="Monte Carlo",
    )
    session_row = F1Session(
        id="session:r",
        session_key=2301,
        meeting_id=meeting.id,
        session_name="Race",
        session_code="R",
        date_start_utc=datetime(2025, 5, 25, 13, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2025, 5, 25, 15, 0, tzinfo=timezone.utc),
        is_practice=False,
        raw_payload={"gmt_offset": "+02:00:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    search_event = make_session_market_event(
        event_id="event-r-h2h",
        market_id="market-r-h2h",
        slug="f1-monaco-grand-prix-head-to-head-matchups",
        title="F1 Monaco Grand Prix: Head to Head Matchups",
        question="Hamilton vs. Leclerc",
        description=(
            "This market is based on whether Lewis Hamilton or Charles Leclerc finishes ahead "
            "of the other at the F1 Monaco Grand Prix, scheduled for May 25, 2025."
        ),
    )

    def fake_list_events(self: object, **_: object) -> list[dict[str, object]]:
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        if "Head-to-Head" in query or "Finish Ahead" in query:
            return {"events": [search_event]}
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2301)
        session.commit()

        assert result["status"] == "completed"
        assert result["auto_mappings"] == 1
        market = session.get(PolymarketMarket, "market-r-h2h")
        assert market is not None
        assert market.taxonomy == "head_to_head_session"
        mapping = session.get(EntityMappingF1ToPolymarket, "market-r-h2h:session:r")
        assert mapping is not None
        assert mapping.matched_by == "session_discovery_public_search"
    finally:
        session.close()


def test_discover_session_polymarket_ignores_prior_year_sprint_market(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:sprint",
        meeting_key=1304,
        season=2026,
        meeting_name="Chinese Grand Prix",
        country_name="China",
        location="Shanghai",
    )
    session_row = F1Session(
        id="session:sprint",
        session_key=2304,
        meeting_id=meeting.id,
        session_name="Sprint",
        session_code="S",
        date_start_utc=datetime(2026, 3, 14, 3, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 14, 4, 0, tzinfo=timezone.utc),
        raw_payload={"gmt_offset": "+08:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    stale_event = make_session_market_event(
        event_id="event-s-2025",
        market_id="market-s-2025",
        slug="will-lando-norris-win-the-2025-chinese-grand-prix-sprint",
        title="China Grand Prix - Sprint Winner",
        question="Will Lando Norris win the 2025 Chinese Grand Prix Sprint?",
        description="Historical 2025 sprint winner market.",
    )

    def fake_list_events(self: object, **_: object) -> list[dict[str, object]]:
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        if "Sprint Winner" in query or "Chinese Grand Prix Sprint" in query:
            return {"events": [stale_event]}
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2304)
        session.commit()

        assert result["status"] == "completed"
        assert result["events"] == 0
        assert result["mapping_candidates"] == 0
        assert result["auto_mappings"] == 0
        assert session.get(PolymarketMarket, "market-s-2025") is None
    finally:
        session.close()


def test_discover_session_polymarket_skips_failing_public_search_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:r2",
        meeting_key=1302,
        season=2025,
        meeting_name="Monaco Grand Prix",
        country_name="Monaco",
        location="Monte Carlo",
    )
    session_row = F1Session(
        id="session:r2",
        session_key=2302,
        meeting_id=meeting.id,
        session_name="Race",
        session_code="R",
        date_start_utc=datetime(2025, 5, 25, 13, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2025, 5, 25, 15, 0, tzinfo=timezone.utc),
        is_practice=False,
        raw_payload={"gmt_offset": "+02:00:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    search_event = make_session_market_event(
        event_id="event-r-h2h-2",
        market_id="market-r-h2h-2",
        slug="f1-monaco-grand-prix-head-to-head-matchups",
        title="F1 Monaco Grand Prix: Head to Head Matchups",
        question="Hamilton vs. Leclerc",
        description=(
            "This market is based on whether Lewis Hamilton or Charles Leclerc finishes ahead "
            "of the other at the F1 Monaco Grand Prix, scheduled for May 25, 2025."
        ),
    )

    def fake_list_events(self: object, **_: object) -> list[dict[str, object]]:
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        if False:
            yield 0, []
        return

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        if "Head-to-Head" in query:
            raise RuntimeError("403")
        if "Finish Ahead" in query:
            return {"events": [search_event]}
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2302)
        session.commit()

        assert result["status"] == "completed"
        assert result["auto_mappings"] == 1
        market = session.get(PolymarketMarket, "market-r-h2h-2")
        assert market is not None
        assert market.taxonomy == "head_to_head_session"
    finally:
        session.close()


def test_discover_session_polymarket_ignores_race_fastest_lap_for_qualifying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(
        id="meeting:q2",
        meeting_key=1303,
        season=2026,
        meeting_name="Japanese Grand Prix",
        country_name="Japan",
        location="Suzuka",
    )
    session_row = F1Session(
        id="session:q2",
        session_key=2303,
        meeting_id=meeting.id,
        session_name="Qualifying",
        session_code="Q",
        date_start_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
        date_end_utc=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
        is_practice=False,
        raw_payload={"gmt_offset": "+09:00:00"},
    )
    session.add_all([meeting, session_row])
    session.commit()

    race_fastest_lap_event = make_session_market_event(
        event_id="event-r-fastest-lap",
        market_id="market-r-fastest-lap",
        slug="f1-japanese-grand-prix-driver-fastest-lap-2026-03-29",
        title="Japanese Grand Prix: Driver Fastest Lap",
        question="Will Pierre Gasly achieve the fastest lap at the 2026 F1 Japanese Grand Prix?",
        description=(
            "This market will resolve in favor of the driver officially credited with the fastest "
            "lap in the final classification following the race. Times from practice sessions, "
            "qualifying, or any other sessions are not considered."
        ),
    )

    def fake_list_events(self: object, **kwargs: object) -> list[dict[str, object]]:
        slug = kwargs.get("slug")
        if slug == "f1-japanese-grand-prix-driver-pole-position-2026-03-28":
            return []
        if slug == "f1-japanese-grand-prix-constructor-pole-position-2026-03-28":
            return []
        if slug == "f1-japanese-grand-prix-pole-winner-2026-03-28":
            return []
        if slug == "f1-japanese-grand-prix-qualifying-2026-03-28":
            return []
        return []

    def fake_iterate_events(self: object, **_: object) -> object:
        yield 0, [race_fastest_lap_event]

    def fake_public_search(self: object, query: str) -> dict[str, object]:
        return {"events": []}

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.iterate_events",
        fake_iterate_events,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket.PolymarketConnector.public_search",
        fake_public_search,
    )

    try:
        result = discover_session_polymarket(context, session_key=2303)
        session.commit()

        assert result["status"] == "completed"
        assert result["events"] == 0
        assert result["markets"] == 0
        assert result["mapping_candidates"] == 0
        assert session.get(PolymarketMarket, "market-r-fastest-lap") is None
    finally:
        session.close()


def test_capture_live_weekend_persists_ws_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, context = build_context(tmp_path)
    meeting = F1Meeting(id="meeting:1", meeting_key=1, season=2025, meeting_name="Qatar GP")
    session_row = F1Session(
        id="session:1001",
        session_key=1001,
        meeting_id=meeting.id,
        session_name="Practice 1",
        session_code="FP1",
        date_start_utc=utc_now(),
        date_end_utc=utc_now(),
        is_practice=True,
        raw_payload={"meeting_key": 1},
    )
    market = PolymarketMarket(
        id="market-1",
        event_id="event-1",
        question="Will Oscar Piastri win Practice 1?",
        condition_id="condition-1",
        taxonomy="driver_fastest_lap_practice",
        taxonomy_confidence=0.9,
        target_session_code="FP1",
        start_at_utc=utc_now(),
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        raw_payload={"id": "market-1"},
    )
    event = PolymarketEvent(
        id="event-1",
        slug="f1-event",
        title="Las Vegas Grand Prix: Practice 1 Fastest Lap",
        active=True,
        closed=False,
        archived=False,
        raw_payload={
            "title": "Las Vegas Grand Prix: Practice 1 Fastest Lap",
            "tags": [{"label": "Formula 1"}],
        },
    )
    token = PolymarketToken(id="token-1", market_id="market-1")
    session.add_all([meeting, session_row, event, market, token])
    session.commit()

    def fake_openf1_stream(
        self: object,
        *,
        topics: tuple[str, ...],
        on_message: Any,
        stop_after_seconds: float,
        message_limit: int | None = None,
    ) -> int:
        on_message(
            SimpleNamespace(
                topic="v1/laps",
                payload={"session_key": 1001, "lap_number": 1},
                observed_at=utc_now(),
            )
        )
        return 1

    def fake_polymarket_stream(
        self: object,
        *,
        asset_ids: list[str],
        on_message: Any,
        stop_after_seconds: float,
        message_limit: int | None = None,
    ) -> int:
        assert asset_ids == ["token-1"]
        on_message(
            SimpleNamespace(
                payload={"event_type": "book", "asset_id": "token-1", "market": "condition-1"},
                observed_at=utc_now(),
            )
        )
        return 1

    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.openf1_live.OpenF1LiveConnector.stream",
        fake_openf1_stream,
    )
    monkeypatch.setattr(
        "f1_polymarket_lab.connectors.polymarket_live.PolymarketLiveConnector.stream_market_messages",
        fake_polymarket_stream,
    )

    try:
        result = capture_live_weekend(
            context,
            session_key=1001,
            market_ids=["market-1"],
            start_buffer_min=0,
            stop_buffer_min=0,
        )
        session.commit()

        assert result["status"] == "completed"
        assert result["openf1_messages"] == 1
        ws_manifest = session.scalars(select(PolymarketWsMessageManifest)).all()
        assert len(ws_manifest) == 1
        assert ws_manifest[0].market_id == "market-1"
        assert ws_manifest[0].token_id == "token-1"
    finally:
        session.close()
