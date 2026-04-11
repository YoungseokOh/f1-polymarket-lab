from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_api.api.v1 import action_routes
from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1CalendarOverride,
    F1Meeting,
    F1Session,
    LiveTradeExecution,
    LiveTradeTicket,
    ModelRun,
    ModelRunPromotion,
    PaperTradeSession,
    PolymarketEvent,
    PolymarketMarket,
)
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def build_test_client(tmp_path: Path) -> TestClient:
    database_path = tmp_path / "api-test.sqlite"
    engine = create_engine(f"sqlite+pysqlite:///{database_path}", future=True)
    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine, expire_on_commit=False)

    with Session(engine) as session:
        session.add_all(
            [
                F1Meeting(
                    id="meeting-2026",
                    meeting_key=2026,
                    season=2026,
                    meeting_name="Australian Grand Prix",
                ),
                F1Meeting(
                    id="meeting-2025",
                    meeting_key=2025,
                    season=2025,
                    meeting_name="Bahrain Grand Prix",
                ),
                F1Meeting(
                    id="meeting:1281",
                    meeting_key=1281,
                    season=2026,
                    round_number=3,
                    meeting_name="Japanese Grand Prix",
                    meeting_slug="japanese-grand-prix",
                    event_format="conventional",
                    circuit_short_name="Suzuka",
                    country_name="Japan",
                    location="Suzuka",
                    start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
                    end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
                ),
                F1Meeting(
                    id="meeting:1282",
                    meeting_key=1282,
                    season=2026,
                    round_number=2,
                    meeting_name="Bahrain Grand Prix",
                    meeting_slug="bahrain-grand-prix",
                    event_format="conventional",
                    circuit_short_name="Sakhir",
                    country_name="Bahrain",
                    location="Sakhir",
                    start_date_utc=datetime(2026, 4, 10, 10, 30, tzinfo=timezone.utc),
                    end_date_utc=datetime(2026, 4, 12, 16, 0, tzinfo=timezone.utc),
                ),
                F1Meeting(
                    id="meeting:1283",
                    meeting_key=1283,
                    season=2026,
                    round_number=3,
                    meeting_name="Saudi Arabian Grand Prix",
                    meeting_slug="saudi-arabian-grand-prix",
                    event_format="conventional",
                    circuit_short_name="Jeddah",
                    country_name="Saudi Arabia",
                    location="Jeddah",
                    start_date_utc=datetime(2026, 4, 17, 16, 30, tzinfo=timezone.utc),
                    end_date_utc=datetime(2026, 4, 19, 18, 0, tzinfo=timezone.utc),
                ),
                F1Meeting(
                    id="meeting:1284",
                    meeting_key=1284,
                    season=2026,
                    round_number=4,
                    meeting_name="Miami Grand Prix",
                    meeting_slug="miami-grand-prix",
                    event_format="sprint",
                    circuit_short_name="Miami",
                    country_name="United States",
                    location="Miami",
                    start_date_utc=datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc),
                    end_date_utc=datetime(2026, 5, 3, 21, 0, tzinfo=timezone.utc),
                ),
                F1Session(
                    id="session-q",
                    session_key=2001,
                    meeting_id="meeting-2026",
                    session_name="Qualifying",
                    session_code="Q",
                    is_practice=False,
                ),
                F1Session(
                    id="session-fp1",
                    session_key=2002,
                    meeting_id="meeting-2026",
                    session_name="Practice 1",
                    session_code="FP1",
                    is_practice=True,
                ),
                F1Session(
                    id="session-old",
                    session_key=1001,
                    meeting_id="meeting-2025",
                    session_name="Race",
                    session_code="R",
                    is_practice=False,
                ),
                F1Session(
                    id="session:11246",
                    session_key=11246,
                    meeting_id="meeting:1281",
                    session_name="Practice 1",
                    session_code="FP1",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 3, 27, 3, 30, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:11247",
                    session_key=11247,
                    meeting_id="meeting:1281",
                    session_name="Practice 2",
                    session_code="FP2",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 3, 27, 7, 0, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:11248",
                    session_key=11248,
                    meeting_id="meeting:1281",
                    session_name="Practice 3",
                    session_code="FP3",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 3, 28, 2, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 3, 28, 3, 30, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:11249",
                    session_key=11249,
                    meeting_id="meeting:1281",
                    session_name="Qualifying",
                    session_code="Q",
                    session_type="Qualifying",
                    date_start_utc=datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12820-fp1",
                    session_key=128201,
                    meeting_id="meeting:1282",
                    session_name="Practice 1",
                    session_code="FP1",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 4, 10, 10, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 10, 11, 30, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:12820-q",
                    session_key=128204,
                    meeting_id="meeting:1282",
                    session_name="Qualifying",
                    session_code="Q",
                    session_type="Qualifying",
                    date_start_utc=datetime(2026, 4, 11, 15, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 11, 16, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12820-r",
                    session_key=128205,
                    meeting_id="meeting:1282",
                    session_name="Race",
                    session_code="R",
                    session_type="Race",
                    date_start_utc=datetime(2026, 4, 12, 15, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 12, 17, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12830-fp1",
                    session_key=128301,
                    meeting_id="meeting:1283",
                    session_name="Practice 1",
                    session_code="FP1",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 4, 17, 16, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 17, 17, 30, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:12830-q",
                    session_key=128304,
                    meeting_id="meeting:1283",
                    session_name="Qualifying",
                    session_code="Q",
                    session_type="Qualifying",
                    date_start_utc=datetime(2026, 4, 18, 18, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 18, 19, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12830-r",
                    session_key=128305,
                    meeting_id="meeting:1283",
                    session_name="Race",
                    session_code="R",
                    session_type="Race",
                    date_start_utc=datetime(2026, 4, 19, 17, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 4, 19, 19, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12840-fp1",
                    session_key=128401,
                    meeting_id="meeting:1284",
                    session_name="Practice 1",
                    session_code="FP1",
                    session_type="Practice",
                    date_start_utc=datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 5, 1, 17, 30, tzinfo=timezone.utc),
                    is_practice=True,
                ),
                F1Session(
                    id="session:12840-sq",
                    session_key=128402,
                    meeting_id="meeting:1284",
                    session_name="Sprint Qualifying",
                    session_code="SQ",
                    session_type="Sprint Qualifying",
                    date_start_utc=datetime(2026, 5, 1, 20, 30, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 5, 1, 21, 15, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12840-s",
                    session_key=128403,
                    meeting_id="meeting:1284",
                    session_name="Sprint",
                    session_code="S",
                    session_type="Sprint",
                    date_start_utc=datetime(2026, 5, 2, 16, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 5, 2, 17, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12840-q",
                    session_key=128404,
                    meeting_id="meeting:1284",
                    session_name="Qualifying",
                    session_code="Q",
                    session_type="Qualifying",
                    date_start_utc=datetime(2026, 5, 2, 20, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 5, 2, 21, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1Session(
                    id="session:12840-r",
                    session_key=128405,
                    meeting_id="meeting:1284",
                    session_name="Race",
                    session_code="R",
                    session_type="Race",
                    date_start_utc=datetime(2026, 5, 3, 19, 0, tzinfo=timezone.utc),
                    date_end_utc=datetime(2026, 5, 3, 21, 0, tzinfo=timezone.utc),
                    is_practice=False,
                ),
                F1CalendarOverride(
                    id="override-bahrain-2026",
                    season=2026,
                    meeting_slug="bahrain-grand-prix",
                    ops_slug="bahrain",
                    status="cancelled",
                    source_label="Formula 1 official",
                    source_url=(
                        "https://www.formula1.com/en/latest/article/"
                        "bahrain-and-saudi-arabian-grands-prix-will-not-take-place-in-april.1hnqllVG85RSt8pbFc5Ivx/"
                    ),
                    note="Cancelled after official schedule update.",
                    is_active=True,
                ),
                F1CalendarOverride(
                    id="override-saudi-2026",
                    season=2026,
                    meeting_slug="saudi-arabian-grand-prix",
                    ops_slug="saudi",
                    status="cancelled",
                    source_label="Formula 1 official",
                    source_url=(
                        "https://www.formula1.com/en/latest/article/"
                        "bahrain-and-saudi-arabian-grands-prix-will-not-take-place-in-april.1hnqllVG85RSt8pbFc5Ivx/"
                    ),
                    note="Cancelled after official schedule update.",
                    is_active=True,
                ),
                PolymarketEvent(
                    id="event-1",
                    slug="event-1",
                    title="Event 1",
                    active=True,
                    closed=False,
                ),
                PolymarketMarket(
                    id="market-good",
                    event_id="event-1",
                    question="Who gets pole?",
                    slug="pole",
                    taxonomy="driver_pole_position",
                    condition_id="condition-good",
                    active=True,
                    closed=False,
                    end_at_utc=datetime(2026, 3, 14, tzinfo=timezone.utc),
                ),
                PolymarketMarket(
                    id="market-unknown",
                    event_id="event-1",
                    question="Unknown market",
                    slug="unknown",
                    taxonomy="legacy_taxonomy",
                    condition_id="condition-unknown",
                    active=True,
                    closed=False,
                    end_at_utc=datetime(2026, 3, 13, tzinfo=timezone.utc),
                ),
                EntityMappingF1ToPolymarket(
                    id="mapping-high",
                    f1_session_id="session-q",
                    polymarket_market_id="market-good",
                    mapping_type="driver_pole_position",
                    confidence=0.91,
                ),
                EntityMappingF1ToPolymarket(
                    id="mapping-low",
                    f1_session_id="session-fp1",
                    polymarket_market_id="market-unknown",
                    mapping_type="other",
                    confidence=0.4,
                ),
                ModelRun(
                    id="model-run-1",
                    stage="multitask_qr",
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    dataset_version="multitask_v1",
                    config_json={"seed": 7},
                    metrics_json={"total_pnl": 11.2, "roi_pct": 14.0, "bet_count": 24},
                    artifact_uri=str(tmp_path / "artifacts" / "model-run-1"),
                    registry_run_id="mlflow-run-1",
                ),
                ModelRunPromotion(
                    id="promotion-1",
                    model_run_id="model-run-1",
                    stage="multitask_qr",
                    status="active",
                    gate_metrics_json={
                        "total_pnl": 11.2,
                        "roi_pct": 14.0,
                        "bet_count": 24,
                        "ece": 0.05,
                        "family_pnl_share_max": 0.5,
                    },
                    promoted_at=datetime(2026, 3, 26, 8, 0, tzinfo=timezone.utc),
                ),
                PaperTradeSession(
                    id="pt-japan-pre",
                    gp_slug="japan_pre",
                    snapshot_id="snapshot-1",
                    model_run_id="model-run-1",
                    status="settled",
                    started_at=datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 3, 26, 9, 5, tzinfo=timezone.utc),
                ),
                LiveTradeTicket(
                    id="live-ticket-1",
                    gp_slug="japan_fp1_fp2",
                    session_code="FP2",
                    market_id="market-good",
                    token_id="token-live-1",
                    snapshot_id="snapshot-live-1",
                    model_run_id="model-run-1",
                    promotion_stage=None,
                    question="Who gets pole?",
                    signal_action="buy_yes",
                    side_label="YES",
                    model_prob=0.61,
                    market_price=0.41,
                    edge=0.20,
                    recommended_size=10.0,
                    observed_spread=0.02,
                    max_spread=0.03,
                    observed_at_utc=datetime(2026, 3, 27, 6, 20, tzinfo=timezone.utc),
                    source_event_type="best_bid_ask",
                    status="filled",
                    rationale_json={"entry_price": 0.41},
                    expires_at=datetime(2026, 3, 27, 6, 40, tzinfo=timezone.utc),
                    created_at=datetime(2026, 3, 27, 6, 20, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 3, 27, 6, 21, tzinfo=timezone.utc),
                ),
                LiveTradeExecution(
                    id="live-execution-1",
                    ticket_id="live-ticket-1",
                    market_id="market-good",
                    side="buy_yes",
                    submitted_size=10.0,
                    actual_fill_size=10.0,
                    actual_fill_price=0.415,
                    submitted_at=datetime(2026, 3, 27, 6, 21, tzinfo=timezone.utc),
                    filled_at=datetime(2026, 3, 27, 6, 21, tzinfo=timezone.utc),
                    operator_note="filled in browser",
                    external_reference="browser-order-1",
                    status="filled",
                    created_at=datetime(2026, 3, 27, 6, 21, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 3, 27, 6, 21, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

    def override_db_session() -> Generator[Session, None, None]:
        session = session_maker()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db_session] = override_db_session
    return TestClient(app)


def test_sessions_endpoint_supports_filters(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/f1/sessions",
            params={
                "season": 2026,
                "meeting_id": "meeting-2026",
                "session_code": "Q",
                "limit": 5,
            },
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload] == ["session-q"]


def test_markets_endpoint_coerces_taxonomy_and_filters(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/polymarket/markets",
            params={"event_id": "event-1", "active": True, "limit": 10},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert {row["taxonomy"] for row in payload} == {
        "driver_pole_position",
        "other",
    }


def test_markets_endpoint_filters_by_market_ids(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/polymarket/markets",
            params={"market_ids": "market-unknown,market-good", "limit": 10},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload] == ["market-good", "market-unknown"]


def test_mappings_endpoint_filters_by_confidence_and_session(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/mappings",
            params={"f1_session_id": "session-q", "min_confidence": 0.8, "limit": 10},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload] == ["mapping-high"]


def test_model_runs_endpoint_includes_registry_and_promotion_fields(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/model-runs")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["id"] == "model-run-1"
    assert payload[0]["registry_run_id"] == "mlflow-run-1"
    assert payload[0]["promotion_status"] == "active"
    assert payload[0]["promoted_at"].startswith("2026-03-26T08:00:00")


def test_live_trading_endpoints_serialize_ticket_and_execution_records(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        tickets_response = client.get(
            "/api/v1/live-trading/tickets",
            params={"gp_slug": "japan_fp1_fp2"},
        )
        executions_response = client.get(
            "/api/v1/live-trading/executions",
            params={"gp_slug": "japan_fp1_fp2"},
        )

    app.dependency_overrides.clear()

    assert tickets_response.status_code == 200
    assert executions_response.status_code == 200
    ticket_payload = tickets_response.json()
    execution_payload = executions_response.json()
    assert ticket_payload[0]["id"] == "live-ticket-1"
    assert ticket_payload[0]["status"] == "filled"
    assert ticket_payload[0]["market_id"] == "market-good"
    assert execution_payload[0]["id"] == "live-execution-1"
    assert execution_payload[0]["ticket_id"] == "live-ticket-1"
    assert execution_payload[0]["actual_fill_price"] == 0.415


def test_weekend_cockpit_status_auto_selects_japan_fp1_q_during_fp1_live(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.utc_now",
        lambda: datetime(2026, 3, 27, 3, 0, tzinfo=timezone.utc),
    )

    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/weekend-cockpit/status")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["auto_selected_gp_short_code"] == "japan_fp1_q"
    assert payload["selected_gp_short_code"] == "japan_fp1_q"
    assert payload["ready_to_run"] is False
    assert payload["model_ready"] is True
    assert payload["required_stage"] == "multitask_qr"
    assert payload["active_model_run_id"] == "model-run-1"
    assert payload["selected_config"]["source_session_code"] == "FP1"
    assert payload["calendar_status"] == "scheduled"
    assert payload["meeting_slug"] == "japanese-grand-prix"


def test_weekend_cockpit_status_skips_cancelled_meetings_and_selects_miami(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.utc_now",
        lambda: datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
    )

    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/weekend-cockpit/status")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["auto_selected_gp_short_code"] == "miami_fp1_sq"
    assert payload["selected_gp_short_code"] == "miami_fp1_sq"
    assert payload["selected_config"]["display_label"] == (
        "Use FP1 results to prepare Sprint Qualifying"
    )
    assert payload["selected_config"]["display_description"] == (
        "Use FP1 results to score Sprint Qualifying pole markets for manual execution."
    )
    assert payload["calendar_status"] == "scheduled"
    assert payload["meeting_slug"] == "miami-grand-prix"
    assert payload["source_conflict"] is False
    assert payload["focus_status"] == "upcoming"
    assert payload["focus_session"]["session_code"] == "FP1"
    assert payload["timeline_completed_codes"] == []
    assert payload["timeline_active_code"] == "FP1"
    assert payload["required_stage"] == "sq_pole_live_v1"
    assert payload["model_ready"] is False
    assert payload["model_blockers"] == [
        "A promoted sq_pole_live_v1 champion is required before paper trading can run."
    ]
    assert [config["short_code"] for config in payload["available_configs"]] == [
        "miami_fp1_sq",
        "miami_sq_sprint",
        "miami_fp1_q",
        "miami_q_r",
    ]
    calendar_slugs = [meeting["meeting_slug"] for meeting in payload["calendar_meetings"]]
    assert "japanese-grand-prix" in calendar_slugs
    assert "miami-grand-prix" in calendar_slugs
    assert [meeting["meeting_slug"] for meeting in payload["cancelled_meetings"]] == [
        "bahrain-grand-prix",
        "saudi-arabian-grand-prix",
    ]


def test_weekend_cockpit_status_blocks_fp1_before_session_end(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.utc_now",
        lambda: datetime(2026, 3, 27, 3, 0, tzinfo=timezone.utc),
    )

    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/weekend-cockpit/status",
            params={"gp_short_code": "japan_fp1_q"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_gp_short_code"] == "japan_fp1_q"
    assert payload["ready_to_run"] is False
    assert payload["blockers"]
    assert payload["model_ready"] is True
    assert payload["required_stage"] == "multitask_qr"
    assert payload["active_model_run_id"] == "model-run-1"
    assert payload["model_blockers"] == []
    assert payload["primary_action_title"] == "Wait for this stage"
    assert payload["primary_action_cta"] == "Not ready yet"
    assert payload["steps"][1]["reason_code"] == "session_in_progress"
    assert payload["steps"][1]["actionable_after_utc"] == "2026-03-27T03:30:00Z"
    assert payload["steps"][1]["resource_label"] == "FP1 results"
    assert "FP1 is still in progress" in payload["blockers"][0]


def test_weekend_cockpit_status_returns_409_for_cancelled_gp_stage(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/weekend-cockpit/status",
            params={"gp_short_code": "bahrain_fp1_q"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "cancelled by calendar override" in response.text


def test_current_weekend_operations_readiness_endpoint_serializes_worker_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.get_current_weekend_operations_readiness",
        lambda *args, **kwargs: {
            "now": "2026-03-27T05:13:00Z",
            "selected_gp_short_code": "japan_fp1_fp2",
            "selected_config": {
                "name": "Japanese Grand Prix",
                "short_code": "japan_fp1_fp2",
                "meeting_key": 1281,
                "season": 2026,
                "target_session_code": "FP2",
                "variant": "fp1_to_fp2",
                "source_session_code": "FP1",
                "market_taxonomy": "driver_fastest_lap_practice",
                "stage_rank": 1,
                "stage_label": "FP1 -> FP2",
                "display_label": "Use FP1 results to prepare FP2",
                "display_description": (
                    "Use FP1 results to find FP2 markets and prepare paper trading."
                ),
            },
            "meeting": None,
            "latest_ended_session": None,
            "next_active_session": None,
            "openf1_credentials_configured": True,
            "actions": [
                {
                    "key": "weekend_cockpit",
                    "label": "Weekend cockpit",
                    "status": "ready",
                    "message": "Ready to run.",
                    "blockers": [],
                    "warnings": [],
                    "meeting_key": 1281,
                    "meeting_name": "Japanese Grand Prix",
                    "gp_short_code": "japan_fp1_fp2",
                    "session_code": "FP2",
                    "session_key": 11247,
                    "actionable_after_utc": None,
                    "openf1_credentials_configured": True,
                    "last_job_run": {
                        "id": "job-1",
                        "job_name": "run-weekend-cockpit",
                        "status": "completed",
                        "records_written": 2,
                        "started_at": "2026-03-27T05:00:00Z",
                        "finished_at": "2026-03-27T05:01:00Z",
                        "error_message": None,
                    },
                    "last_report_path": "/tmp/run-weekend-cockpit.json",
                }
            ],
            "blockers": [],
            "warnings": [],
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/operations/current-weekend-readiness",
            params={"gp_short_code": "japan_fp1_fp2", "season": 2026, "meeting_key": 1281},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_gp_short_code"] == "japan_fp1_fp2"
    assert payload["actions"][0]["key"] == "weekend_cockpit"
    assert payload["actions"][0]["last_job_run"]["job_name"] == "run-weekend-cockpit"


def test_current_weekend_operations_readiness_endpoint_supports_season_only_query(
    tmp_path: Path,
) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/operations/current-weekend-readiness",
            params={"season": 2026},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_config"]["season"] == 2026
    assert payload["selected_gp_short_code"]
    assert isinstance(payload["actions"], list)


def test_run_weekend_cockpit_endpoint_serializes_worker_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.run_weekend_cockpit",
        lambda *args, **kwargs: {
            "action": "run-weekend-cockpit",
            "status": "ok",
            "message": "Weekend cockpit complete",
            "gp_short_code": "japan_pre",
            "snapshot_id": "snapshot-1",
            "model_run_id": "model-run-1",
            "pt_session_id": "pt-session-1",
            "job_run_id": "job-weekend-1",
            "report_path": "/tmp/run-weekend-cockpit.json",
            "preflight_summary": {
                "key": "weekend_cockpit",
                "label": "Weekend cockpit",
                "status": "ready",
                "message": "Ready to run.",
                "blockers": [],
                "warnings": [],
                "meeting_key": 1281,
                "meeting_name": "Japanese Grand Prix",
                "gp_short_code": "japan_pre",
                "session_code": "Q",
                "session_key": 11249,
                "actionable_after_utc": None,
                "openf1_credentials_configured": True,
                "last_job_run": None,
                "last_report_path": "/tmp/run-weekend-cockpit.json",
            },
            "warnings": ["Latest paper session already exists."],
            "executed_steps": [
                {
                    "key": "sync_calendar",
                    "label": "Sync calendar",
                    "status": "completed",
                    "detail": "Calendar sync finished.",
                    "session_code": None,
                    "session_key": None,
                    "count": 5,
                }
            ],
            "details": {
                "trades_executed": 4,
                "settlement": {
                    "settled_session_ids": ["pt-prev"],
                    "settled_gp_slugs": ["japan_fp3"],
                    "settled_positions": 2,
                    "manual_positions_settled": 1,
                    "unresolved_positions": 0,
                    "unresolved_session_ids": [],
                    "winner_driver_id": "driver:12",
                },
            },
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post("/api/v1/actions/run-weekend-cockpit", json={})

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["gp_short_code"] == "japan_pre"
    assert payload["job_run_id"] == "job-weekend-1"
    assert payload["report_path"] == "/tmp/run-weekend-cockpit.json"
    assert payload["preflight_summary"]["key"] == "weekend_cockpit"
    assert payload["warnings"] == ["Latest paper session already exists."]
    assert payload["executed_steps"][0]["status"] == "completed"
    assert payload["details"]["settlement"]["settled_positions"] == 2


def test_run_weekend_cockpit_endpoint_returns_409_for_blockers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_run_weekend_cockpit(*args, **kwargs):
        raise ValueError("FP1 ends at 2026-03-27T03:30:00+00:00")

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.run_weekend_cockpit",
        fail_run_weekend_cockpit,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/run-weekend-cockpit",
            json={"gp_short_code": "japan_fp1"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "FP1 ends at 2026-03-27T03:30:00+00:00" in response.text


def test_backfill_backtests_endpoint_serializes_worker_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.backtest.backfill_backtests",
        lambda *args, **kwargs: {
            "status": "completed",
            "gp_short_code": "japan_fp3",
            "processed": [
                {
                    "gp_short_code": "japan_fp3",
                    "snapshot_id": "snapshot-1",
                    "bet_count": 1,
                    "total_pnl": 9.6,
                }
            ],
            "skipped": [],
            "processed_count": 1,
            "skipped_count": 0,
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/backfill-backtests",
            json={"gp_short_code": "japan_fp3"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "backfill-backtests"
    assert payload["details"]["processed_count"] == 1
    assert "Settled 1 snapshot" in payload["message"]


def test_run_backtest_endpoint_accepts_dynamic_ops_stage_code(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_build_snapshot(*args, **kwargs):
        config = kwargs.get("config") or args[1]
        calls["gp_short_code"] = config.short_code
        return {"snapshot_id": "snapshot-1"}

    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.build_snapshot",
        fake_build_snapshot,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.run_baseline",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.backtest.settle_single_gp",
        lambda *args, **kwargs: {
            "backtest": {
                "backtest_run_id": "bt-miami",
                "metrics": {"bet_count": 2, "total_pnl": 3.5, "roi_pct": 0.175},
            }
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/run-backtest",
            json={"gp_short_code": "miami_fp1_q"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "run-backtest"
    assert calls["gp_short_code"] == "miami_fp1_q"
    assert "miami_fp1_q" in payload["message"]


def test_run_backtest_endpoint_returns_409_for_missing_polymarket_mappings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.build_snapshot",
        lambda *args, **kwargs: {"snapshot_id": "snapshot-1"},
    )
    monkeypatch.setattr(
        "f1_polymarket_worker.gp_registry.run_baseline",
        lambda *args, **kwargs: None,
    )

    def fail_settlement(*args, **kwargs):
        raise ValueError("No Polymarket mappings found for SQ session")

    monkeypatch.setattr(
        "f1_polymarket_worker.backtest.settle_single_gp",
        fail_settlement,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/run-backtest",
            json={"gp_short_code": "china"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert response.json()["detail"] == "No Polymarket mappings found for SQ session"


def test_run_paper_trade_endpoint_accepts_dynamic_ops_stage_code(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_run_gp_paper_trade_pipeline(*args, **kwargs):
        config = kwargs.get("config") or args[1]
        calls["gp_short_code"] = config.short_code
        return {"trades_executed": 2, "total_pnl": 1.25}

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.run_gp_paper_trade_pipeline",
        fake_run_gp_paper_trade_pipeline,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/run-paper-trade",
            json={"gp_short_code": "miami_fp1_q"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "run-paper-trade"
    assert calls["gp_short_code"] == "miami_fp1_q"
    assert "Miami Grand Prix" in payload["message"]


def test_ingest_demo_endpoint_starts_background_job(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_start(session_maker, **kwargs):
        calls["session_maker"] = session_maker
        calls.update(kwargs)

    monkeypatch.setattr(
        "f1_polymarket_api.api.v1.action_routes._start_ingest_demo_background",
        fake_start,
    )

    with build_test_client(tmp_path) as client:
        response = client.post("/api/v1/actions/ingest-demo", json={})

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "ingest-demo"
    assert payload["details"]["queued"] is True
    assert calls["season"] == 2026
    assert calls["weekends"] == 1
    assert calls["market_batches"] == 1


def test_ingest_demo_endpoint_returns_409_when_job_is_already_running(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_api.api.v1.action_routes._start_ingest_demo_background",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            action_routes._ActionBusyError(
                "Another write action is already in progress or the database is temporarily busy. "
                "Wait for it to finish, then retry."
            )
        ),
    )

    with build_test_client(tmp_path) as client:
        response = client.post("/api/v1/actions/ingest-demo", json={})

    app.dependency_overrides.clear()

    assert response.status_code == 409


def test_refresh_latest_session_endpoint_serializes_worker_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_refresh(*args, **kwargs):
        calls.update(kwargs)
        return {
            "action": "refresh-latest-session",
            "status": "ok",
            "message": (
                "Updated latest ended session Q for Japanese Grand Prix. "
                "Artifacts refreshed: 1, skipped: 0."
            ),
            "meeting_id": "meeting:1281",
            "meeting_name": "Japanese Grand Prix",
            "refreshed_session": {
                "id": "session:11249",
                "session_key": 11249,
                "session_code": "Q",
                "session_name": "Qualifying",
                "date_end_utc": "2026-03-28T07:00:00Z",
            },
            "f1_records_written": 42,
            "markets_discovered": 3,
            "mappings_written": 1,
            "markets_hydrated": 2,
            "artifacts_refreshed": [
                {
                    "gp_short_code": "japan_fp3",
                    "status": "processed",
                    "snapshot_id": "snapshot:fp3",
                    "rebuilt_snapshot": True,
                    "bet_count": 1,
                    "total_pnl": 9.6,
                    "reason": None,
                }
            ],
        }

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.refresh_latest_session_for_meeting",
        fake_refresh,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-latest-session",
            json={
                "meeting_id": "meeting:1281",
                "search_fallback": False,
                "discover_max_pages": 1,
                "hydrate_market_history": False,
                "sync_calendar": False,
                "hydrate_f1_session_data": False,
                "include_extended_f1_data": False,
                "include_heavy_f1_data": False,
                "refresh_artifacts": False,
            },
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meeting_id"] == "meeting:1281"
    assert payload["refreshed_session"]["session_code"] == "Q"
    assert payload["markets_hydrated"] == 2
    assert payload["artifacts_refreshed"][0]["gp_short_code"] == "japan_fp3"
    assert calls == {
        "meeting_id": "meeting:1281",
        "search_fallback": False,
        "discover_max_pages": 1,
        "hydrate_market_history": False,
        "sync_calendar": False,
        "hydrate_f1_session_data": False,
        "include_extended_f1_data": False,
        "include_heavy_f1_data": False,
        "refresh_artifacts": False,
    }


def test_gp_registry_endpoint_surfaces_dynamic_active_ops_configs(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/actions/gp-registry")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    short_codes = [item["short_code"] for item in payload]
    assert "bahrain_fp1_q" not in short_codes
    assert "saudi_fp1_q" not in short_codes
    assert "miami_fp1_sq" in short_codes
    assert payload[0]["calendar_status"] == "scheduled"
    assert payload[0]["meeting_slug"] in {"japanese-grand-prix", "miami-grand-prix"}


def test_ops_calendar_endpoint_includes_cancelled_meetings_with_sources(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/ops-calendar",
            params={"include_cancelled": True, "season": 2026},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    by_slug = {item["meeting_slug"]: item for item in payload}
    assert by_slug["bahrain-grand-prix"]["status"] == "cancelled"
    assert by_slug["saudi-arabian-grand-prix"]["status"] == "cancelled"
    assert by_slug["bahrain-grand-prix"]["source_conflict"] is True
    assert by_slug["bahrain-grand-prix"]["source_url"].startswith(
        "https://www.formula1.com/en/latest/article/"
    )
    assert by_slug["miami-grand-prix"]["status"] == "scheduled"


def test_set_and_clear_calendar_override_actions_round_trip(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        set_response = client.post(
            "/api/v1/actions/set-calendar-override",
            json={
                "season": 2026,
                "meeting_slug": "miami-grand-prix",
                "status": "postponed",
                "source_label": "Ops test",
                "note": "temporary hold",
            },
        )
        clear_response = client.post(
            "/api/v1/actions/clear-calendar-override",
            json={"season": 2026, "meeting_slug": "miami-grand-prix"},
        )

    app.dependency_overrides.clear()

    assert set_response.status_code == 200
    assert clear_response.status_code == 200
    assert set_response.json()["details"]["status"] == "postponed"
    assert clear_response.json()["details"]["is_active"] is False

def test_refresh_latest_session_endpoint_returns_404_for_missing_meeting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_refresh(*args, **kwargs):
        raise KeyError("Meeting not found: missing-meeting")

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.refresh_latest_session_for_meeting",
        fail_refresh,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-latest-session",
            json={"meeting_id": "missing-meeting"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert "Meeting not found: missing-meeting" in response.text


def test_refresh_latest_session_endpoint_returns_409_for_unfinished_meeting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_refresh(*args, **kwargs):
        raise ValueError("No ended sessions are available yet for Japanese Grand Prix.")

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.refresh_latest_session_for_meeting",
        fail_refresh,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-latest-session",
            json={"meeting_id": "meeting:1281"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "No ended sessions are available yet for Japanese Grand Prix." in response.text


def test_capture_live_weekend_endpoint_serializes_worker_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.capture_live_weekend",
        lambda *args, **kwargs: {
            "job_run_id": "job-live-1",
            "status": "completed",
            "message": "Captured 20s of live data for Qualifying across 12 market(s).",
            "session_key": 11249,
            "capture_seconds": 20,
            "openf1_messages": 14,
            "polymarket_messages": 9,
            "market_count": 12,
            "polymarket_market_ids": ["market-1", "market-2"],
            "records_written": 31,
            "report_path": "/tmp/capture-live-weekend.json",
            "preflight_summary": {
                "key": "live_capture",
                "label": "Live capture",
                "status": "ready",
                "message": "Ready to capture.",
                "blockers": [],
                "warnings": [],
                "meeting_key": 1281,
                "meeting_name": "Japanese Grand Prix",
                "gp_short_code": "japan_fp2_q",
                "session_code": "Q",
                "session_key": 11249,
                "actionable_after_utc": None,
                "openf1_credentials_configured": True,
                "last_job_run": None,
                "last_report_path": "/tmp/capture-live-weekend.json",
            },
            "warnings": ["Latest linked markets already loaded."],
            "summary": {
                "openf1_topics": [{"key": "v1/laps", "count": 14}],
                "polymarket_event_types": [{"key": "book", "count": 9}],
                "observed_market_count": 1,
                "observed_token_count": 1,
                "market_quotes": [
                    {
                        "market_id": "market-1",
                        "token_id": "token-1",
                        "outcome": "Yes",
                        "event_type": "best_bid_ask",
                        "observed_at_utc": "2026-03-28T06:20:00Z",
                        "price": 0.41,
                        "best_bid": 0.4,
                        "best_ask": 0.42,
                        "midpoint": 0.41,
                        "spread": 0.02,
                        "size": 12.0,
                        "side": "buy",
                    }
                ],
            },
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/capture-live-weekend",
            json={"session_key": 11249, "capture_seconds": 20},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "capture-live-weekend"
    assert payload["session_key"] == 11249
    assert payload["capture_seconds"] == 20
    assert payload["polymarket_messages"] == 9
    assert payload["market_count"] == 12
    assert payload["report_path"] == "/tmp/capture-live-weekend.json"
    assert payload["preflight_summary"]["key"] == "live_capture"
    assert payload["warnings"] == ["Latest linked markets already loaded."]
    assert payload["summary"]["openf1_topics"] == [{"key": "v1/laps", "count": 14}]
    assert payload["summary"]["polymarket_event_types"] == [{"key": "book", "count": 9}]
    assert payload["summary"]["market_quotes"][0]["market_id"] == "market-1"
    assert payload["summary"]["market_quotes"][0]["event_type"] == "best_bid_ask"


def test_capture_live_weekend_endpoint_returns_409_when_session_not_live(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_capture(*args, **kwargs):
        raise ValueError(
            "Qualifying live capture is available only between "
            "2026-03-28T06:00:00+00:00 and 2026-03-28T07:00:00+00:00."
        )

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.capture_live_weekend",
        fail_capture,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/capture-live-weekend",
            json={"session_key": 11249, "capture_seconds": 20},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "Qualifying live capture is available only between" in response.text


def test_execute_manual_live_paper_trade_endpoint_serializes_worker_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_execute(*args, **kwargs):
        captured.update(kwargs)
        return {
            "action": "execute-manual-live-paper-trade",
            "status": "ok",
            "message": "Opened manual YES paper trade.",
            "gp_short_code": "japan_fp1_fp2",
            "market_id": "market-1",
            "pt_session_id": "pt-live-1",
            "signal_action": "buy_yes",
            "quantity": 10.0,
            "entry_price": 0.41,
            "stake_cost": 4.1,
            "market_price": 0.41,
            "model_prob": 0.62,
            "edge": 0.21,
            "side_label": "YES",
            "reason": "signal_accepted",
        }

    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.execute_manual_live_paper_trade",
        fake_execute,
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/execute-manual-live-paper-trade",
            json={
                "gp_short_code": "japan_fp1_fp2",
                "market_id": "market-1",
                "token_id": "token-1",
                "model_run_id": "model-run-live",
                "snapshot_id": "snapshot-live",
                "model_prob": 0.62,
                "market_price": 0.41,
                "observed_at_utc": "2026-03-27T06:20:00Z",
                "observed_spread": 0.02,
                "source_event_type": "best_bid_ask",
                "min_edge": 0.07,
                "max_spread": 0.03,
                "bet_size": 12,
            },
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "execute-manual-live-paper-trade"
    assert payload["gp_short_code"] == "japan_fp1_fp2"
    assert payload["market_id"] == "market-1"
    assert payload["pt_session_id"] == "pt-live-1"
    assert payload["signal_action"] == "buy_yes"
    assert payload["stake_cost"] == 4.1
    assert captured["observed_spread"] == 0.02
    assert captured["min_edge"] == 0.07
    assert captured["max_spread"] == 0.03
    assert captured["bet_size"] == 12


def test_driver_affinity_report_endpoint_serializes_worker_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.driver_affinity.get_driver_affinity_report",
        lambda *args, **kwargs: {
            "season": 2026,
            "meeting_key": 1281,
            "meeting": {
                "id": "meeting:1281",
                "meeting_key": 1281,
                "season": 2026,
                "meeting_name": "Japanese Grand Prix",
                "circuit_short_name": "Suzuka",
                "country_name": "Japan",
                "location": "Suzuka",
                "start_date_utc": "2026-03-27T02:30:00Z",
                "end_date_utc": "2026-03-29T07:00:00Z",
            },
            "computed_at_utc": "2026-03-27T08:45:00Z",
            "as_of_utc": "2026-03-27T08:45:00Z",
            "lookback_start_season": 2024,
            "session_code_weights": {"Q": 1.0, "FP3": 0.8, "FP2": 0.6, "FP1": 0.4},
            "season_weights": {"2026": 1.0, "2025": 0.65, "2024": 0.4},
            "track_weights": {
                "s1_fraction": 0.35,
                "s2_fraction": 0.44,
                "s3_fraction": 0.21,
            },
            "source_session_codes_included": ["FP1", "FP2"],
            "source_max_session_end_utc": "2026-03-27T07:00:00Z",
            "latest_ended_relevant_session_code": "FP2",
            "latest_ended_relevant_session_end_utc": "2026-03-27T07:00:00Z",
            "entry_count": 1,
            "is_fresh": True,
            "stale_reason": None,
            "entries": [
                {
                    "canonical_driver_key": "lando norris",
                    "display_driver_id": "driver:1",
                    "display_name": "Lando NORRIS",
                    "display_broadcast_name": "L NORRIS",
                    "driver_number": 1,
                    "team_id": "team:mclaren",
                    "team_name": "McLaren",
                    "country_code": "GBR",
                    "headshot_url": None,
                    "rank": 1,
                    "affinity_score": 1.23,
                    "s1_strength": 1.0,
                    "s2_strength": 1.2,
                    "s3_strength": 1.1,
                    "track_s1_fraction": 0.35,
                    "track_s2_fraction": 0.44,
                    "track_s3_fraction": 0.21,
                    "contributing_session_count": 6,
                    "contributing_session_codes": ["Q", "FP2"],
                    "latest_contributing_session_code": "FP2",
                    "latest_contributing_session_end_utc": "2026-03-27T07:00:00Z",
                }
            ],
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.get(
            "/api/v1/driver-affinity",
            params={"season": 2026, "meeting_key": 1281},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meeting_key"] == 1281
    assert payload["is_fresh"] is True
    assert payload["meeting"]["round_number"] is None
    assert payload["entries"][0]["display_name"] == "Lando NORRIS"


def test_refresh_driver_affinity_endpoint_returns_blocked_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.driver_affinity.refresh_driver_affinity",
        lambda *args, **kwargs: {
            "action": "refresh-driver-affinity",
            "status": "blocked",
            "message": (
                "Driver affinity needs newer ended session data, "
                "but OpenF1 credentials are missing."
            ),
            "season": 2026,
            "meeting_key": 1281,
            "computed_at_utc": None,
            "source_max_session_end_utc": "2026-03-27T07:00:00Z",
            "hydrated_session_keys": [],
            "job_run_id": "job-affinity-1",
            "report_path": "/tmp/driver-affinity.json",
            "preflight_summary": {
                "key": "driver_affinity",
                "label": "Driver affinity refresh",
                "status": "blocked",
                "message": (
                    "Driver affinity needs newer ended session data, "
                    "but OpenF1 credentials are missing."
                ),
                "blockers": [
                    "Driver affinity needs newer ended session data, "
                    "but OpenF1 credentials are missing."
                ],
                "warnings": ["Missing hydration for FP2."],
                "meeting_key": 1281,
                "meeting_name": "Japanese Grand Prix",
                "gp_short_code": None,
                "session_code": "FP2",
                "session_key": 11247,
                "actionable_after_utc": "2026-03-27T07:00:00Z",
                "openf1_credentials_configured": False,
                "last_job_run": None,
                "last_report_path": "/tmp/driver-affinity.json",
                "missing_session_keys": [11247],
                "report_is_fresh": False,
                "latest_ended_session_code": "FP2",
                "latest_ended_session_end_utc": "2026-03-27T07:00:00Z",
            },
            "warnings": ["Missing hydration for FP2."],
            "report": None,
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-driver-affinity",
            json={"season": 2026, "meeting_key": 1281},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["meeting_key"] == 1281
    assert payload["job_run_id"] == "job-affinity-1"
    assert payload["report_path"] == "/tmp/driver-affinity.json"
    assert payload["preflight_summary"]["missing_session_keys"] == [11247]
    assert payload["warnings"] == ["Missing hydration for FP2."]


def test_refresh_driver_affinity_endpoint_accepts_report_without_round_number(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.driver_affinity.refresh_driver_affinity",
        lambda *args, **kwargs: {
            "action": "refresh-driver-affinity",
            "status": "refreshed",
            "message": "Driver affinity refreshed.",
            "season": 2026,
            "meeting_key": 1281,
            "computed_at_utc": "2026-03-28T04:30:00Z",
            "source_max_session_end_utc": "2026-03-28T03:30:00Z",
            "hydrated_session_keys": [],
            "report": {
                "season": 2026,
                "meeting_key": 1281,
                "meeting": {
                    "id": "meeting:1281",
                    "meeting_key": 1281,
                    "season": 2026,
                    "meeting_name": "Japanese Grand Prix",
                    "circuit_short_name": "Suzuka",
                    "country_name": "Japan",
                    "location": "Suzuka",
                    "start_date_utc": "2026-03-27T02:30:00Z",
                    "end_date_utc": "2026-03-29T07:00:00Z",
                },
                "computed_at_utc": "2026-03-28T04:30:00Z",
                "as_of_utc": "2026-03-28T04:30:00Z",
                "lookback_start_season": 2024,
                "session_code_weights": {"Q": 1.0, "FP3": 0.8, "FP2": 0.6, "FP1": 0.4},
                "season_weights": {"2026": 1.0, "2025": 0.65, "2024": 0.4},
                "track_weights": {
                    "s1_fraction": 0.35,
                    "s2_fraction": 0.44,
                    "s3_fraction": 0.21,
                },
                "source_session_codes_included": ["FP1", "FP2", "FP3"],
                "source_max_session_end_utc": "2026-03-28T03:30:00Z",
                "latest_ended_relevant_session_code": "FP3",
                "latest_ended_relevant_session_end_utc": "2026-03-28T03:30:00Z",
                "entry_count": 1,
                "is_fresh": True,
                "stale_reason": None,
                "entries": [
                    {
                        "canonical_driver_key": "kimi antonelli",
                        "display_driver_id": "driver:12",
                        "display_name": "Kimi ANTONELLI",
                        "display_broadcast_name": "K ANTONELLI",
                        "driver_number": 12,
                        "team_id": "team:mercedes",
                        "team_name": "Mercedes",
                        "country_code": "ITA",
                        "headshot_url": None,
                        "rank": 1,
                        "affinity_score": 1.5,
                        "s1_strength": 1.2,
                        "s2_strength": 1.4,
                        "s3_strength": 1.3,
                        "track_s1_fraction": 0.35,
                        "track_s2_fraction": 0.44,
                        "track_s3_fraction": 0.21,
                        "contributing_session_count": 6,
                        "contributing_session_codes": ["Q", "FP3"],
                        "latest_contributing_session_code": "FP3",
                        "latest_contributing_session_end_utc": "2026-03-28T03:30:00Z",
                    }
                ],
            },
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-driver-affinity",
            json={"season": 2026, "meeting_key": 1281},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "refreshed"
    assert payload["report"]["meeting"]["round_number"] is None
