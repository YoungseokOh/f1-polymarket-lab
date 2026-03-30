from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
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
                    circuit_short_name="Suzuka",
                    country_name="Japan",
                    location="Suzuka",
                    start_date_utc=datetime(2026, 3, 27, 2, 30, tzinfo=timezone.utc),
                    end_date_utc=datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc),
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
                PaperTradeSession(
                    id="pt-japan-pre",
                    gp_slug="japan_pre",
                    snapshot_id="snapshot-1",
                    model_run_id="model-run-1",
                    status="settled",
                    started_at=datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 3, 26, 9, 5, tzinfo=timezone.utc),
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


def test_weekend_cockpit_status_auto_selects_pre_weekend_before_fp1_end(
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
    assert payload["auto_selected_gp_short_code"] == "japan_pre"
    assert payload["selected_gp_short_code"] == "japan_pre"
    assert payload["ready_to_run"] is True
    assert payload["selected_config"]["source_session_code"] is None


def test_weekend_cockpit_status_auto_selects_fp1_to_fp2_between_fp1_end_and_fp2_start(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.utc_now",
        lambda: datetime(2026, 3, 27, 4, 56, tzinfo=timezone.utc),
    )

    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/weekend-cockpit/status")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["auto_selected_gp_short_code"] == "japan_fp1_fp2"
    assert payload["selected_gp_short_code"] == "japan_fp1_fp2"
    assert payload["selected_config"]["display_label"] == "Use FP1 results to prepare FP2"
    assert payload["selected_config"]["display_description"] == (
        "Use FP1 results to find FP2 markets and prepare paper trading."
    )
    assert payload["focus_status"] == "upcoming"
    assert payload["focus_session"]["session_code"] == "FP2"
    assert payload["timeline_completed_codes"] == ["FP1"]
    assert payload["timeline_active_code"] == "FP2"
    assert payload["primary_action_title"] == "Update to latest"
    assert payload["primary_action_description"] == (
        "This latest update will load FP1 results first and prepare FP2 markets."
    )
    assert payload["primary_action_cta"] == "Update to latest"
    assert payload["explanation"] == (
        "This stage uses FP1 results to settle finished FP1 tickets, find FP2 markets, "
        "and when ready continue into paper trading."
    )
    assert payload["steps"][2]["resource_label"] == "FP1 tickets"
    assert payload["steps"][2]["reason_code"] == "waiting_for_source_results"
    assert payload["steps"][3]["resource_label"] == "FP2 markets"
    assert payload["steps"][3]["reason_code"] == "ready_to_discover"
    assert [config["short_code"] for config in payload["available_configs"]] == [
        "japan_pre",
        "japan_fp1_fp2",
        "japan_fp1",
        "japan_fp2_q",
        "japan_fp3",
        "japan_q_race",
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
            params={"gp_short_code": "japan_fp1"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_gp_short_code"] == "japan_fp1"
    assert payload["ready_to_run"] is False
    assert payload["blockers"]
    assert payload["primary_action_title"] == "Wait for this stage"
    assert payload["primary_action_cta"] == "Not ready yet"
    assert payload["steps"][1]["reason_code"] == "session_in_progress"
    assert payload["steps"][1]["actionable_after_utc"] == "2026-03-27T03:30:00Z"
    assert payload["steps"][1]["resource_label"] == "FP1 results"
    assert "FP1 is still in progress" in payload["blockers"][0]


def test_weekend_cockpit_status_blocks_fp2_to_q_during_fp2_live(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.utc_now",
        lambda: datetime(2026, 3, 27, 6, 30, tzinfo=timezone.utc),
    )

    with build_test_client(tmp_path) as client:
        response = client.get("/api/v1/weekend-cockpit/status")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["auto_selected_gp_short_code"] == "japan_fp2_q"
    assert payload["selected_gp_short_code"] == "japan_fp2_q"
    assert payload["focus_status"] == "live"
    assert payload["focus_session"]["session_code"] == "FP2"
    assert payload["timeline_completed_codes"] == ["FP1"]
    assert payload["timeline_active_code"] == "FP2"
    assert payload["ready_to_run"] is False
    assert "FP2 is still in progress" in payload["blockers"][0]


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


def test_refresh_latest_session_endpoint_serializes_worker_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "f1_polymarket_worker.weekend_ops.refresh_latest_session_for_meeting",
        lambda *args, **kwargs: {
            "action": "refresh-latest-session",
            "status": "ok",
            "message": "Updated latest ended session Q for Japanese Grand Prix.",
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
        },
    )

    with build_test_client(tmp_path) as client:
        response = client.post(
            "/api/v1/actions/refresh-latest-session",
            json={"meeting_id": "meeting:1281"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["meeting_id"] == "meeting:1281"
    assert payload["refreshed_session"]["session_code"] == "Q"
    assert payload["markets_hydrated"] == 2


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
