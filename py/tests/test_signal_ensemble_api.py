from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    EnsemblePrediction,
    ModelPrediction,
    ModelRun,
    SignalDiagnostic,
    SignalRegistryEntry,
    SignalSnapshot,
    TradeDecision,
)
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def build_test_client(tmp_path: Path) -> TestClient:
    database_path = tmp_path / "signal-ensemble-api.sqlite"
    engine = create_engine(f"sqlite+pysqlite:///{database_path}", future=True)
    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine, expire_on_commit=False)

    with Session(engine) as session:
        session.add(
            ModelRun(
                id="ensemble-run-1",
                stage="signal_ensemble_v1",
                model_family="signal_ensemble",
                model_name="anchor_ridge_stacking",
                dataset_version="features_v1",
                feature_snapshot_id="snapshot-1",
                metrics_json={"row_count": 4},
            )
        )
        session.add(
            SignalRegistryEntry(
                id="signal-registry-1",
                signal_code="pace_delta_signal",
                signal_family="pace_delta",
                description="Latest pace softmax.",
                version="v1",
                config_json={"applicable_market_groups": ["driver_outright"]},
                is_active=True,
            )
        )
        session.add(
            SignalSnapshot(
                id="signal-snapshot-1",
                model_run_id="ensemble-run-1",
                feature_snapshot_id="snapshot-1",
                market_id="market-1",
                token_id="token-1",
                event_id="event-1",
                market_taxonomy="driver_pole_position",
                market_group="driver_outright",
                meeting_key=1281,
                as_of_ts=datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
                signal_code="pace_delta_signal",
                signal_version="v1",
                p_yes_raw=0.34,
                p_yes_calibrated=0.39,
                p_market_ref=0.31,
                delta_logit=0.18,
                freshness_sec=120.0,
                coverage_flag=True,
                metadata_json={"used_features": ["fp3_position"]},
            )
        )
        session.add(
            SignalDiagnostic(
                id="signal-diagnostic-1",
                model_run_id="ensemble-run-1",
                signal_code="pace_delta_signal",
                market_group="driver_outright",
                phase_bucket="overall",
                brier=0.16,
                log_loss=0.49,
                ece=0.03,
                skill_vs_market=0.02,
                coverage_rate=1.0,
                residual_correlation_json={"pace_delta_signal": {"pace_delta_signal": 1.0}},
                stability_json={"fit_row_count": 12},
                metrics_json={"row_count": 12},
            )
        )
        session.add(
            EnsemblePrediction(
                id="ensemble-prediction-1",
                model_run_id="ensemble-run-1",
                feature_snapshot_id="snapshot-1",
                market_id="market-1",
                token_id="token-1",
                event_id="event-1",
                market_taxonomy="driver_pole_position",
                market_group="driver_outright",
                meeting_key=1281,
                as_of_ts=datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
                p_market_ref=0.31,
                p_yes_ensemble=0.44,
                z_market=-0.8,
                z_ensemble=-0.2,
                intercept=0.05,
                disagreement_score=0.08,
                effective_n=2.1,
                uncertainty_score=0.24,
                contributions_json={"pace_delta_signal": 0.41},
                coverage_json={"supported": True},
                metadata_json={"signal_deltas": {"pace_delta_signal": 0.22}},
            )
        )
        session.add(
            TradeDecision(
                id="trade-decision-1",
                model_run_id="ensemble-run-1",
                ensemble_prediction_id="ensemble-prediction-1",
                feature_snapshot_id="snapshot-1",
                market_id="market-1",
                token_id="token-1",
                event_id="event-1",
                market_taxonomy="driver_pole_position",
                market_group="driver_outright",
                meeting_key=1281,
                as_of_ts=datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
                side="YES",
                edge=0.07,
                threshold=0.05,
                spread=0.02,
                depth=24.0,
                kelly_fraction_raw=0.13,
                disagreement_penalty=0.76,
                liquidity_factor=0.82,
                size_fraction=0.08,
                decision_status="trade",
                decision_reason="positive_yes_edge",
                metadata_json={"yes_entry_price": 0.35},
            )
        )
        session.add(
            ModelPrediction(
                id="model-prediction-1",
                model_run_id="ensemble-run-1",
                market_id="market-1",
                token_id="token-1",
                as_of_ts=datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc),
                probability_yes=0.44,
                probability_no=0.56,
                raw_score=-0.2,
                calibration_version="v1",
            )
        )
        session.add(
            ModelPrediction(
                id="model-prediction-2",
                model_run_id="ensemble-run-1",
                market_id="market-2",
                token_id="token-2",
                as_of_ts=datetime(2026, 4, 8, 10, 5, tzinfo=timezone.utc),
                probability_yes=0.21,
                probability_no=0.79,
                raw_score=-1.1,
                calibration_version="v1",
            )
        )
        session.commit()

    def override() -> Generator[Session, None, None]:
        db = session_maker()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db_session] = override
    return TestClient(app)


def test_signal_ensemble_endpoints_return_generic_payloads(tmp_path: Path) -> None:
    with build_test_client(tmp_path) as client:
        registry = client.get("/api/v1/signals/registry")
        snapshots = client.get("/api/v1/signals/snapshots", params={"market_id": "market-1"})
        diagnostics = client.get("/api/v1/signals/diagnostics")
        predictions = client.get("/api/v1/ensemble/predictions", params={"market_id": "market-1"})
        decisions = client.get("/api/v1/trade-decisions", params={"market_id": "market-1"})
        model_predictions = client.get("/api/v1/predictions", params={"market_id": "market-1"})

    app.dependency_overrides.clear()

    assert registry.status_code == 200
    assert registry.json()[0]["signal_code"] == "pace_delta_signal"

    assert snapshots.status_code == 200
    assert snapshots.json()[0]["market_group"] == "driver_outright"

    assert diagnostics.status_code == 200
    assert diagnostics.json()[0]["skill_vs_market"] == 0.02

    assert predictions.status_code == 200
    prediction_payload = predictions.json()[0]
    assert prediction_payload["p_yes_ensemble"] == 0.44
    assert prediction_payload["contributions_json"]["pace_delta_signal"] == 0.41

    assert decisions.status_code == 200
    assert decisions.json()[0]["decision_status"] == "trade"

    assert model_predictions.status_code == 200
    assert len(model_predictions.json()) == 1
    assert model_predictions.json()[0]["market_id"] == "market-1"
