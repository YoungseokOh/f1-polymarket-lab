from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone

import pytest
from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.models import (
    EnsemblePrediction,
    ModelRun,
    SignalRegistryEntry,
)
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.postgres_integration


def test_signal_ensemble_endpoints_return_postgres_rows(postgres_engine: Engine) -> None:
    session_maker = sessionmaker(bind=postgres_engine, expire_on_commit=False)

    with Session(postgres_engine) as session:
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
        session.commit()

    def override() -> Generator[Session, None, None]:
        db = session_maker()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db_session] = override
    try:
        with TestClient(app) as client:
            registry = client.get("/api/v1/signals/registry")
            predictions = client.get(
                "/api/v1/ensemble/predictions",
                params={"market_id": "market-1"},
            )
    finally:
        app.dependency_overrides.clear()

    assert registry.status_code == 200
    assert registry.json()[0]["signal_code"] == "pace_delta_signal"

    assert predictions.status_code == 200
    assert predictions.json()[0]["p_yes_ensemble"] == 0.44
