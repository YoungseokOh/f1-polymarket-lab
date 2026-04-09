from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone

import pytest
from f1_polymarket_api.dependencies import get_db_session
from f1_polymarket_api.main import app
from f1_polymarket_lab.storage.models import ModelRun, ModelRunPromotion
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.postgres_integration


def test_model_runs_endpoint_returns_postgres_rows(postgres_engine: Engine) -> None:
    session_maker = sessionmaker(bind=postgres_engine, expire_on_commit=False)

    with Session(postgres_engine) as session:
        session.add(
            ModelRun(
                id="model-run-1",
                stage="multitask_qr",
                model_family="torch_multitask",
                model_name="shared_encoder_multitask_v2",
                dataset_version="dataset-v1",
                feature_snapshot_id="snapshot-1",
                metrics_json={"roi_pct": 12.5},
                artifact_uri="/tmp/model-run-1",
                registry_run_id="mlflow-run-1",
            )
        )
        session.add(
            ModelRunPromotion(
                id="promotion-1",
                model_run_id="model-run-1",
                stage="multitask_qr",
                status="active",
                gate_metrics_json={"roi_pct": 12.5},
                promoted_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
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
            response = client.get("/api/v1/model-runs")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["id"] == "model-run-1"
    assert payload[0]["registry_run_id"] == "mlflow-run-1"
    assert payload[0]["promotion_status"] == "active"
