from f1_polymarket_api.main import app
from fastapi.testclient import TestClient


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "api"
    assert payload["status"] == "ok"
