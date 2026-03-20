from __future__ import annotations

import httpx
import pytest
from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.connectors.openf1 import OpenF1Connector


def test_optional_openf1_dataset_returns_empty_list_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = OpenF1Connector()

    def fake_get(url: str, *, params: dict[str, int]) -> httpx.Response:
        assert url.endswith("/intervals")
        assert params == {"session_key": 9222}
        return httpx.Response(
            404,
            request=httpx.Request("GET", url, params=params),
        )

    monkeypatch.setattr(connector.client, "get", fake_get)

    assert connector.fetch_intervals(9222) == []


def test_session_core_dataset_returns_empty_list_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = OpenF1Connector()

    def fake_get(url: str, *, params: dict[str, int]) -> httpx.Response:
        assert url.endswith("/drivers")
        assert params == {"session_key": 9079}
        return httpx.Response(
            404,
            request=httpx.Request("GET", url, params=params),
        )

    monkeypatch.setattr(connector.client, "get", fake_get)

    assert connector.fetch_drivers(9079) == []


def test_required_openf1_dataset_still_raises_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = OpenF1Connector()

    def fake_get(url: str, *, params: dict[str, int]) -> httpx.Response:
        return httpx.Response(
            404,
            request=httpx.Request("GET", url, params=params),
        )

    monkeypatch.setattr(connector.client, "get", fake_get)

    with pytest.raises(httpx.HTTPStatusError):
        connector.fetch_sessions(2023)


def test_openf1_connector_reads_configured_throttle_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("OPENF1_MAX_REQUESTS_PER_MINUTE", "18")
    monkeypatch.setenv("OPENF1_MAX_REQUESTS_PER_SECOND", "1")

    connector = OpenF1Connector()

    assert connector._max_requests_per_minute == 18
    assert connector._max_requests_per_second == 1

    get_settings.cache_clear()
