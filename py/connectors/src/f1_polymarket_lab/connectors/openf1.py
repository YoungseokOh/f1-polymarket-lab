from __future__ import annotations

from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class OpenF1Connector:
    base_url = "https://api.openf1.org/v1"

    def __init__(self) -> None:
        self.client = httpx.Client(timeout=30.0, headers={"User-Agent": "f1-polymarket-lab/0.1.0"})

    @retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5), reraise=True)
    def _get(self, path: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self.client.get(f"{self.base_url}/{path}", params=params)
        response.raise_for_status()
        return list(response.json())

    def fetch_dataset(self, dataset: str, **params: Any) -> list[dict[str, Any]]:
        return self._get(dataset, params=params)

    def fetch_sessions(self, year: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("sessions", year=year)

    def fetch_drivers(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("drivers", session_key=session_key)

    def fetch_session_results(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("session_result", session_key=session_key)

    def fetch_laps(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("laps", session_key=session_key)

    def fetch_stints(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("stints", session_key=session_key)

    def fetch_weather(self, meeting_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("weather", meeting_key=meeting_key)

    def fetch_race_control(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("race_control", session_key=session_key)

    def fetch_positions(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("position", session_key=session_key)

    def fetch_intervals(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("intervals", session_key=session_key)

    def fetch_pit(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("pit", session_key=session_key)

    def fetch_car_data(
        self, session_key: int, driver_number: int | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"session_key": session_key}
        if driver_number is not None:
            params["driver_number"] = driver_number
        return self.fetch_dataset("car_data", **params)

    def fetch_location(
        self, session_key: int, driver_number: int | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"session_key": session_key}
        if driver_number is not None:
            params["driver_number"] = driver_number
        return self.fetch_dataset("location", **params)

    def fetch_team_radio(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("team_radio", session_key=session_key)

    def fetch_starting_grid(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("starting_grid", session_key=session_key)
