from __future__ import annotations

from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class JolpicaConnector:
    base_url = "https://api.jolpi.ca/ergast/f1"

    def __init__(self) -> None:
        self.client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "f1-polymarket-lab/0.1.0"},
        )

    @retry(wait=wait_exponential(min=2, max=20), stop=stop_after_attempt(5), reraise=True)
    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> dict[str, Any] | None:
        response = self.client.get(f"{self.base_url}/{path}", params=params)
        if allow_404 and response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Jolpica payload for path={path} was not an object")
        return payload

    def _race_table(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> list[dict[str, Any]]:
        payload = self._get_json(path, params=params, allow_404=allow_404)
        if payload is None:
            return []
        return list(payload.get("MRData", {}).get("RaceTable", {}).get("Races", []))

    def fetch_races(self, season: int) -> list[dict[str, Any]]:
        return self._race_table(f"{season}.json")

    def fetch_results(
        self,
        season: int,
        round_number: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._race_table(f"{season}/{round_number}/results.json", params={"limit": limit})

    def fetch_qualifying(
        self, season: int, round_number: int, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        return self._race_table(f"{season}/{round_number}/qualifying.json", params={"limit": limit})

    def fetch_sprint(
        self,
        season: int,
        round_number: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._race_table(
            f"{season}/{round_number}/sprint.json",
            params={"limit": limit},
            allow_404=True,
        )

    def fetch_pitstops(
        self, season: int, round_number: int, *, limit: int = 2000
    ) -> list[dict[str, Any]]:
        return self._race_table(
            f"{season}/{round_number}/pitstops.json",
            params={"limit": limit},
            allow_404=True,
        )

    def fetch_laps(
        self,
        season: int,
        round_number: int,
        *,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        return self._race_table(
            f"{season}/{round_number}/laps.json",
            params={"limit": limit},
            allow_404=True,
        )
