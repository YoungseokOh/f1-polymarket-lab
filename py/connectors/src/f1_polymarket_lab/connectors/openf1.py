from __future__ import annotations

import time
from collections import deque
from threading import Lock
from typing import Any, ClassVar

import httpx
from f1_polymarket_lab.common import get_settings
from tenacity import retry, stop_after_attempt, wait_exponential


class OpenF1Connector:
    base_url = "https://api.openf1.org/v1"
    token_url = "https://api.openf1.org/token"
    _request_timestamps: ClassVar[deque[float]] = deque()
    _request_lock: ClassVar[Lock] = Lock()
    _max_requests_per_minute: ClassVar[int] = 24
    _max_requests_per_second: ClassVar[int] = 2

    def __init__(self) -> None:
        self.client = httpx.Client(timeout=30.0, headers={"User-Agent": "f1-polymarket-lab/0.1.0"})
        self._access_token: str | None = None
        self._access_token_expiry_monotonic: float = 0.0
        self._configure_limits()

    @classmethod
    def _configure_limits(cls) -> None:
        settings = get_settings()
        with cls._request_lock:
            cls._max_requests_per_minute = max(1, int(settings.openf1_max_requests_per_minute))
            cls._max_requests_per_second = max(1, int(settings.openf1_max_requests_per_second))

    @classmethod
    def _throttle(cls) -> None:
        while True:
            with cls._request_lock:
                now = time.monotonic()
                while cls._request_timestamps and now - cls._request_timestamps[0] >= 60.0:
                    cls._request_timestamps.popleft()

                wait_seconds = 0.0
                minute_count = len(cls._request_timestamps)
                recent_second = [stamp for stamp in cls._request_timestamps if now - stamp < 1.0]
                if minute_count >= cls._max_requests_per_minute:
                    wait_seconds = max(
                        wait_seconds,
                        60.0 - (now - cls._request_timestamps[0]) + 0.01,
                    )
                if len(recent_second) >= cls._max_requests_per_second:
                    wait_seconds = max(wait_seconds, 1.0 - (now - recent_second[0]) + 0.01)

                if wait_seconds <= 0:
                    cls._request_timestamps.append(now)
                    return

            time.sleep(wait_seconds)

    @retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5), reraise=True)
    def _get(
        self,
        path: str,
        *,
        params: dict[str, Any],
        allow_404: bool = False,
    ) -> list[dict[str, Any]]:
        self._throttle()
        response = self.client.get(
            f"{self.base_url}/{path}",
            params=params,
            headers=self._auth_headers(),
        )
        if response.status_code == 401 and self._can_authenticate():
            self._invalidate_access_token()
            response = self.client.get(
                f"{self.base_url}/{path}",
                params=params,
                headers=self._auth_headers(),
            )
        if allow_404 and response.status_code == 404:
            return []
        response.raise_for_status()
        return list(response.json())

    def _can_authenticate(self) -> bool:
        settings = get_settings()
        return bool(settings.openf1_username and settings.openf1_password)

    def _invalidate_access_token(self) -> None:
        self._access_token = None
        self._access_token_expiry_monotonic = 0.0

    def _auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        token = self._access_token_or_none()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _access_token_or_none(self) -> str | None:
        settings = get_settings()
        if not settings.openf1_username or not settings.openf1_password:
            return None
        now = time.monotonic()
        if self._access_token and now < self._access_token_expiry_monotonic:
            return self._access_token
        token = self.fetch_access_token(
            username=settings.openf1_username,
            password=settings.openf1_password,
        )
        self._access_token = token
        self._access_token_expiry_monotonic = now + 3300.0
        return token

    @retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5), reraise=True)
    def fetch_access_token(self, *, username: str, password: str) -> str:
        response = self.client.post(
            self.token_url,
            data={
                "grant_type": "password",
                "username": username,
                "password": password,
            },
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not isinstance(token, str) or not token:
            raise ValueError("OpenF1 token response did not include access_token")
        return token

    def fetch_dataset(
        self,
        dataset: str,
        *,
        allow_404: bool = False,
        **params: Any,
    ) -> list[dict[str, Any]]:
        return self._get(dataset, params=params, allow_404=allow_404)

    def fetch_sessions(self, year: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("sessions", year=year)

    def fetch_drivers(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("drivers", session_key=session_key, allow_404=True)

    def fetch_session_results(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("session_result", session_key=session_key, allow_404=True)

    def fetch_laps(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("laps", session_key=session_key, allow_404=True)

    def fetch_stints(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("stints", session_key=session_key, allow_404=True)

    def fetch_weather(self, meeting_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("weather", meeting_key=meeting_key, allow_404=True)

    def fetch_race_control(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("race_control", session_key=session_key, allow_404=True)

    def fetch_positions(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("position", session_key=session_key, allow_404=True)

    def fetch_intervals(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("intervals", session_key=session_key, allow_404=True)

    def fetch_pit(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("pit", session_key=session_key, allow_404=True)

    def fetch_car_data(
        self, session_key: int, driver_number: int | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"session_key": session_key}
        if driver_number is not None:
            params["driver_number"] = driver_number
        return self.fetch_dataset("car_data", allow_404=True, **params)

    def fetch_location(
        self, session_key: int, driver_number: int | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"session_key": session_key}
        if driver_number is not None:
            params["driver_number"] = driver_number
        return self.fetch_dataset("location", allow_404=True, **params)

    def fetch_team_radio(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("team_radio", session_key=session_key, allow_404=True)

    def fetch_starting_grid(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch_dataset("starting_grid", session_key=session_key, allow_404=True)
