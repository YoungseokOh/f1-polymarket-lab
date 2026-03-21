"""Structured data collection layer wrapping OpenF1Connector.

Provides high-level methods to fetch complete meeting data, session results,
and full session detail (results + laps + stints) in a single call.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .openf1 import OpenF1Connector

logger = logging.getLogger(__name__)


@dataclass
class SessionData:
    """Aggregate result for a single session."""

    session_key: int
    session_info: dict[str, Any]
    results: list[dict[str, Any]]
    drivers: list[dict[str, Any]]
    laps: list[dict[str, Any]] = field(default_factory=list)
    stints: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class MeetingData:
    """Aggregate result for a full meeting (all sessions)."""

    meeting_key: int
    sessions: list[dict[str, Any]]
    drivers: list[dict[str, Any]]
    session_data: dict[int, SessionData] = field(default_factory=dict)
    weather: list[dict[str, Any]] = field(default_factory=list)


class DataCollector:
    """High-level data collector wrapping :class:`OpenF1Connector`."""

    def __init__(self, connector: OpenF1Connector | None = None) -> None:
        self.connector = connector or OpenF1Connector()

    def collect_session_results(self, session_key: int) -> SessionData:
        """Fetch session results and driver list for a single session."""
        results = self.connector.fetch_session_results(session_key)
        drivers = self.connector.fetch_drivers(session_key)
        sessions = self.connector.fetch_dataset("sessions", session_key=session_key)
        session_info = sessions[0] if sessions else {}
        unique_drivers = _deduplicate_drivers(drivers)
        logger.info(
            "collect_session_results session_key=%d results=%d drivers=%d",
            session_key,
            len(results),
            len(unique_drivers),
        )
        return SessionData(
            session_key=session_key,
            session_info=session_info,
            results=results,
            drivers=unique_drivers,
        )

    def collect_full_session(self, session_key: int) -> SessionData:
        """Fetch results, laps, stints, and drivers for a single session."""
        data = self.collect_session_results(session_key)
        data.laps = self.connector.fetch_laps(session_key)
        data.stints = self.connector.fetch_stints(session_key)
        logger.info(
            "collect_full_session session_key=%d laps=%d stints=%d",
            session_key,
            len(data.laps),
            len(data.stints),
        )
        return data

    def collect_meeting_data(
        self,
        meeting_key: int,
        year: int,
        *,
        include_laps: bool = False,
        include_weather: bool = False,
    ) -> MeetingData:
        """Fetch all sessions, drivers, and results for a meeting.

        Parameters
        ----------
        meeting_key:
            The OpenF1 meeting key.
        year:
            The season year (used to find sessions).
        include_laps:
            If ``True``, also fetch laps and stints for each session.
        include_weather:
            If ``True``, also fetch weather data for the meeting.
        """
        all_sessions = self.connector.fetch_sessions(year)
        meeting_sessions = [
            s for s in all_sessions if s.get("meeting_key") == meeting_key
        ]
        if not meeting_sessions:
            logger.warning("No sessions found for meeting_key=%d year=%d", meeting_key, year)

        all_drivers: list[dict[str, Any]] = []
        session_data: dict[int, SessionData] = {}

        for sess in meeting_sessions:
            sk = sess["session_key"]
            if include_laps:
                sd = self.collect_full_session(sk)
            else:
                sd = self.collect_session_results(sk)
            session_data[sk] = sd
            all_drivers.extend(sd.drivers)

        unique_drivers = _deduplicate_drivers(all_drivers)
        weather: list[dict[str, Any]] = []
        if include_weather:
            weather = self.connector.fetch_weather(meeting_key)

        logger.info(
            "collect_meeting_data meeting_key=%d sessions=%d drivers=%d",
            meeting_key,
            len(meeting_sessions),
            len(unique_drivers),
        )
        return MeetingData(
            meeting_key=meeting_key,
            sessions=meeting_sessions,
            drivers=unique_drivers,
            session_data=session_data,
            weather=weather,
        )


def _deduplicate_drivers(drivers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return unique drivers by driver_number, keeping the first occurrence."""
    seen: set[int] = set()
    unique: list[dict[str, Any]] = []
    for d in drivers:
        dn = d.get("driver_number")
        if dn is not None and dn not in seen:
            seen.add(dn)
            unique.append(d)
    return unique
