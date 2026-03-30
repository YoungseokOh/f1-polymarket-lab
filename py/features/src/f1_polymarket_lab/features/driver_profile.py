"""Driver sector strength and track affinity profiles.

Computes driver ability scores from historical qualifying/practice lap data:
  - Per-sector relative pace (S1, S2, S3) averaged across recent sessions
  - Track sector weight profiles (fraction of lap time per sector)
  - Driver x track affinity score (weighted sector strengths)
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

DEFAULT_AFFINITY_SESSION_CODES: tuple[str, ...] = ("Q", "FP3", "FP2", "FP1")
DEFAULT_AFFINITY_SESSION_WEIGHTS: dict[str, float] = {
    "Q": 1.0,
    "FP3": 0.8,
    "FP2": 0.6,
    "FP1": 0.4,
}
DEFAULT_AFFINITY_SEASON_WEIGHTS: dict[int, float] = {
    2026: 1.0,
    2025: 0.65,
    2024: 0.4,
}
_MIN_UTC_DATETIME = datetime.min.replace(tzinfo=timezone.utc)


def canonical_driver_identity(
    *,
    full_name: str | None = None,
    broadcast_name: str | None = None,
    driver_id: str | None = None,
) -> str:
    for value in (full_name, broadcast_name, driver_id):
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return "unknown-driver"


def build_driver_identity_map(db: Session) -> dict[str, str]:
    rows = db.execute(
        text("""
            SELECT id, full_name, broadcast_name
            FROM f1_drivers
        """)
    ).fetchall()
    return {
        row.id: canonical_driver_identity(
            full_name=row.full_name,
            broadcast_name=row.broadcast_name,
            driver_id=row.id,
        )
        for row in rows
    }


def _zscore_within_session(
    values: list[tuple[str, float]],
) -> dict[str, float]:
    """Return z-score map {driver_identity: z} for a list of values."""
    if len(values) < 3:
        return {}
    vals = [v for _, v in values]
    mean = sum(vals) / len(vals)
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(variance)
    if std < 1e-9:
        return {d: 0.0 for d, _ in values}
    return {d: -(v - mean) / std for d, v in values}


def compute_driver_sector_profiles(
    db: Session,
    *,
    circuit_key: int | None = None,
    circuit_short_name: str | None = None,
    meeting_key: int | None = None,
    season_exact: int | None = None,
    session_codes: tuple[str, ...] = DEFAULT_AFFINITY_SESSION_CODES,
    min_season: int = 2024,
    n_sessions: int = 12,
    decay: float = 0.85,
    as_of_utc: datetime | None = None,
    session_code_weights: dict[str, float] | None = None,
    season_weights: dict[int, float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return per-driver sector strength profiles keyed by canonical identity."""
    session_codes_sql = ", ".join(f"'{c}'" for c in session_codes)
    conditions = [
        f"s.session_code IN ({session_codes_sql})",
        "m.season >= :min_season",
        "l.sector_1_seconds IS NOT NULL",
        "l.sector_1_seconds > 5",
        "l.sector_2_seconds IS NOT NULL",
        "l.sector_2_seconds > 5",
        "l.sector_3_seconds IS NOT NULL",
        "l.sector_3_seconds > 5",
    ]
    params: dict[str, Any] = {"min_season": min_season}
    if season_exact is not None:
        conditions.append("m.season = :season_exact")
        params["season_exact"] = season_exact
    if meeting_key is not None:
        conditions.append("m.meeting_key = :meeting_key")
        params["meeting_key"] = meeting_key
    if as_of_utc is not None:
        conditions.append("s.date_end_utc <= :as_of_utc")
        params["as_of_utc"] = as_of_utc
    query = text(f"""
        SELECT l.driver_id,
               s.id AS session_id,
               s.session_code,
               m.season,
               m.circuit_short_name,
               s.date_end_utc,
               MIN(l.sector_1_seconds) AS best_s1,
               MIN(l.sector_2_seconds) AS best_s2,
               MIN(l.sector_3_seconds) AS best_s3
        FROM f1_laps l
        JOIN f1_sessions s ON s.id = l.session_id
        JOIN f1_meetings m ON m.id = s.meeting_id
        WHERE {" AND ".join(conditions)}
        GROUP BY l.driver_id, s.id, m.id
        HAVING COUNT(*) >= 2
        ORDER BY s.date_end_utc DESC, s.id
    """)
    rows = db.execute(query, params).fetchall()

    identity_map = build_driver_identity_map(db)
    session_code_weights = session_code_weights or DEFAULT_AFFINITY_SESSION_WEIGHTS
    season_weights = season_weights or DEFAULT_AFFINITY_SEASON_WEIGHTS

    sessions: dict[str, list[tuple[str, float, float, float]]] = defaultdict(list)
    session_meta: dict[str, tuple[datetime | None, str, int, str | None]] = {}
    for row in rows:
        driver_identity = identity_map.get(
            row.driver_id,
            canonical_driver_identity(driver_id=row.driver_id),
        )
        sessions[row.session_id].append(
            (driver_identity, row.best_s1, row.best_s2, row.best_s3)
        )
        session_meta[row.session_id] = (
            row.date_end_utc,
            row.circuit_short_name or "",
            int(row.season),
            row.session_code,
        )

    sorted_sessions = sorted(
        sessions.keys(),
        key=lambda sid: session_meta[sid][0] or _MIN_UTC_DATETIME,
        reverse=True,
    )

    driver_s1_wsum: dict[str, float] = defaultdict(float)
    driver_s2_wsum: dict[str, float] = defaultdict(float)
    driver_s3_wsum: dict[str, float] = defaultdict(float)
    driver_wtot: dict[str, float] = defaultdict(float)
    seen_sessions_per_driver: dict[str, int] = defaultdict(int)
    driver_session_codes: dict[str, set[str]] = defaultdict(set)
    driver_latest_session_code: dict[str, str] = {}
    driver_latest_session_end_utc: dict[str, datetime | None] = {}

    for sid in sorted_sessions:
        session_rows = sessions[sid]
        session_end_utc, session_circuit, session_season, session_code = session_meta[sid]
        circuit = session_circuit.lower()
        same_circuit = bool(
            (circuit_key and circuit and _circuit_matches(circuit_key, circuit))
            or (
                circuit_short_name
                and circuit
                and circuit_short_name.strip().lower() in circuit
            )
        )
        circuit_boost = 2.0 if same_circuit else 1.0
        code_weight = session_code_weights.get(session_code or "", 0.0)
        season_weight = season_weights.get(session_season, 0.25)
        if code_weight <= 0 or season_weight <= 0:
            continue

        s1_zscores = _zscore_within_session([(d, s1) for d, s1, _, _ in session_rows])
        s2_zscores = _zscore_within_session([(d, s2) for d, _, s2, _ in session_rows])
        s3_zscores = _zscore_within_session([(d, s3) for d, _, _, s3 in session_rows])

        for driver_identity in s1_zscores:
            rank = seen_sessions_per_driver[driver_identity]
            if rank >= n_sessions:
                continue
            weight = (decay**rank) * circuit_boost * code_weight * season_weight
            driver_s1_wsum[driver_identity] += s1_zscores[driver_identity] * weight
            if s2_zscores.get(driver_identity) is not None:
                driver_s2_wsum[driver_identity] += s2_zscores[driver_identity] * weight
            if s3_zscores.get(driver_identity) is not None:
                driver_s3_wsum[driver_identity] += s3_zscores[driver_identity] * weight
            driver_wtot[driver_identity] += weight
            seen_sessions_per_driver[driver_identity] += 1
            if session_code:
                driver_session_codes[driver_identity].add(session_code)
            if driver_identity not in driver_latest_session_code:
                driver_latest_session_code[driver_identity] = session_code or ""
                driver_latest_session_end_utc[driver_identity] = session_end_utc

    result: dict[str, dict[str, Any]] = {}
    all_drivers = set(driver_s1_wsum) | set(driver_s2_wsum) | set(driver_s3_wsum)
    for driver_identity in all_drivers:
        wtot = driver_wtot.get(driver_identity, 0.0)
        result[driver_identity] = {
            "s1_strength": driver_s1_wsum[driver_identity] / wtot if wtot else 0.0,
            "s2_strength": driver_s2_wsum[driver_identity] / wtot if wtot else 0.0,
            "s3_strength": driver_s3_wsum[driver_identity] / wtot if wtot else 0.0,
            "n_sessions": seen_sessions_per_driver.get(driver_identity, 0),
            "session_codes": sorted(driver_session_codes.get(driver_identity, set())),
            "latest_session_code": driver_latest_session_code.get(driver_identity),
            "latest_session_end_utc": driver_latest_session_end_utc.get(driver_identity),
        }
    return result


def compute_track_sector_weights(
    db: Session,
    *,
    circuit_short_name: str,
    session_codes: tuple[str, ...] = ("Q",),
    min_season: int = 2024,
    as_of_utc: datetime | None = None,
) -> dict[str, float]:
    """Return sector time fractions for a circuit."""
    sector_total = "l.sector_1_seconds + l.sector_2_seconds + l.sector_3_seconds"
    session_codes_sql = ", ".join(f"'{c}'" for c in session_codes)
    conditions = [
        f"s.session_code IN ({session_codes_sql})",
        "m.circuit_short_name = :circuit",
        "m.season >= :min_season",
        "l.sector_1_seconds > 5",
        "l.sector_2_seconds > 5",
        "l.sector_3_seconds > 5",
        "l.sector_1_seconds IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "circuit": circuit_short_name,
        "min_season": min_season,
    }
    if as_of_utc is not None:
        conditions.append("s.date_end_utc <= :as_of_utc")
        params["as_of_utc"] = as_of_utc
    query = text(f"""
        SELECT
            AVG(l.sector_1_seconds / ({sector_total})) AS s1_frac,
            AVG(l.sector_2_seconds / ({sector_total})) AS s2_frac,
            AVG(l.sector_3_seconds / ({sector_total})) AS s3_frac,
            COUNT(*) AS lap_count
        FROM f1_laps l
        JOIN f1_sessions s ON s.id = l.session_id
        JOIN f1_meetings m ON m.id = s.meeting_id
        WHERE {" AND ".join(conditions)}
    """)
    row = db.execute(query, params).fetchone()

    if row is None or row.lap_count < 20 or row.s1_frac is None:
        return {"s1_fraction": 1 / 3, "s2_fraction": 1 / 3, "s3_fraction": 1 / 3}

    return {
        "s1_fraction": float(row.s1_frac),
        "s2_fraction": float(row.s2_frac),
        "s3_fraction": float(row.s3_frac),
    }


def compute_driver_track_affinity(
    *,
    driver_profile: dict[str, float],
    track_weights: dict[str, float],
) -> float:
    """Combine driver sector strengths with track sector weights."""
    return (
        driver_profile.get("s1_strength", 0.0) * track_weights.get("s1_fraction", 1 / 3)
        + driver_profile.get("s2_strength", 0.0) * track_weights.get("s2_fraction", 1 / 3)
        + driver_profile.get("s3_strength", 0.0) * track_weights.get("s3_fraction", 1 / 3)
    )


def _circuit_matches(circuit_key: int, circuit_short_name: str) -> bool:
    """Rough lookup: does this circuit_short_name correspond to circuit_key?"""
    key_to_name_fragments: dict[int, list[str]] = {
        10: ["melbourne", "australia"],
        49: ["shanghai", "china"],
        63: ["sakhir", "bahrain"],
        36: ["jeddah", "saudi"],
        77: ["miami"],
        7: ["imola"],
        17: ["monte carlo", "monaco"],
        4: ["barcelona", "catalunya"],
        70: ["spielberg", "austria", "red bull ring"],
        3: ["silverstone"],
        14: ["spa"],
        32: ["hungaroring"],
        23: ["zandvoort"],
        16: ["monza"],
        75: ["baku", "azerbaijan"],
        61: ["singapore"],
        39: ["suzuka", "japan"],
        73: ["lusail", "qatar"],
        56: ["yas marina", "abu dhabi"],
        69: ["las vegas"],
        22: ["austin", "cota"],
        15: ["mexico city", "mexico"],
        18: ["interlagos", "brazil"],
        21: ["montreal", "canada"],
    }
    fragments = key_to_name_fragments.get(circuit_key, [])
    name_lower = circuit_short_name.lower()
    return any(fragment in name_lower for fragment in fragments)


def enrich_rows_with_driver_profiles(
    rows: list[dict[str, Any]],
    *,
    db: Session,
    circuit_key: int | None = None,
    circuit_short_name: str | None = None,
    as_of_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """Add driver sector profile and track affinity columns to snapshot rows."""
    if not rows:
        return rows

    driver_profiles = compute_driver_sector_profiles(
        db,
        circuit_key=circuit_key,
        circuit_short_name=circuit_short_name,
        as_of_utc=as_of_utc,
    )
    track_weights = (
        compute_track_sector_weights(
            db,
            circuit_short_name=circuit_short_name,
            as_of_utc=as_of_utc,
        )
        if circuit_short_name
        else {"s1_fraction": 1 / 3, "s2_fraction": 1 / 3, "s3_fraction": 1 / 3}
    )
    identity_map = build_driver_identity_map(db)

    enriched = []
    for row in rows:
        driver_id = row.get("driver_id")
        driver_identity = identity_map.get(
            driver_id or "",
            canonical_driver_identity(driver_id=driver_id),
        )
        profile = driver_profiles.get(driver_identity, {})
        affinity = compute_driver_track_affinity(
            driver_profile=profile,
            track_weights=track_weights,
        )
        enriched.append(
            {
                **row,
                "driver_s1_strength": profile.get("s1_strength", 0.0),
                "driver_s2_strength": profile.get("s2_strength", 0.0),
                "driver_s3_strength": profile.get("s3_strength", 0.0),
                "driver_profile_sessions": profile.get("n_sessions", 0),
                "track_s1_fraction": track_weights["s1_fraction"],
                "track_s2_fraction": track_weights["s2_fraction"],
                "track_s3_fraction": track_weights["s3_fraction"],
                "driver_track_affinity": affinity,
            }
        )
    return enriched
