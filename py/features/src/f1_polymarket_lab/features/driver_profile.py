"""Driver sector strength and track affinity profiles.

Computes driver ability scores from historical qualifying/practice lap data:
  - Per-sector relative pace (S1, S2, S3) averaged across recent sessions
  - Track sector weight profiles (fraction of lap time per sector)
  - Driver × track affinity score (weighted sector strengths)
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _zscore_within_session(
    values: list[tuple[str, float]],
) -> dict[str, float]:
    """Return z-score map {driver_id: z} for a list of (driver_id, value) pairs.

    Lower value = faster = negative z-score (we invert to make positive = better).
    Returns empty dict if fewer than 3 drivers have data.
    """
    if len(values) < 3:
        return {}
    vals = [v for _, v in values]
    mean = sum(vals) / len(vals)
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(variance)
    if std < 1e-9:
        return {d: 0.0 for d, _ in values}
    # Invert: negative lap time z-score → positive strength
    return {d: -(v - mean) / std for d, v in values}


def compute_driver_sector_profiles(
    db: Session,
    *,
    circuit_key: int | None = None,
    session_codes: tuple[str, ...] = ("Q", "FP3"),
    min_season: int = 2023,
    n_sessions: int = 12,
) -> dict[str, dict[str, float]]:
    """Return per-driver sector strength profiles.

    Result schema: {driver_id: {s1_strength, s2_strength, s3_strength, n_sessions}}

    Strength values are z-scores (positive = faster than peers).
    Averaged across the last *n_sessions* sessions where the driver has data.
    If *circuit_key* is provided, sessions at that circuit get 2× weight.
    """
    session_codes_sql = ", ".join(f"'{c}'" for c in session_codes)
    query = text(f"""
        SELECT l.driver_id,
               s.id AS session_id,
               m.season,
               m.circuit_short_name,
               m.start_date_utc,
               MIN(l.sector_1_seconds) AS best_s1,
               MIN(l.sector_2_seconds) AS best_s2,
               MIN(l.sector_3_seconds) AS best_s3
        FROM f1_laps l
        JOIN f1_sessions s ON s.id = l.session_id
        JOIN f1_meetings m ON m.id = s.meeting_id
        WHERE s.session_code IN ({session_codes_sql})
          AND m.season >= :min_season
          AND l.sector_1_seconds IS NOT NULL
          AND l.sector_1_seconds > 5
          AND l.sector_2_seconds IS NOT NULL
          AND l.sector_2_seconds > 5
          AND l.sector_3_seconds IS NOT NULL
          AND l.sector_3_seconds > 5
        GROUP BY l.driver_id, s.id, m.id
        HAVING COUNT(*) >= 2
        ORDER BY m.start_date_utc DESC, s.id
    """)
    rows = db.execute(query, {"min_season": min_season}).fetchall()

    # Group by session_id
    sessions: dict[str, list[tuple[str, float, float, float]]] = defaultdict(list)
    session_meta: dict[str, tuple[str, str]] = {}  # session_id -> (date, circuit)
    for row in rows:
        sessions[row.session_id].append(
            (row.driver_id, row.best_s1, row.best_s2, row.best_s3)
        )
        session_meta[row.session_id] = (row.start_date_utc, row.circuit_short_name)

    # Sort sessions by date descending (most recent first)
    sorted_sessions = sorted(
        sessions.keys(),
        key=lambda sid: session_meta[sid][0] or "",
        reverse=True,
    )

    # Accumulate driver z-scores across sessions (most recent n_sessions)
    driver_s1: dict[str, list[float]] = defaultdict(list)
    driver_s2: dict[str, list[float]] = defaultdict(list)
    driver_s3: dict[str, list[float]] = defaultdict(list)
    seen_sessions_per_driver: dict[str, int] = defaultdict(int)

    for sid in sorted_sessions:
        session_rows = sessions[sid]
        circuit = (session_meta[sid][1] or "").lower()

        s1_zscores = _zscore_within_session([(d, s1) for d, s1, _, _ in session_rows])
        s2_zscores = _zscore_within_session([(d, s2) for d, _, s2, _ in session_rows])
        s3_zscores = _zscore_within_session([(d, s3) for d, _, _, s3 in session_rows])

        for driver_id in s1_zscores:
            if seen_sessions_per_driver[driver_id] >= n_sessions:
                continue
            # 2× weight for same circuit (if specified)
            same_circuit = bool(circuit_key and circuit and _circuit_matches(circuit_key, circuit))
            weight = 2.0 if same_circuit else 1.0
            for _ in range(int(weight)):
                if s1_zscores.get(driver_id) is not None:
                    driver_s1[driver_id].append(s1_zscores[driver_id])
                if s2_zscores.get(driver_id) is not None:
                    driver_s2[driver_id].append(s2_zscores[driver_id])
                if s3_zscores.get(driver_id) is not None:
                    driver_s3[driver_id].append(s3_zscores[driver_id])
            seen_sessions_per_driver[driver_id] += 1

    result: dict[str, dict[str, float]] = {}
    all_drivers = set(driver_s1) | set(driver_s2) | set(driver_s3)
    for driver_id in all_drivers:
        s1_vals = driver_s1.get(driver_id, [])
        s2_vals = driver_s2.get(driver_id, [])
        s3_vals = driver_s3.get(driver_id, [])
        result[driver_id] = {
            "s1_strength": sum(s1_vals) / len(s1_vals) if s1_vals else 0.0,
            "s2_strength": sum(s2_vals) / len(s2_vals) if s2_vals else 0.0,
            "s3_strength": sum(s3_vals) / len(s3_vals) if s3_vals else 0.0,
            "n_sessions": seen_sessions_per_driver.get(driver_id, 0),
        }
    return result


def compute_track_sector_weights(
    db: Session,
    *,
    circuit_short_name: str,
    session_codes: tuple[str, ...] = ("Q",),
    min_season: int = 2023,
) -> dict[str, float]:
    """Return sector time fractions for a circuit: {s1_fraction, s2_fraction, s3_fraction}.

    Fractions sum to 1.0. Uses historical Q lap data for this circuit.
    Falls back to equal weights (0.333, 0.333, 0.333) if insufficient data.
    """
    _s = "l.sector_1_seconds + l.sector_2_seconds + l.sector_3_seconds"
    session_codes_sql = ", ".join(f"'{c}'" for c in session_codes)
    query = text(f"""
        SELECT
            AVG(l.sector_1_seconds / ({_s})) AS s1_frac,
            AVG(l.sector_2_seconds / ({_s})) AS s2_frac,
            AVG(l.sector_3_seconds / ({_s})) AS s3_frac,
            COUNT(*) AS lap_count
        FROM f1_laps l
        JOIN f1_sessions s ON s.id = l.session_id
        JOIN f1_meetings m ON m.id = s.meeting_id
        WHERE s.session_code IN ({session_codes_sql})
          AND m.circuit_short_name = :circuit
          AND m.season >= :min_season
          AND l.sector_1_seconds > 5
          AND l.sector_2_seconds > 5
          AND l.sector_3_seconds > 5
          AND l.sector_1_seconds IS NOT NULL
    """)
    row = db.execute(
        query,
        {
            "circuit": circuit_short_name,
            "min_season": min_season,
        },
    ).fetchone()

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
    """Combine driver sector strengths with track sector weights.

    Returns a single affinity score: higher = driver is relatively stronger at this track.
    """
    return (
        driver_profile.get("s1_strength", 0.0) * track_weights.get("s1_fraction", 1 / 3)
        + driver_profile.get("s2_strength", 0.0) * track_weights.get("s2_fraction", 1 / 3)
        + driver_profile.get("s3_strength", 0.0) * track_weights.get("s3_fraction", 1 / 3)
    )


def _circuit_matches(circuit_key: int, circuit_short_name: str) -> bool:
    """Rough lookup: does this circuit_short_name correspond to circuit_key?"""
    # Pulled from OpenF1 circuit_key values in our data
    _KEY_TO_NAME_FRAGMENTS: dict[int, list[str]] = {
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
    fragments = _KEY_TO_NAME_FRAGMENTS.get(circuit_key, [])
    name_lower = circuit_short_name.lower()
    return any(frag in name_lower for frag in fragments)


def enrich_rows_with_driver_profiles(
    rows: list[dict[str, Any]],
    *,
    db: Session,
    circuit_key: int | None = None,
    circuit_short_name: str | None = None,
) -> list[dict[str, Any]]:
    """Add driver sector profile and track affinity columns to snapshot rows.

    Adds: driver_s1_strength, driver_s2_strength, driver_s3_strength,
          track_s1_fraction, track_s2_fraction, track_s3_fraction,
          driver_track_affinity
    """
    if not rows:
        return rows

    driver_profiles = compute_driver_sector_profiles(db, circuit_key=circuit_key)
    track_weights = (
        compute_track_sector_weights(db, circuit_short_name=circuit_short_name)
        if circuit_short_name
        else {"s1_fraction": 1 / 3, "s2_fraction": 1 / 3, "s3_fraction": 1 / 3}
    )

    enriched = []
    for row in rows:
        driver_id = row.get("driver_id")
        profile = driver_profiles.get(driver_id or "", {})
        affinity = compute_driver_track_affinity(
            driver_profile=profile, track_weights=track_weights
        )
        enriched.append({
            **row,
            "driver_s1_strength": profile.get("s1_strength", 0.0),
            "driver_s2_strength": profile.get("s2_strength", 0.0),
            "driver_s3_strength": profile.get("s3_strength", 0.0),
            "driver_profile_sessions": profile.get("n_sessions", 0),
            "track_s1_fraction": track_weights["s1_fraction"],
            "track_s2_fraction": track_weights["s2_fraction"],
            "track_s3_fraction": track_weights["s3_fraction"],
            "driver_track_affinity": affinity,
        })
    return enriched
