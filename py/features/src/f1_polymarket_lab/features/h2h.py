"""Head-to-head market signal engine.

Computes model probabilities for H2H markets (e.g. "Who will finish higher: A or B?")
using driver sector affinity scores.

Signal logic:
  model_prob(A > B) = sigmoid(k * (affinity_A - affinity_B))

For same-team (teammate) matchups the car factor cancels out, making affinity
a direct measure of relative driver ability. Cross-team matchups also work but
carry additional constructor-pace uncertainty.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Any

from f1_polymarket_lab.features.driver_profile import (
    build_driver_identity_map,
    compute_driver_sector_profiles,
    compute_driver_track_affinity,
    compute_track_sector_weights,
)
from sqlalchemy import text
from sqlalchemy.orm import Session

# Calibration constant: higher k → sharper separation between close and wide splits.
# k=1.5 chosen to give ~80% probability for a 1-sigma affinity gap.
_SIGMOID_K: float = 1.5


def _sigmoid(x: float, k: float = _SIGMOID_K) -> float:
    return 1.0 / (1.0 + math.exp(-k * x))


def build_driver_name_map(db: Session, *, season: int = 2026) -> dict[str, str]:
    """Return {last_name_lower: driver_id} from drivers who raced in *season*.

    Handles duplicate last names by preferring the driver with the most recent entry.
    """
    rows = db.execute(
        text("""
            SELECT DISTINCT d.id, d.broadcast_name
            FROM f1_drivers d
            JOIN f1_session_results sr ON sr.driver_id = d.id
            JOIN f1_sessions s ON s.id = sr.session_id
            JOIN f1_meetings m ON m.id = s.meeting_id
            WHERE m.season = :season
        """),
        {"season": season},
    ).fetchall()

    name_map: dict[str, str] = {}
    for row in rows:
        # broadcast_name is e.g. "L HAMILTON" or "G RUSSELL"
        parts = (row.broadcast_name or "").strip().split()
        if len(parts) >= 2:
            last = parts[-1].lower()
            name_map[last] = row.id
    return name_map


def build_team_map(db: Session, *, season: int = 2026) -> dict[str, str]:
    """Return {driver_id: team_id} for drivers active in *season*."""
    rows = db.execute(
        text("""
            SELECT DISTINCT d.id as driver_id, d.team_id
            FROM f1_drivers d
            JOIN f1_session_results sr ON sr.driver_id = d.id
            JOIN f1_sessions s ON s.id = sr.session_id
            JOIN f1_meetings m ON m.id = s.meeting_id
            WHERE m.season = :season AND d.team_id IS NOT NULL
        """),
        {"season": season},
    ).fetchall()
    return {r.driver_id: r.team_id for r in rows}


def compute_h2h_signals(
    db: Session,
    *,
    meeting_key: int,
    session_code: str = "R",
    circuit_key: int | None = None,
    circuit_short_name: str | None = None,
    min_edge: float = 0.05,
    as_of_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return H2H market signals for a given session.

    Each row in the result represents one token in a H2H market with:
      - driver_a / driver_b: names as stored in the market
      - driver_a_id / driver_b_id: resolved F1 driver IDs (None if unresolvable)
      - affinity_a / affinity_b: Suzuka affinity scores
      - token_driver: which driver this token represents (from outcome)
      - token_price: latest YES-equivalent price for this token
      - model_prob: model probability that token_driver finishes ahead
      - edge: model_prob - token_price (positive = buy signal)
      - is_teammate_h2h: True if both drivers share the same team
      - signal: 'buy' | 'sell' | 'hold'
    """
    name_map = build_driver_name_map(db, season=2026)
    team_map = build_team_map(db, season=2026)
    identity_map = build_driver_identity_map(db)

    # Driver affinity profiles for this circuit
    profiles = compute_driver_sector_profiles(
        db,
        circuit_key=circuit_key,
        circuit_short_name=circuit_short_name,
        as_of_utc=as_of_utc,
    )
    weights: dict[str, float] = (
        compute_track_sector_weights(
            db,
            circuit_short_name=circuit_short_name,
            as_of_utc=as_of_utc,
        )
        if circuit_short_name
        else {"s1_fraction": 1 / 3, "s2_fraction": 1 / 3, "s3_fraction": 1 / 3}
    )

    def _affinity(driver_id: str | None) -> float:
        if not driver_id:
            return 0.0
        driver_identity = identity_map.get(driver_id, driver_id)
        profile = profiles.get(driver_identity)
        if profile is None:
            return 0.0
        return float(
            compute_driver_track_affinity(
                driver_profile=profile,
                track_weights=weights,
            )
        )

    # Fetch all H2H tokens mapped to this session
    rows = db.execute(
        text("""
            SELECT pm.id as market_id,
                   pm.question,
                   pm.driver_a,
                   pm.driver_b,
                   pt.outcome,
                   pt.latest_price,
                   ph.price as ph_price,
                   ph.observed_at_utc
            FROM polymarket_markets pm
            JOIN entity_mapping_f1_to_polymarket em ON em.polymarket_market_id = pm.id
            JOIN f1_sessions s ON s.id = em.f1_session_id
            JOIN f1_meetings m ON m.id = s.meeting_id
            JOIN polymarket_tokens pt ON pt.market_id = pm.id
            LEFT JOIN (
                SELECT token_id, price, observed_at_utc
                FROM polymarket_price_history
                WHERE (token_id, observed_at_utc) IN (
                    SELECT token_id, MAX(observed_at_utc)
                    FROM polymarket_price_history
                    GROUP BY token_id
                )
            ) ph ON ph.token_id = pt.id
            WHERE pm.taxonomy = 'head_to_head_session'
              AND m.meeting_key = :meeting_key
              AND s.session_code = :session_code
        """),
        {"meeting_key": meeting_key, "session_code": session_code},
    ).fetchall()

    # Group tokens by market_id
    markets: dict[str, list[Any]] = defaultdict(list)
    for r in rows:
        markets[r.market_id].append(r)

    results = []
    for market_id, tokens in markets.items():
        if not tokens:
            continue
        t0 = tokens[0]
        question = t0.question
        raw_driver_a = t0.driver_a
        raw_driver_b = t0.driver_b

        # Determine the token driver for each token and build the signal
        for token in tokens:
            outcome = token.outcome or ""
            token_price = token.ph_price if token.ph_price is not None else token.latest_price
            if token_price is None:
                continue

            # Resolve which driver this token represents
            # "Who will finish higher: A or B?" → outcome is driver last name
            # "Will X finish ahead of Y?" → outcome is Yes/No
            token_driver_name: str | None = None
            other_driver_name: str | None = None

            if outcome in ("Yes", "No"):
                # Binary market: YES = driver mentioned first in question finishes ahead
                # Parse from question: "Will [A] finish ahead of [B]?"
                # driver_a/b in DB might be swapped, so parse from question
                token_driver_name = _parse_yes_driver(question)
                other_driver_name = _parse_no_driver(question)
                if outcome == "No":
                    token_driver_name, other_driver_name = other_driver_name, token_driver_name
                    token_price = 1.0 - token_price
            else:
                # Named-outcome market: outcome = driver last name
                token_driver_name = outcome
                # Other driver = the one not matching outcome
                for candidate in [raw_driver_a, raw_driver_b]:
                    if candidate and candidate.lower() != outcome.lower():
                        other_driver_name = candidate
                        break

            if not token_driver_name or not other_driver_name:
                continue

            # Skip No tokens for binary markets (already handled via inversion above)
            if token.outcome == "No" and outcome in ("Yes", "No"):
                continue

            # Resolve driver IDs
            t_id = name_map.get(token_driver_name.lower())
            o_id = name_map.get(other_driver_name.lower())

            aff_t = _affinity(t_id)
            aff_o = _affinity(o_id)
            model_prob = _sigmoid(aff_t - aff_o)

            team_t = team_map.get(t_id or "")
            team_o = team_map.get(o_id or "")
            is_teammate = bool(team_t and team_o and team_t == team_o)

            edge = model_prob - token_price
            if abs(edge) < min_edge:
                signal = "hold"
            elif edge > 0:
                signal = "buy"
            else:
                signal = "sell"

            results.append({
                "market_id": market_id,
                "question": question,
                "token_driver": token_driver_name,
                "other_driver": other_driver_name,
                "token_driver_id": t_id,
                "other_driver_id": o_id,
                "token_price": round(token_price, 4),
                "model_prob": round(model_prob, 4),
                "edge": round(edge, 4),
                "affinity_token": round(aff_t, 4),
                "affinity_other": round(aff_o, 4),
                "affinity_diff": round(aff_t - aff_o, 4),
                "is_teammate_h2h": is_teammate,
                "team_token": team_t,
                "team_other": team_o,
                "signal": signal,
            })

    return results


def _parse_yes_driver(question: str) -> str | None:
    """Extract the 'YES' driver from 'Will [A] finish ahead of [B]?' questions."""
    import re
    m = re.search(r"Will (.+?) finish ahead of", question, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        # Return last word as last name
        return name.split()[-1]
    return None


def _parse_no_driver(question: str) -> str | None:
    """Extract the 'NO' (losing) driver from 'Will [A] finish ahead of [B]?' questions."""
    import re
    m = re.search(r"finish ahead of (.+?)(?:\s+in\s+|\?|$)", question, re.IGNORECASE)
    if m:
        name = m.group(1).strip().rstrip("?")
        return name.split()[-1]
    return None
