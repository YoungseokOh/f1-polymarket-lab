"""GP registry and generic quicktest pipeline factory.

Each Grand Prix is described by a ``GPConfig`` dataclass.  The three
factory functions — ``build_snapshot``, ``run_baseline``, and
``generate_report`` — replace the per-GP hardcoded functions that
previously lived in ``quicktest.py``.

To add a brand-new GP, just append a ``GPConfig`` to ``GP_REGISTRY``.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from f1_polymarket_lab.common import MarketTaxonomy, ensure_dir, stable_uuid, utc_now
from f1_polymarket_lab.features.driver_profile import enrich_rows_with_driver_profiles
from f1_polymarket_lab.models.calibration import serialize_reliability_diagram
from f1_polymarket_lab.storage.models import (
    DatasetVersionManifest,
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Lap,
    F1Meeting,
    F1Session,
    F1SessionResult,
    F1Stint,
    FeatureSnapshot,
    ModelPrediction,
    ModelRun,
    PolymarketMarket,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
    SnapshotRunManifest,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    record_lake_object_manifest,
    start_job_run,
)
from f1_polymarket_worker.pipeline import (
    PipelineContext,
    ensure_default_feature_registry,
    hydrate_polymarket_market,
)

EPSILON = 1e-6


# ---------------------------------------------------------------------------
# GPConfig dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class GPConfig:
    """Describes a single GP quicktest variant."""

    name: str
    """Human-readable GP name, e.g. ``"Australian Grand Prix"``."""

    short_code: str
    """Slug-safe short code, e.g. ``"aus"``."""

    meeting_key: int
    season: int

    target_session_code: str
    """The target session code for markets: ``"Q"`` or ``"SQ"``."""

    snapshot_type: str
    snapshot_dataset: str
    baseline_stage: str
    baseline_names: tuple[str, str, str]
    """The three baseline model names, e.g. ``("market_implied", "fp1_pace", "hybrid")``."""

    report_slug: str
    min_edge: float = 0.05

    entry_offset_min: int = 10
    fidelity: int = 60

    pace_signal: str = "fp1"
    """``"fp1"`` for live FP1 data or ``"form"`` for pre-weekend historical form."""

    title_suffix: str = "Q Pole Quick Test"
    """Title suffix for the rendered markdown report."""

    notes: tuple[str, ...] = (
        "This is a paper-edge quick test, not an executable orderbook backtest.",
    )

    variant: str = "fp1"
    """``"fp1"`` for the standard FP1→target pipeline, ``"pre_weekend"`` for
    the form-based pre-weekend approach."""

    source_session_code: str | None = "FP1"
    """The session that must be available before snapshot construction."""

    market_taxonomy: MarketTaxonomy = "driver_pole_position"
    """The primary target taxonomy required for this GP variant."""

    stage_rank: int = 10
    """Ordering for weekend cockpit stage selection within the same meeting."""

    required_model_stage: str | None = None
    """The promoted model stage required for live scoring and ticket generation."""

    live_min_edge: float = 0.05
    live_bet_size: float = 10.0
    live_max_daily_loss: float = 100.0
    live_max_spread: float | None = 0.03
    live_ticket_ttl_min: int = 20


# ---------------------------------------------------------------------------
# GP Registry — add new GPs here
# ---------------------------------------------------------------------------
GP_REGISTRY: list[GPConfig] = [
    GPConfig(
        name="Chinese Grand Prix",
        short_code="china",
        meeting_key=1280,
        season=2026,
        target_session_code="SQ",
        snapshot_type="fp1_to_sq_pole_quicktest",
        snapshot_dataset="china_fp1_to_sq_pole_snapshot",
        baseline_stage="china_sq_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-chinese-grand-prix-sq-pole-quicktest",
        title_suffix="SQ Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Chinese GP FP1 -> Sprint Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Australian Grand Prix",
        short_code="aus",
        meeting_key=1279,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="aus_fp1_to_q_pole_snapshot",
        baseline_stage="aus_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-australian-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Australian GP FP1 -> Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_pre",
        meeting_key=1281,
        season=2026,
        target_session_code="Q",
        snapshot_type="pre_weekend_q_pole_quicktest",
        snapshot_dataset="japan_pre_weekend_q_pole_snapshot",
        baseline_stage="japan_q_pole_quicktest",
        baseline_names=("market_implied", "form_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-q-pole-quicktest",
        pace_signal="form",
        variant="pre_weekend",
        source_session_code=None,
        title_suffix="Q Pole Pre-Weekend Quick Test",
        notes=(
            "This is a PRE-WEEKEND prediction — no FP1 data available yet.",
            "Form signal is derived from AUS + China GP historical pace.",
            "The universe is limited to Japanese GP Qualifying pole markets.",
        ),
        stage_rank=0,
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_fp1_fp2",
        meeting_key=1281,
        season=2026,
        target_session_code="FP2",
        snapshot_type="japan_fp1_to_fp2_fastest_lap_quicktest",
        snapshot_dataset="japan_fp1_to_fp2_fastest_lap_snapshot",
        baseline_stage="japan_fp1_fp2_fastest_lap_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-fp1-fp2-fastest-lap-quicktest",
        variant="fp1_to_fp2",
        market_taxonomy="driver_fastest_lap_practice",
        title_suffix="FP1-to-FP2 Fastest Lap Quick Test",
        notes=(
            "Uses FP1 pace to evaluate Practice 2 fastest-lap markets before FP2 begins.",
            "The universe is limited to Japanese GP Practice 2 fastest-lap markets.",
        ),
        stage_rank=1,
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_fp1",
        meeting_key=1281,
        season=2026,
        target_session_code="Q",
        snapshot_type="japan_fp1_to_q_pole_quicktest",
        snapshot_dataset="japan_fp1_to_q_pole_snapshot",
        baseline_stage="japan_fp1_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-fp1-q-pole-quicktest",
        variant="fp1_to_q",
        title_suffix="FP1-to-Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Japanese GP FP1 -> Qualifying pole markets.",
        ),
        stage_rank=2,
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_fp2_q",
        meeting_key=1281,
        season=2026,
        target_session_code="Q",
        snapshot_type="japan_fp2_to_q_pole_quicktest",
        snapshot_dataset="japan_fp2_to_q_pole_snapshot",
        baseline_stage="japan_fp2_q_pole_quicktest",
        baseline_names=("market_implied", "fp2_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-fp2-q-pole-quicktest",
        variant="fp2_to_q",
        source_session_code="FP2",
        title_suffix="FP2-to-Q Pole Quick Test",
        notes=(
            "Uses FP2 pace as the latest practice signal before qualifying.",
            "The universe is limited to Japanese GP FP2 -> Qualifying pole markets.",
        ),
        stage_rank=3,
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_fp3",
        meeting_key=1281,
        season=2026,
        target_session_code="Q",
        snapshot_type="japan_fp3_to_q_pole_quicktest",
        snapshot_dataset="japan_fp3_to_q_pole_snapshot",
        baseline_stage="japan_fp3_q_pole_quicktest",
        baseline_names=("market_implied", "fp3_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-fp3-q-pole-quicktest",
        variant="fp3_to_q",
        source_session_code="FP3",
        title_suffix="FP3-to-Q Pole Quick Test",
        notes=(
            "Uses FP3 pace (most recent before qualifying) as primary signal.",
            "The universe is limited to Japanese GP FP3 -> Qualifying pole markets.",
        ),
        stage_rank=4,
    ),
    GPConfig(
        name="Japanese Grand Prix",
        short_code="japan_q_race",
        meeting_key=1281,
        season=2026,
        target_session_code="R",
        snapshot_type="japan_q_to_race_winner_snapshot",
        snapshot_dataset="japan_q_to_race_winner_snapshot",
        baseline_stage="japan_q_race_winner_quicktest",
        baseline_names=("market_implied", "pre_race_pace", "hybrid"),
        report_slug="2026-japanese-grand-prix-q-race-winner-quicktest",
        variant="q_to_race",
        source_session_code="Q",
        market_taxonomy="race_winner",
        title_suffix="Q-to-Race Winner Quick Test",
        notes=(
            "Paper-edge study for race winner prediction using FP1 through Q signals.",
            "Universe is limited to Japanese GP race winner markets.",
        ),
        stage_rank=5,
    ),
    GPConfig(
        name="Bahrain Grand Prix",
        short_code="bahrain",
        meeting_key=1282,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="bahrain_fp1_to_q_pole_snapshot",
        baseline_stage="bahrain_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-bahrain-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Bahrain GP FP1 -> Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Saudi Arabian Grand Prix",
        short_code="saudi",
        meeting_key=1283,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="saudi_fp1_to_q_pole_snapshot",
        baseline_stage="saudi_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-saudi-arabian-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Saudi Arabian GP FP1 -> Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Miami Grand Prix",
        short_code="miami_fp1_sq",
        meeting_key=1284,
        season=2026,
        target_session_code="SQ",
        snapshot_type="miami_fp1_to_sq_pole_live_snapshot",
        snapshot_dataset="miami_fp1_to_sq_pole_live_snapshot",
        baseline_stage="sq_pole_live_v1",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-miami-grand-prix-sq-pole-live",
        title_suffix="SQ Pole Live",
        notes=(
            "Miami live stage for FP1 -> Sprint Qualifying pole markets.",
            "Used for operator tickets and manual execution support.",
        ),
        variant="fp1_to_sq",
        required_model_stage="sq_pole_live_v1",
        stage_rank=1,
    ),
    GPConfig(
        name="Miami Grand Prix",
        short_code="miami_sq_sprint",
        meeting_key=1284,
        season=2026,
        target_session_code="S",
        snapshot_type="miami_sq_to_sprint_winner_live_snapshot",
        snapshot_dataset="miami_sq_to_sprint_winner_live_snapshot",
        baseline_stage="sprint_winner_live_v1",
        baseline_names=("market_implied", "sq_pace", "hybrid"),
        report_slug="2026-miami-grand-prix-sprint-winner-live",
        title_suffix="Sprint Winner Live",
        notes=(
            "Miami live stage for Sprint Qualifying -> Sprint winner markets.",
            "Used for operator tickets and manual execution support.",
        ),
        variant="sq_to_sprint",
        source_session_code="SQ",
        market_taxonomy="sprint_winner",
        required_model_stage="sprint_winner_live_v1",
        stage_rank=2,
    ),
    GPConfig(
        name="Miami Grand Prix",
        short_code="miami_fp1_q",
        meeting_key=1284,
        season=2026,
        target_session_code="Q",
        snapshot_type="miami_fp1_to_q_pole_live_snapshot",
        snapshot_dataset="miami_fp1_to_q_pole_live_snapshot",
        baseline_stage="miami_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-miami-grand-prix-q-pole-live",
        title_suffix="Q Pole Live",
        notes=(
            "Miami live stage for FP1 -> Qualifying pole markets.",
            "Live scoring uses the promoted multitask_qr champion.",
        ),
        variant="fp1_to_q",
        required_model_stage="multitask_qr",
        stage_rank=3,
    ),
    GPConfig(
        name="Miami Grand Prix",
        short_code="miami_q_r",
        meeting_key=1284,
        season=2026,
        target_session_code="R",
        snapshot_type="miami_q_to_race_winner_live_snapshot",
        snapshot_dataset="miami_q_to_race_winner_live_snapshot",
        baseline_stage="miami_q_race_winner_quicktest",
        baseline_names=("market_implied", "pre_race_pace", "hybrid"),
        report_slug="2026-miami-grand-prix-race-winner-live",
        title_suffix="Race Winner Live",
        notes=(
            "Miami live stage for Q -> Race winner markets.",
            "Live scoring uses the promoted multitask_qr champion.",
        ),
        variant="q_to_race",
        source_session_code="Q",
        market_taxonomy="race_winner",
        required_model_stage="multitask_qr",
        stage_rank=4,
    ),
    GPConfig(
        name="Emilia Romagna Grand Prix",
        short_code="imola",
        meeting_key=1285,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="imola_fp1_to_q_pole_snapshot",
        baseline_stage="imola_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-emilia-romagna-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Emilia Romagna GP FP1 -> Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Monaco Grand Prix",
        short_code="monaco",
        meeting_key=1286,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="monaco_fp1_to_q_pole_snapshot",
        baseline_stage="monaco_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-monaco-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Monaco GP FP1 -> Qualifying pole markets.",
        ),
    ),
    GPConfig(
        name="Spanish Grand Prix",
        short_code="spain",
        meeting_key=1287,
        season=2026,
        target_session_code="Q",
        snapshot_type="fp1_to_q_pole_quicktest",
        snapshot_dataset="spain_fp1_to_q_pole_snapshot",
        baseline_stage="spain_q_pole_quicktest",
        baseline_names=("market_implied", "fp1_pace", "hybrid"),
        report_slug="2026-spanish-grand-prix-q-pole-quicktest",
        title_suffix="Q Pole Quick Test",
        notes=(
            "This is a paper-edge quick test, not an executable orderbook backtest.",
            "The universe is limited to Spanish GP FP1 -> Qualifying pole markets.",
        ),
    ),
]


def get_gp_config(short_code: str) -> GPConfig:
    """Look up a GP config by short_code.  Raises ``KeyError`` if not found."""
    for config in GP_REGISTRY:
        if config.short_code == short_code:
            return config
    raise KeyError(f"Unknown GP short_code: {short_code!r}")


def resolve_gp_config(
    short_code: str,
    *,
    db: Session | None = None,
    now: datetime | None = None,
) -> GPConfig:
    """Resolve dynamic ops-stage configs first, then fall back to the legacy registry."""
    if db is not None:
        from f1_polymarket_worker.ops_calendar import get_ops_stage_config

        try:
            _, config = get_ops_stage_config(db, short_code=short_code, now=now)
            return config
        except KeyError:
            pass
    return get_gp_config(short_code)


def config_stage_label(config: GPConfig) -> str:
    if config.source_session_code is None:
        return f"Pre-Weekend -> {config.target_session_code}"
    return f"{config.source_session_code} -> {config.target_session_code}"


def _session_display_name(session_code: str | None) -> str:
    return {
        None: "Pre-weekend",
        "FP1": "FP1",
        "FP2": "FP2",
        "FP3": "FP3",
        "SQ": "Sprint Qualifying",
        "S": "Sprint",
        "Q": "Qualifying",
        "R": "Race",
    }.get(session_code, session_code or "Session")


def config_display_label(config: GPConfig) -> str:
    target_name = _session_display_name(config.target_session_code)
    if config.source_session_code is None:
        return f"Prepare {target_name} markets"
    source_name = _session_display_name(config.source_session_code)
    return f"Use {source_name} results to prepare {target_name}"


def config_display_description(config: GPConfig) -> str:
    if config.source_session_code is None:
        return "Review Qualifying markets using pre-practice information only."
    if config.target_session_code == "FP2":
        return "Use FP1 results to find FP2 markets and prepare paper trading."
    if config.target_session_code == "SQ":
        return "Use FP1 results to score Sprint Qualifying pole markets for manual execution."
    if config.target_session_code == "S":
        return "Use Sprint Qualifying results to score Sprint winner markets for manual execution."
    if config.target_session_code == "Q":
        source_name = _session_display_name(config.source_session_code)
        return f"Use {source_name} results to find Qualifying markets and prepare paper trading."
    if config.target_session_code == "R":
        return "Use FP1 through Qualifying results to find Race markets and prepare paper trading."
    source_name = _session_display_name(config.source_session_code)
    target_name = _session_display_name(config.target_session_code)
    return f"Use {source_name} results to find {target_name} markets and prepare paper trading."


def resolve_baseline_name(config: GPConfig, baseline: str | None) -> str:
    """Resolve a requested baseline name against the configured baseline tuple."""
    if baseline and baseline in config.baseline_names:
        return baseline
    if (
        baseline in {"fp1_pace", "fp2_pace", "fp3_pace", "form_pace", "pre_race_pace"}
        and len(config.baseline_names) > 1
    ):
        return config.baseline_names[1]
    if "hybrid" in config.baseline_names:
        return "hybrid"
    return config.baseline_names[-1]


def select_model_run_id(
    config: GPConfig,
    model_run_ids: list[str],
    *,
    baseline: str | None,
) -> tuple[str, str]:
    """Pick the model-run id matching the requested baseline name."""
    if not model_run_ids:
        raise ValueError("No model runs produced")
    baseline_name = resolve_baseline_name(config, baseline)
    try:
        baseline_idx = config.baseline_names.index(baseline_name)
    except ValueError:
        baseline_idx = len(model_run_ids) - 1
    return model_run_ids[min(baseline_idx, len(model_run_ids) - 1)], baseline_name


# ---------------------------------------------------------------------------
# Internal helpers (migrated from quicktest.py, kept private)
# ---------------------------------------------------------------------------


class _CounterLike:
    def __init__(self) -> None:
        self.counts: dict[str, int] = defaultdict(int)
        self.distinct_counts: dict[str, set[int | None]] = defaultdict(set)

    def increment(self, key: str) -> None:
        self.counts[key] += 1

    def add_distinct(self, key: str, value: int | None) -> None:
        self.distinct_counts[key].add(value)


def _load_sessions(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
    target_session_code: str,
) -> tuple[
    F1Meeting,
    dict[str, F1Session],
    F1Session,
    F1Session | None,
    F1Session | None,
    F1Session,
]:
    """Load the meeting, session map, FP1/FP2/FP3 sessions, and target session."""
    meeting = ctx.db.scalar(select(F1Meeting).where(F1Meeting.meeting_key == meeting_key))
    if meeting is None:
        raise ValueError(f"meeting_key={meeting_key} not found")
    if meeting.season != season:
        raise ValueError(
            f"meeting_key={meeting_key} belongs to season={meeting.season}, expected {season}"
        )
    sessions = ctx.db.scalars(
        select(F1Session).where(F1Session.meeting_id == meeting.id)
    ).all()
    sessions_by_code = {
        row.session_code: row for row in sessions if row.session_code is not None
    }
    fp1_session = sessions_by_code.get("FP1")
    fp2_session = sessions_by_code.get("FP2")
    fp3_session = sessions_by_code.get("FP3")
    target_session = sessions_by_code.get(target_session_code)
    if fp1_session is None or target_session is None:
        raise ValueError(
            f"meeting_key={meeting_key} must contain FP1 and {target_session_code} sessions"
        )
    return meeting, sessions_by_code, fp1_session, fp2_session, fp3_session, target_session


def _load_target_markets(
    ctx: PipelineContext,
    *,
    target_session: F1Session,
    target_session_code: str,
    market_taxonomy: MarketTaxonomy,
) -> list[PolymarketMarket]:
    """Fetch mapped target-session markets for a single taxonomy."""
    market_ids = ctx.db.scalars(
        select(EntityMappingF1ToPolymarket.polymarket_market_id)
        .where(
            EntityMappingF1ToPolymarket.f1_session_id == target_session.id,
            EntityMappingF1ToPolymarket.polymarket_market_id.is_not(None),
        )
        .distinct()
    ).all()
    label = target_session_code
    if not market_ids:
        raise ValueError(f"No Polymarket mappings found for {label} session")
    markets = list(
        ctx.db.scalars(
            select(PolymarketMarket)
            .where(
                PolymarketMarket.id.in_(market_ids),
                PolymarketMarket.target_session_code == target_session_code,
                PolymarketMarket.taxonomy == market_taxonomy,
            )
            .order_by(PolymarketMarket.question.asc())
        ).all()
    )
    if not markets:
        pretty_taxonomy = market_taxonomy.replace("_", " ")
        raise ValueError(f"No {pretty_taxonomy} markets mapped to {label} session")
    return markets


def _hydrate_missing_market_history(
    ctx: PipelineContext,
    *,
    markets: list[PolymarketMarket],
    fidelity: int,
) -> int:
    hydrated = 0
    for market in markets:
        history_count = ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketPriceHistory)
            .where(PolymarketPriceHistory.market_id == market.id)
        ) or 0
        if history_count > 0:
            continue
        hydrate_polymarket_market(ctx, market_id=market.id, fidelity=fidelity)
        ctx.db.flush()
        hydrated += 1
    return hydrated


def _load_yes_tokens(ctx: PipelineContext, *, market_ids: list[str]) -> dict[str, PolymarketToken]:
    rows = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id.in_(market_ids))
    ).all()
    return {
        row.market_id: row
        for row in rows
        if (row.outcome or "").strip().lower() == "yes"
    }


def _load_price_history(
    ctx: PipelineContext,
    *,
    market_ids: list[str],
    token_ids: list[str],
) -> dict[str, list[PolymarketPriceHistory]]:
    grouped: dict[str, list[PolymarketPriceHistory]] = defaultdict(list)
    if not market_ids or not token_ids:
        return grouped
    rows = ctx.db.scalars(
        select(PolymarketPriceHistory)
        .where(
            PolymarketPriceHistory.market_id.in_(market_ids),
            PolymarketPriceHistory.token_id.in_(token_ids),
        )
        .order_by(PolymarketPriceHistory.observed_at_utc.asc())
    ).all()
    for row in rows:
        grouped[row.market_id].append(row)
    return grouped


def _load_trades(
    ctx: PipelineContext,
    *,
    market_ids: list[str],
) -> dict[str, list[PolymarketTrade]]:
    grouped: dict[str, list[PolymarketTrade]] = defaultdict(list)
    if not market_ids:
        return grouped
    rows = ctx.db.scalars(
        select(PolymarketTrade)
        .where(PolymarketTrade.market_id.in_(market_ids))
        .order_by(PolymarketTrade.trade_timestamp_utc.asc())
    ).all()
    for row in rows:
        grouped[row.market_id].append(row)
    return grouped


def _build_driver_map(drivers: list[F1Driver]) -> dict[str, F1Driver]:
    mapping: dict[str, F1Driver] = {}
    for driver in drivers:
        for value in [
            driver.full_name,
            driver.broadcast_name,
            driver.last_name,
            driver.first_name,
            (
                None
                if not driver.first_name or not driver.last_name
                else f"{driver.first_name} {driver.last_name}"
            ),
        ]:
            if value:
                mapping[_normalize_name(value)] = driver
    return mapping


def _match_market_driver(
    *,
    market: PolymarketMarket,
    drivers: list[F1Driver],
    driver_map: dict[str, F1Driver],
) -> F1Driver | None:
    for candidate in [
        market.driver_a,
        (market.raw_payload or {}).get("groupItemTitle"),
        market.question,
    ]:
        if not candidate:
            continue
        normalized = _normalize_name(str(candidate))
        exact = driver_map.get(normalized)
        if exact is not None:
            return exact
    question_text = f"{market.question} {market.description or ''}".lower()
    matches_by_key: dict[str, F1Driver] = {}
    for driver in drivers:
        for value in [driver.full_name, driver.broadcast_name, driver.last_name]:
            if value and value.lower() in question_text:
                dedupe_key = (
                    str(driver.driver_number)
                    if driver.driver_number is not None
                    else _normalize_name(driver.full_name or driver.id)
                )
                matches_by_key.setdefault(dedupe_key, driver)
                break
    if len(matches_by_key) == 1:
        return next(iter(matches_by_key.values()))
    return None


def _resolve_driver_with_available_results(
    *,
    driver: F1Driver,
    drivers: list[F1Driver],
    available_driver_ids: set[str],
) -> F1Driver:
    if driver.id in available_driver_ids:
        return driver
    if driver.driver_number is None:
        return driver
    for candidate in drivers:
        if candidate.driver_number == driver.driver_number and candidate.id in available_driver_ids:
            return candidate
    return driver


def _select_entry_price_point(
    *,
    rows: list[PolymarketPriceHistory],
    window_start: datetime,
    window_end: datetime,
) -> PolymarketPriceHistory | None:
    in_window = [
        row for row in rows if window_start <= _require_utc(row.observed_at_utc) < window_end
    ]
    if in_window:
        return in_window[0]
    before_start = [row for row in rows if _require_utc(row.observed_at_utc) < window_end]
    if before_start:
        return before_start[-1]
    return None


def _entry_selection_rule(
    *,
    observed_at: datetime,
    window_start: datetime,
    window_end: datetime,
    fallback_label: str = "fallback_last_before_target_start",
) -> str:
    if window_start <= observed_at < window_end:
        return "first_observation_in_window"
    return fallback_label


def _result_gap_seconds(result: F1SessionResult) -> float | None:
    if result.gap_to_leader_seconds is not None:
        return float(result.gap_to_leader_seconds)
    if result.gap_to_leader_status == "leader":
        return 0.0
    return None


def _team_best_gap_to_leader(
    *,
    drivers: list[F1Driver],
    fp1_results: list[F1SessionResult],
) -> dict[str, float]:
    team_by_driver = {driver.id: driver.team_id for driver in drivers if driver.team_id}
    team_best: dict[str, float] = {}
    for result in fp1_results:
        if result.driver_id is None:
            continue
        team_id = team_by_driver.get(result.driver_id)
        if team_id is None:
            continue
        gap = _result_gap_seconds(result)
        if gap is None:
            continue
        current = team_best.get(team_id)
        if current is None or gap < current:
            team_best[team_id] = gap
    return team_best


def _require_utc(value: datetime | None) -> datetime:
    if value is None:
        raise ValueError("expected a timestamp value")
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _normalize_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _coalesce_spread(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return best_ask - best_bid


# ---------------------------------------------------------------------------
# Enrichment & evaluation helpers
# ---------------------------------------------------------------------------


def _enrich_snapshot_probabilities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["event_id"])].append(dict(row))

    enriched: list[dict[str, Any]] = []
    for group_rows in grouped.values():
        market_probs = _normalized_market_probabilities(group_rows)
        target_session_code = str(group_rows[0].get("target_session_code") or "")
        pace_signals = (
            _pre_race_pace_signals(group_rows, target_session_code=target_session_code)
            if target_session_code in {"R", "S"}
            else _practice_pace_signals(group_rows)
        )
        pace_probs = _softmax(pace_signals)
        market_signals = [math.log(max(prob, EPSILON)) for prob in market_probs]
        hybrid_signals = [
            market_signals[index] + (2.0 * pace_signals[index])
            for index in range(len(group_rows))
        ]
        hybrid_probs = _softmax(hybrid_signals)

        for index, row in enumerate(group_rows):
            row["market_normalized_prob"] = market_probs[index]
            row["market_signal"] = market_signals[index]
            row["market_implied_probability"] = market_probs[index]
            row["pace_signal"] = pace_signals[index]
            row["pace_probability"] = pace_probs[index]
            row["fp1_pace_signal"] = pace_signals[index]
            row["fp1_pace_probability"] = pace_probs[index]
            row["hybrid_signal"] = hybrid_signals[index]
            row["hybrid_probability"] = hybrid_probs[index]
            enriched.append(row)
    return enriched


def _normalized_market_probabilities(rows: list[dict[str, Any]]) -> list[float]:
    raw = [max(float(row["entry_yes_price"] or 0.0), 0.0) for row in rows]
    total = sum(raw)
    if total <= 0:
        return [1.0 / len(rows)] * len(rows)
    return [value / total for value in raw]


def _fp1_pace_signals(rows: list[dict[str, Any]]) -> list[float]:
    sign_specs = (
        ("fp1_position", -1.0),
        ("fp1_gap_to_leader_seconds", -1.0),
        ("fp1_teammate_gap_seconds", -1.0),
        ("fp1_lap_count", 1.0),
        ("fp1_stint_count", 1.0),
    )
    zscore_by_feature = {
        feature_name: _zscore_map(rows, feature_name)
        for feature_name, _ in sign_specs
    }
    signals: list[float] = []
    for row in rows:
        contributions: list[float] = []
        row_id = str(row["row_id"])
        for feature_name, sign in sign_specs:
            zscore = zscore_by_feature[feature_name].get(row_id)
            if zscore is None:
                continue
            contributions.append(sign * zscore)
        signals.append(sum(contributions) / len(contributions) if contributions else 0.0)
    return signals


def _practice_pace_signals(rows: list[dict[str, Any]]) -> list[float]:
    """Multi-session pace signal using the latest available FP session (FP3 > FP2 > FP1)."""
    if any(row.get("fp3_position") is not None for row in rows):
        prefix = "fp3"
    elif any(row.get("fp2_position") is not None for row in rows):
        prefix = "fp2"
    else:
        prefix = "fp1"

    sign_specs = (
        (f"{prefix}_position", -1.0),
        (f"{prefix}_gap_to_leader_seconds", -1.0),
        (f"{prefix}_teammate_gap_seconds", -1.0),
        (f"{prefix}_lap_count", 1.0),
        (f"{prefix}_stint_count", 1.0),
    )
    zscore_by_feature = {
        feature_name: _zscore_map(rows, feature_name)
        for feature_name, _ in sign_specs
    }
    signals: list[float] = []
    for row in rows:
        contributions: list[float] = []
        row_id = str(row["row_id"])
        for feature_name, sign in sign_specs:
            zscore = zscore_by_feature[feature_name].get(row_id)
            if zscore is None:
                continue
            contributions.append(sign * zscore)
        signals.append(sum(contributions) / len(contributions) if contributions else 0.0)
    return signals


def _pre_race_pace_signals(
    rows: list[dict[str, Any]],
    *,
    target_session_code: str,
) -> list[float]:
    """Weighted pre-event signal for Sprint or Race outright markets."""
    if target_session_code == "S":
        feature_weights = (
            ("fp1", 0.8),
            ("sq", 5.0),
        )
    else:
        feature_weights = (
            ("fp1", 0.3),
            ("fp2", 0.5),
            ("fp3", 0.8),
            ("q", 5.0),
        )
    sign_specs: list[tuple[str, float, float]] = []
    for prefix, weight in feature_weights:
        sign_specs.extend(
            [
                (f"{prefix}_position", -1.0, weight),
                (f"{prefix}_gap_to_leader_seconds", -1.0, weight),
                (f"{prefix}_teammate_gap_seconds", -1.0, weight),
            ]
        )
        if prefix.startswith("fp"):
            sign_specs.extend(
                [
                    (f"{prefix}_lap_count", 1.0, weight),
                    (f"{prefix}_stint_count", 1.0, weight),
                ]
            )

    zscore_by_feature = {
        feature_name: _zscore_map(rows, feature_name)
        for feature_name, _sign, _weight in sign_specs
    }
    signals: list[float] = []
    for row in rows:
        weighted_sum = 0.0
        total_weight = 0.0
        row_id = str(row["row_id"])
        for feature_name, sign, weight in sign_specs:
            zscore = zscore_by_feature[feature_name].get(row_id)
            if zscore is None:
                continue
            weighted_sum += sign * zscore * weight
            total_weight += weight
        signals.append(weighted_sum / total_weight if total_weight > 0 else 0.0)
    return signals


def _zscore_map(rows: list[dict[str, Any]], feature_name: str) -> dict[str, float | None]:
    values = [
        float(row[feature_name])
        for row in rows
        if row.get(feature_name) is not None
    ]
    if not values:
        return {str(row["row_id"]): None for row in rows}
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std = math.sqrt(variance)
    if std <= 0:
        return {
            str(row["row_id"]): 0.0 if row.get(feature_name) is not None else None
            for row in rows
        }
    return {
        str(row["row_id"]): (
            None
            if row.get(feature_name) is None
            else (float(row[feature_name]) - mean) / std
        )
        for row in rows
    }


def _softmax(values: list[float]) -> list[float]:
    if not values:
        return []
    max_value = max(values)
    exps = [math.exp(value - max_value) for value in values]
    total = sum(exps)
    if total <= 0:
        return [1.0 / len(values)] * len(values)
    return [value / total for value in exps]


def _evaluate_probability_rows(
    *,
    rows: list[dict[str, Any]],
    probability_key: str,
    price_key: str,
    min_edge: float,
) -> dict[str, Any]:
    labeled_rows = [row for row in rows if row.get("label_yes") is not None]
    labels = [int(row["label_yes"]) for row in labeled_rows]
    brier = (
        None
        if not labeled_rows
        else sum(
            (float(row[probability_key]) - label) ** 2
            for row, label in zip(labeled_rows, labels, strict=True)
        )
        / len(labeled_rows)
    )
    log_loss = (
        None
        if not labeled_rows
        else -sum(
            label * math.log(max(float(row[probability_key]), EPSILON))
            + (1 - label) * math.log(max(1 - float(row[probability_key]), EPSILON))
            for row, label in zip(labeled_rows, labels, strict=True)
        )
        / len(labeled_rows)
    )
    top1_hit = (
        None
        if not labeled_rows
        else _top_k_hit(rows=labeled_rows, probability_key=probability_key, k=1)
    )
    top3_hit = (
        None
        if not labeled_rows
        else _top_k_hit(rows=labeled_rows, probability_key=probability_key, k=3)
    )
    selected = [
        row
        for row in rows
        if float(row[probability_key]) - float(row[price_key]) >= min_edge
    ]
    labeled_selected = [row for row in selected if row.get("label_yes") is not None]
    realized_pnl = [
        (1.0 - float(row[price_key])) if int(row["label_yes"]) == 1 else -float(row[price_key])
        for row in labeled_selected
    ]
    hit_rate = (
        sum(int(row["label_yes"]) for row in labeled_selected) / len(labeled_selected)
        if labeled_selected
        else None
    )
    avg_edge = (
        sum(float(row[probability_key]) - float(row[price_key]) for row in selected) / len(selected)
        if selected
        else None
    )
    paper_ev = (
        sum(float(row[probability_key]) - float(row[price_key]) for row in selected) / len(selected)
        if selected
        else None
    )
    calibration_buckets = (
        {}
        if not labeled_rows
        else serialize_reliability_diagram(
            np.array(labels, dtype=float),
            np.array([float(row[probability_key]) for row in labeled_rows], dtype=float),
        )
    )
    return {
        "row_count": len(rows),
        "brier_score": brier,
        "log_loss": log_loss,
        "calibration_buckets": calibration_buckets,
        "top1_hit": top1_hit,
        "top3_hit": top3_hit,
        "bet_count": len(selected),
        "paper_edge_hit_rate": hit_rate,
        "average_edge": avg_edge,
        "average_paper_ev": paper_ev,
        "realized_pnl_total": sum(realized_pnl) if labeled_selected else None,
        "realized_pnl_avg": (sum(realized_pnl) / len(realized_pnl)) if realized_pnl else None,
    }


def _top_k_hit(*, rows: list[dict[str, Any]], probability_key: str, k: int) -> float:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["event_id"])].append(row)
    hits: list[float] = []
    for event_rows in grouped.values():
        ranked = sorted(event_rows, key=lambda row: float(row[probability_key]), reverse=True)
        hits.append(1.0 if any(int(row["label_yes"]) == 1 for row in ranked[:k]) else 0.0)
    return sum(hits) / len(hits) if hits else 0.0


def _quicktest_report_dir(*, root: Path, season: int, slug: str) -> Path:
    return root / "reports" / "research" / str(season) / slug


def _render_quicktest_markdown(
    report: dict[str, Any],
    *,
    title_suffix: str = "SQ Pole Quick Test",
) -> str:
    lines = [
        f"# {report['meeting']['season']} {report['meeting']['meeting_name']} {title_suffix}",
        "",
        f"- Snapshot id: `{report['snapshot_id']}`",
        f"- Snapshot type: `{report['snapshot_type']}`",
        f"- Markets: `{report['market_count']}`",
        f"- Drivers: `{report['driver_count']}`",
        f"- Rows: `{report['row_count']}`",
        f"- Min edge: `{report['min_edge']}`",
        "",
        "## Baselines",
    ]
    for model_name, payload in sorted(report["baselines"].items()):
        metrics = payload["metrics"]
        lines.append(
            f"- `{model_name}` "
            f"brier={metrics.get('brier_score')} "
            f"log_loss={metrics.get('log_loss')} "
            f"top1={metrics.get('top1_hit')} "
            f"bets={metrics.get('bet_count')} "
            f"pnl={metrics.get('realized_pnl_total')}"
        )
    lines.extend(["", "## Top Hybrid Predictions"])
    for row in report["top_hybrid_predictions"]:
        lines.append(
            f"- `{row['driver_name']}` market=`{row['market_id']}` "
            f"prob={row['hybrid_probability']:.4f} "
            f"price={row['entry_yes_price']:.4f} "
            f"edge={row['paper_edge']:.4f} "
            f"label={row['label_yes']}"
        )
    lines.extend(["", "## Selected Bets"])
    if report["selected_yes_bets"]:
        for row in report["selected_yes_bets"]:
            lines.append(
                f"- `{row['driver_name']}` "
                f"prob={row['hybrid_probability']:.4f} "
                f"price={row['entry_yes_price']:.4f} "
                f"edge={row['paper_edge']:.4f} "
                f"label={row['label_yes']}"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Notes"])
    for note in report["notes"]:
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Factory: build_snapshot
# ---------------------------------------------------------------------------


def build_snapshot(
    ctx: PipelineContext,
    config: GPConfig,
    *,
    meeting_key: int | None = None,
    season: int | None = None,
    entry_offset_min: int | None = None,
    fidelity: int | None = None,
) -> dict[str, Any]:
    """Build a feature snapshot for any GP described by *config*."""
    meeting_key = meeting_key or config.meeting_key
    season = season or config.season
    entry_offset_min = entry_offset_min if entry_offset_min is not None else config.entry_offset_min
    fidelity = fidelity if fidelity is not None else config.fidelity

    job_name = f"build-{config.short_code}-snapshot"
    definition = ensure_job_definition(
        ctx.db,
        job_name=job_name,
        source="derived",
        dataset=config.snapshot_dataset,
        description=f"Build {config.name} {config.title_suffix} feature snapshot.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "meeting_key": meeting_key,
            "season": season,
            "entry_offset_min": entry_offset_min,
            "fidelity": fidelity,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    ensure_default_feature_registry(ctx)

    if config.variant == "pre_weekend":
        return _build_pre_weekend_snapshot(
            ctx, config, run=run, meeting_key=meeting_key, season=season
        )

    return _build_session_to_target_snapshot(
        ctx,
        config,
        run=run,
        meeting_key=meeting_key,
        season=season,
        entry_offset_min=entry_offset_min,
        fidelity=fidelity,
    )


def _build_session_to_target_snapshot(
    ctx: PipelineContext,
    config: GPConfig,
    *,
    run: Any,
    meeting_key: int,
    season: int,
    entry_offset_min: int,
    fidelity: int,
) -> dict[str, Any]:
    """Build a snapshot for a source session and target-session market family."""
    (
        meeting,
        sessions_by_code,
        fp1_session,
        fp2_session,
        fp3_session,
        target_session,
    ) = _load_sessions(
        ctx,
        meeting_key=meeting_key,
        season=season,
        target_session_code=config.target_session_code,
    )
    markets = _load_target_markets(
        ctx,
        target_session=target_session,
        target_session_code=config.target_session_code,
        market_taxonomy=config.market_taxonomy,
    )
    hydrated_markets = _hydrate_missing_market_history(ctx, markets=markets, fidelity=fidelity)
    source_session = sessions_by_code.get(config.source_session_code or "FP1")
    if source_session is None:
        raise ValueError(f"Source session {config.source_session_code!r} not found")
    sq_session = sessions_by_code.get("SQ")
    q_session = (
        sessions_by_code.get("Q")
        if config.target_session_code == "R"
        else None
    )

    yes_tokens = _load_yes_tokens(ctx, market_ids=[m.id for m in markets])
    price_history = _load_price_history(
        ctx,
        market_ids=[m.id for m in markets],
        token_ids=[t.id for t in yes_tokens.values()],
    )
    trades = _load_trades(ctx, market_ids=[m.id for m in markets])
    drivers = list(ctx.db.scalars(select(F1Driver)).all())
    fp1_results = list(
        ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == fp1_session.id)
        ).all()
    )
    target_results = list(
        ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == target_session.id)
        ).all()
    )
    fp1_laps = list(
        ctx.db.scalars(select(F1Lap).where(F1Lap.session_id == fp1_session.id)).all()
    )
    fp1_stints = list(
        ctx.db.scalars(select(F1Stint).where(F1Stint.session_id == fp1_session.id)).all()
    )

    # FP2 and FP3 data (optional)
    fp2_results = (
        list(ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == fp2_session.id)
        ).all())
        if fp2_session else []
    )
    fp2_laps = (
        list(ctx.db.scalars(
            select(F1Lap).where(F1Lap.session_id == fp2_session.id)
        ).all())
        if fp2_session else []
    )
    fp2_stints = (
        list(ctx.db.scalars(
            select(F1Stint).where(F1Stint.session_id == fp2_session.id)
        ).all())
        if fp2_session else []
    )
    fp3_results = (
        list(ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == fp3_session.id)
        ).all())
        if fp3_session else []
    )
    fp3_laps = (
        list(ctx.db.scalars(
            select(F1Lap).where(F1Lap.session_id == fp3_session.id)
        ).all())
        if fp3_session else []
    )
    fp3_stints = (
        list(ctx.db.scalars(
            select(F1Stint).where(F1Stint.session_id == fp3_session.id)
        ).all())
        if fp3_session else []
    )
    sq_results = (
        list(ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == sq_session.id)
        ).all())
        if sq_session else []
    )
    q_results = (
        list(ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == q_session.id)
        ).all())
        if q_session else []
    )

    fp2_results_by_driver = {r.driver_id: r for r in fp2_results if r.driver_id is not None}
    fp3_results_by_driver = {r.driver_id: r for r in fp3_results if r.driver_id is not None}
    sq_results_by_driver = {r.driver_id: r for r in sq_results if r.driver_id is not None}
    q_results_by_driver = {r.driver_id: r for r in q_results if r.driver_id is not None}
    results_by_driver = {r.driver_id: r for r in fp1_results if r.driver_id is not None}
    source_results_by_driver = {
        "FP1": results_by_driver,
        "FP2": fp2_results_by_driver,
        "FP3": fp3_results_by_driver,
        "SQ": sq_results_by_driver,
        "Q": q_results_by_driver,
    }.get(config.source_session_code or "FP1", results_by_driver)
    all_result_driver_ids = (
        set(results_by_driver)
        | set(fp2_results_by_driver)
        | set(fp3_results_by_driver)
        | set(sq_results_by_driver)
        | set(q_results_by_driver)
    )
    source_result_reason = f"missing_{(config.source_session_code or 'fp1').lower()}_result"
    target_is_practice = config.target_session_code in {"FP1", "FP2", "FP3"}
    winner_driver_id = (
        None
        if target_is_practice
        else next(
            (r.driver_id for r in target_results if r.driver_id is not None and r.position == 1),
            None,
        )
    )
    lap_count_by_driver = _CounterLike()
    for lap in fp1_laps:
        if lap.driver_id is not None:
            lap_count_by_driver.increment(lap.driver_id)
    stint_count_by_driver = _CounterLike()
    for stint in fp1_stints:
        if stint.driver_id is not None:
            stint_count_by_driver.add_distinct(stint.driver_id, stint.stint_number)

    driver_map = _build_driver_map(drivers)
    team_best_gap = _team_best_gap_to_leader(drivers=drivers, fp1_results=fp1_results)
    fp2_team_best_gap = (
        _team_best_gap_to_leader(drivers=drivers, fp1_results=fp2_results)
        if fp2_results else {}
    )
    fp3_team_best_gap = (
        _team_best_gap_to_leader(drivers=drivers, fp1_results=fp3_results)
        if fp3_results else {}
    )
    sq_team_best_gap = (
        _team_best_gap_to_leader(drivers=drivers, fp1_results=sq_results)
        if sq_results else {}
    )
    q_team_best_gap = (
        _team_best_gap_to_leader(drivers=drivers, fp1_results=q_results)
        if q_results else {}
    )

    fp2_lap_count_by_driver = _CounterLike()
    for lap in fp2_laps:
        if lap.driver_id is not None:
            fp2_lap_count_by_driver.increment(lap.driver_id)
    fp3_lap_count_by_driver = _CounterLike()
    for lap in fp3_laps:
        if lap.driver_id is not None:
            fp3_lap_count_by_driver.increment(lap.driver_id)
    fp2_stint_count_by_driver = _CounterLike()
    for stint in fp2_stints:
        if stint.driver_id is not None:
            fp2_stint_count_by_driver.add_distinct(stint.driver_id, stint.stint_number)
    fp3_stint_count_by_driver = _CounterLike()
    for stint in fp3_stints:
        if stint.driver_id is not None:
            fp3_stint_count_by_driver.add_distinct(stint.driver_id, stint.stint_number)

    entry_floor = _require_utc(source_session.date_end_utc) + timedelta(minutes=entry_offset_min)
    target_start = _require_utc(target_session.date_start_utc)
    rows: list[dict[str, Any]] = []
    exclusion_reasons = _CounterLike()
    session_result_pairs = 0

    target_code_lower = config.target_session_code.lower()
    fallback_label = f"fallback_last_before_{target_code_lower}_start"

    for market in markets:
        token = yes_tokens.get(market.id)
        if token is None:
            exclusion_reasons.increment("missing_yes_token")
            continue
        driver = _match_market_driver(market=market, drivers=drivers, driver_map=driver_map)
        if driver is None:
            exclusion_reasons.increment("missing_driver_match")
            continue
        driver = _resolve_driver_with_available_results(
            driver=driver,
            drivers=drivers,
            available_driver_ids=all_result_driver_ids,
        )
        source_result = source_results_by_driver.get(driver.id)
        if source_result is None:
            exclusion_reasons.increment(source_result_reason)
            continue
        fp1_result = results_by_driver.get(driver.id)
        entry = _select_entry_price_point(
            rows=price_history.get(market.id, []),
            window_start=entry_floor,
            window_end=target_start,
        )
        if entry is None:
            exclusion_reasons.increment(f"missing_pre_{target_code_lower}_price_history")
            continue

        driver_gap = _result_gap_seconds(fp1_result) if fp1_result is not None else None
        team_gap = team_best_gap.get(driver.team_id or "") if fp1_result is not None else None
        teammate_gap = (
            None if driver_gap is None or team_gap is None else driver_gap - team_gap
        )

        # FP2 intermediate values
        _fp2_r = fp2_results_by_driver.get(driver.id) if fp2_results else None
        _fp2_gap = _result_gap_seconds(_fp2_r) if _fp2_r is not None else None
        _fp2_tg = fp2_team_best_gap.get(driver.team_id or "") if fp2_results else None
        _fp2_teammate = (
            (_fp2_gap - _fp2_tg)
            if _fp2_gap is not None and _fp2_tg is not None
            else None
        )

        # FP3 intermediate values
        _fp3_r = fp3_results_by_driver.get(driver.id) if fp3_results else None
        _fp3_gap = _result_gap_seconds(_fp3_r) if _fp3_r is not None else None
        _fp3_tg = fp3_team_best_gap.get(driver.team_id or "") if fp3_results else None
        _fp3_teammate = (
            (_fp3_gap - _fp3_tg)
            if _fp3_gap is not None and _fp3_tg is not None
            else None
        )
        _sq_r = sq_results_by_driver.get(driver.id) if sq_results else None
        _sq_gap = _result_gap_seconds(_sq_r) if _sq_r is not None else None
        _sq_tg = sq_team_best_gap.get(driver.team_id or "") if sq_results else None
        _sq_teammate = (
            (_sq_gap - _sq_tg)
            if _sq_gap is not None and _sq_tg is not None
            else None
        )
        _q_r = q_results_by_driver.get(driver.id) if q_results else None
        _q_gap = _result_gap_seconds(_q_r) if _q_r is not None else None
        _q_tg = q_team_best_gap.get(driver.team_id or "") if q_results else None
        _q_teammate = (
            (_q_gap - _q_tg)
            if _q_gap is not None and _q_tg is not None
            else None
        )

        # Best-practice aggregates
        _all_gaps = [g for g in [driver_gap, _fp2_gap, _fp3_gap] if g is not None]
        _fp1_pos = fp1_result.position if fp1_result is not None else None
        _fp2_pos = _fp2_r.position if _fp2_r is not None else None
        _fp3_pos = _fp3_r.position if _fp3_r is not None else None
        _all_positions = [p for p in [_fp1_pos, _fp2_pos, _fp3_pos] if p is not None]

        pre_entry_trades = [
            trade
            for trade in trades.get(market.id, [])
            if _require_utc(trade.trade_timestamp_utc) <= _require_utc(entry.observed_at_utc)
        ]
        last_trade_ts = (
            None
            if not pre_entry_trades
            else max(_require_utc(t.trade_timestamp_utc) for t in pre_entry_trades)
        )
        last_trade_age_seconds = (
            None
            if last_trade_ts is None
            else (_require_utc(entry.observed_at_utc) - last_trade_ts).total_seconds()
        )
        session_result_pairs += 1
        rows.append(
            {
                "row_id": stable_uuid(config.snapshot_type, market.id),
                "meeting_key": meeting.meeting_key,
                "meeting_id": meeting.id,
                "meeting_name": meeting.meeting_name,
                "event_id": market.event_id,
                "market_id": market.id,
                "market_slug": market.slug,
                "market_question": market.question,
                "market_taxonomy": market.taxonomy,
                "source_session_code": config.source_session_code,
                "target_session_code": config.target_session_code,
                "token_id": token.id,
                "driver_id": driver.id,
                "driver_name": (
                    driver.full_name or driver.broadcast_name or driver.last_name or driver.id
                ),
                "driver_last_name": driver.last_name,
                "team_id": driver.team_id,
                "fp1_session_id": fp1_session.id,
                f"{target_code_lower}_session_id": target_session.id,
                "fp1_end_utc": _require_utc(fp1_session.date_end_utc),
                f"{target_code_lower}_start_utc": target_start,
                "entry_window_start_utc": entry_floor,
                "entry_observed_at_utc": _require_utc(entry.observed_at_utc),
                "entry_selection_rule": _entry_selection_rule(
                    observed_at=_require_utc(entry.observed_at_utc),
                    window_start=entry_floor,
                    window_end=target_start,
                    fallback_label=fallback_label,
                ),
                "entry_yes_price": entry.price,
                "entry_midpoint": entry.midpoint,
                "entry_best_bid": entry.best_bid,
                "entry_best_ask": entry.best_ask,
                "entry_spread": _coalesce_spread(entry.best_bid, entry.best_ask),
                "trade_count_pre_entry": len(pre_entry_trades),
                "last_trade_age_seconds": last_trade_age_seconds,
                "fp1_position": _fp1_pos,
                "fp1_result_time_seconds": (
                    fp1_result.result_time_seconds if fp1_result is not None else None
                ),
                "fp1_gap_to_leader_seconds": driver_gap,
                "fp1_teammate_gap_seconds": teammate_gap,
                "fp1_team_best_gap_to_leader_seconds": team_gap,
                "fp1_lap_count": (
                    lap_count_by_driver.counts.get(driver.id, 0)
                    if fp1_result is not None
                    else None
                ),
                "fp1_stint_count": (
                    len(stint_count_by_driver.distinct_counts.get(driver.id, set()))
                    if fp1_result is not None
                    else None
                ),
                # FP2 features
                "fp2_position": _fp2_pos,
                "fp2_result_time_seconds": (
                    _fp2_r.result_time_seconds if _fp2_r is not None else None
                ),
                "fp2_gap_to_leader_seconds": _fp2_gap,
                "fp2_teammate_gap_seconds": _fp2_teammate,
                "fp2_team_best_gap_to_leader_seconds": _fp2_tg,
                "fp2_lap_count": (
                    fp2_lap_count_by_driver.counts.get(driver.id, 0)
                    if fp2_results else None
                ),
                "fp2_stint_count": (
                    len(fp2_stint_count_by_driver.distinct_counts.get(driver.id, set()))
                    if fp2_results else None
                ),
                # FP3 features
                "fp3_position": _fp3_pos,
                "fp3_result_time_seconds": (
                    _fp3_r.result_time_seconds if _fp3_r is not None else None
                ),
                "fp3_gap_to_leader_seconds": _fp3_gap,
                "fp3_teammate_gap_seconds": _fp3_teammate,
                "fp3_team_best_gap_to_leader_seconds": _fp3_tg,
                "fp3_lap_count": (
                    fp3_lap_count_by_driver.counts.get(driver.id, 0)
                    if fp3_results else None
                ),
                "fp3_stint_count": (
                    len(fp3_stint_count_by_driver.distinct_counts.get(driver.id, set()))
                    if fp3_results else None
                ),
                # SQ features
                "sq_position": _sq_r.position if _sq_r is not None else None,
                "sq_result_time_seconds": (
                    _sq_r.result_time_seconds if _sq_r is not None else None
                ),
                "sq_gap_to_leader_seconds": _sq_gap,
                "sq_teammate_gap_seconds": _sq_teammate,
                "sq_team_best_gap_to_leader_seconds": _sq_tg,
                # Q features
                "q_position": _q_r.position if _q_r is not None else None,
                "q_result_time_seconds": (
                    _q_r.result_time_seconds if _q_r is not None else None
                ),
                "q_gap_to_leader_seconds": _q_gap,
                "q_teammate_gap_seconds": _q_teammate,
                "q_team_best_gap_to_leader_seconds": _q_tg,
                # Best practice features (best across all available FP sessions)
                "best_practice_gap_to_leader_seconds": (
                    min(_all_gaps) if _all_gaps else None
                ),
                "best_practice_position": (
                    min(_all_positions) if _all_positions else None
                ),
                "latest_fp_number": 3 if fp3_results else (2 if fp2_results else 1),
                "latest_pre_race_session_code": (
                    "Q"
                    if q_results
                    else (
                        "SQ"
                        if sq_results
                        else ("FP3" if fp3_results else ("FP2" if fp2_results else "FP1"))
                    )
                ),
                "label_yes": (
                    None
                    if winner_driver_id is None
                    else 1 if winner_driver_id == driver.id else 0
                ),
            }
        )

    if not rows:
        error_message = (
            f"Could not build {config.name} quick-test snapshot; "
            f"exclusions={dict(sorted(exclusion_reasons.counts.items()))}"
        )
        finish_job_run(ctx.db, run, status="failed", records_written=0, error_message=error_message)
        raise ValueError(error_message)

    # Enrich with driver sector profiles and track affinity
    rows = enrich_rows_with_driver_profiles(
        rows,
        db=ctx.db,
        circuit_key=getattr(meeting, "circuit_key", None),
        circuit_short_name=getattr(meeting, "circuit_short_name", None),
        as_of_utc=entry_floor,
    )

    snapshot_id = stable_uuid(config.snapshot_type, meeting_key, season, entry_offset_min, "v1")
    silver_object = ctx.lake.write_silver_object(
        config.snapshot_dataset,
        rows,
        partition={"season": str(meeting.season), "meeting_key": str(meeting.meeting_key)},
    )
    if silver_object is None:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message="silver snapshot write returned no object",
        )
        raise ValueError("silver snapshot write returned no object")
    record_lake_object_manifest(
        ctx.db,
        object_ref=silver_object,
        job_run_id=run.id,
        metadata_json={"snapshot_type": config.snapshot_type, "meeting_key": meeting_key},
    )

    version = silver_object.checksum[:16]
    dataset_version_id = stable_uuid("dataset-version", config.snapshot_dataset, version)
    upsert_records(
        ctx.db,
        DatasetVersionManifest,
        [
            {
                "id": dataset_version_id,
                "dataset_name": config.snapshot_dataset,
                "storage_tier": silver_object.storage_tier,
                "version": version,
                "manifest_json": {
                    "snapshot_id": snapshot_id,
                    "meeting_key": meeting_key,
                    "row_count": len(rows),
                    "object_path": str(silver_object.path),
                    "checksum": silver_object.checksum,
                },
                "created_at": utc_now(),
            }
        ],
        conflict_columns=["dataset_name", "version"],
    )
    upsert_records(
        ctx.db,
        FeatureSnapshot,
        [
            {
                "id": snapshot_id,
                "market_id": None,
                "session_id": target_session.id,
                "as_of_ts": entry_floor,
                "snapshot_type": config.snapshot_type,
                "feature_version": "v1",
                "storage_path": str(silver_object.path),
                "source_cutoffs": {
                    "meeting_key": meeting.meeting_key,
                    "meeting_name": meeting.meeting_name,
                    "fp1_session_key": fp1_session.session_key,
                    f"{target_code_lower}_session_key": target_session.session_key,
                    "fp1_end_utc": _require_utc(fp1_session.date_end_utc).isoformat(),
                    f"{target_code_lower}_start_utc": target_start.isoformat(),
                    "entry_offset_min": entry_offset_min,
                    "entry_selection_policy": (
                        "first price observation in window, "
                        f"fallback to last observation before {config.target_session_code} start"
                    ),
                    "excluded_markets": dict(sorted(exclusion_reasons.counts.items())),
                },
                "row_count": len(rows),
            }
        ],
    )
    upsert_records(
        ctx.db,
        SnapshotRunManifest,
        [
            {
                "id": stable_uuid("snapshot-run", snapshot_id, run.id),
                "feature_snapshot_id": snapshot_id,
                "run_name": f"build-{config.short_code}-snapshot",
                "source_cutoffs": {
                    "entry_offset_min": entry_offset_min,
                    "hydrated_markets": hydrated_markets,
                    "session_result_pairs": session_result_pairs,
                },
                "dataset_version_id": dataset_version_id,
                "created_at": utc_now(),
            }
        ],
    )

    finish_job_run(ctx.db, run, status="completed", records_written=len(rows))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "snapshot_id": snapshot_id,
        "meeting_key": meeting.meeting_key,
        "row_count": len(rows),
        "markets_considered": len(markets),
        "markets_hydrated": hydrated_markets,
        "excluded_markets": dict(sorted(exclusion_reasons.counts.items())),
        "storage_path": str(silver_object.path),
    }


def _build_pre_weekend_snapshot(
    ctx: PipelineContext,
    config: GPConfig,
    *,
    run: Any,
    meeting_key: int,
    season: int,
) -> dict[str, Any]:
    """Pre-weekend snapshot: uses historical form instead of live FP1."""
    meeting = ctx.db.scalar(
        select(F1Meeting).where(
            F1Meeting.meeting_key == meeting_key, F1Meeting.season == season
        )
    )
    if meeting is None:
        raise ValueError(f"meeting_key={meeting_key} not found for season={season}")

    q_session = ctx.db.scalar(
        select(F1Session).where(
            F1Session.meeting_id == meeting.id, F1Session.session_code == "Q"
        )
    )
    if q_session is None:
        raise ValueError(f"No Q session found for meeting_key={meeting_key}")

    fp1_session = ctx.db.scalar(
        select(F1Session).where(
            F1Session.meeting_id == meeting.id, F1Session.session_code == "FP1"
        )
    )
    if fp1_session is None:
        raise ValueError(f"No FP1 session found for meeting_key={meeting_key}")

    drivers = list(ctx.db.scalars(select(F1Driver)).all())
    driver_map = _build_driver_map(drivers)
    q_markets = _load_target_markets(
        ctx,
        target_session=q_session,
        target_session_code="Q",
        market_taxonomy="driver_pole_position",
    )

    yes_tokens = _load_yes_tokens(ctx, market_ids=[m.id for m in q_markets])
    price_history = _load_price_history(
        ctx,
        market_ids=[m.id for m in q_markets],
        token_ids=[t.id for t in yes_tokens.values()],
    )
    trades = _load_trades(ctx, market_ids=[m.id for m in q_markets])

    q_start = _require_utc(q_session.date_start_utc)
    entry_floor = q_start - timedelta(days=7)

    # Try FP1 results first; if unavailable (pre-weekend), fall back to the
    # most recent qualifying results from prior meetings in the same season.
    form_results = list(
        ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == fp1_session.id)
        ).all()
    )
    if not form_results:
        # Fall back to the most recent prior-season Q session with results
        prior_q_session = ctx.db.scalar(
            select(F1Session)
            .join(F1Meeting, F1Meeting.id == F1Session.meeting_id)
            .where(
                F1Meeting.season == season,
                F1Session.session_code.in_(["Q", "SQ"]),
                F1Session.date_start_utc < fp1_session.date_start_utc,
            )
            .order_by(F1Session.date_start_utc.desc())
        )
        if prior_q_session is not None:
            form_results = list(
                ctx.db.scalars(
                    select(F1SessionResult).where(
                        F1SessionResult.session_id == prior_q_session.id
                    )
                ).all()
            )
    form_by_driver: dict[str, F1SessionResult] = {}
    for fr in form_results:
        if fr.driver_id is not None:
            form_by_driver[fr.driver_id] = fr
    form_driver_ids = set(form_by_driver)

    q_results = list(
        ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == q_session.id)
        ).all()
    )
    q_winner_driver_id = next(
        (r.driver_id for r in q_results if r.driver_id is not None and r.position == 1),
        None,
    )

    rows: list[dict[str, Any]] = []
    exclusion_reasons = _CounterLike()

    for market in q_markets:
        token = yes_tokens.get(market.id)
        if token is None:
            exclusion_reasons.increment("missing_yes_token")
            continue
        driver = _match_market_driver(market=market, drivers=drivers, driver_map=driver_map)
        if driver is None:
            exclusion_reasons.increment("missing_driver_match")
            continue
        driver = _resolve_driver_with_available_results(
            driver=driver,
            drivers=drivers,
            available_driver_ids=form_driver_ids,
        )
        form_result = form_by_driver.get(driver.id)
        if form_result is None:
            exclusion_reasons.increment("missing_form_result")
            continue
        entry = _select_entry_price_point(
            rows=price_history.get(market.id, []),
            window_start=entry_floor,
            window_end=q_start,
        )
        if entry is None:
            exclusion_reasons.increment("missing_pre_q_price_history")
            continue

        form_gap = form_result.gap_to_leader_seconds or 0.0
        pre_entry_trades = [
            trade
            for trade in trades.get(market.id, [])
            if _require_utc(trade.trade_timestamp_utc) <= _require_utc(entry.observed_at_utc)
        ]
        last_trade_ts = (
            None
            if not pre_entry_trades
            else max(_require_utc(t.trade_timestamp_utc) for t in pre_entry_trades)
        )
        last_trade_age_seconds = (
            None
            if last_trade_ts is None
            else (_require_utc(entry.observed_at_utc) - last_trade_ts).total_seconds()
        )
        rows.append(
            {
                "row_id": stable_uuid(config.snapshot_type, market.id),
                "meeting_key": meeting.meeting_key,
                "meeting_id": meeting.id,
                "meeting_name": meeting.meeting_name,
                "event_id": market.event_id,
                "market_id": market.id,
                "market_slug": market.slug,
                "market_question": market.question,
                "market_taxonomy": market.taxonomy,
                "token_id": token.id,
                "driver_id": driver.id,
                "driver_name": (
                    driver.full_name or driver.broadcast_name or driver.last_name or driver.id
                ),
                "driver_last_name": driver.last_name,
                "team_id": driver.team_id,
                "q_session_id": q_session.id,
                "q_start_utc": q_start,
                "entry_window_start_utc": entry_floor,
                "entry_observed_at_utc": _require_utc(entry.observed_at_utc),
                "entry_selection_rule": _entry_selection_rule(
                    observed_at=_require_utc(entry.observed_at_utc),
                    window_start=entry_floor,
                    window_end=q_start,
                    fallback_label="fallback_last_before_q_start",
                ),
                "entry_yes_price": entry.price,
                "entry_midpoint": entry.midpoint,
                "entry_best_bid": entry.best_bid,
                "entry_best_ask": entry.best_ask,
                "entry_spread": _coalesce_spread(entry.best_bid, entry.best_ask),
                "trade_count_pre_entry": len(pre_entry_trades),
                "last_trade_age_seconds": last_trade_age_seconds,
                "fp1_position": form_result.position,
                "fp1_result_time_seconds": form_result.result_time_seconds,
                "fp1_gap_to_leader_seconds": form_gap,
                "fp1_teammate_gap_seconds": None,
                "fp1_team_best_gap_to_leader_seconds": None,
                "fp1_lap_count": form_result.number_of_laps or 0,
                "fp1_stint_count": 0,
                "label_yes": 1 if q_winner_driver_id == driver.id else 0,
            }
        )

    if not rows:
        error_message = (
            f"Could not build {config.name} pre-weekend snapshot; "
            f"exclusions={dict(sorted(exclusion_reasons.counts.items()))}"
        )
        finish_job_run(ctx.db, run, status="failed", records_written=0, error_message=error_message)
        raise ValueError(error_message)

    rows = enrich_rows_with_driver_profiles(
        rows,
        db=ctx.db,
        circuit_key=getattr(meeting, "circuit_key", None),
        circuit_short_name=getattr(meeting, "circuit_short_name", None),
        as_of_utc=entry_floor,
    )
    enriched_rows = _enrich_snapshot_probabilities(rows)

    snapshot_id = stable_uuid(config.snapshot_type, meeting_key, season, "v1")
    silver_object = ctx.lake.write_silver_object(
        config.snapshot_dataset,
        enriched_rows,
        partition={"season": str(meeting.season), "meeting_key": str(meeting.meeting_key)},
    )
    if silver_object is None:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message="silver snapshot write returned no object",
        )
        raise ValueError("silver snapshot write returned no object")
    record_lake_object_manifest(
        ctx.db,
        object_ref=silver_object,
        job_run_id=run.id,
        metadata_json={"snapshot_type": config.snapshot_type, "meeting_key": meeting_key},
    )

    version = silver_object.checksum[:16]
    dataset_version_id = stable_uuid("dataset-version", config.snapshot_dataset, version)
    upsert_records(
        ctx.db,
        DatasetVersionManifest,
        [
            {
                "id": dataset_version_id,
                "dataset_name": config.snapshot_dataset,
                "storage_tier": silver_object.storage_tier,
                "version": version,
                "manifest_json": {
                    "snapshot_id": snapshot_id,
                    "meeting_key": meeting_key,
                    "row_count": len(enriched_rows),
                    "object_path": str(silver_object.path),
                    "checksum": silver_object.checksum,
                },
                "created_at": utc_now(),
            }
        ],
        conflict_columns=["dataset_name", "version"],
    )
    upsert_records(
        ctx.db,
        FeatureSnapshot,
        [
            {
                "id": snapshot_id,
                "market_id": None,
                "session_id": q_session.id,
                "as_of_ts": entry_floor,
                "snapshot_type": config.snapshot_type,
                "feature_version": "v1",
                "storage_path": str(silver_object.path),
                "source_cutoffs": {
                    "meeting_key": meeting.meeting_key,
                    "meeting_name": meeting.meeting_name,
                    "approach": "pre_weekend_form",
                    "q_start_utc": q_start.isoformat(),
                    "exclusion_reasons": dict(sorted(exclusion_reasons.counts.items())),
                },
                "row_count": len(enriched_rows),
            }
        ],
    )
    upsert_records(
        ctx.db,
        SnapshotRunManifest,
        [
            {
                "id": stable_uuid("snapshot-run", snapshot_id, run.id),
                "feature_snapshot_id": snapshot_id,
                "run_name": f"build-{config.short_code}-snapshot",
                "source_cutoffs": {
                    "meeting_key": meeting_key,
                    "approach": "pre_weekend_form",
                    "exclusion_reasons": dict(sorted(exclusion_reasons.counts.items())),
                },
                "dataset_version_id": dataset_version_id,
                "created_at": utc_now(),
            }
        ],
    )

    finish_job_run(ctx.db, run, status="completed", records_written=len(enriched_rows))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "snapshot_id": snapshot_id,
        "row_count": len(enriched_rows),
    }


# ---------------------------------------------------------------------------
# Factory: run_baseline
# ---------------------------------------------------------------------------


def run_baseline(
    ctx: PipelineContext,
    config: GPConfig,
    *,
    snapshot_id: str,
    min_edge: float | None = None,
) -> dict[str, Any]:
    """Run the three baseline models for any GP described by *config*."""
    min_edge = min_edge if min_edge is not None else config.min_edge

    job_name = f"run-{config.short_code}-baseline"
    definition = ensure_job_definition(
        ctx.db,
        job_name=job_name,
        source="derived",
        dataset="model_predictions",
        description=f"Run {config.name} {config.title_suffix} baseline models.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"snapshot_id": snapshot_id, "min_edge": min_edge},
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "snapshot_id": snapshot_id}

    snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
    if snapshot is None or snapshot.storage_path is None:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"snapshot_id={snapshot_id} not found",
        )
        raise ValueError(f"snapshot_id={snapshot_id} not found")

    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    if not rows:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"snapshot_id={snapshot_id} contains no rows",
        )
        raise ValueError(f"snapshot_id={snapshot_id} contains no rows")

    enriched_rows = _enrich_snapshot_probabilities(rows)
    model_run_records: list[dict[str, Any]] = []
    prediction_records: list[dict[str, Any]] = []
    metrics_summary: dict[str, dict[str, Any]] = {}

    baselines = (
        (config.baseline_names[0], "market_implied_probability", "market_signal"),
        (config.baseline_names[1], "pace_probability", "pace_signal"),
        (config.baseline_names[2], "hybrid_probability", "hybrid_signal"),
    )

    for baseline_name, probability_key, raw_score_key in baselines:
        model_run_id = stable_uuid("model-run", snapshot_id, baseline_name)
        metrics = _evaluate_probability_rows(
            rows=enriched_rows,
            probability_key=probability_key,
            price_key="entry_yes_price",
            min_edge=min_edge,
        )
        metrics_summary[baseline_name] = metrics
        model_run_records.append(
            {
                "id": model_run_id,
                "stage": config.baseline_stage,
                "model_family": "baseline",
                "model_name": baseline_name,
                "dataset_version": snapshot.feature_version,
                "feature_snapshot_id": snapshot.id,
                "test_start": snapshot.as_of_ts,
                "test_end": snapshot.as_of_ts,
                "config_json": {
                    "snapshot_type": snapshot.snapshot_type,
                    "min_edge": min_edge,
                    "probability_key": probability_key,
                },
                "metrics_json": metrics,
                "artifact_uri": snapshot.storage_path,
                "created_at": utc_now(),
            }
        )
        for row in enriched_rows:
            probability_yes = float(row[probability_key])
            prediction_records.append(
                {
                    "id": stable_uuid("prediction", model_run_id, row["market_id"]),
                    "model_run_id": model_run_id,
                    "market_id": row["market_id"],
                    "token_id": row["token_id"],
                    "as_of_ts": row["entry_observed_at_utc"],
                    "probability_yes": probability_yes,
                    "probability_no": 1.0 - probability_yes,
                    "raw_score": float(row[raw_score_key]),
                    "calibration_version": "none",
                    "explanation_json": {
                        "driver_name": row["driver_name"],
                        "event_id": row["event_id"],
                        "entry_yes_price": row["entry_yes_price"],
                        "label_yes": row["label_yes"],
                    },
                }
            )

    upsert_records(ctx.db, ModelRun, model_run_records)
    upsert_records(ctx.db, ModelPrediction, prediction_records)
    finish_job_run(ctx.db, run, status="completed", records_written=len(prediction_records))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "snapshot_id": snapshot_id,
        "model_runs": [r["id"] for r in model_run_records],
        "metrics_summary": metrics_summary,
    }


# ---------------------------------------------------------------------------
# Factory: generate_report
# ---------------------------------------------------------------------------


def generate_report(
    ctx: PipelineContext,
    config: GPConfig,
    *,
    snapshot_id: str,
    report_slug: str | None = None,
    min_edge: float | None = None,
) -> dict[str, Any]:
    """Write a quicktest research report for any GP described by *config*."""
    min_edge = min_edge if min_edge is not None else config.min_edge

    job_name = f"report-{config.short_code}-quicktest"
    definition = ensure_job_definition(
        ctx.db,
        job_name=job_name,
        source="derived",
        dataset="research_report",
        description=f"Write {config.name} {config.title_suffix} research report.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "snapshot_id": snapshot_id,
            "report_slug": report_slug,
            "min_edge": min_edge,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "snapshot_id": snapshot_id}

    snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
    if snapshot is None or snapshot.storage_path is None:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"snapshot_id={snapshot_id} not found",
        )
        raise ValueError(f"snapshot_id={snapshot_id} not found")

    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    if not rows:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"snapshot_id={snapshot_id} contains no rows",
        )
        raise ValueError(f"snapshot_id={snapshot_id} contains no rows")

    meeting_key = int(rows[0]["meeting_key"])
    meeting = ctx.db.scalar(select(F1Meeting).where(F1Meeting.meeting_key == meeting_key))
    if meeting is None:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"meeting_key={meeting_key} not found",
        )
        raise ValueError(f"meeting_key={meeting_key} not found")

    model_runs = ctx.db.scalars(
        select(ModelRun)
        .where(
            ModelRun.feature_snapshot_id == snapshot_id,
            ModelRun.stage == config.baseline_stage,
        )
        .order_by(ModelRun.model_name.asc())
    ).all()
    if not model_runs:
        finish_job_run(
            ctx.db, run, status="failed", records_written=0,
            error_message=f"no model runs found for snapshot_id={snapshot_id}",
        )
        raise ValueError(f"no model runs found for snapshot_id={snapshot_id}")

    predictions = ctx.db.scalars(
        select(ModelPrediction).where(
            ModelPrediction.model_run_id.in_([mr.id for mr in model_runs])
        )
    ).all()
    predictions_by_model: dict[str, list[ModelPrediction]] = defaultdict(list)
    for prediction in predictions:
        predictions_by_model[prediction.model_run_id].append(prediction)

    baselines = {
        mr.model_name: {"model_run_id": mr.id, "metrics": mr.metrics_json or {}}
        for mr in model_runs
    }
    hybrid_run = next((mr for mr in model_runs if mr.model_name == "hybrid"), None)
    hybrid_predictions = [] if hybrid_run is None else predictions_by_model.get(hybrid_run.id, [])
    hybrid_prediction_by_market = {
        r.market_id: r for r in hybrid_predictions if r.market_id
    }

    ranked_hybrid = sorted(
        [
            {
                "market_id": row["market_id"],
                "driver_name": row["driver_name"],
                "entry_yes_price": row["entry_yes_price"],
                "label_yes": row["label_yes"],
                "hybrid_probability": hybrid_prediction_by_market[row["market_id"]].probability_yes,
                "paper_edge": hybrid_prediction_by_market[row["market_id"]].probability_yes
                - float(row["entry_yes_price"]),
            }
            for row in rows
            if row["market_id"] in hybrid_prediction_by_market
        ],
        key=lambda item: item["hybrid_probability"],
        reverse=True,
    )
    selected_bets = [row for row in ranked_hybrid if row["paper_edge"] >= min_edge]

    report = {
        "generated_at": utc_now().isoformat(),
        "snapshot_id": snapshot_id,
        "snapshot_type": snapshot.snapshot_type,
        "meeting": {
            "meeting_key": meeting.meeting_key,
            "meeting_name": meeting.meeting_name,
            "season": meeting.season,
        },
        "row_count": len(rows),
        "market_count": len({row["market_id"] for row in rows}),
        "driver_count": len({row["driver_id"] for row in rows}),
        "source_cutoffs": snapshot.source_cutoffs,
        "min_edge": min_edge,
        "baselines": baselines,
        "top_hybrid_predictions": ranked_hybrid[:5],
        "selected_yes_bets": selected_bets[:10],
        "notes": list(config.notes),
    }

    slug = report_slug or config.report_slug
    report_dir = _quicktest_report_dir(
        root=ctx.settings.data_root, season=meeting.season, slug=slug
    )
    ensure_dir(report_dir)
    (report_dir / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    (report_dir / "summary.md").write_text(
        _render_quicktest_markdown(report, title_suffix=config.title_suffix), encoding="utf-8"
    )

    finish_job_run(ctx.db, run, status="completed", records_written=len(rows))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "snapshot_id": snapshot_id,
        "report_dir": str(report_dir),
        "baseline_count": len(model_runs),
        "selected_bets": len(selected_bets),
    }
