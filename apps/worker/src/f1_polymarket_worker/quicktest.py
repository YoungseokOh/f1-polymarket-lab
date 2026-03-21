"""Backward-compatible wrapper functions for the quicktest pipeline.

All logic now lives in :mod:`f1_polymarket_worker.gp_registry`.  The public
functions here delegate to the three generic factory functions so that CLI,
tests, and ``run_backtest_2gp.py`` continue to work without changes.
"""

from __future__ import annotations

from typing import Any

from f1_polymarket_worker.gp_registry import (
    _enrich_snapshot_probabilities as _enrich_snapshot_probabilities,
)
from f1_polymarket_worker.gp_registry import (
    build_snapshot,
    generate_report,
    get_gp_config,
    run_baseline,
)
from f1_polymarket_worker.pipeline import PipelineContext

# Re-export constants for backward compat
_CHINA = get_gp_config("china")
_AUS = get_gp_config("aus")
_JAPAN_PRE = get_gp_config("japan_pre")
_JAPAN_FP1 = get_gp_config("japan_fp1")

CHINA_DEFAULT_MEETING_KEY = _CHINA.meeting_key
CHINA_DEFAULT_SEASON = _CHINA.season
CHINA_SNAPSHOT_TYPE = _CHINA.snapshot_type
CHINA_SNAPSHOT_DATASET = _CHINA.snapshot_dataset
CHINA_BASELINE_STAGE = _CHINA.baseline_stage
CHINA_BASELINE_NAMES = _CHINA.baseline_names
CHINA_REPORT_SLUG = _CHINA.report_slug
CHINA_MIN_EDGE = _CHINA.min_edge

AUS_DEFAULT_MEETING_KEY = _AUS.meeting_key
AUS_DEFAULT_SEASON = _AUS.season
AUS_SNAPSHOT_TYPE = _AUS.snapshot_type
AUS_SNAPSHOT_DATASET = _AUS.snapshot_dataset
AUS_BASELINE_STAGE = _AUS.baseline_stage
AUS_BASELINE_NAMES = _AUS.baseline_names
AUS_REPORT_SLUG = _AUS.report_slug
AUS_MIN_EDGE = _AUS.min_edge

JAPAN_DEFAULT_MEETING_KEY = _JAPAN_PRE.meeting_key
JAPAN_DEFAULT_SEASON = _JAPAN_PRE.season
JAPAN_SNAPSHOT_TYPE = _JAPAN_PRE.snapshot_type
JAPAN_SNAPSHOT_DATASET = _JAPAN_PRE.snapshot_dataset
JAPAN_BASELINE_STAGE = _JAPAN_PRE.baseline_stage
JAPAN_BASELINE_NAMES = _JAPAN_PRE.baseline_names
JAPAN_REPORT_SLUG = _JAPAN_PRE.report_slug
JAPAN_MIN_EDGE = _JAPAN_PRE.min_edge

JAPAN_FP1_SNAPSHOT_TYPE = _JAPAN_FP1.snapshot_type
JAPAN_FP1_SNAPSHOT_DATASET = _JAPAN_FP1.snapshot_dataset
JAPAN_FP1_BASELINE_STAGE = _JAPAN_FP1.baseline_stage
JAPAN_FP1_BASELINE_NAMES = _JAPAN_FP1.baseline_names
JAPAN_FP1_REPORT_SLUG = _JAPAN_FP1.report_slug
JAPAN_FP1_MIN_EDGE = _JAPAN_FP1.min_edge


EPSILON = 1e-6


# ---------------------------------------------------------------------------
# Build wrappers
# ---------------------------------------------------------------------------


def build_china_fp1_to_sq_snapshot(
    ctx: PipelineContext,
    *,
    meeting_key: int = CHINA_DEFAULT_MEETING_KEY,
    season: int = CHINA_DEFAULT_SEASON,
    entry_offset_min: int = 10,
    fidelity: int = 60,
) -> dict[str, Any]:
    return build_snapshot(
        ctx,
        _CHINA,
        meeting_key=meeting_key,
        season=season,
        entry_offset_min=entry_offset_min,
        fidelity=fidelity,
    )


def build_aus_fp1_to_q_snapshot(
    ctx: PipelineContext,
    *,
    meeting_key: int = AUS_DEFAULT_MEETING_KEY,
    season: int = AUS_DEFAULT_SEASON,
    entry_offset_min: int = 10,
    fidelity: int = 60,
) -> dict[str, Any]:
    return build_snapshot(
        ctx,
        _AUS,
        meeting_key=meeting_key,
        season=season,
        entry_offset_min=entry_offset_min,
        fidelity=fidelity,
    )


def build_japan_pre_weekend_snapshot(
    ctx: PipelineContext,
    *,
    meeting_key: int = JAPAN_DEFAULT_MEETING_KEY,
    season: int = JAPAN_DEFAULT_SEASON,
    entry_offset_min: int = 10,
    fidelity: int = 60,
) -> dict[str, Any]:
    return build_snapshot(
        ctx,
        _JAPAN_PRE,
        meeting_key=meeting_key,
        season=season,
        entry_offset_min=entry_offset_min,
        fidelity=fidelity,
    )


def build_japan_fp1_to_q_snapshot(
    ctx: PipelineContext,
    *,
    meeting_key: int = JAPAN_DEFAULT_MEETING_KEY,
    season: int = JAPAN_DEFAULT_SEASON,
    entry_offset_min: int = 10,
    fidelity: int = 60,
) -> dict[str, Any]:
    return build_snapshot(
        ctx,
        _JAPAN_FP1,
        meeting_key=meeting_key,
        season=season,
        entry_offset_min=entry_offset_min,
        fidelity=fidelity,
    )


# ---------------------------------------------------------------------------
# Run baseline wrappers
# ---------------------------------------------------------------------------


def run_china_sq_pole_baseline(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    min_edge: float = CHINA_MIN_EDGE,
) -> dict[str, Any]:
    return run_baseline(ctx, _CHINA, snapshot_id=snapshot_id, min_edge=min_edge)


def run_aus_q_pole_baseline(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    min_edge: float = AUS_MIN_EDGE,
) -> dict[str, Any]:
    return run_baseline(ctx, _AUS, snapshot_id=snapshot_id, min_edge=min_edge)


def run_japan_q_pole_baseline(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    min_edge: float = JAPAN_MIN_EDGE,
) -> dict[str, Any]:
    return run_baseline(ctx, _JAPAN_PRE, snapshot_id=snapshot_id, min_edge=min_edge)


def run_japan_fp1_q_pole_baseline(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    min_edge: float = JAPAN_FP1_MIN_EDGE,
) -> dict[str, Any]:
    return run_baseline(ctx, _JAPAN_FP1, snapshot_id=snapshot_id, min_edge=min_edge)


# ---------------------------------------------------------------------------
# Report wrappers
# ---------------------------------------------------------------------------


def report_china_sq_pole_quicktest(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    report_slug: str | None = None,
    min_edge: float = CHINA_MIN_EDGE,
) -> dict[str, Any]:
    return generate_report(
        ctx, _CHINA, snapshot_id=snapshot_id, report_slug=report_slug, min_edge=min_edge
    )


def report_aus_q_pole_quicktest(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    report_slug: str | None = None,
    min_edge: float = AUS_MIN_EDGE,
) -> dict[str, Any]:
    return generate_report(
        ctx, _AUS, snapshot_id=snapshot_id, report_slug=report_slug, min_edge=min_edge
    )


def report_japan_q_pole_quicktest(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    report_slug: str | None = None,
    min_edge: float = JAPAN_MIN_EDGE,
) -> dict[str, Any]:
    return generate_report(
        ctx, _JAPAN_PRE, snapshot_id=snapshot_id, report_slug=report_slug, min_edge=min_edge
    )


def report_japan_fp1_q_pole_quicktest(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    report_slug: str | None = None,
    min_edge: float = JAPAN_FP1_MIN_EDGE,
) -> dict[str, Any]:
    return generate_report(
        ctx, _JAPAN_FP1, snapshot_id=snapshot_id, report_slug=report_slug, min_edge=min_edge
    )
