"""Walk-forward backtesting engine for F1 Polymarket predictions.

Turns paper-edge quicktest results into executable-price PnL calculations by:
  1. Collecting resolution outcomes from closed markets
  2. Re-pricing entries at bid/ask (with midpoint fallback)
  3. Settling positions against actual outcomes
  4. Recording BacktestOrder / BacktestPosition / BacktestResult rows
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from f1_polymarket_lab.common import ensure_dir, stable_uuid, utc_now
from f1_polymarket_lab.storage.models import (
    BacktestOrder,
    BacktestPosition,
    BacktestResult,
    F1Meeting,
    FeatureSnapshot,
    PolymarketMarket,
    PolymarketResolution,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import func, select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)
from f1_polymarket_worker.pipeline import PipelineContext

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BACKTEST_STAGE = "pole_position_backtest"
DEFAULT_BET_SIZE = 10.0
DEFAULT_MIN_EDGE = 0.05
FEE_RATE = 0.02  # Polymarket taker fee ~2%


@dataclass(frozen=True, slots=True)
class CheckpointPolicyConfig:
    open_edge: float = 0.05
    add_edge: float = 0.08
    reduce_edge: float = 0.03
    close_edge: float = 0.0
    bet_size: float = DEFAULT_BET_SIZE


def _decide_checkpoint_action(
    *,
    current_edge: float,
    current_position: float,
    config: CheckpointPolicyConfig,
) -> str:
    if current_position <= 0.0 and current_edge >= config.open_edge:
        return "open"
    if current_position > 0.0 and current_edge >= config.add_edge:
        return "add"
    if current_position > 0.0 and current_edge <= config.close_edge:
        return "close"
    if current_position > 0.0 and current_edge <= config.reduce_edge:
        return "reduce"
    return "hold"


def settle_stateful_checkpoint_backtest(
    checkpoint_rows: list[dict[str, Any]],
    *,
    policy: CheckpointPolicyConfig,
) -> list[dict[str, Any]]:
    state: dict[str, float] = {}
    decisions: list[dict[str, Any]] = []

    for row in checkpoint_rows:
        market_id = str(row["market_id"])
        current_position = state.get(market_id, 0.0)
        action = _decide_checkpoint_action(
            current_edge=float(row["edge"]),
            current_position=current_position,
            config=policy,
        )
        if action == "open":
            current_position = policy.bet_size
        elif action == "add":
            current_position += policy.bet_size
        elif action == "reduce":
            current_position = max(0.0, current_position - policy.bet_size)
        elif action == "close":
            current_position = 0.0

        state[market_id] = current_position
        decisions.append({**row, "action": action, "position_after": current_position})

    return decisions


# ---------------------------------------------------------------------------
# Phase 1 — Resolution collection
# ---------------------------------------------------------------------------


def collect_resolutions(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
) -> dict[str, Any]:
    """Collect resolution outcomes for all closed markets linked to a meeting.

    Returns summary dict with counts of resolved markets.
    """
    definition = ensure_job_definition(
        ctx.db,
        job_name="collect-resolutions",
        source="polymarket",
        dataset="resolution",
        description="Collect resolution outcomes for closed markets.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"meeting_key": meeting_key, "season": season},
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    meeting = ctx.db.scalar(
        select(F1Meeting).where(
            F1Meeting.meeting_key == meeting_key, F1Meeting.season == season
        )
    )
    if meeting is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message=f"meeting_key={meeting_key} not found",
        )
        raise ValueError(f"meeting_key={meeting_key} not found for season={season}")

    # Find all markets mapped to this meeting's sessions
    from f1_polymarket_lab.storage.models import (
        EntityMappingF1ToPolymarket,
        F1Session,
    )

    sessions = ctx.db.scalars(
        select(F1Session).where(F1Session.meeting_id == meeting.id)
    ).all()
    session_ids = [s.id for s in sessions]

    mappings = ctx.db.scalars(
        select(EntityMappingF1ToPolymarket).where(
            EntityMappingF1ToPolymarket.f1_session_id.in_(session_ids)
        )
    ).all()
    market_ids = list({m.polymarket_market_id for m in mappings if m.polymarket_market_id})

    if not market_ids:
        finish_job_run(
            ctx.db,
            run,
            status="completed",
            records_written=0,
        )
        return {
            "job_run_id": run.id,
            "status": "completed",
            "markets_found": 0,
            "resolutions_written": 0,
        }

    resolution_rows: list[dict[str, Any]] = []

    for market_id in market_ids:
        market = ctx.db.get(PolymarketMarket, market_id)
        if market is None:
            continue

        # Check if resolution already exists
        existing = ctx.db.scalar(
            select(func.count())
            .select_from(PolymarketResolution)
            .where(PolymarketResolution.market_id == market_id)
        )
        if existing and existing > 0:
            continue

        # For closed markets, resolution is in the raw_payload
        if market.closed and market.raw_payload is not None:
            resolved_at = None
            resolve_date_str = market.raw_payload.get("resolveDate") or market.raw_payload.get(
                "endDate"
            )
            if resolve_date_str:
                try:
                    resolved_at = datetime.fromisoformat(
                        str(resolve_date_str).replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    resolved_at = None

            resolution_rows.append(
                {
                    "id": f"resolution:{market_id}",
                    "market_id": market_id,
                    "resolved_at_utc": resolved_at,
                    "result": market.raw_payload.get("result"),
                    "outcome": market.raw_payload.get("outcome"),
                    "raw_payload": market.raw_payload,
                }
            )

    if resolution_rows:
        upsert_records(ctx.db, PolymarketResolution, resolution_rows)

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(resolution_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "markets_found": len(market_ids),
        "resolutions_written": len(resolution_rows),
    }


# ---------------------------------------------------------------------------
# Phase 2 — Backtest engine
# ---------------------------------------------------------------------------


def _get_executable_entry_price(
    row: dict[str, Any],
) -> tuple[float, float]:
    """Return (entry_price, slippage) using best_ask when available.

    Falls back to midpoint (entry_yes_price) when bid/ask is missing.
    For YES buy orders, the executable price is the best_ask.
    """
    best_ask = row.get("entry_best_ask")
    midpoint = float(row["entry_yes_price"])

    if best_ask is not None and float(best_ask) > 0:
        ask = float(best_ask)
        slippage = ask - midpoint
        return ask, max(slippage, 0.0)

    # Fallback: estimate spread from market data, use midpoint + half spread
    spread = row.get("entry_spread")
    if spread is not None and float(spread) > 0:
        estimated_ask = midpoint + float(spread) / 2
        return estimated_ask, float(spread) / 2

    # Pure midpoint fallback
    return midpoint, 0.0


def _resolve_market_outcome(
    resolution: PolymarketResolution | None,
    row: dict[str, Any],
) -> int | None:
    """Determine if this market resolved YES (1) or NO (0).

    Uses label_yes from snapshot when available (ground truth from F1 results),
    falls back to resolution record.
    """
    # Snapshot already has label from F1 session results
    label = row.get("label_yes")
    if label is not None:
        return int(label)

    # Fallback to resolution record
    if resolution is not None and resolution.result is not None:
        return 1 if resolution.result.upper() == "YES" else 0

    return None


def settle_backtest(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    strategy_name: str = "hybrid_flat_bet",
    model_name: str = "hybrid",
    stage: str = BACKTEST_STAGE,
    min_edge: float = DEFAULT_MIN_EDGE,
    bet_size: float = DEFAULT_BET_SIZE,
) -> dict[str, Any]:
    """Run a full backtest settlement for one GP snapshot.

    1. Load snapshot rows + model predictions
    2. Load resolution outcomes
    3. Select bets based on edge threshold
    4. Calculate PnL at executable prices
    5. Record BacktestOrder, BacktestPosition, BacktestResult
    """
    definition = ensure_job_definition(
        ctx.db,
        job_name="settle-backtest",
        source="model",
        dataset="backtest",
        description="Settle backtest positions against market resolutions.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "snapshot_id": snapshot_id,
            "strategy_name": strategy_name,
            "model_name": model_name,
            "min_edge": min_edge,
            "bet_size": bet_size,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    # Load snapshot
    snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
    if snapshot is None or snapshot.storage_path is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message=f"snapshot_id={snapshot_id} not found",
        )
        raise ValueError(f"snapshot_id={snapshot_id} not found")

    rows = pl.read_parquet(snapshot.storage_path).to_dicts()
    if not rows:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message="snapshot contains no rows",
        )
        raise ValueError(f"snapshot_id={snapshot_id} contains no rows")

    # Enrich with probabilities (reuse quicktest logic)
    from f1_polymarket_worker.quicktest import _enrich_snapshot_probabilities

    enriched_rows = _enrich_snapshot_probabilities(rows)

    # Determine probability key based on model_name
    probability_key_map = {
        "market_implied": "market_implied_probability",
        "fp1_pace": "fp1_pace_probability",
        "form_pace": "fp1_pace_probability",
        "hybrid": "hybrid_probability",
    }
    probability_key = probability_key_map.get(model_name, "hybrid_probability")

    # Load resolution records
    market_ids = list({row["market_id"] for row in enriched_rows})
    resolutions = ctx.db.scalars(
        select(PolymarketResolution).where(
            PolymarketResolution.market_id.in_(market_ids)
        )
    ).all()
    resolution_by_market: dict[str, PolymarketResolution] = {
        r.market_id: r for r in resolutions
    }

    # Get meeting info for date range
    if not enriched_rows:
        finish_job_run(ctx.db, run, status="completed", records_written=0)
        return {
            "job_run_id": run.id,
            "status": "completed",
            "backtest_run_id": stable_uuid("backtest", snapshot_id, strategy_name, model_name),
            "snapshot_id": snapshot_id,
            "strategy_name": strategy_name,
            "model_name": model_name,
            "bets_placed": 0,
            "metrics": _compute_backtest_metrics([]),
            "settled_rows": [],
        }
    meeting_key = int(enriched_rows[0]["meeting_key"])
    meeting = ctx.db.scalar(
        select(F1Meeting).where(F1Meeting.meeting_key == meeting_key)
    )

    backtest_run_id = stable_uuid("backtest", snapshot_id, strategy_name, model_name)

    order_records: list[dict[str, Any]] = []
    position_records: list[dict[str, Any]] = []
    settled_rows: list[dict[str, Any]] = []
    skipped_unresolved: int = 0
    skipped_tiny_price: int = 0

    for row in enriched_rows:
        prob = float(row[probability_key])
        entry_price, slippage = _get_executable_entry_price(row)

        if entry_price < 0.005:
            skipped_tiny_price += 1
            continue

        # Use negRisk-normalized market probability for edge calculation.
        # entry_yes_price is inflated in negRisk batches (all YES prices sum > 1),
        # so comparing model_prob vs raw price systematically overstates apparent edge.
        # market_normalized_prob = entry_yes_price / sum(all_yes_prices_in_event)
        market_ref = float(row.get("market_normalized_prob") or entry_price)
        edge = prob - market_ref
        if edge < min_edge:
            continue

        market_id = row["market_id"]
        token_id = row.get("token_id", "")
        resolution = resolution_by_market.get(market_id)
        outcome = _resolve_market_outcome(resolution, row)

        if outcome is None:
            skipped_unresolved += 1
            continue

        # PnL: YES wins → payout $1 per share, so profit = (1 - entry_price) * quantity
        # YES loses → loss = entry_price * quantity
        quantity = bet_size / entry_price if entry_price > 0 else 0.0
        fees = quantity * entry_price * FEE_RATE
        pnl = (
            (1.0 - entry_price) * quantity - fees
            if outcome == 1
            else -entry_price * quantity - fees
        )

        order_id = stable_uuid("bt-order", backtest_run_id, market_id)
        position_id = stable_uuid("bt-position", backtest_run_id, market_id)

        executed_at = None
        entry_ts = row.get("entry_observed_at_utc")
        if entry_ts is not None:
            if isinstance(entry_ts, str):
                try:
                    executed_at = datetime.fromisoformat(entry_ts.replace("Z", "+00:00"))
                except ValueError:
                    executed_at = None
            elif isinstance(entry_ts, datetime):
                executed_at = entry_ts

        order_records.append(
            {
                "id": order_id,
                "backtest_run_id": backtest_run_id,
                "market_id": market_id,
                "token_id": token_id,
                "side": "YES",
                "quantity": quantity,
                "limit_price": entry_price,
                "executed_price": entry_price,
                "executed_at": executed_at,
                "fees": fees,
                "slippage": slippage,
                "raw_json": {
                    "driver_name": row.get("driver_name"),
                    "model_probability": prob,
                    "edge": edge,
                    "outcome": outcome,
                },
            }
        )

        resolved_at = resolution.resolved_at_utc if resolution else None
        position_records.append(
            {
                "id": position_id,
                "backtest_run_id": backtest_run_id,
                "market_id": market_id,
                "token_id": token_id,
                "quantity": quantity,
                "avg_entry_price": entry_price,
                "opened_at": executed_at,
                "closed_at": resolved_at,
                "realized_pnl": pnl,
                "unrealized_pnl": 0.0,
                "status": "settled",
            }
        )

        settled_rows.append(
            {
                "market_id": market_id,
                "driver_name": row.get("driver_name", ""),
                "model_probability": prob,
                "entry_price": entry_price,
                "edge": edge,
                "outcome": outcome,
                "quantity": quantity,
                "pnl": pnl,
                "slippage": slippage,
            }
        )

    # Compute aggregate metrics
    metrics = _compute_backtest_metrics(settled_rows, bet_size=bet_size)

    # Persist
    if order_records:
        upsert_records(ctx.db, BacktestOrder, order_records)
    if position_records:
        upsert_records(ctx.db, BacktestPosition, position_records)

    result_record = {
        "id": stable_uuid("bt-result", backtest_run_id),
        "backtest_run_id": backtest_run_id,
        "strategy_name": strategy_name,
        "stage": stage,
        "start_at": meeting.start_date_utc if meeting else None,
        "end_at": meeting.end_date_utc if meeting else None,
        "metrics_json": metrics,
        "equity_curve_path": None,
        "trades_path": None,
        "created_at": utc_now(),
    }
    upsert_records(ctx.db, BacktestResult, [result_record])

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(order_records),
    )

    return {
        "job_run_id": run.id,
        "status": "completed",
        "backtest_run_id": backtest_run_id,
        "snapshot_id": snapshot_id,
        "strategy_name": strategy_name,
        "model_name": model_name,
        "bets_placed": len(settled_rows),
        "metrics": metrics,
        "settled_rows": settled_rows,
        "skipped_unresolved": skipped_unresolved,
        "skipped_tiny_price": skipped_tiny_price,
    }


# ---------------------------------------------------------------------------
# Phase 3 — Extended metrics
# ---------------------------------------------------------------------------

EPSILON = 1e-6


def _compute_backtest_metrics(
    settled_rows: list[dict[str, Any]],
    *,
    bet_size: float = DEFAULT_BET_SIZE,
) -> dict[str, Any]:
    """Compute comprehensive backtest metrics from settled positions."""
    if not settled_rows:
        return {
            "bet_count": 0,
            "total_wagered": 0.0,
            "total_pnl": 0.0,
            "roi_pct": 0.0,
            "hit_rate": 0.0,
            "avg_edge": 0.0,
            "avg_pnl_per_bet": 0.0,
            "max_win": 0.0,
            "max_loss": 0.0,
            "brier_score": None,
            "calibration_buckets": {},
        }

    pnls = [r["pnl"] for r in settled_rows]
    outcomes = [r["outcome"] for r in settled_rows]
    probs = [r["model_probability"] for r in settled_rows]
    edges = [r["edge"] for r in settled_rows]

    total_wagered = bet_size * len(settled_rows)
    total_pnl = sum(pnls)
    wins = sum(1 for o in outcomes if o == 1)
    hit_rate = wins / len(outcomes) if outcomes else 0.0
    roi_pct = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0.0

    # Brier score (on the selected bets only)
    brier = sum(
        (prob - outcome) ** 2 for prob, outcome in zip(probs, outcomes, strict=True)
    ) / len(probs)

    # Calibration buckets (10% intervals)
    buckets: dict[str, dict[str, Any]] = {}
    for prob, outcome in zip(probs, outcomes, strict=True):
        clamped = max(0.0, min(prob, 0.9999))
        bucket_key = f"{int(clamped * 10) * 10}-{int(clamped * 10) * 10 + 10}%"
        if bucket_key not in buckets:
            buckets[bucket_key] = {"count": 0, "wins": 0, "avg_prob": 0.0}
        buckets[bucket_key]["count"] += 1
        buckets[bucket_key]["wins"] += outcome
    for bucket_key in buckets:
        b = buckets[bucket_key]
        b["actual_rate"] = b["wins"] / b["count"] if b["count"] > 0 else 0.0

    # Sharpe-like ratio (per-bet PnL mean / std)
    mean_pnl = total_pnl / len(pnls)
    if len(pnls) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance) if variance > 0 else EPSILON
        sharpe = mean_pnl / std_pnl
    else:
        sharpe = 0.0

    return {
        "bet_count": len(settled_rows),
        "wins": wins,
        "losses": len(settled_rows) - wins,
        "hit_rate": round(hit_rate, 4),
        "total_wagered": round(total_wagered, 2),
        "total_pnl": round(total_pnl, 2),
        "roi_pct": round(roi_pct, 2),
        "avg_edge": round(sum(edges) / len(edges), 4),
        "avg_pnl_per_bet": round(mean_pnl, 2),
        "max_win": round(max(pnls), 2),
        "max_loss": round(min(pnls), 2),
        "sharpe": round(sharpe, 4),
        "brier_score": round(brier, 6),
        "calibration_buckets": buckets,
    }


# ---------------------------------------------------------------------------
# Phase 4 — Walk-forward multi-GP backtest
# ---------------------------------------------------------------------------


def run_walk_forward_backtest(
    ctx: PipelineContext,
    *,
    gp_configs: list[dict[str, Any]],
    strategy_name: str = "hybrid_flat_bet",
    model_name: str = "hybrid",
    min_edge: float = DEFAULT_MIN_EDGE,
    bet_size: float = DEFAULT_BET_SIZE,
) -> dict[str, Any]:
    """Run walk-forward backtest across multiple GPs in chronological order.

    gp_configs is a list of dicts, each with:
      - meeting_key: int
      - season: int
      - snapshot_id: str  (from a previous build_*_snapshot call)
    """
    definition = ensure_job_definition(
        ctx.db,
        job_name="walk-forward-backtest",
        source="model",
        dataset="backtest",
        description="Walk-forward backtest across multiple GPs.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "gp_count": len(gp_configs),
            "strategy_name": strategy_name,
            "model_name": model_name,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    gp_results: list[dict[str, Any]] = []
    cumulative_pnl = 0.0
    equity_curve: list[dict[str, Any]] = []

    for gp_config in gp_configs:
        meeting_key = gp_config["meeting_key"]
        season = gp_config["season"]
        snapshot_id = gp_config["snapshot_id"]

        # Step 1: Collect resolutions for this GP
        resolution_result = collect_resolutions(
            ctx, meeting_key=meeting_key, season=season
        )

        # Step 2: Settle backtest
        settle_result = settle_backtest(
            ctx,
            snapshot_id=snapshot_id,
            strategy_name=strategy_name,
            model_name=model_name,
            min_edge=min_edge,
            bet_size=bet_size,
        )

        gp_pnl = settle_result["metrics"].get("total_pnl", 0.0)
        cumulative_pnl += gp_pnl

        meeting = ctx.db.scalar(
            select(F1Meeting).where(
                F1Meeting.meeting_key == meeting_key, F1Meeting.season == season
            )
        )
        gp_name = meeting.meeting_name if meeting else f"GP-{meeting_key}"

        equity_curve.append(
            {
                "gp": gp_name,
                "meeting_key": meeting_key,
                "gp_pnl": gp_pnl,
                "cumulative_pnl": round(cumulative_pnl, 2),
                "bets": settle_result["metrics"].get("bet_count", 0),
            }
        )

        gp_results.append(
            {
                "gp_name": gp_name,
                "meeting_key": meeting_key,
                "snapshot_id": snapshot_id,
                "resolution_result": resolution_result,
                "backtest_result": settle_result,
            }
        )

    # Aggregate season metrics
    all_settled = []
    for gp in gp_results:
        all_settled.extend(gp["backtest_result"].get("settled_rows", []))

    season_metrics = _compute_backtest_metrics(all_settled, bet_size=bet_size)
    season_metrics["equity_curve"] = equity_curve
    season_metrics["gp_count"] = len(gp_configs)

    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=len(all_settled),
    )

    return {
        "job_run_id": run.id,
        "status": "completed",
        "strategy_name": strategy_name,
        "model_name": model_name,
        "season_metrics": season_metrics,
        "gp_results": gp_results,
        "equity_curve": equity_curve,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def render_backtest_report(
    result: dict[str, Any],
    *,
    title: str = "2026 Season Backtest (R1–R2)",
) -> str:
    """Render a walk-forward backtest result as markdown."""
    sm = result["season_metrics"]
    lines = [
        f"# {title}",
        "",
        f"- Strategy: `{result['strategy_name']}`",
        f"- Model: `{result['model_name']}`",
        f"- GPs: `{sm.get('gp_count', 0)}`",
        f"- Total bets: `{sm.get('bet_count', 0)}`",
        f"- Total wagered: `${sm.get('total_wagered', 0):.2f}`",
        f"- Total PnL: `${sm.get('total_pnl', 0):.2f}`",
        f"- ROI: `{sm.get('roi_pct', 0):.1f}%`",
        f"- Hit rate: `{sm.get('hit_rate', 0):.1%}`",
        f"- Sharpe: `{sm.get('sharpe', 0):.4f}`",
        f"- Brier score: `{sm.get('brier_score', 'N/A')}`",
        "",
        "## Equity Curve",
        "",
        "| GP | Bets | GP PnL | Cumulative PnL |",
        "|----|------|--------|----------------|",
    ]

    for point in result.get("equity_curve", []):
        lines.append(
            f"| {point['gp']} | {point['bets']} "
            f"| ${point['gp_pnl']:.2f} | ${point['cumulative_pnl']:.2f} |"
        )

    # Per-GP detail
    for gp in result.get("gp_results", []):
        gp_name = gp["gp_name"]
        bt = gp["backtest_result"]
        metrics = bt.get("metrics", {})
        settled = bt.get("settled_rows", [])

        lines.extend(
            [
                "",
                f"## {gp_name}",
                "",
                f"- Bets: `{metrics.get('bet_count', 0)}`",
                f"- PnL: `${metrics.get('total_pnl', 0):.2f}`",
                f"- ROI: `{metrics.get('roi_pct', 0):.1f}%`",
                f"- Hit rate: `{metrics.get('hit_rate', 0):.1%}`",
                "",
            ]
        )

        if settled:
            lines.extend(
                [
                    "| Driver | Model Prob | Entry Price | Edge | Result | PnL |",
                    "|--------|-----------|-------------|------|--------|-----|",
                ]
            )
            for row in sorted(settled, key=lambda r: r["edge"], reverse=True):
                result_str = "WIN" if row["outcome"] == 1 else "LOSS"
                lines.append(
                    f"| {row['driver_name']} | {row['model_probability']:.1%} "
                    f"| ${row['entry_price']:.4f} | {row['edge']:.1%} "
                    f"| {result_str} | ${row['pnl']:.2f} |"
                )

    # Calibration
    calibration = sm.get("calibration_buckets", {})
    if calibration:
        lines.extend(
            [
                "",
                "## Calibration",
                "",
                "| Bucket | Count | Wins | Actual Rate |",
                "|--------|-------|------|-------------|",
            ]
        )
        for bucket_key in sorted(calibration):
            b = calibration[bucket_key]
            lines.append(
                f"| {bucket_key} | {b['count']} | {b['wins']} "
                f"| {b.get('actual_rate', 0):.1%} |"
            )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Entry prices use best_ask when available, midpoint + spread/2 fallback.",
            "- Position sizing: flat bet per qualifying signal above min_edge threshold.",
            "- This is a paper backtest — no actual orders were submitted.",
            f"- Generated at: {utc_now().isoformat()}",
        ]
    )
    return "\n".join(lines) + "\n"


def save_backtest_report(
    ctx: PipelineContext,
    result: dict[str, Any],
    *,
    slug: str = "2026-season-backtest-r1-r2",
    title: str = "2026 Season Backtest (R1–R2)",
) -> Path:
    """Render and save backtest report to docs/research/."""
    report_md = render_backtest_report(result, title=title)
    report_json = json.dumps(result, indent=2, sort_keys=True, default=str)

    report_dir = ctx.settings.data_root / "reports" / "research" / "2026" / slug
    ensure_dir(report_dir)

    md_path = report_dir / "summary.md"
    json_path = report_dir / "summary.json"

    md_path.write_text(report_md, encoding="utf-8")
    json_path.write_text(report_json, encoding="utf-8")

    # Also save to docs/research/
    docs_path = Path("docs/research") / f"{slug}.md"
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text(report_md, encoding="utf-8")

    return docs_path


# ---------------------------------------------------------------------------
# Phase 5 — GP accumulation helper
# ---------------------------------------------------------------------------


def settle_single_gp(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
    snapshot_id: str,
    strategy_name: str = "hybrid_flat_bet",
    model_name: str = "hybrid",
    min_edge: float = DEFAULT_MIN_EDGE,
    bet_size: float = DEFAULT_BET_SIZE,
) -> dict[str, Any]:
    """Convenience wrapper: collect resolutions + settle backtest for one GP.

    Use this when a new GP completes and you want to add it to the backtest
    history without re-running the full walk-forward loop.
    """
    res = collect_resolutions(ctx, meeting_key=meeting_key, season=season)
    settle = settle_backtest(
        ctx,
        snapshot_id=snapshot_id,
        strategy_name=strategy_name,
        model_name=model_name,
        min_edge=min_edge,
        bet_size=bet_size,
    )
    return {
        "meeting_key": meeting_key,
        "season": season,
        "resolution": res,
        "backtest": settle,
    }
