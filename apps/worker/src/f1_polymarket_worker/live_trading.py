from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from f1_polymarket_lab.common import stable_uuid, utc_now
from f1_polymarket_lab.storage.models import (
    EntityMappingF1ToPolymarket,
    F1Meeting,
    F1Session,
    FeatureSnapshot,
    LiveTradeExecution,
    LiveTradeTicket,
    ModelPrediction,
    PolymarketMarket,
    PolymarketPriceHistory,
)
from sqlalchemy import func, select

from f1_polymarket_worker.gp_registry import (
    GPConfig,
    build_snapshot,
    run_baseline,
    select_model_run_id,
)
from f1_polymarket_worker.model_registry import (
    MULTITASK_PROMOTION_STAGE,
    get_active_promoted_model_run,
    required_model_stage_for_gp,
    score_promoted_multitask_snapshot,
)
from f1_polymarket_worker.multitask_snapshot import build_multitask_feature_snapshots
from f1_polymarket_worker.ops_calendar import get_ops_stage_config
from f1_polymarket_worker.pipeline import PipelineContext


def _ensure_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=timezone.utc)
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _session_is_live(session: F1Session | None, *, now: datetime) -> bool:
    if session is None or session.date_start_utc is None or session.date_end_utc is None:
        return False
    return _ensure_utc(session.date_start_utc) <= now <= _ensure_utc(session.date_end_utc)


def _meeting_for_config(ctx: PipelineContext, *, config: GPConfig) -> F1Meeting | None:
    return ctx.db.scalar(
        select(F1Meeting).where(
            F1Meeting.meeting_key == config.meeting_key,
            F1Meeting.season == config.season,
        )
    )


def _session_by_code(
    ctx: PipelineContext,
    *,
    meeting_id: str,
    session_code: str | None,
) -> F1Session | None:
    if session_code is None:
        return None
    return ctx.db.scalar(
        select(F1Session).where(
            F1Session.meeting_id == meeting_id,
            F1Session.session_code == session_code,
        )
    )


def _current_market_probability(
    market: PolymarketMarket,
    latest_price: PolymarketPriceHistory | None,
) -> tuple[float | None, float | None, datetime | None]:
    best_bid = market.best_bid
    best_ask = market.best_ask
    if best_bid is not None and best_ask is not None:
        return (
            (best_bid + best_ask) / 2.0,
            best_ask - best_bid,
            None if latest_price is None else latest_price.observed_at_utc,
        )
    if latest_price is not None:
        if latest_price.best_bid is not None and latest_price.best_ask is not None:
            return (
                (latest_price.best_bid + latest_price.best_ask) / 2.0,
                latest_price.best_ask - latest_price.best_bid,
                latest_price.observed_at_utc,
            )
        if latest_price.midpoint is not None:
            return latest_price.midpoint, None, latest_price.observed_at_utc
        if latest_price.price is not None:
            return latest_price.price, None, latest_price.observed_at_utc
    if market.last_trade_price is not None:
        return (
            market.last_trade_price,
            None,
            None if latest_price is None else latest_price.observed_at_utc,
        )
    return None, None, None


def _signal_action(
    *,
    model_prob: float,
    market_price: float,
    min_edge: float,
) -> tuple[str, str, float]:
    edge = model_prob - market_price
    if edge >= min_edge:
        return "buy_yes", "YES", edge
    if -edge >= min_edge:
        return "buy_no", "NO", edge
    return "skip", "", edge


def _entry_price_for_signal(*, signal_action: str, market_price: float) -> float | None:
    if signal_action == "buy_yes":
        return market_price
    if signal_action == "buy_no":
        return 1.0 - market_price
    return None


def _latest_price_history_by_market(
    ctx: PipelineContext,
    *,
    market_ids: list[str],
) -> dict[str, PolymarketPriceHistory]:
    if not market_ids:
        return {}
    rows = ctx.db.scalars(
        select(PolymarketPriceHistory)
        .where(PolymarketPriceHistory.market_id.in_(market_ids))
        .order_by(PolymarketPriceHistory.observed_at_utc.desc())
    ).all()
    latest: dict[str, PolymarketPriceHistory] = {}
    for row in rows:
        latest.setdefault(row.market_id, row)
    return latest


def _latest_open_ticket_for_market(
    ctx: PipelineContext,
    *,
    gp_slug: str,
    market_id: str,
) -> LiveTradeTicket | None:
    return ctx.db.scalar(
        select(LiveTradeTicket)
        .where(
            LiveTradeTicket.gp_slug == gp_slug,
            LiveTradeTicket.market_id == market_id,
            LiveTradeTicket.status.in_(("open", "submitted")),
        )
        .order_by(LiveTradeTicket.created_at.desc())
        .limit(1)
    )


def _daily_committed_loss_budget(
    ctx: PipelineContext,
    *,
    gp_slug: str,
    now: datetime,
) -> float:
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    rows = ctx.db.execute(
        select(LiveTradeExecution.actual_fill_size, LiveTradeExecution.actual_fill_price)
        .join(LiveTradeTicket, LiveTradeTicket.id == LiveTradeExecution.ticket_id)
        .where(
            LiveTradeTicket.gp_slug == gp_slug,
            LiveTradeExecution.status.in_(("submitted", "filled")),
            LiveTradeExecution.submitted_at >= day_start,
        )
    ).all()
    total = 0.0
    for fill_size, fill_price in rows:
        if fill_size is None or fill_price is None:
            continue
        total += float(fill_size) * float(fill_price)
    return total


def _resolve_live_scored_run(
    ctx: PipelineContext,
    *,
    config: GPConfig,
) -> dict[str, Any]:
    required_stage = required_model_stage_for_gp(config)
    active = None if required_stage is None else get_active_promoted_model_run(
        ctx.db,
        stage=required_stage,
    )
    if required_stage is not None and active is None:
        raise ValueError(f"No active promoted champion exists for stage={required_stage}")

    if required_stage == MULTITASK_PROMOTION_STAGE:
        checkpoint = config.source_session_code
        if checkpoint is None:
            raise ValueError(f"config={config.short_code} is missing source_session_code")
        snap_result = build_multitask_feature_snapshots(
            ctx,
            meeting_key=config.meeting_key,
            season=config.season,
            checkpoints=(checkpoint,),
            stage=required_stage,
        )
        snapshot_ids = snap_result.get("snapshot_ids", [])
        if not snapshot_ids:
            raise ValueError(f"Multitask snapshot build failed: {snap_result}")
        snapshot_id = str(snapshot_ids[0])
        snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
        if snapshot is None:
            raise KeyError(f"snapshot_id={snapshot_id} not found")
        score_result = score_promoted_multitask_snapshot(
            ctx.db,
            data_root=Path(ctx.settings.data_root),
            snapshot=snapshot,
            stage=required_stage,
        )
        return {
            "snapshot_id": snapshot_id,
            "model_run_id": str(score_result["model_run_id"]),
            "required_stage": required_stage,
            "active_model_run_id": None if active is None else active.id,
        }

    snap_result = build_snapshot(
        ctx,
        config,
        meeting_key=config.meeting_key,
        season=config.season,
        entry_offset_min=config.entry_offset_min,
        fidelity=config.fidelity,
    )
    snapshot_id = snap_result.get("snapshot_id")
    if not snapshot_id:
        raise ValueError(f"Snapshot build failed: {snap_result}")
    baseline_result = run_baseline(
        ctx,
        config,
        snapshot_id=str(snapshot_id),
        min_edge=config.live_min_edge,
    )
    model_run_ids = baseline_result.get("model_runs", [])
    model_run_id, _ = select_model_run_id(
        config,
        model_run_ids,
        baseline=None if active is None else active.model_name,
    )
    return {
        "snapshot_id": str(snapshot_id),
        "model_run_id": model_run_id,
        "required_stage": required_stage,
        "active_model_run_id": active.id,
    }


def build_live_signal_board(
    ctx: PipelineContext,
    *,
    gp_short_code: str,
    limit: int = 12,
) -> dict[str, Any]:
    _, config = get_ops_stage_config(
        ctx.db,
        short_code=gp_short_code,
        now=_ensure_utc(utc_now()),
    )
    required_stage = required_model_stage_for_gp(config)
    blockers: list[str] = []
    scored_run: dict[str, Any] | None = None
    try:
        scored_run = _resolve_live_scored_run(ctx, config=config)
    except Exception as exc:
        blockers.append(str(exc))

    if scored_run is None:
        return {
            "gp_short_code": config.short_code,
            "required_stage": required_stage,
            "active_model_run_id": None,
            "model_run_id": None,
            "snapshot_id": None,
            "rows": [],
            "blockers": blockers,
        }

    predictions = ctx.db.scalars(
        select(ModelPrediction)
        .where(ModelPrediction.model_run_id == scored_run["model_run_id"])
        .order_by(ModelPrediction.probability_yes.desc(), ModelPrediction.market_id.asc())
    ).all()
    market_ids = [prediction.market_id for prediction in predictions if prediction.market_id]
    markets = ctx.db.scalars(
        select(PolymarketMarket).where(PolymarketMarket.id.in_(market_ids))
    ).all()
    markets_by_id = {market.id: market for market in markets}
    latest_price_by_market = _latest_price_history_by_market(ctx, market_ids=market_ids)

    rows: list[dict[str, Any]] = []
    for prediction in predictions:
        market_id = prediction.market_id
        if market_id is None:
            continue
        market = markets_by_id.get(market_id)
        if market is None:
            continue
        market_price, spread, observed_at = _current_market_probability(
            market,
            latest_price_by_market.get(market_id),
        )
        model_prob = prediction.probability_yes
        if model_prob is None:
            continue
        signal_action, side_label, edge = (
            _signal_action(
                model_prob=float(model_prob),
                market_price=float(market_price),
                min_edge=config.live_min_edge,
            )
            if market_price is not None
            else ("skip", "", 0.0)
        )
        rows.append(
            {
                "market_id": market.id,
                "token_id": prediction.token_id,
                "question": market.question,
                "session_code": config.target_session_code,
                "promotion_stage": scored_run["required_stage"],
                "model_run_id": scored_run["model_run_id"],
                "snapshot_id": scored_run["snapshot_id"],
                "model_prob": float(model_prob),
                "market_price": None if market_price is None else float(market_price),
                "edge": None if market_price is None else edge,
                "spread": None if spread is None else float(spread),
                "signal_action": signal_action,
                "side_label": side_label or None,
                "recommended_size": config.live_bet_size,
                "max_spread": config.live_max_spread,
                "observed_at_utc": (
                    None if observed_at is None else _ensure_utc(observed_at).isoformat()
                ),
                "event_type": None,
            }
        )

    rows.sort(
        key=lambda row: (
            row["edge"] is None,
            -abs(float(row["edge"])) if row["edge"] is not None else 0.0,
            float(row["model_prob"]),
            str(row["market_id"]),
        )
    )
    return {
        "gp_short_code": config.short_code,
        "required_stage": scored_run["required_stage"],
        "active_model_run_id": scored_run["active_model_run_id"],
        "model_run_id": scored_run["model_run_id"],
        "snapshot_id": scored_run["snapshot_id"],
        "rows": rows[:limit],
        "blockers": blockers,
    }


@dataclass(frozen=True, slots=True)
class LiveTradeSummary:
    ticket_count: int
    open_ticket_count: int
    filled_ticket_count: int
    cancelled_ticket_count: int
    execution_count: int
    filled_execution_count: int


def summarize_live_trading(
    ctx: PipelineContext,
    *,
    gp_slug: str,
) -> LiveTradeSummary:
    ticket_status_rows = ctx.db.execute(
        select(LiveTradeTicket.status, func.count())
        .where(LiveTradeTicket.gp_slug == gp_slug)
        .group_by(LiveTradeTicket.status)
    ).all()
    execution_status_rows = ctx.db.execute(
        select(LiveTradeExecution.status, func.count())
        .join(LiveTradeTicket, LiveTradeTicket.id == LiveTradeExecution.ticket_id)
        .where(LiveTradeTicket.gp_slug == gp_slug)
        .group_by(LiveTradeExecution.status)
    ).all()
    ticket_counts = {str(status): int(count) for status, count in ticket_status_rows}
    execution_counts = {str(status): int(count) for status, count in execution_status_rows}
    return LiveTradeSummary(
        ticket_count=sum(ticket_counts.values()),
        open_ticket_count=ticket_counts.get("open", 0) + ticket_counts.get("submitted", 0),
        filled_ticket_count=ticket_counts.get("filled", 0),
        cancelled_ticket_count=ticket_counts.get("cancelled", 0),
        execution_count=sum(execution_counts.values()),
        filled_execution_count=execution_counts.get("filled", 0),
    )


def create_live_trade_ticket(
    ctx: PipelineContext,
    *,
    gp_short_code: str,
    market_id: str,
    observed_market_price: float | None = None,
    observed_spread: float | None = None,
    observed_at_utc: datetime | None = None,
    source_event_type: str | None = None,
    bet_size: float | None = None,
    min_edge: float | None = None,
    max_spread: float | None = None,
) -> dict[str, Any]:
    observed_now = _ensure_utc(observed_at_utc or utc_now())
    _, config = get_ops_stage_config(
        ctx.db,
        short_code=gp_short_code,
        now=observed_now,
    )
    meeting = _meeting_for_config(ctx, config=config)
    if meeting is None:
        raise ValueError(f"meeting_key={config.meeting_key} not loaded")
    target_session = _session_by_code(
        ctx,
        meeting_id=meeting.id,
        session_code=config.target_session_code,
    )
    now = observed_now
    if not _session_is_live(target_session, now=now):
        raise ValueError(
            f"{config.target_session_code} session is outside the live execution window."
        )

    signal_board = build_live_signal_board(ctx, gp_short_code=gp_short_code, limit=100)
    if signal_board["blockers"]:
        raise ValueError(signal_board["blockers"][0])
    row = next((item for item in signal_board["rows"] if item["market_id"] == market_id), None)
    if row is None:
        raise KeyError(f"market_id={market_id} is not linked to {config.short_code}")
    market = ctx.db.get(PolymarketMarket, market_id)
    if market is None:
        raise KeyError(f"market_id={market_id} not found")
    mapped = ctx.db.scalar(
        select(EntityMappingF1ToPolymarket).where(
            EntityMappingF1ToPolymarket.f1_session_id == target_session.id,
            EntityMappingF1ToPolymarket.polymarket_market_id == market_id,
        )
    )
    if mapped is None:
        raise ValueError(f"market_id={market_id} is not mapped to {config.target_session_code}")

    market_price = observed_market_price
    if market_price is None:
        market_price = row["market_price"]
    if market_price is None:
        raise ValueError("Live quote is unavailable for this market.")
    spread = observed_spread if observed_spread is not None else row["spread"]
    resolved_min_edge = config.live_min_edge if min_edge is None else min_edge
    resolved_size = config.live_bet_size if bet_size is None else bet_size
    resolved_max_spread = config.live_max_spread if max_spread is None else max_spread

    if resolved_max_spread is not None:
        if spread is None:
            raise ValueError("Live spread is unavailable for this market.")
        if float(spread) > resolved_max_spread:
            raise ValueError(
                f"Spread {float(spread):.3f} exceeds max {resolved_max_spread:.3f}."
            )

    existing_open_ticket = _latest_open_ticket_for_market(
        ctx,
        gp_slug=config.short_code,
        market_id=market_id,
    )
    if existing_open_ticket is not None:
        raise ValueError(
            f"market_id={market_id} already has an open live ticket ({existing_open_ticket.id})."
        )

    signal_action, side_label, edge = _signal_action(
        model_prob=float(row["model_prob"]),
        market_price=float(market_price),
        min_edge=resolved_min_edge,
    )
    if signal_action == "skip":
        raise ValueError(
            f"Edge {edge:.3f} does not clear the live minimum edge {resolved_min_edge:.3f}."
        )

    entry_price = _entry_price_for_signal(
        signal_action=signal_action,
        market_price=float(market_price),
    )
    if entry_price is None:
        raise ValueError("Unable to resolve entry price for live ticket.")
    committed_budget = _daily_committed_loss_budget(ctx, gp_slug=config.short_code, now=now)
    incremental_budget = resolved_size * entry_price
    if committed_budget + incremental_budget > config.live_max_daily_loss:
        raise ValueError(
            "Daily loss budget exceeded for live trading: "
            f"{committed_budget + incremental_budget:.2f} > {config.live_max_daily_loss:.2f}."
        )

    ticket_id = stable_uuid(
        "live-trade-ticket",
        config.short_code,
        market_id,
        signal_board["model_run_id"],
        now.isoformat(),
    )
    ticket = LiveTradeTicket(
        id=ticket_id,
        gp_slug=config.short_code,
        session_code=config.target_session_code,
        market_id=market.id,
        token_id=row["token_id"],
        snapshot_id=signal_board["snapshot_id"],
        model_run_id=signal_board["model_run_id"],
        promotion_stage=signal_board["required_stage"],
        question=market.question,
        signal_action=signal_action,
        side_label=side_label,
        model_prob=float(row["model_prob"]),
        market_price=float(market_price),
        edge=float(edge),
        recommended_size=float(resolved_size),
        observed_spread=None if spread is None else float(spread),
        max_spread=resolved_max_spread,
        observed_at_utc=now,
        source_event_type=source_event_type,
        status="open",
        rationale_json={
            "entry_price": entry_price,
            "daily_committed_budget": committed_budget,
            "daily_incremental_budget": incremental_budget,
            "target_session_code": config.target_session_code,
        },
        expires_at=now + timedelta(minutes=config.live_ticket_ttl_min),
        created_at=now,
        updated_at=now,
    )
    ctx.db.add(ticket)
    ctx.db.flush()
    return {
        "action": "create-live-trade-ticket",
        "status": "ok",
        "message": (
            f"Created {side_label} live ticket for {market.question} at "
            f"{entry_price:.3f} with edge {edge:.3f}."
        ),
        "ticket_id": ticket.id,
        "gp_short_code": config.short_code,
        "market_id": market.id,
        "model_run_id": ticket.model_run_id,
        "snapshot_id": ticket.snapshot_id,
        "promotion_stage": ticket.promotion_stage,
        "signal_action": ticket.signal_action,
        "side_label": ticket.side_label,
        "recommended_size": ticket.recommended_size,
        "market_price": ticket.market_price,
        "model_prob": ticket.model_prob,
        "edge": ticket.edge,
        "observed_spread": ticket.observed_spread,
        "max_spread": ticket.max_spread,
        "observed_at_utc": ticket.observed_at_utc,
        "expires_at": ticket.expires_at,
    }


def record_live_trade_fill(
    ctx: PipelineContext,
    *,
    ticket_id: str,
    submitted_size: float,
    actual_fill_size: float | None = None,
    actual_fill_price: float | None = None,
    submitted_at: datetime | None = None,
    filled_at: datetime | None = None,
    operator_note: str | None = None,
    external_reference: str | None = None,
    status: str = "filled",
    realized_pnl: float | None = None,
) -> dict[str, Any]:
    ticket = ctx.db.get(LiveTradeTicket, ticket_id)
    if ticket is None:
        raise KeyError(f"ticket_id={ticket_id} not found")
    if ticket.status in {"cancelled", "filled"}:
        raise ValueError(f"ticket_id={ticket_id} is already {ticket.status}")
    if submitted_size <= 0:
        raise ValueError("submitted_size must be positive")
    if status == "filled":
        if actual_fill_size is None or actual_fill_price is None:
            raise ValueError("filled executions require actual_fill_size and actual_fill_price")
        if actual_fill_size <= 0 or actual_fill_price <= 0:
            raise ValueError("actual fill values must be positive")

    now = _ensure_utc(submitted_at or utc_now())
    existing = ctx.db.scalar(
        select(LiveTradeExecution).where(LiveTradeExecution.ticket_id == ticket_id)
    )
    execution_id = (
        existing.id
        if existing is not None
        else stable_uuid("live-trade-execution", ticket_id)
    )
    execution = LiveTradeExecution(
        id=execution_id,
        ticket_id=ticket.id,
        market_id=ticket.market_id,
        side=ticket.signal_action,
        submitted_size=float(submitted_size),
        actual_fill_size=None if actual_fill_size is None else float(actual_fill_size),
        actual_fill_price=None if actual_fill_price is None else float(actual_fill_price),
        submitted_at=now,
        filled_at=None if filled_at is None else _ensure_utc(filled_at),
        operator_note=operator_note,
        external_reference=external_reference,
        realized_pnl=realized_pnl,
        status=status,
        created_at=now if existing is None else existing.created_at,
        updated_at=_ensure_utc(utc_now()),
    )
    if existing is None:
        ctx.db.add(execution)
    else:
        existing.market_id = execution.market_id
        existing.side = execution.side
        existing.submitted_size = execution.submitted_size
        existing.actual_fill_size = execution.actual_fill_size
        existing.actual_fill_price = execution.actual_fill_price
        existing.submitted_at = execution.submitted_at
        existing.filled_at = execution.filled_at
        existing.operator_note = execution.operator_note
        existing.external_reference = execution.external_reference
        existing.realized_pnl = execution.realized_pnl
        existing.status = execution.status
        existing.updated_at = execution.updated_at

    ticket.status = "filled" if status == "filled" else "submitted"
    ticket.updated_at = _ensure_utc(utc_now())
    ctx.db.flush()
    return {
        "action": "record-live-trade-fill",
        "status": "ok",
        "message": f"Recorded {status} execution for ticket {ticket.id}.",
        "ticket_id": ticket.id,
        "execution_id": execution_id,
        "execution_status": status,
        "ticket_status": ticket.status,
    }


def cancel_live_trade_ticket(
    ctx: PipelineContext,
    *,
    ticket_id: str,
    operator_note: str | None = None,
) -> dict[str, Any]:
    ticket = ctx.db.get(LiveTradeTicket, ticket_id)
    if ticket is None:
        raise KeyError(f"ticket_id={ticket_id} not found")
    if ticket.status == "filled":
        raise ValueError(f"ticket_id={ticket_id} is already filled")
    ticket.status = "cancelled"
    ticket.updated_at = _ensure_utc(utc_now())
    execution = ctx.db.scalar(
        select(LiveTradeExecution).where(LiveTradeExecution.ticket_id == ticket_id)
    )
    if execution is not None:
        execution.status = "cancelled"
        if operator_note:
            execution.operator_note = operator_note
        execution.updated_at = _ensure_utc(utc_now())
    ctx.db.flush()
    return {
        "action": "cancel-live-trade-ticket",
        "status": "ok",
        "message": f"Cancelled live ticket {ticket.id}.",
        "ticket_id": ticket.id,
        "ticket_status": ticket.status,
    }
