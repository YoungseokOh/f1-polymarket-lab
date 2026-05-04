from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl
from f1_polymarket_lab.common import ensure_dir, stable_uuid, utc_now
from f1_polymarket_lab.features.compute import compute_features
from f1_polymarket_lab.storage.models import (
    F1Driver,
    F1SessionResult,
    FeatureSnapshot,
    PolymarketToken,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select

from f1_polymarket_worker.gp_registry import (
    _build_driver_map,
    _load_price_history,
    _load_sessions,
    _load_target_markets,
    _load_trades,
    _match_market_driver,
    _normalize_name,
    _require_utc,
    _result_gap_seconds,
    _select_entry_price_point,
)
from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)
from f1_polymarket_worker.pipeline import PipelineContext

CHECKPOINTS = ("FP1", "FP2", "FP3", "Q")
FAMILY_BY_TAXONOMY = {
    "driver_pole_position": "pole",
    "constructor_pole_position": "constructor_pole",
    "race_winner": "winner",
    "head_to_head_session": "h2h",
}
TAXONOMIES_BY_TARGET = {
    "Q": ("driver_pole_position", "constructor_pole_position"),
    "R": ("race_winner", "head_to_head_session"),
}
log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CheckpointWindow:
    checkpoint: str
    visible_sessions: tuple[str, ...]


def _checkpoint_window(checkpoint: str) -> CheckpointWindow:
    windows = {
        "FP1": ("FP1",),
        "FP2": ("FP1", "FP2"),
        "FP3": ("FP1", "FP2", "FP3"),
        "Q": ("FP1", "FP2", "FP3", "Q"),
    }
    try:
        visible_sessions = windows[checkpoint]
    except KeyError as exc:
        raise ValueError(f"unsupported checkpoint={checkpoint!r}") from exc
    return CheckpointWindow(checkpoint=checkpoint, visible_sessions=visible_sessions)


def build_multitask_checkpoint_rows(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
    checkpoint: str,
    entry_offset_min: int = 10,
) -> list[dict[str, Any]]:
    drivers = list(ctx.db.scalars(select(F1Driver)).all())
    driver_map = _build_driver_map(drivers)
    window = _checkpoint_window(checkpoint)
    rows: list[dict[str, Any]] = []
    for target_session_code in ("Q", "R"):
        try:
            rows.extend(
                _build_market_family_rows(
                    ctx,
                    meeting_key=meeting_key,
                    season=season,
                    target_session_code=target_session_code,
                    checkpoint=window,
                    drivers=drivers,
                    driver_map=driver_map,
                    entry_offset_min=entry_offset_min,
                )
            )
        except (KeyError, ValueError) as exc:
            log.info(
                "Skipping multitask target rows for meeting_key=%s target=%s checkpoint=%s: %s",
                meeting_key,
                target_session_code,
                checkpoint,
                exc,
            )

    if not rows:
        return []
    computed = compute_features(
        pl.DataFrame(rows),
        zscore=False,
        log=False,
        interactions=False,
        cross_gp=False,
    )
    return cast(list[dict[str, Any]], computed.to_dicts())


def build_multitask_feature_snapshots(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
    checkpoints: tuple[str, ...] = CHECKPOINTS,
    stage: str = "multitask_qr",
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="build-multitask-qr-snapshots",
        source="derived",
        dataset="multitask_feature_snapshot",
        description="Build checkpoint-aware Q/R feature snapshots for multitask modeling.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "meeting_key": meeting_key,
            "season": season,
            "checkpoints": list(checkpoints),
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {
            "job_run_id": run.id,
            "status": "planned",
            "meeting_key": meeting_key,
            "season": season,
            "checkpoints": list(checkpoints),
        }

    root = ensure_dir(
        Path(ctx.settings.data_root) / "feature_snapshots" / "multitask" / str(season)
    )
    snapshot_records: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []

    for checkpoint in checkpoints:
        rows = build_multitask_checkpoint_rows(
            ctx,
            meeting_key=meeting_key,
            season=season,
            checkpoint=checkpoint,
        )
        snapshot_id = stable_uuid("multitask-snapshot", season, meeting_key, checkpoint)
        parquet_path = root / f"{meeting_key}_{checkpoint}.parquet"
        pl.DataFrame(rows).write_parquet(parquet_path)
        snapshot_records.append(
            {
                "id": snapshot_id,
                "market_id": None,
                "session_id": None,
                "as_of_ts": utc_now(),
                "snapshot_type": f"{stage}_{checkpoint.lower()}",
                "feature_version": "multitask_v1",
                "storage_path": str(parquet_path),
                "source_cutoffs": {
                    "meeting_key": meeting_key,
                    "season": season,
                    "checkpoint": checkpoint,
                },
                "row_count": len(rows),
            }
        )
        manifest_rows.append(
            {
                "meeting_key": meeting_key,
                "season": season,
                "checkpoint": checkpoint,
                "snapshot_id": snapshot_id,
                "path": str(parquet_path),
            }
        )

    upsert_records(ctx.db, FeatureSnapshot, snapshot_records)
    manifest_path = root / "manifest.json"
    existing_rows: list[dict[str, Any]] = []
    if manifest_path.exists():
        existing_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        existing_rows = [
            row for row in existing_payload.get("snapshots", []) if isinstance(row, dict)
        ]
    rows_by_key = {
        (
            int(row.get("season", season)),
            int(row.get("meeting_key", meeting_key)),
            str(row.get("checkpoint")),
        ): row
        for row in existing_rows
        if row.get("meeting_key") is not None and row.get("checkpoint") is not None
    }
    for row in manifest_rows:
        rows_by_key[(season, meeting_key, str(row["checkpoint"]))] = row
    checkpoint_rank = {checkpoint: index for index, checkpoint in enumerate(CHECKPOINTS)}
    merged_rows = sorted(
        rows_by_key.values(),
        key=lambda row: (
            int(row.get("season", 0)),
            int(row.get("meeting_key", 0)),
            checkpoint_rank.get(str(row.get("checkpoint")), 99),
        ),
    )
    manifest_path.write_text(
        json.dumps({"snapshots": merged_rows}, indent=2),
        encoding="utf-8",
    )
    finish_job_run(ctx.db, run, status="completed", records_written=len(snapshot_records))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "snapshot_ids": [record["id"] for record in snapshot_records],
        "manifest_path": str(manifest_path),
    }


def _build_market_family_rows(
    ctx: PipelineContext,
    *,
    meeting_key: int,
    season: int,
    target_session_code: str,
    checkpoint: CheckpointWindow,
    drivers: list[F1Driver],
    driver_map: dict[str, F1Driver],
    entry_offset_min: int,
) -> list[dict[str, Any]]:
    _, sessions_by_code, _, _, _, target_session = _load_sessions(
        ctx,
        meeting_key=meeting_key,
        season=season,
        target_session_code=target_session_code,
    )
    result_maps = _load_result_maps(ctx, sessions_by_code)
    target_results = result_maps.get(target_session_code, {})
    rows: list[dict[str, Any]] = []

    for taxonomy in TAXONOMIES_BY_TARGET[target_session_code]:
        markets = _load_target_markets(
            ctx,
            target_session=target_session,
            target_session_code=target_session_code,
            market_taxonomy=taxonomy,
        )
        tokens_by_market = _load_market_tokens(ctx, market_ids=[market.id for market in markets])
        price_history = _load_price_history(
            ctx,
            market_ids=[market.id for market in markets],
            token_ids=[token.id for tokens in tokens_by_market.values() for token in tokens],
        )
        trades = _load_trades(ctx, market_ids=[market.id for market in markets])

        last_visible_session = sessions_by_code[checkpoint.visible_sessions[-1]]
        entry_floor = _require_utc(last_visible_session.date_end_utc) + timedelta(
            minutes=entry_offset_min
        )
        target_start = _require_utc(target_session.date_start_utc)

        for market in markets:
            token = _select_market_token(market=market, tokens=tokens_by_market.get(market.id, []))
            if token is None:
                continue

            point = _select_entry_price_point(
                rows=price_history.get(market.id, []),
                window_start=entry_floor,
                window_end=target_start,
            )
            if point is None:
                continue

            family = FAMILY_BY_TAXONOMY[taxonomy]
            if family == "h2h":
                rows.extend(
                    _build_h2h_rows(
                        meeting_key=meeting_key,
                        market=market,
                        token=token,
                        point=point,
                        checkpoint=checkpoint,
                        target_session_code=target_session_code,
                        result_maps=result_maps,
                        driver_map=driver_map,
                        trades=trades.get(market.id, []),
                    )
                )
                continue

            row = _build_binary_market_row(
                meeting_key=meeting_key,
                market=market,
                token=token,
                point=point,
                checkpoint=checkpoint,
                family=family,
                target_session_code=target_session_code,
                drivers=drivers,
                driver_map=driver_map,
                result_maps=result_maps,
                target_results=target_results,
                trades=trades.get(market.id, []),
            )
            if row is not None:
                rows.append(row)

    return rows


def _load_result_maps(
    ctx: PipelineContext,
    sessions_by_code: dict[str, Any],
) -> dict[str, dict[str, F1SessionResult]]:
    result_maps: dict[str, dict[str, F1SessionResult]] = {}
    for code in ("FP1", "FP2", "FP3", "Q", "R"):
        session = sessions_by_code.get(code)
        if session is None:
            result_maps[code] = {}
            continue
        rows = ctx.db.scalars(
            select(F1SessionResult).where(F1SessionResult.session_id == session.id)
        ).all()
        result_maps[code] = {row.driver_id: row for row in rows if row.driver_id is not None}
    return result_maps


def _load_market_tokens(
    ctx: PipelineContext,
    *,
    market_ids: list[str],
) -> dict[str, list[PolymarketToken]]:
    grouped: dict[str, list[PolymarketToken]] = defaultdict(list)
    if not market_ids:
        return grouped
    rows = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id.in_(market_ids))
    ).all()
    for row in rows:
        grouped[row.market_id].append(row)
    return grouped


def _select_market_token(
    *,
    market: Any,
    tokens: list[PolymarketToken],
) -> PolymarketToken | None:
    family = FAMILY_BY_TAXONOMY.get(market.taxonomy)
    if family == "h2h":
        candidates = {
            _normalize_name(value)
            for value in (market.driver_a, (market.raw_payload or {}).get("groupItemTitle"))
            if value
        }
        for token in tokens:
            outcome = _normalize_name(token.outcome or "")
            if outcome in candidates:
                return token
        return tokens[0] if tokens else None

    for token in tokens:
        if (token.outcome or "").strip().lower() == "yes":
            return token
    return tokens[0] if tokens else None


def _checkpoint_flags(checkpoint: CheckpointWindow) -> dict[str, bool]:
    visible = set(checkpoint.visible_sessions)
    return {
        "has_fp1": "FP1" in visible,
        "has_fp2": "FP2" in visible,
        "has_fp3": "FP3" in visible,
        "has_q": "Q" in visible,
    }


def _result_for_checkpoint(
    checkpoint: CheckpointWindow,
    result_maps: dict[str, dict[str, F1SessionResult]],
    *,
    session_code: str,
    driver_id: str | None,
) -> F1SessionResult | None:
    if driver_id is None or session_code not in checkpoint.visible_sessions:
        return None
    return result_maps.get(session_code, {}).get(driver_id)


def _trade_summary(
    *,
    trades: list[Any],
    as_of_ts: Any,
) -> tuple[int, float | None]:
    observed_at = _require_utc(as_of_ts)
    pre_entry_trades = [
        trade
        for trade in trades
        if _require_utc(trade.trade_timestamp_utc) <= observed_at
    ]
    if not pre_entry_trades:
        return 0, None
    last_trade_at = max(_require_utc(trade.trade_timestamp_utc) for trade in pre_entry_trades)
    return len(pre_entry_trades), (observed_at - last_trade_at).total_seconds()


def _build_binary_market_row(
    *,
    meeting_key: int,
    market: Any,
    token: PolymarketToken,
    point: Any,
    checkpoint: CheckpointWindow,
    family: str,
    target_session_code: str,
    drivers: list[F1Driver],
    driver_map: dict[str, F1Driver],
    result_maps: dict[str, dict[str, F1SessionResult]],
    target_results: dict[str, F1SessionResult],
    trades: list[Any],
) -> dict[str, Any] | None:
    matched_driver = _match_market_driver(
        market=market,
        drivers=drivers,
        driver_map=driver_map,
    )
    driver_id = getattr(matched_driver, "id", None)
    if driver_id is None:
        return None

    fp1_result = _result_for_checkpoint(
        checkpoint,
        result_maps,
        session_code="FP1",
        driver_id=driver_id,
    )
    fp2_result = _result_for_checkpoint(
        checkpoint,
        result_maps,
        session_code="FP2",
        driver_id=driver_id,
    )
    fp3_result = _result_for_checkpoint(
        checkpoint,
        result_maps,
        session_code="FP3",
        driver_id=driver_id,
    )
    q_result = _result_for_checkpoint(
        checkpoint,
        result_maps,
        session_code="Q",
        driver_id=driver_id,
    )
    target_result = target_results.get(driver_id)
    trade_count, last_trade_age_seconds = _trade_summary(
        trades=trades,
        as_of_ts=point.observed_at_utc,
    )

    return {
        "meeting_key": meeting_key,
        "event_id": market.event_id,
        "market_id": market.id,
        "token_id": token.id,
        "target_session_code": target_session_code,
        "target_market_family": family,
        "as_of_checkpoint": checkpoint.checkpoint,
        "as_of_ts": point.observed_at_utc,
        "driver_id": driver_id,
        "driver_name": getattr(matched_driver, "full_name", None) or market.driver_a,
        "entry_yes_price": float(point.price or token.latest_price or 0.0),
        "entry_best_bid": point.best_bid,
        "entry_best_ask": point.best_ask,
        "entry_spread": market.spread,
        "entry_midpoint": point.midpoint if point.midpoint is not None else point.price,
        "trade_count_pre_entry": trade_count,
        "last_trade_age_seconds": last_trade_age_seconds,
        "fp1_position": getattr(fp1_result, "position", None),
        "fp1_gap_to_leader_seconds": _result_gap_seconds(fp1_result) if fp1_result else None,
        "fp2_position": getattr(fp2_result, "position", None),
        "fp2_gap_to_leader_seconds": _result_gap_seconds(fp2_result) if fp2_result else None,
        "fp3_position": getattr(fp3_result, "position", None),
        "fp3_gap_to_leader_seconds": _result_gap_seconds(fp3_result) if fp3_result else None,
        "qualifying_position": getattr(q_result, "position", None),
        "qualifying_gap_to_pole_seconds": _result_gap_seconds(q_result) if q_result else None,
        "label_yes": int(getattr(target_result, "position", None) == 1) if target_result else 0,
        **_checkpoint_flags(checkpoint),
    }


def _build_h2h_rows(
    *,
    meeting_key: int,
    market: Any,
    token: PolymarketToken,
    point: Any,
    checkpoint: CheckpointWindow,
    target_session_code: str,
    result_maps: dict[str, dict[str, F1SessionResult]],
    driver_map: dict[str, F1Driver],
    trades: list[Any],
) -> list[dict[str, Any]]:
    driver_a = driver_map.get(_normalize_name(market.driver_a or ""))
    driver_b = driver_map.get(_normalize_name(market.driver_b or ""))
    if driver_a is None or driver_b is None:
        return []

    q_result_a = _result_for_checkpoint(
        checkpoint, result_maps, session_code="Q", driver_id=driver_a.id
    )
    target_result_a = result_maps.get(target_session_code, {}).get(driver_a.id)
    target_result_b = result_maps.get(target_session_code, {}).get(driver_b.id)
    trade_count, last_trade_age_seconds = _trade_summary(
        trades=trades,
        as_of_ts=point.observed_at_utc,
    )

    return [
        {
            "meeting_key": meeting_key,
            "event_id": market.event_id,
            "market_id": market.id,
            "token_id": token.id,
            "target_session_code": target_session_code,
            "target_market_family": "h2h",
            "as_of_checkpoint": checkpoint.checkpoint,
            "as_of_ts": point.observed_at_utc,
            "driver_id": driver_a.id,
            "driver_name": driver_a.full_name or market.driver_a,
            "opponent_driver_id": driver_b.id,
            "opponent_driver_name": driver_b.full_name or market.driver_b,
            "entry_yes_price": float(point.price or token.latest_price or 0.0),
            "entry_best_bid": point.best_bid,
            "entry_best_ask": point.best_ask,
            "entry_spread": market.spread,
            "entry_midpoint": point.midpoint if point.midpoint is not None else point.price,
            "trade_count_pre_entry": trade_count,
            "last_trade_age_seconds": last_trade_age_seconds,
            "qualifying_position": getattr(q_result_a, "position", None),
            "qualifying_gap_to_pole_seconds": (
                _result_gap_seconds(q_result_a) if q_result_a is not None else None
            ),
            "label_yes": int(
                getattr(target_result_a, "position", 999)
                < getattr(target_result_b, "position", 999)
            ),
            **_checkpoint_flags(checkpoint),
        }
    ]
