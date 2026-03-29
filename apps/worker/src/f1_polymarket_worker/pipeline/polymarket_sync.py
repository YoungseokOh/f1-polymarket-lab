from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from f1_polymarket_lab.common import (
    payload_checksum,
    stable_uuid,
    utc_now,
)
from f1_polymarket_lab.connectors import (
    PolymarketConnector,
    parse_market_taxonomy,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.models import (
    MarketTaxonomyLabel,
    MarketTaxonomyVersion,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketMarketRule,
    PolymarketMarketStatusHistory,
    PolymarketOpenInterestHistory,
    PolymarketOrderbookLevel,
    PolymarketOrderbookSnapshot,
    PolymarketPriceHistory,
    PolymarketResolution,
    PolymarketToken,
    PolymarketTrade,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    get_cursor_state,
    start_job_run,
    upsert_cursor_state,
)

from .context import (
    PipelineContext,
    best_levels,
    compute_imbalance,
    extract_event_rows,
    parse_dt,
    persist_fetch,
    persist_silver,
)


def ensure_taxonomy_version(ctx: PipelineContext) -> MarketTaxonomyVersion:
    existing = ctx.db.scalar(
        select(MarketTaxonomyVersion).where(MarketTaxonomyVersion.version_name == "heuristic-v1")
    )
    if existing is not None:
        return existing
    version = MarketTaxonomyVersion(
        version_name="heuristic-v1",
        parser_name="parse_market_taxonomy",
        rule_hash=payload_checksum("heuristic-v1"),
        notes="Initial slug/question/rules heuristic parser.",
    )
    ctx.db.add(version)
    ctx.db.flush()
    return version


def _flush_polymarket_catalog_dependencies(ctx: PipelineContext) -> None:
    """Persist parent market rows before child tables in SQLite fallback mode."""
    ctx.db.flush()


def sync_polymarket_catalog(
    ctx: PipelineContext,
    *,
    max_pages: int = 1,
    batch_size: int = 100,
    active: bool | None = None,
    closed: bool | None = None,
    archived: bool | None = None,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="sync-polymarket-catalog",
        source="polymarket",
        dataset="catalog",
        description="Sync Polymarket events, markets, tokens, and rule metadata.",
        default_cursor={"offset": 0},
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="catalog",
        cursor_key="markets",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "max_pages": max_pages,
            "batch_size": batch_size,
            "active": active,
            "closed": closed,
            "archived": archived,
        },
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned"}

    connector = PolymarketConnector()
    taxonomy_version = ensure_taxonomy_version(ctx)
    event_rows: dict[str, dict[str, Any]] = {}
    market_rows: list[dict[str, Any]] = []
    token_rows: list[dict[str, Any]] = []
    rule_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    last_offset = 0

    for offset, batch in connector.iterate_markets(
        batch_size=batch_size,
        max_pages=max_pages,
        active=active,
        closed=closed,
        archived=archived,
    ):
        last_offset = offset
        persist_fetch(
            ctx,
            job_run_id=run.id,
            batch=FetchBatch(
                source="polymarket",
                dataset="markets",
                endpoint="/markets",
                params={
                    "limit": batch_size,
                    "offset": offset,
                    "active": active,
                    "closed": closed,
                    "archived": archived,
                },
                payload=batch,
                response_status=200,
                checkpoint=str(offset),
            ),
            partition={"offset": str(offset)},
        )
        for event in extract_event_rows(batch):
            event_rows[event["id"]] = event
        for market in batch:
            event_context = market.get("events", [{}])[0] if market.get("events") else {}
            parsed = parse_market_taxonomy(
                market.get("question") or "",
                " ".join(
                    str(value or "")
                    for value in [
                        market.get("description"),
                        event_context.get("description"),
                    ]
                    if value
                )
                or None,
                title=event_context.get("title"),
            )
            token_ids = json.loads(market.get("clobTokenIds") or "[]")
            outcomes = json.loads(market.get("outcomes") or "[]")
            prices = json.loads(market.get("outcomePrices") or "[]")
            event_id = str(market["events"][0]["id"]) if market.get("events") else None
            best_bid = float(market["bestBid"]) if market.get("bestBid") is not None else None
            best_ask = float(market["bestAsk"]) if market.get("bestAsk") is not None else None
            market_rows.append(
                {
                    "id": str(market["id"]),
                    "event_id": event_id,
                    "question": market.get("question") or "",
                    "slug": market.get("slug"),
                    "condition_id": market["conditionId"],
                    "question_id": market.get("questionID"),
                    "market_type": market.get("marketType"),
                    "sports_market_type": market.get("sportsMarketType"),
                    "taxonomy": parsed.taxonomy,
                    "taxonomy_confidence": parsed.confidence,
                    "target_session_code": parsed.target_session_code,
                    "driver_a": parsed.driver_a,
                    "driver_b": parsed.driver_b,
                    "team_name": parsed.team_name,
                    "resolution_source": market.get("resolutionSource"),
                    "rules_text": market.get("description"),
                    "description": market.get("description"),
                    "start_at_utc": parse_dt(market.get("startDate")),
                    "end_at_utc": parse_dt(market.get("endDate")),
                    "accepting_orders": bool(market.get("acceptingOrders")),
                    "active": bool(market.get("active")),
                    "closed": bool(market.get("closed")),
                    "archived": bool(market.get("archived")),
                    "enable_order_book": bool(market.get("enableOrderBook")),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": (best_ask - best_bid)
                    if best_ask is not None and best_bid is not None
                    else None,
                    "last_trade_price": (
                        float(market["lastTradePrice"])
                        if market.get("lastTradePrice") is not None
                        else None
                    ),
                    "volume": market.get("volumeNum"),
                    "liquidity": market.get("liquidityNum"),
                    "clob_token_ids": token_ids,
                    "raw_payload": market,
                }
            )
            rule_rows.append(
                {
                    "id": f"rule:{market['id']}",
                    "market_id": str(market["id"]),
                    "rules_text": market.get("description"),
                    "resolution_text": market.get("resolutionSource"),
                    "parsed_metadata": {
                        "taxonomy": parsed.taxonomy,
                        "target_session_code": parsed.target_session_code,
                        "driver_a": parsed.driver_a,
                        "driver_b": parsed.driver_b,
                        "team_name": parsed.team_name,
                    },
                    "raw_payload": market,
                }
            )
            status_rows.append(
                {
                    "id": f"{market['id']}:{offset}",
                    "market_id": str(market["id"]),
                    "observed_at_utc": utc_now(),
                    "active": bool(market.get("active")),
                    "closed": bool(market.get("closed")),
                    "archived": bool(market.get("archived")),
                    "accepting_orders": bool(market.get("acceptingOrders")),
                    "raw_payload": market,
                }
            )
            label_rows.append(
                {
                    "id": stable_uuid(
                        "market-taxonomy-label",
                        str(market["id"]),
                        taxonomy_version.id,
                    ),
                    "market_id": str(market["id"]),
                    "taxonomy_version_id": taxonomy_version.id,
                    "taxonomy": parsed.taxonomy,
                    "confidence": parsed.confidence,
                    "label_status": "candidate",
                    "target_session_code": parsed.target_session_code,
                    "parsed_metadata": parsed.metadata
                    | {
                        "driver_a": parsed.driver_a or "",
                        "driver_b": parsed.driver_b or "",
                        "team_name": parsed.team_name or "",
                    },
                    "created_at": utc_now(),
                }
            )
            for index, token_id in enumerate(token_ids):
                token_rows.append(
                    {
                        "id": token_id,
                        "market_id": str(market["id"]),
                        "outcome": outcomes[index] if index < len(outcomes) else None,
                        "outcome_index": index,
                        "latest_price": float(prices[index]) if index < len(prices) else None,
                        "raw_payload": {"token_id": token_id},
                    }
                )

    upsert_records(ctx.db, PolymarketEvent, event_rows.values())
    upsert_records(ctx.db, PolymarketMarket, market_rows)
    upsert_records(ctx.db, PolymarketToken, token_rows)
    upsert_records(ctx.db, PolymarketMarketRule, rule_rows)
    upsert_records(ctx.db, PolymarketMarketStatusHistory, status_rows)
    _flush_polymarket_catalog_dependencies(ctx)
    upsert_records(ctx.db, MarketTaxonomyLabel, label_rows)
    _flush_polymarket_catalog_dependencies(ctx)
    persist_silver(
        ctx, job_run_id=run.id, dataset="polymarket_events", records=list(event_rows.values())
    )
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_markets", records=market_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_tokens", records=token_rows)
    persist_silver(ctx, job_run_id=run.id, dataset="polymarket_market_rules", records=rule_rows)

    upsert_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="catalog",
        cursor_key="markets",
        cursor_value={"last_offset": last_offset, "synced_at": utc_now().isoformat()},
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={"last_offset": last_offset, "synced_at": utc_now().isoformat()},
        records_written=len(market_rows) + len(token_rows),
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "markets": len(market_rows),
        "events": len(event_rows),
        "tokens": len(token_rows),
    }


def hydrate_polymarket_market(
    ctx: PipelineContext, *, market_id: str, fidelity: int = 60
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="hydrate-polymarket-market",
        source="polymarket",
        dataset="market_history",
        description="Hydrate one Polymarket market with executable microstructure history.",
        schedule_hint="manual",
    )
    cursor_state = get_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="market_history",
        cursor_key=market_id,
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"market_id": market_id, "fidelity": fidelity},
        cursor_before=None if cursor_state is None else cursor_state.cursor_value,
    )
    if not ctx.execute:
        finish_job_run(
            ctx.db,
            run,
            status="planned",
            cursor_after=None if cursor_state is None else cursor_state.cursor_value,
            records_written=0,
        )
        return {"job_run_id": run.id, "status": "planned", "market_id": market_id}

    connector = PolymarketConnector()
    market = ctx.db.get(PolymarketMarket, market_id)
    if market is None:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            error_message=f"market_id={market_id} not found; run sync-polymarket-catalog first",
        )
        raise ValueError(f"market_id={market_id} not found")

    tokens = ctx.db.scalars(
        select(PolymarketToken).where(PolymarketToken.market_id == market_id)
    ).all()
    history_rows: list[dict[str, Any]] = []
    orderbook_rows: list[dict[str, Any]] = []
    orderbook_level_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    open_interest_rows: list[dict[str, Any]] = []
    resolution_rows: list[dict[str, Any]] = []
    fetch_errors: list[dict[str, str]] = []

    def _safe_fetch(label: str, fetcher: Any, fallback: Any = None) -> Any:
        try:
            return fetcher()
        except Exception as exc:
            fetch_errors.append({"source": label, "error": str(exc)})
            return fallback

    open_interest = _safe_fetch(
        "open_interest",
        lambda: connector.get_open_interest(market.condition_id),
    )
    if open_interest is not None:
        open_interest_rows.append(
            {
                "id": f"{market_id}:oi:{utc_now().strftime('%Y%m%dT%H%M%S')}",
                "market_id": market_id,
                "token_id": None,
                "observed_at_utc": utc_now(),
                "open_interest": open_interest,
                "raw_payload": {
                    "condition_id": market.condition_id,
                    "open_interest": open_interest,
                },
            }
        )

    for token in tokens:
        token_source = f"token:{token.id}"
        book = _safe_fetch(
            f"{token_source}:order_book",
            lambda token_id=token.id: connector.get_order_book(token_id),
        )
        midpoint = _safe_fetch(
            f"{token_source}:midpoint",
            lambda token_id=token.id: connector.get_midpoint(token_id),
        )
        spread = _safe_fetch(
            f"{token_source}:spread",
            lambda token_id=token.id: connector.get_spread(token_id),
        )
        last_trade_price = _safe_fetch(
            f"{token_source}:last_trade_price",
            lambda token_id=token.id: connector.get_last_trade_price(token_id),
        )
        if book is not None:
            observed_at = datetime.fromtimestamp(
                int(book["timestamp"]) / 1000,
                tz=timezone.utc,
            )
            best_bid, best_ask = best_levels(book)
            snapshot_id = f"{market_id}:{token.id}:{book.get('timestamp')}"
            orderbook_rows.append(
                {
                    "id": snapshot_id,
                    "market_id": market_id,
                    "token_id": token.id,
                    "observed_at_utc": observed_at,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": spread,
                    "bid_depth_json": book.get("bids"),
                    "ask_depth_json": book.get("asks"),
                    "imbalance": compute_imbalance(book),
                    "raw_payload": book,
                }
            )
            for side, levels in (("bid", book.get("bids", [])), ("ask", book.get("asks", []))):
                for index, level in enumerate(levels):
                    orderbook_level_rows.append(
                        {
                            "id": f"{snapshot_id}:{side}:{index}",
                            "snapshot_id": snapshot_id,
                            "market_id": market_id,
                            "token_id": token.id,
                            "side": side,
                            "level_index": index,
                            "price": float(level["price"]),
                            "size": float(level["size"]),
                        }
                    )
        for point in _safe_fetch(
            f"{token_source}:price_history",
            lambda token_id=token.id: connector.get_price_history(token_id, fidelity=fidelity),
            [],
        ):
            history_rows.append(
                {
                    "id": f"{market_id}:{token.id}:{point['t']}",
                    "market_id": market_id,
                    "token_id": token.id,
                    "observed_at_utc": datetime.fromtimestamp(
                        int(point["t"]),
                        tz=timezone.utc,
                    ),
                    "price": point.get("p"),
                    "midpoint": midpoint,
                    "best_bid": None,
                    "best_ask": None,
                    "source_kind": "clob",
                    "raw_payload": point | {"last_trade_price": last_trade_price},
                }
            )

    for trade in _safe_fetch(
        "trades",
        lambda: connector.get_trades(market.condition_id, limit=500),
        [],
    ):
        trade_rows.append(
            {
                "id": f"{market_id}:{trade.get('transactionHash') or payload_checksum(trade)[:16]}",
                "market_id": market_id,
                "token_id": trade.get("asset"),
                "condition_id": market.condition_id,
                "trade_timestamp_utc": datetime.fromtimestamp(
                    int(trade["timestamp"]) / 1000,
                    tz=timezone.utc,
                ),
                "side": trade.get("side"),
                "price": trade.get("price"),
                "size": trade.get("size"),
                "outcome": trade.get("outcome"),
                "transaction_hash": trade.get("transactionHash"),
                "raw_payload": trade,
            }
        )

    if market.closed and market.raw_payload is not None:
        resolution_rows.append(
            {
                "id": f"resolution:{market_id}",
                "market_id": market_id,
                "resolved_at_utc": parse_dt(
                    market.raw_payload.get("resolveDate") or market.raw_payload.get("endDate")
                ),
                "result": market.raw_payload.get("result"),
                "outcome": market.raw_payload.get("outcome"),
                "raw_payload": market.raw_payload,
            }
        )

    if orderbook_rows:
        upsert_records(ctx.db, PolymarketOrderbookSnapshot, orderbook_rows)
        upsert_records(ctx.db, PolymarketOrderbookLevel, orderbook_level_rows)
    if history_rows:
        upsert_records(ctx.db, PolymarketPriceHistory, history_rows)
    if trade_rows:
        upsert_records(ctx.db, PolymarketTrade, trade_rows)
    if open_interest_rows:
        upsert_records(ctx.db, PolymarketOpenInterestHistory, open_interest_rows)
    if resolution_rows:
        upsert_records(ctx.db, PolymarketResolution, resolution_rows)

    partition = {"market_id": market_id}
    if orderbook_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_orderbook_snapshots",
            records=orderbook_rows,
            partition=partition,
        )
    if history_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_price_history",
            records=history_rows,
            partition=partition,
        )
    if trade_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_trades",
            records=trade_rows,
            partition=partition,
        )
    if open_interest_rows:
        persist_silver(
            ctx,
            job_run_id=run.id,
            dataset="polymarket_open_interest_history",
            records=open_interest_rows,
            partition=partition,
        )

    upsert_cursor_state(
        ctx.db,
        source="polymarket",
        dataset="market_history",
        cursor_key=market_id,
        cursor_value={
            "market_id": market_id,
            "synced_at": utc_now().isoformat(),
            "fetch_errors": fetch_errors,
        },
    )
    records_written = (
        len(orderbook_rows)
        + len(orderbook_level_rows)
        + len(history_rows)
        + len(trade_rows)
        + len(open_interest_rows)
        + len(resolution_rows)
    )
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        cursor_after={
            "market_id": market_id,
            "synced_at": utc_now().isoformat(),
            "fetch_errors": fetch_errors,
        },
        records_written=records_written,
    )
    return {
        "job_run_id": run.id,
        "status": "completed",
        "market_id": market_id,
        "records_written": records_written,
        "fetch_errors": fetch_errors,
    }
