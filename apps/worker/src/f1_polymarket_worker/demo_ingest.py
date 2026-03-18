from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from f1_polymarket_lab.common import get_settings, payload_checksum, slugify, utc_now
from f1_polymarket_lab.connectors import (
    FastF1ScheduleConnector,
    OpenF1Connector,
    PolymarketConnector,
    parse_market_taxonomy,
)
from f1_polymarket_lab.features import default_feature_registry
from f1_polymarket_lab.storage.lake import LakeWriter
from f1_polymarket_lab.storage.models import (
    CircuitMetadata,
    EntityMappingF1ToPolymarket,
    F1Driver,
    F1Lap,
    F1Meeting,
    F1RaceControl,
    F1Session,
    F1SessionResult,
    F1Stint,
    F1Team,
    F1Weather,
    FeatureRegistry,
    PolymarketEvent,
    PolymarketMarket,
    PolymarketMarketRule,
    PolymarketOrderbookSnapshot,
    PolymarketPriceHistory,
    PolymarketToken,
    PolymarketTrade,
    SourceFetchLog,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy.orm import Session


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text)


def session_code_from_name(name: str) -> str | None:
    mapping = {
        "Practice 1": "FP1",
        "Practice 2": "FP2",
        "Practice 3": "FP3",
        "Qualifying": "Q",
        "Sprint": "S",
        "Race": "R",
    }
    return mapping.get(name)


def normalize_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, list):
        numeric = [float(item) for item in value if item not in (None, "")]
        return min(numeric) if numeric else None
    return float(value)


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    return str(value)


def record_fetch(
    db: Session,
    *,
    source: str,
    dataset: str,
    endpoint: str,
    params: dict[str, Any],
    payload: Any,
    bronze_path: str,
    response_status: int = 200,
    status: str = "ok",
) -> None:
    now = utc_now()
    record = {
        "id": f"{source}:{dataset}:{payload_checksum([endpoint, params, bronze_path])[:24]}",
        "source": source,
        "dataset": dataset,
        "endpoint": endpoint,
        "request_params": params,
        "status": status,
        "response_status": response_status,
        "records_fetched": len(payload) if isinstance(payload, list) else 1,
        "bronze_path": bronze_path,
        "checksum": payload_checksum(payload),
        "checkpoint": None,
        "error_message": None,
        "started_at": now,
        "finished_at": now,
    }
    upsert_records(db, SourceFetchLog, [record])


def ingest_f1_demo(
    db: Session,
    lake: LakeWriter,
    *,
    season: int,
    weekends: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    openf1 = OpenF1Connector()
    fastf1 = FastF1ScheduleConnector(get_settings().data_root / "cache" / "fastf1")

    raw_sessions = openf1.fetch_sessions(season)
    bronze_path = lake.write_bronze("openf1", "sessions", raw_sessions)
    record_fetch(
        db,
        source="openf1",
        dataset="sessions",
        endpoint="/v1/sessions",
        params={"year": season},
        payload=raw_sessions,
        bronze_path=str(bronze_path),
    )

    sessions_by_meeting: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for record in raw_sessions:
        sessions_by_meeting[int(record["meeting_key"])].append(record)

    selected_meeting_keys = [
        meeting_key
        for meeting_key, records in sorted(sessions_by_meeting.items())
        if any(str(record.get("session_name", "")).startswith("Practice") for record in records)
    ][:weekends]
    try:
        schedule_records = fastf1.fetch_event_schedule(season)
    except Exception:
        schedule_records = []

    schedule_by_location = {
        str(record.get("Location", "")).lower(): record for record in schedule_records
    }

    meeting_rows: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    driver_rows: dict[str, dict[str, Any]] = {}
    team_rows: dict[str, dict[str, Any]] = {}
    result_rows: list[dict[str, Any]] = []
    lap_rows: list[dict[str, Any]] = []
    stint_rows: list[dict[str, Any]] = []
    weather_rows: list[dict[str, Any]] = []
    race_control_rows: list[dict[str, Any]] = []
    circuit_rows: dict[str, dict[str, Any]] = {}

    for meeting_key in selected_meeting_keys:
        meeting_sessions = sessions_by_meeting[meeting_key]
        first_session = sorted(meeting_sessions, key=lambda item: item["date_start"])[0]
        schedule_row = schedule_by_location.get(str(first_session.get("location", "")).lower(), {})
        meeting_id = f"meeting:{meeting_key}"
        meeting_rows.append(
            {
                "id": meeting_id,
                "source": "openf1",
                "meeting_key": meeting_key,
                "season": season,
                "round_number": schedule_row.get("RoundNumber"),
                "meeting_name": str(
                    schedule_row.get("EventName") or first_session.get("country_name")
                ),
                "meeting_official_name": schedule_row.get("OfficialEventName"),
                "circuit_short_name": first_session.get("circuit_short_name"),
                "country_name": first_session.get("country_name"),
                "location": first_session.get("location"),
                "start_date_utc": parse_dt(first_session.get("date_start")),
                "end_date_utc": parse_dt(
                    sorted(meeting_sessions, key=lambda item: item["date_end"])[-1]["date_end"]
                ),
                "raw_payload": first_session,
            }
        )

        circuit_key = str(first_session.get("circuit_key"))
        circuit_rows[circuit_key] = {
            "id": circuit_key,
            "circuit_name": first_session.get("circuit_short_name"),
            "country_name": first_session.get("country_name"),
            "track_cluster": None,
            "length_km": None,
            "turns": None,
            "altitude_m": None,
            "clockwise": None,
            "raw_payload": {"circuit_key": first_session.get("circuit_key")},
        }

        weather = openf1.fetch_weather(meeting_key)
        bronze_path = lake.write_bronze("openf1", f"weather_meeting_{meeting_key}", weather)
        record_fetch(
            db,
            source="openf1",
            dataset="weather",
            endpoint="/v1/weather",
            params={"meeting_key": meeting_key},
            payload=weather,
            bronze_path=str(bronze_path),
        )
        for item in weather:
            record_id = f"{meeting_key}:{item.get('date')}"
            weather_rows.append(
                {
                    "id": record_id,
                    "meeting_id": meeting_id,
                    "session_id": None,
                    "observed_at_utc": parse_dt(item.get("date")),
                    "air_temperature_c": item.get("air_temperature"),
                    "humidity_pct": item.get("humidity"),
                    "pressure_hpa": item.get("pressure"),
                    "rainfall": item.get("rainfall"),
                    "track_temperature_c": item.get("track_temperature"),
                    "wind_direction_deg": item.get("wind_direction"),
                    "wind_speed_mps": item.get("wind_speed"),
                    "raw_payload": item,
                }
            )

        for session in meeting_sessions:
            session_key = int(session["session_key"])
            session_id = f"session:{session_key}"
            session_name = str(session["session_name"])
            session_rows.append(
                {
                    "id": session_id,
                    "source": "openf1",
                    "session_key": session_key,
                    "meeting_id": meeting_id,
                    "session_name": session_name,
                    "session_type": session.get("session_type"),
                    "session_code": session_code_from_name(session_name),
                    "date_start_utc": parse_dt(session.get("date_start")),
                    "date_end_utc": parse_dt(session.get("date_end")),
                    "status": "complete" if session.get("date_end") else "scheduled",
                    "session_order": None,
                    "is_practice": session_name.startswith("Practice"),
                    "raw_payload": session,
                }
            )

            if not session_name.startswith("Practice"):
                continue

            drivers = openf1.fetch_drivers(session_key)
            bronze_path = lake.write_bronze("openf1", f"drivers_session_{session_key}", drivers)
            record_fetch(
                db,
                source="openf1",
                dataset="drivers",
                endpoint="/v1/drivers",
                params={"session_key": session_key},
                payload=drivers,
                bronze_path=str(bronze_path),
            )
            for driver in drivers:
                driver_id = f"driver:{driver['driver_number']}"
                team_id = f"team:{slugify(str(driver.get('team_name', 'unknown')))}"
                driver_rows[driver_id] = {
                    "id": driver_id,
                    "source": "openf1",
                    "driver_number": int(driver["driver_number"]),
                    "broadcast_name": driver.get("broadcast_name"),
                    "full_name": driver.get("full_name"),
                    "first_name": driver.get("first_name"),
                    "last_name": driver.get("last_name"),
                    "name_acronym": driver.get("name_acronym"),
                    "team_id": team_id,
                    "country_code": driver.get("country_code"),
                    "headshot_url": driver.get("headshot_url"),
                    "raw_payload": driver,
                }
                team_rows[team_id] = {
                    "id": team_id,
                    "source": "openf1",
                    "team_name": driver.get("team_name") or "Unknown",
                    "team_color": driver.get("team_colour"),
                    "raw_payload": driver,
                }

            results = openf1.fetch_session_results(session_key)
            bronze_path = lake.write_bronze("openf1", f"session_result_{session_key}", results)
            record_fetch(
                db,
                source="openf1",
                dataset="session_result",
                endpoint="/v1/session_result",
                params={"session_key": session_key},
                payload=results,
                bronze_path=str(bronze_path),
            )
            for result in results:
                driver_id = f"driver:{result.get('driver_number')}"
                result_rows.append(
                    {
                        "id": f"{session_key}:{result.get('driver_number')}",
                        "session_id": session_id,
                        "driver_id": driver_id,
                        "position": result.get("position"),
                        "fastest_lap_seconds": normalize_float(result.get("duration")),
                        "gap_to_leader": normalize_text(result.get("gap_to_leader")),
                        "number_of_laps": result.get("number_of_laps"),
                        "raw_payload": result,
                    }
                )

            laps = openf1.fetch_laps(session_key)
            bronze_path = lake.write_bronze("openf1", f"laps_{session_key}", laps)
            record_fetch(
                db,
                source="openf1",
                dataset="laps",
                endpoint="/v1/laps",
                params={"session_key": session_key},
                payload=laps,
                bronze_path=str(bronze_path),
            )
            for lap in laps:
                lap_rows.append(
                    {
                        "id": ":".join(
                            [
                                str(session_key),
                                str(lap.get("driver_number")),
                                str(lap.get("lap_number")),
                            ]
                        ),
                        "session_id": session_id,
                        "driver_id": f"driver:{lap.get('driver_number')}",
                        "lap_number": int(lap.get("lap_number", 0)),
                        "lap_start_utc": parse_dt(lap.get("date_start")),
                        "lap_end_utc": parse_dt(lap.get("date_end")),
                        "lap_duration_seconds": lap.get("lap_duration"),
                        "is_pit_out_lap": lap.get("is_pit_out_lap"),
                        "stint_number": lap.get("stint_number"),
                        "sector_1_seconds": lap.get("duration_sector_1"),
                        "sector_2_seconds": lap.get("duration_sector_2"),
                        "sector_3_seconds": lap.get("duration_sector_3"),
                        "speed_trap_kph": lap.get("i1_speed"),
                        "raw_payload": lap,
                    }
                )

            stints = openf1.fetch_stints(session_key)
            bronze_path = lake.write_bronze("openf1", f"stints_{session_key}", stints)
            record_fetch(
                db,
                source="openf1",
                dataset="stints",
                endpoint="/v1/stints",
                params={"session_key": session_key},
                payload=stints,
                bronze_path=str(bronze_path),
            )
            for stint in stints:
                stint_rows.append(
                    {
                        "id": ":".join(
                            [
                                str(session_key),
                                str(stint.get("driver_number")),
                                str(stint.get("stint_number")),
                            ]
                        ),
                        "session_id": session_id,
                        "driver_id": f"driver:{stint.get('driver_number')}",
                        "stint_number": int(stint.get("stint_number", 0)),
                        "compound": stint.get("compound"),
                        "lap_start": stint.get("lap_start"),
                        "lap_end": stint.get("lap_end"),
                        "tyre_age_at_start": stint.get("tyre_age_at_start"),
                        "raw_payload": stint,
                    }
                )

            race_control = openf1.fetch_race_control(session_key)
            bronze_path = lake.write_bronze("openf1", f"race_control_{session_key}", race_control)
            record_fetch(
                db,
                source="openf1",
                dataset="race_control",
                endpoint="/v1/race_control",
                params={"session_key": session_key},
                payload=race_control,
                bronze_path=str(bronze_path),
            )
            for message in race_control:
                race_control_rows.append(
                    {
                        "id": f"{session_key}:{payload_checksum(message)[:16]}",
                        "meeting_id": meeting_id,
                        "session_id": session_id,
                        "driver_number": message.get("driver_number"),
                        "category": message.get("category"),
                        "message": message.get("message"),
                        "flag": message.get("flag"),
                        "scope": message.get("scope"),
                        "observed_at_utc": parse_dt(message.get("date")),
                        "raw_payload": message,
                    }
                )

    upsert_records(db, F1Meeting, meeting_rows)
    upsert_records(db, F1Session, session_rows)
    upsert_records(db, F1Driver, driver_rows.values())
    upsert_records(db, F1Team, team_rows.values())
    upsert_records(db, F1SessionResult, result_rows)
    upsert_records(db, F1Lap, lap_rows)
    upsert_records(db, F1Stint, stint_rows)
    upsert_records(db, F1Weather, weather_rows)
    upsert_records(db, F1RaceControl, race_control_rows)
    upsert_records(db, CircuitMetadata, circuit_rows.values())

    lake.write_silver("f1_meetings", meeting_rows, partition=f"season={season}")
    lake.write_silver("f1_sessions", session_rows, partition=f"season={season}")
    lake.write_silver("f1_session_results", result_rows, partition=f"season={season}")
    lake.write_silver("f1_laps", lap_rows, partition=f"season={season}")
    lake.write_silver("f1_stints", stint_rows, partition=f"season={season}")
    lake.write_silver("f1_weather", weather_rows, partition=f"season={season}")
    lake.write_silver("f1_race_control", race_control_rows, partition=f"season={season}")

    return meeting_rows, session_rows


def extract_event_rows(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for market in markets:
        for event in market.get("events", []):
            events[str(event["id"])] = {
                "id": str(event["id"]),
                "ticker": event.get("ticker"),
                "slug": event["slug"],
                "title": event["title"],
                "description": event.get("description"),
                "category": event.get("category"),
                "subcategory": event.get("subcategory"),
                "start_at_utc": parse_dt(event.get("startDate")),
                "end_at_utc": parse_dt(event.get("endDate")),
                "active": bool(event.get("active")),
                "closed": bool(event.get("closed")),
                "archived": bool(event.get("archived")),
                "liquidity": event.get("liquidity"),
                "volume": event.get("volume"),
                "open_interest": event.get("openInterest"),
                "resolution_source": event.get("resolutionSource"),
                "raw_payload": event,
            }
    return list(events.values())


def best_levels(book: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if book is None:
        return None, None
    bid = float(book["bids"][0]["price"]) if book.get("bids") else None
    ask = float(book["asks"][0]["price"]) if book.get("asks") else None
    return bid, ask


def compute_imbalance(book: dict[str, Any] | None) -> float | None:
    if book is None:
        return None
    bid_depth = sum(float(level["size"]) for level in book.get("bids", [])[:5])
    ask_depth = sum(float(level["size"]) for level in book.get("asks", [])[:5])
    total = bid_depth + ask_depth
    if total == 0:
        return None
    return (bid_depth - ask_depth) / total


def ingest_polymarket_demo(
    db: Session,
    lake: LakeWriter,
    *,
    market_batches: int,
) -> list[dict[str, Any]]:
    connector = PolymarketConnector()

    f1_keywords = [
        "formula 1",
        "grand prix",
        "fp1",
        "fp2",
        "fp3",
        "practice",
        "fastest lap",
        "red flag",
        "safety car",
    ]

    candidate_markets: list[dict[str, Any]] = []
    for batch_index in range(market_batches):
        markets = connector.list_markets(
            limit=100, offset=batch_index * 100, closed=True, active=False
        )
        bronze_path = lake.write_bronze("polymarket", f"markets_batch_{batch_index}", markets)
        record_fetch(
            db,
            source="polymarket",
            dataset="markets",
            endpoint="/markets",
            params={"limit": 100, "offset": batch_index * 100, "closed": True, "active": False},
            payload=markets,
            bronze_path=str(bronze_path),
        )
        for market in markets:
            haystack = " ".join(
                [
                    str(market.get("question") or ""),
                    str(market.get("description") or ""),
                    str(market.get("slug") or ""),
                ]
            ).lower()
            if any(keyword in haystack for keyword in f1_keywords):
                candidate_markets.append(market)

    if not candidate_markets:
        candidate_markets = connector.list_markets(limit=12, active=True, closed=False)
        bronze_path = lake.write_bronze("polymarket", "markets_active_fallback", candidate_markets)
        record_fetch(
            db,
            source="polymarket",
            dataset="markets_fallback",
            endpoint="/markets",
            params={"limit": 12, "active": True, "closed": False},
            payload=candidate_markets,
            bronze_path=str(bronze_path),
        )

    event_rows = extract_event_rows(candidate_markets)
    market_rows: list[dict[str, Any]] = []
    token_rows: list[dict[str, Any]] = []
    rule_rows: list[dict[str, Any]] = []
    orderbook_rows: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []

    for market in candidate_markets:
        parsed = parse_market_taxonomy(market.get("question") or "", market.get("description"))
        token_ids = json.loads(market.get("clobTokenIds") or "[]")
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

        outcomes = json.loads(market.get("outcomes") or "[]")
        prices = json.loads(market.get("outcomePrices") or "[]")
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

            book = connector.get_order_book(token_id)
            midpoint = connector.get_midpoint(token_id)
            spread = connector.get_spread(token_id)

            if book is not None:
                best_bid_book, best_ask_book = best_levels(book)
                orderbook_rows.append(
                    {
                        "id": f"{market['id']}:{token_id}:{book.get('timestamp')}",
                        "market_id": str(market["id"]),
                        "token_id": token_id,
                        "observed_at_utc": datetime.fromtimestamp(
                            int(book["timestamp"]) / 1000,
                            tz=timezone.utc,
                        ),
                        "best_bid": best_bid_book,
                        "best_ask": best_ask_book,
                        "spread": spread,
                        "bid_depth_json": book.get("bids"),
                        "ask_depth_json": book.get("asks"),
                        "imbalance": compute_imbalance(book),
                        "raw_payload": book,
                    }
                )

            for point in connector.get_price_history(token_id):
                observed_at = datetime.fromtimestamp(int(point["t"]), tz=timezone.utc)
                history_rows.append(
                    {
                        "id": f"{market['id']}:{token_id}:{point['t']}",
                        "market_id": str(market["id"]),
                        "token_id": token_id,
                        "observed_at_utc": observed_at,
                        "price": point.get("p"),
                        "midpoint": midpoint,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "source_kind": "clob",
                        "raw_payload": point,
                    }
                )

            for trade in connector.get_trades(market["conditionId"], limit=200):
                if trade.get("asset") not in token_ids:
                    continue
                trade_rows.append(
                    {
                        "id": ":".join(
                            [
                                str(market["id"]),
                                str(trade.get("transactionHash") or payload_checksum(trade)[:12]),
                            ]
                        ),
                        "market_id": str(market["id"]),
                        "token_id": trade.get("asset"),
                        "condition_id": market["conditionId"],
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

    upsert_records(db, PolymarketEvent, event_rows)
    upsert_records(db, PolymarketMarket, market_rows)
    upsert_records(db, PolymarketToken, token_rows)
    upsert_records(db, PolymarketMarketRule, rule_rows)
    upsert_records(db, PolymarketOrderbookSnapshot, orderbook_rows)
    upsert_records(db, PolymarketPriceHistory, history_rows)
    upsert_records(db, PolymarketTrade, trade_rows)

    lake.write_silver("polymarket_events", event_rows)
    lake.write_silver("polymarket_markets", market_rows)
    lake.write_silver("polymarket_tokens", token_rows)
    lake.write_silver("polymarket_orderbook_snapshots", orderbook_rows)
    lake.write_silver("polymarket_price_history", history_rows)
    lake.write_silver("polymarket_trades", trade_rows)

    return market_rows


def build_mappings(
    db: Session,
    *,
    meetings: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    markets: list[dict[str, Any]],
) -> None:
    _ = meetings
    session_candidates = [
        session for session in sessions if session["session_code"] in {"FP1", "FP2", "FP3"}
    ]

    mapping_rows: list[dict[str, Any]] = []
    for market in markets:
        if market["taxonomy"] == "other" or market["target_session_code"] is None:
            continue

        event_start = market["start_at_utc"]
        best_match: dict[str, Any] | None = None
        for session in session_candidates:
            if session["session_code"] != market["target_session_code"]:
                continue
            if event_start is None or session["date_start_utc"] is None:
                continue
            delta_days = abs((event_start - session["date_start_utc"]).total_seconds()) / 86400
            if delta_days > 5:
                continue
            if (
                best_match is None
                or delta_days
                < abs((event_start - best_match["date_start_utc"]).total_seconds()) / 86400
            ):
                best_match = session

        if best_match is None:
            continue

        mapping_rows.append(
            {
                "id": f"mapping:{market['id']}:{best_match['id']}",
                "f1_meeting_id": best_match["meeting_id"],
                "f1_session_id": best_match["id"],
                "polymarket_event_id": market["event_id"],
                "polymarket_market_id": market["id"],
                "mapping_type": market["taxonomy"],
                "confidence": market["taxonomy_confidence"],
                "matched_by": "session_code_and_time_window",
                "notes": None,
                "override_flag": False,
            }
        )

    upsert_records(db, EntityMappingF1ToPolymarket, mapping_rows)


def seed_feature_registry(db: Session) -> None:
    rows = [
        {
            "id": f"feature:{definition.feature_name}",
            "feature_name": definition.feature_name,
            "feature_group": definition.feature_group,
            "description": definition.description,
            "data_type": definition.data_type,
            "version": definition.version,
            "owner": "platform",
            "created_at": utc_now(),
        }
        for definition in default_feature_registry()
    ]
    upsert_records(db, FeatureRegistry, rows)


def ingest_demo(db: Session, *, season: int, weekends: int, market_batches: int) -> None:
    settings = get_settings()
    lake = LakeWriter(settings.data_root)
    meetings, sessions = ingest_f1_demo(db, lake, season=season, weekends=weekends)
    markets = ingest_polymarket_demo(db, lake, market_batches=market_batches)
    build_mappings(db, meetings=meetings, sessions=sessions, markets=markets)
    seed_feature_registry(db)
