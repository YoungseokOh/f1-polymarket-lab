from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from f1_polymarket_lab.common import (
    Settings,
    get_settings,
)
from f1_polymarket_lab.common import (
    normalize_float as common_normalize_float,
)
from f1_polymarket_lab.connectors.base import FetchBatch
from f1_polymarket_lab.storage.lake import LakeObject, LakeWriter
from sqlalchemy.orm import Session

from f1_polymarket_worker.lineage import (
    record_fetch_batch,
    record_lake_object_manifest,
)


@dataclass(slots=True)
class JobResult:
    records_written: int = 0
    cursor_after: dict[str, Any] | None = None
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineContext:
    db: Session
    execute: bool = False
    settings: Settings = field(default_factory=get_settings)
    lake: LakeWriter = field(init=False)

    def __post_init__(self) -> None:
        self.lake = LakeWriter(self.settings.data_root)


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        match = re.match(
            r"^(?P<prefix>.+T\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?(?P<tz>[+-]\d{2}:\d{2}(?::\d{2})?)?$",
            text,
        )
        if match is None:
            raise
        fraction = match.group("fraction") or ""
        timezone_text = match.group("tz") or ""
        normalized_fraction = ""
        if fraction:
            normalized_fraction = f".{fraction[1:7].ljust(6, '0')}"
        if timezone_text.count(":") == 2 and timezone_text.endswith(":00"):
            timezone_text = timezone_text[:-3]
        normalized = f"{match.group('prefix')}{normalized_fraction}{timezone_text}"
        return datetime.fromisoformat(normalized)


def normalize_float(value: Any) -> float | None:
    return cast(float | None, common_normalize_float(value))


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


def persist_fetch(
    ctx: PipelineContext,
    *,
    job_run_id: str,
    batch: FetchBatch,
    partition: dict[str, str] | None = None,
) -> LakeObject:
    bronze_object = ctx.lake.write_bronze_object(
        batch.source,
        batch.dataset,
        batch.payload,
        partition=partition,
    )
    record_lake_object_manifest(
        ctx.db,
        object_ref=bronze_object,
        job_run_id=job_run_id,
        metadata_json={"endpoint": batch.endpoint, "request_params": batch.params},
    )
    record_fetch_batch(
        ctx.db,
        batch=batch,
        bronze_object=bronze_object,
        job_run_id=job_run_id,
    )
    return bronze_object


def persist_silver(
    ctx: PipelineContext,
    *,
    job_run_id: str,
    dataset: str,
    records: list[dict[str, Any]],
    partition: dict[str, str] | None = None,
) -> None:
    object_ref = ctx.lake.write_silver_object(dataset, records, partition=partition)
    if object_ref is None:
        return
    record_lake_object_manifest(
        ctx.db,
        object_ref=object_ref,
        job_run_id=job_run_id,
        metadata_json={"normalized_dataset": dataset},
    )
