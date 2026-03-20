from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def payload_checksum(payload: Any) -> str:
    content = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def stable_uuid(*parts: object) -> str:
    content = "::".join(str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, content))


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "unknown"


def parse_utc_offset(value: str | None) -> timedelta | None:
    if value is None:
        return None
    match = re.fullmatch(r"([+-]?)(\d{2}):(\d{2})(?::(\d{2}))?", value.strip())
    if match is None:
        return None
    sign_text, hours_text, minutes_text, seconds_text = match.groups()
    sign = -1 if sign_text == "-" else 1
    delta = timedelta(
        hours=int(hours_text),
        minutes=int(minutes_text),
        seconds=int(seconds_text or "0"),
    )
    return delta * sign


def timestamp_date_variants(
    value: datetime | None,
    *,
    gmt_offset: str | None = None,
) -> tuple[date, ...]:
    if value is None:
        return ()
    observed = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    variants = [observed.date()]
    offset = parse_utc_offset(gmt_offset)
    if offset is not None:
        local_date = (observed + offset).date()
        if local_date not in variants:
            variants.append(local_date)
    return tuple(variants)
