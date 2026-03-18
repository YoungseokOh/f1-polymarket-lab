from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class FetchBatch:
    source: str
    dataset: str
    endpoint: str
    params: dict[str, Any]
    payload: Any
    response_status: int
    checkpoint: str | None = None
    source_emitted_at: datetime | None = None
