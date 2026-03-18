from __future__ import annotations

from pathlib import Path
from typing import Any

import fastf1
from f1_polymarket_lab.common import ensure_dir


class FastF1ScheduleConnector:
    def __init__(self, cache_dir: Path) -> None:
        fastf1.Cache.enable_cache(str(ensure_dir(cache_dir)))

    def fetch_event_schedule(self, year: int) -> list[dict[str, Any]]:
        schedule = fastf1.get_event_schedule(year)
        records = schedule.to_dict(orient="records")
        return [dict(record) for record in records]
