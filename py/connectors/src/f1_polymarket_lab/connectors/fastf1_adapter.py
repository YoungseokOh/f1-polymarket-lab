from __future__ import annotations

from pathlib import Path
from typing import Any

from f1_polymarket_lab.common import ensure_dir


def _get_fastf1() -> Any:
    """Lazy-import fastf1 so the package can be imported without it installed."""
    try:
        import fastf1
    except ModuleNotFoundError as exc:
        msg = "fastf1 is required for FastF1ScheduleConnector – install it with: pip install fastf1"
        raise ImportError(msg) from exc
    return fastf1


class FastF1ScheduleConnector:
    def __init__(self, cache_dir: Path) -> None:
        _get_fastf1().Cache.enable_cache(str(ensure_dir(cache_dir)))

    def fetch_event_schedule(self, year: int) -> list[dict[str, Any]]:
        schedule = _get_fastf1().get_event_schedule(year)
        records = schedule.to_dict(orient="records")
        return [dict(record) for record in records]
