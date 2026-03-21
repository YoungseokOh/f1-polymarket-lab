from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from .f1db import F1DBConnector
from .jolpica import JolpicaConnector
from .openf1 import OpenF1Connector
from .polymarket import PolymarketConnector
from .taxonomy import ParsedMarket, infer_market_scheduled_date, parse_market_taxonomy

if TYPE_CHECKING:
    from .collector import DataCollector, MeetingData, SessionData
    from .fastf1_adapter import FastF1ScheduleConnector
    from .openf1_live import OpenF1LiveConnector
    from .polymarket_live import PolymarketLiveConnector

# Lazy-loaded modules that require optional dependencies (fastf1, paho, websockets)
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "DataCollector": (".collector", "DataCollector"),
    "MeetingData": (".collector", "MeetingData"),
    "SessionData": (".collector", "SessionData"),
    "FastF1ScheduleConnector": (".fastf1_adapter", "FastF1ScheduleConnector"),
    "OpenF1LiveConnector": (".openf1_live", "OpenF1LiveConnector"),
    "PolymarketLiveConnector": (".polymarket_live", "PolymarketLiveConnector"),
}


def __getattr__(name: str) -> object:
    if name in _LAZY_IMPORTS:
        module_path, attr = _LAZY_IMPORTS[name]
        mod = importlib.import_module(module_path, __package__)
        return getattr(mod, attr)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "DataCollector",
    "F1DBConnector",
    "FastF1ScheduleConnector",
    "JolpicaConnector",
    "MeetingData",
    "OpenF1Connector",
    "OpenF1LiveConnector",
    "ParsedMarket",
    "PolymarketConnector",
    "PolymarketLiveConnector",
    "SessionData",
    "infer_market_scheduled_date",
    "parse_market_taxonomy",
]
