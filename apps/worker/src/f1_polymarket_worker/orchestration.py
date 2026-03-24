from __future__ import annotations

# Thin re-export shim — the real implementations live in the domain modules.
# All existing callers (cli.py, routes.py, tests) continue to work unchanged.
from f1_polymarket_worker.f1_backfill import (
    backfill_f1_history,
    backfill_f1_history_all,
    hydrate_polymarket_f1_history,
)
from f1_polymarket_worker.market_discovery import (
    discover_session_polymarket,
    sync_polymarket_f1_catalog,
)
from f1_polymarket_worker.weekend_ops import (
    capture_live_weekend,
    validate_f1_weekend_subset,
)

__all__ = [
    "backfill_f1_history",
    "backfill_f1_history_all",
    "capture_live_weekend",
    "discover_session_polymarket",
    "hydrate_polymarket_f1_history",
    "sync_polymarket_f1_catalog",
    "validate_f1_weekend_subset",
]
