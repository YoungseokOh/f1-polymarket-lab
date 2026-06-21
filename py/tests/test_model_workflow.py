from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from f1_polymarket_worker.model_workflow import _earliest_as_of_ts


def _write_snapshot(path: Path, as_of: datetime) -> dict[str, str]:
    pl.DataFrame({"as_of_ts": [as_of], "label_yes": [0]}).write_parquet(path)
    return {"path": str(path)}


def test_earliest_as_of_ts_orders_negative_round_keys_chronologically(tmp_path: Path) -> None:
    # jolpica synthetic meetings use negative round-encoded keys: -202605 (round 5)
    # sorts AFTER -202607 (round 7) numerically, which would reverse the season.
    # Ordering by as-of timestamp must recover the true chronological sequence.
    r5 = _write_snapshot(tmp_path / "r5.parquet", datetime(2026, 5, 22, tzinfo=timezone.utc))
    r6 = _write_snapshot(tmp_path / "r6.parquet", datetime(2026, 6, 5, tzinfo=timezone.utc))
    r7 = _write_snapshot(tmp_path / "r7.parquet", datetime(2026, 6, 12, tzinfo=timezone.utc))
    grouped = {-202605: [r5], -202606: [r6], -202607: [r7]}

    ordered = sorted(grouped, key=lambda mk: (_earliest_as_of_ts(grouped[mk]), mk))

    assert ordered == [-202605, -202606, -202607]
    # Naive integer sort would have produced the reversed (future-first) order.
    assert sorted(grouped) == [-202607, -202606, -202605]


def test_earliest_as_of_ts_falls_back_to_aware_max(tmp_path: Path) -> None:
    fallback = _earliest_as_of_ts([{"path": ""}])
    assert fallback.tzinfo is not None
