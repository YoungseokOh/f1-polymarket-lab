"""Standalone N-GP walk-forward backtest runner.

Connects to the project database, builds snapshots for configured GPs,
runs the walk-forward backtest, and prints the results.

Usage:
    uv run python run_backtest_2gp.py                  # AUS + China (2 GP)
    uv run python run_backtest_2gp.py --gps 3          # AUS + China + Japan
    uv run python run_backtest_2gp.py --min-edge 0.03 --bet-size 20
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from f1_polymarket_lab.common import get_settings
from f1_polymarket_lab.storage.db import db_session
from f1_polymarket_worker.backtest import (
    run_walk_forward_backtest,
    save_backtest_report,
)
from f1_polymarket_worker.pipeline import PipelineContext
from f1_polymarket_worker.quicktest import (
    AUS_DEFAULT_MEETING_KEY,
    AUS_DEFAULT_SEASON,
    CHINA_DEFAULT_MEETING_KEY,
    CHINA_DEFAULT_SEASON,
    JAPAN_DEFAULT_MEETING_KEY,
    JAPAN_DEFAULT_SEASON,
    build_aus_fp1_to_q_snapshot,
    build_china_fp1_to_sq_snapshot,
    build_japan_fp1_to_q_snapshot,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GP_CONFIGS = [
    {
        "name": "Australian Grand Prix",
        "meeting_key": AUS_DEFAULT_MEETING_KEY,
        "season": AUS_DEFAULT_SEASON,
        "builder": build_aus_fp1_to_q_snapshot,
    },
    {
        "name": "Chinese Grand Prix",
        "meeting_key": CHINA_DEFAULT_MEETING_KEY,
        "season": CHINA_DEFAULT_SEASON,
        "builder": build_china_fp1_to_sq_snapshot,
    },
    {
        "name": "Japanese Grand Prix",
        "meeting_key": JAPAN_DEFAULT_MEETING_KEY,
        "season": JAPAN_DEFAULT_SEASON,
        "builder": build_japan_fp1_to_q_snapshot,
    },
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run N-GP walk-forward backtest")
    parser.add_argument("--gps", type=int, default=2, help="Number of GPs to include (default: 2)")
    parser.add_argument("--min-edge", type=float, default=0.05, help="Minimum edge threshold")
    parser.add_argument("--bet-size", type=float, default=10.0, help="Flat bet size in USD")
    parser.add_argument("--strategy", default="hybrid_flat_bet", help="Strategy name")
    parser.add_argument("--model", default="hybrid", help="Model name")
    args = parser.parse_args()

    gp_count = min(args.gps, len(GP_CONFIGS))
    active_gps = GP_CONFIGS[:gp_count]
    settings = get_settings()

    gp_range = f"R1\u2013R{gp_count}"
    gp_names = " \u2192 ".join(gp["name"].split()[0] + " GP" for gp in active_gps)

    print("=" * 65)
    print(f"  2026 Season Walk-Forward Backtest ({gp_range})")
    print(f"  {gp_names}")
    print(f"  Strategy: {args.strategy}  |  Model: {args.model}")
    print(f"  Min edge: {args.min_edge:.0%}  |  Bet size: ${args.bet_size:.0f}")
    print("=" * 65)

    with db_session(settings.database_url) as session:
        ctx = PipelineContext(db=session, execute=True, settings=settings)

        # Step 1: Build snapshots for each GP
        gp_walk_configs: list[dict] = []

        for gp in active_gps:
            print(f"\n── Building snapshot: {gp['name']} (key={gp['meeting_key']}) ──")
            snap = gp["builder"](ctx, meeting_key=gp["meeting_key"], season=gp["season"])
            snapshot_id = snap["snapshot_id"]
            print(f"  snapshot_id = {snapshot_id}")
            print(f"  row_count   = {snap['row_count']}")
            print(f"  hydrated    = {snap.get('markets_hydrated', 0)}")

            gp_walk_configs.append(
                {
                    "meeting_key": gp["meeting_key"],
                    "season": gp["season"],
                    "snapshot_id": snapshot_id,
                }
            )

        # Step 2: Run walk-forward backtest
        print("\n── Running walk-forward backtest ──────────────────────────────")
        result = run_walk_forward_backtest(
            ctx,
            gp_configs=gp_walk_configs,
            strategy_name=args.strategy,
            model_name=args.model,
            min_edge=args.min_edge,
            bet_size=args.bet_size,
        )

        # Step 3: Print results
        sm = result["season_metrics"]
        print("\n── Season Summary ────────────────────────────────────────────")
        print(f"  GPs evaluated : {sm.get('gp_count', 0)}")
        print(f"  Total bets    : {sm.get('bet_count', 0)}")
        print(f"  Total wagered : ${sm.get('total_wagered', 0):.2f}")
        print(f"  Total PnL     : ${sm.get('total_pnl', 0):+.2f}")
        print(f"  ROI           : {sm.get('roi_pct', 0):+.1f}%")
        print(f"  Hit rate      : {sm.get('hit_rate', 0):.1%}")
        print(f"  Sharpe        : {sm.get('sharpe', 0):.4f}")
        print(f"  Brier score   : {sm.get('brier_score', 'N/A')}")

        print("\n── Equity Curve ──────────────────────────────────────────────")
        print(f"  {'GP':<30s}  {'Bets':>5}  {'GP PnL':>10}  {'Cumulative':>12}")
        print("  " + "─" * 60)
        for point in result.get("equity_curve", []):
            print(
                f"  {point['gp']:<30s}  {point['bets']:>5}  "
                f"${point['gp_pnl']:>+9.2f}  ${point['cumulative_pnl']:>+11.2f}"
            )

        # Per-GP detail
        for gp in result.get("gp_results", []):
            gp_name = gp["gp_name"]
            bt = gp["backtest_result"]
            settled = bt.get("settled_rows", [])
            if settled:
                print(f"\n── {gp_name} — Bet Detail ─────────────────────────────")
                hdr = f"  {'Driver':<22s}  {'Prob':>6}  {'Entry':>6}"
                hdr += f"  {'Edge':>6}  {'Result':>6}  {'PnL':>8}"
                print(hdr)
                print("  " + "─" * 60)
                for row in sorted(settled, key=lambda r: r["edge"], reverse=True):
                    result_str = "WIN" if row["outcome"] == 1 else "LOSS"
                    print(
                        f"  {row['driver_name']:<22s}  "
                        f"{row['model_probability']:>5.1%}  "
                        f"{row['entry_price']:>5.2f}¢  "
                        f"{row['edge']:>+5.1%}  "
                        f"{result_str:>6}  "
                        f"${row['pnl']:>+7.2f}"
                    )

        # Step 4: Save report
        slug = f"2026-season-backtest-r1-r{gp_count}"
        print("\n── Saving reports ────────────────────────────────────────────")
        report_path = save_backtest_report(
            ctx,
            result,
            slug=slug,
            title=f"2026 Season Backtest ({gp_range})",
        )
        print(f"  Markdown report : {report_path}")

        # Also save raw JSON
        output_dir = Path("docs/research")
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / f"{slug}.json"
        json_path.write_text(
            json.dumps(result, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        print(f"  JSON results    : {json_path}")

    print("\n✓  Backtest complete.\n")


if __name__ == "__main__":
    main()
