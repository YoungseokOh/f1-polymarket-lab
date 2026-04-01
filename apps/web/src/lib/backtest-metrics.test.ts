import { describe, expect, it } from "vitest";

import {
  backtestBetCount,
  backtestHitRate,
  backtestPnl,
  backtestWinCount,
} from "./backtest-metrics";

describe("backtest-metrics", () => {
  it("reads current backtest metric keys", () => {
    const metrics = {
      bet_count: 4,
      wins: 3,
      hit_rate: 0.75,
      total_pnl: 18.4,
    };

    expect(backtestBetCount(metrics)).toBe(4);
    expect(backtestWinCount(metrics)).toBe(3);
    expect(backtestHitRate(metrics)).toBe(0.75);
    expect(backtestPnl(metrics)).toBe(18.4);
  });

  it("falls back to legacy dashboard metric keys", () => {
    const metrics = {
      total_bets: 5,
      winning_bets: 2,
      realized_pnl_total: -3.5,
    };

    expect(backtestBetCount(metrics)).toBe(5);
    expect(backtestWinCount(metrics)).toBe(2);
    expect(backtestHitRate(metrics)).toBe(0.4);
    expect(backtestPnl(metrics)).toBe(-3.5);
  });
});
