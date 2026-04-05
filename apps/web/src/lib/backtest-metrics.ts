type RawMetrics = Record<string, unknown> | null | undefined;

function metricNumber(metrics: RawMetrics, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = metrics?.[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

export function backtestBetCount(metrics: RawMetrics): number | null {
  return metricNumber(metrics, "bet_count", "total_bets");
}

export function backtestWinCount(metrics: RawMetrics): number | null {
  return metricNumber(metrics, "wins", "winning_bets");
}

export function backtestPnl(metrics: RawMetrics): number | null {
  return metricNumber(metrics, "total_pnl", "realized_pnl_total");
}

export function backtestHitRate(metrics: RawMetrics): number | null {
  const direct = metricNumber(metrics, "hit_rate");
  if (direct != null) {
    return direct;
  }

  const wins = backtestWinCount(metrics);
  const bets = backtestBetCount(metrics);
  if (wins == null || bets == null || bets <= 0) {
    return null;
  }

  return wins / bets;
}
