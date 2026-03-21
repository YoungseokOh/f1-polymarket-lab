import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

export default async function BacktestPage() {
  const [backtestResults, snapshots] = await Promise.all([
    sdk.backtestResults().catch(() => []),
    sdk.snapshots().catch(() => []),
  ]);

  const totalBets = backtestResults.reduce((sum, r) => {
    const metrics = r.metricsJson as Record<string, number> | null;
    return sum + (metrics?.total_bets ?? 0);
  }, 0);

  return (
    <main className="mx-auto flex min-h-screen max-w-7xl flex-col gap-8 px-6 py-10">
      <header>
        <Link href="/" className="text-xs text-cyan-300/80 hover:text-cyan-200">
          ← Home
        </Link>
        <h1 className="mt-2 text-3xl font-semibold text-white">
          Backtest Results
        </h1>
      </header>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Backtest Runs"
          value={backtestResults.length}
          hint="settled backtests"
        />
        <StatCard
          label="Feature Snapshots"
          value={snapshots.length}
          hint="GP snapshot datasets"
        />
        <StatCard
          label="Total Bets"
          value={totalBets}
          hint="across all backtests"
        />
        <StatCard
          label="Strategies"
          value={[...new Set(backtestResults.map((r) => r.strategyName))].length}
          hint="distinct strategies tested"
        />
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Panel title="Backtest Results" eyebrow="Performance">
          <div className="space-y-3 text-sm">
            {backtestResults.length === 0 ? (
              <p className="text-slate-400">No backtest results yet.</p>
            ) : (
              backtestResults.slice(0, 10).map((result) => {
                const metrics = result.metricsJson as Record<string, number> | null;
                return (
                  <div
                    key={result.id}
                    className="flex items-center justify-between rounded border border-white/5 px-3 py-2"
                  >
                    <div>
                      <span className="text-white">{result.strategyName}</span>
                      <span className="ml-2 text-slate-400">({result.stage})</span>
                    </div>
                    <div className="flex gap-2">
                      {metrics?.realized_pnl_total !== undefined && (
                        <Badge
                          tone={metrics.realized_pnl_total >= 0 ? "good" : "warn"}
                        >
                          PnL: ${metrics.realized_pnl_total.toFixed(2)}
                        </Badge>
                      )}
                      {metrics?.total_bets !== undefined && (
                        <Badge>{metrics.total_bets} bets</Badge>
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </Panel>

        <Panel title="Feature Snapshots" eyebrow="Data">
          <div className="space-y-3 text-sm">
            {snapshots.length === 0 ? (
              <p className="text-slate-400">No snapshots yet.</p>
            ) : (
              snapshots.slice(0, 10).map((snap) => (
                <div
                  key={snap.id}
                  className="flex items-center justify-between rounded border border-white/5 px-3 py-2"
                >
                  <span className="text-white">{snap.snapshotType}</span>
                  <div className="flex gap-2">
                    <Badge>{snap.featureVersion}</Badge>
                    <Badge>{snap.rowCount ?? 0} rows</Badge>
                  </div>
                </div>
              ))
            )}
          </div>
        </Panel>
      </section>
    </main>
  );
}
