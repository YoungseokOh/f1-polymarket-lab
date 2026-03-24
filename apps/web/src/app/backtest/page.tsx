import { sdk } from "@f1/ts-sdk";
import { StatCard } from "@f1/ui";
import { BacktestActions } from "../_components/backtest-actions";
import { BacktestTableSection } from "../_components/backtest-table-section";

export const revalidate = 300;

export default async function BacktestPage() {
  const [backtestResults, snapshots] = await Promise.all([
    sdk.backtestResults().catch(() => []),
    sdk.snapshots().catch(() => []),
  ]);

  const totalBets = backtestResults.reduce((sum, r) => {
    const metrics = r.metricsJson as Record<string, number> | null;
    return sum + (metrics?.total_bets ?? 0);
  }, 0);

  const totalPnl = backtestResults.reduce((sum, r) => {
    const metrics = r.metricsJson as Record<string, number> | null;
    return sum + (metrics?.realized_pnl_total ?? 0);
  }, 0);

  // Build cumulative PnL chart data
  const pnlLabels = backtestResults.map((r) =>
    new Date(r.createdAt).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
  );
  const pnlCumulative: number[] = [];
  let running = 0;
  for (const r of backtestResults) {
    running +=
      (r.metricsJson as Record<string, number> | null)?.realized_pnl_total ?? 0;
    pnlCumulative.push(running);
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <div>
        <h1 className="text-xl font-bold text-white">Backtest</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Strategy backtesting results and feature snapshots
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Backtest Runs"
          value={backtestResults.length}
          hint="settled backtests"
        />
        <StatCard label="Total Bets" value={totalBets} hint="across all runs" />
        <StatCard
          label="Total PnL"
          value={`$${totalPnl.toFixed(2)}`}
          hint="realized P&L"
        />
        <StatCard
          label="Snapshots"
          value={snapshots.length}
          hint="feature datasets"
        />
      </section>

      <BacktestActions />

      <BacktestTableSection
        backtestResults={backtestResults}
        snapshots={snapshots}
        pnlLabels={pnlLabels}
        pnlCumulative={pnlCumulative}
      />
    </div>
  );
}
