import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import {
  backtestBetCount,
  backtestHitRate,
  backtestPnl,
} from "../../lib/backtest-metrics";
import { formatUsd } from "../../lib/display";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { BacktestActions } from "../_components/backtest-actions";
import { BacktestTableSection } from "../_components/backtest-table-section";

export const revalidate = 300;

export default async function BacktestPage() {
  const [backtestResultsState, snapshotsState, sessionsState, meetingsState] =
    await Promise.all([
      loadResource(sdk.backtestResults, [], "Backtest results"),
      loadResource(sdk.snapshots, [], "Feature snapshots"),
      loadResource(() => sdk.sessions({ limit: 250 }), [], "Session feed"),
      loadResource(() => sdk.meetings({ limit: 100 }), [], "Meeting feed"),
    ]);

  const backtestResults = backtestResultsState.data;
  const snapshots = snapshotsState.data;
  const sessions = sessionsState.data;
  const meetings = meetingsState.data;
  const degradedMessages = collectResourceErrors([
    backtestResultsState,
    snapshotsState,
    sessionsState,
    meetingsState,
  ]);

  const totalBets = backtestResults.reduce((sum, result) => {
    return sum + (backtestBetCount(result.metricsJson) ?? 0);
  }, 0);
  const totalPnl = backtestResults.reduce((sum, result) => {
    return sum + (backtestPnl(result.metricsJson) ?? 0);
  }, 0);
  const bestRunPnl = backtestResults.reduce<number | null>((best, result) => {
    const pnl = backtestPnl(result.metricsJson);
    if (pnl == null) {
      return best;
    }
    if (best == null || pnl > best) {
      return pnl;
    }
    return best;
  }, null);
  const aggregateHitRate =
    totalBets > 0
      ? backtestResults.reduce((wins, result) => {
          const metrics = result.metricsJson;
          const hitRate = backtestHitRate(metrics);
          const betCount = backtestBetCount(metrics);
          if (hitRate == null || betCount == null) {
            return wins;
          }
          return wins + hitRate * betCount;
        }, 0) / totalBets
      : null;

  const pnlLabels = backtestResults.map((result) =>
    new Date(result.createdAt).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
  );
  const pnlCumulative: number[] = [];
  let runningPnl = 0;
  for (const result of backtestResults) {
    runningPnl += backtestPnl(result.metricsJson) ?? 0;
    pnlCumulative.push(runningPnl);
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Research backtests</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          Use this page to see how a strategy would have performed on archived
          F1 markets, which feature snapshots were used, and where to rerun the
          latest experiment.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Settled runs"
          value={backtestResults.length}
          hint="Completed historical evaluations"
        />
        <StatCard
          label="Placed bets"
          value={totalBets}
          hint="Across all runs shown here"
        />
        <StatCard
          label="Total PnL"
          value={formatUsd(totalPnl)}
          hint="Combined realized outcome"
        />
        <StatCard
          label="Hit rate"
          value={
            aggregateHitRate != null
              ? `${(aggregateHitRate * 100).toFixed(1)}%`
              : "—"
          }
          hint={
            bestRunPnl != null
              ? `Best run ${formatUsd(bestRunPnl)}`
              : "No settled winning run yet"
          }
        />
      </section>

      <Panel title="How to read these results" eyebrow="Quick guide">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">Experiment label</p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              The experiment name explains which Grand Prix stage and market
              family the backtest is evaluating. It replaces the raw internal
              stage slug.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">Window and bets</p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Evaluation window shows the historical period covered. Bets, hit
              rate, ROI, and PnL summarize what the strategy actually would have
              done in that period.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Snapshots are the source data
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Feature snapshots are the stored datasets used to train models and
              run backtests. If a result looks odd, inspect the related snapshot
              first.
            </p>
          </div>
        </div>
      </Panel>

      <BacktestActions />

      <BacktestTableSection
        backtestResults={backtestResults}
        snapshots={snapshots}
        sessions={sessions}
        meetings={meetings}
        pnlLabels={pnlLabels}
        pnlCumulative={pnlCumulative}
      />
    </div>
  );
}
