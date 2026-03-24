"use client";

import type { BacktestResult, FeatureSnapshot } from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import { PnlAreaChart } from "./charts/pnl-area-chart";
import { type Column, DataTable } from "./data-table";

const btColumns: Column<BacktestResult>[] = [
  {
    key: "strategy",
    header: "Strategy",
    render: (r) => (
      <span className="font-medium text-white">{r.strategyName}</span>
    ),
    sortValue: (r) => r.strategyName,
  },
  {
    key: "stage",
    header: "Stage",
    render: (r) => <Badge>{r.stage}</Badge>,
    sortValue: (r) => r.stage,
  },
  {
    key: "pnl",
    header: "PnL",
    render: (r) => {
      const metrics = r.metricsJson as Record<string, number> | null;
      const pnl = metrics?.realized_pnl_total;
      if (pnl == null) return <span className="text-xs text-[#6b7280]">—</span>;
      return (
        <span
          className={`font-semibold tabular-nums ${pnl >= 0 ? "text-race-green" : "text-race-red"}`}
        >
          ${pnl.toFixed(2)}
        </span>
      );
    },
    sortValue: (r) =>
      (r.metricsJson as Record<string, number> | null)?.realized_pnl_total ?? 0,
  },
  {
    key: "bets",
    header: "Bets",
    render: (r) => {
      const totalBets = (r.metricsJson as Record<string, number> | null)
        ?.total_bets;
      return (
        <span className="tabular-nums text-sm text-[#d1d5db]">
          {totalBets ?? "—"}
        </span>
      );
    },
    sortValue: (r) =>
      (r.metricsJson as Record<string, number> | null)?.total_bets ?? 0,
  },
  {
    key: "winRate",
    header: "Win Rate",
    render: (r) => {
      const metrics = r.metricsJson as Record<string, number> | null;
      const wins = metrics?.winning_bets;
      const total = metrics?.total_bets;
      if (wins == null || !total)
        return <span className="text-xs text-[#6b7280]">—</span>;
      return (
        <span className="tabular-nums text-sm text-[#d1d5db]">
          {((wins / total) * 100).toFixed(1)}%
        </span>
      );
    },
    sortValue: (r) => {
      const m = r.metricsJson as Record<string, number> | null;
      return m?.winning_bets && m?.total_bets
        ? m.winning_bets / m.total_bets
        : 0;
    },
  },
  {
    key: "created",
    header: "Created",
    render: (r) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(r.createdAt).toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
        })}
      </span>
    ),
    sortValue: (r) => r.createdAt,
  },
];

const snapColumns: Column<FeatureSnapshot>[] = [
  {
    key: "type",
    header: "Type",
    render: (s) => (
      <span className="font-medium text-white">{s.snapshotType}</span>
    ),
    sortValue: (s) => s.snapshotType,
  },
  {
    key: "version",
    header: "Version",
    render: (s) => <Badge>{s.featureVersion}</Badge>,
    sortValue: (s) => s.featureVersion,
  },
  {
    key: "rows",
    header: "Rows",
    render: (s) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {s.rowCount ?? "—"}
      </span>
    ),
    sortValue: (s) => s.rowCount ?? 0,
  },
  {
    key: "asOf",
    header: "As Of",
    render: (s) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(s.asOfTs).toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
        })}
      </span>
    ),
    sortValue: (s) => s.asOfTs,
  },
];

export function BacktestTableSection({
  backtestResults,
  snapshots,
  pnlLabels,
  pnlCumulative,
}: {
  backtestResults: BacktestResult[];
  snapshots: FeatureSnapshot[];
  pnlLabels: string[];
  pnlCumulative: number[];
}) {
  return (
    <>
      {pnlCumulative.length > 1 && (
        <Panel title="Cumulative PnL" eyebrow="Performance">
          <PnlAreaChart
            labels={pnlLabels}
            values={pnlCumulative}
            height={280}
          />
        </Panel>
      )}

      <Panel
        title="Backtest Results"
        eyebrow={`${backtestResults.length} results`}
      >
        <DataTable
          columns={btColumns}
          data={backtestResults}
          emptyMessage="No backtest results yet."
        />
      </Panel>

      <Panel
        title="Feature Snapshots"
        eyebrow={`${snapshots.length} snapshots`}
      >
        <DataTable
          columns={snapColumns}
          data={snapshots}
          emptyMessage="No snapshots yet."
        />
      </Panel>
    </>
  );
}
