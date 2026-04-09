"use client";

import type {
  BacktestResult,
  F1Meeting,
  F1Session,
  FeatureSnapshot,
} from "@f1/shared-types";
import { Panel } from "@f1/ui";

import {
  backtestBetCount,
  backtestHitRate,
  backtestPnl,
} from "../../lib/backtest-metrics";
import {
  describeStage,
  formatDateRangeShort,
  formatDateTimeShort,
  formatPercentValue,
  formatUsd,
} from "../../lib/display";
import { PnlAreaChart } from "./charts/pnl-area-chart";
import { type Column, DataTable } from "./data-table";

type ResultRow = {
  id: string;
  strategyName: string;
  stageLabel: string;
  windowLabel: string;
  betCount: number | null;
  hitRate: number | null;
  roiPct: number | null;
  pnl: number | null;
  createdAt: string;
};

type SnapshotRow = {
  id: string;
  snapshotLabel: string;
  contextLabel: string;
  rowCount: number | null;
  featureVersion: string;
  asOfTs: string;
};

const resultColumns: Column<ResultRow>[] = [
  {
    key: "experiment",
    header: "Experiment",
    render: (row) => (
      <div>
        <p className="font-medium text-white">{row.stageLabel}</p>
        <p className="mt-1 text-xs text-[#9ca3af]">{row.strategyName}</p>
      </div>
    ),
    sortValue: (row) => `${row.stageLabel}:${row.strategyName}`,
  },
  {
    key: "window",
    header: "Evaluation window",
    render: (row) => (
      <span className="text-sm text-[#d1d5db]">{row.windowLabel}</span>
    ),
    sortValue: (row) => row.windowLabel,
  },
  {
    key: "bets",
    header: "Bets",
    render: (row) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {row.betCount ?? "—"}
      </span>
    ),
    sortValue: (row) => row.betCount ?? -1,
  },
  {
    key: "hitRate",
    header: "Hit rate",
    render: (row) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {row.hitRate != null ? `${(row.hitRate * 100).toFixed(1)}%` : "—"}
      </span>
    ),
    sortValue: (row) => row.hitRate ?? -1,
  },
  {
    key: "roi",
    header: "ROI",
    render: (row) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {formatPercentValue(row.roiPct)}
      </span>
    ),
    sortValue: (row) => row.roiPct ?? -999,
  },
  {
    key: "pnl",
    header: "PnL",
    render: (row) => (
      <span
        className={`font-semibold tabular-nums ${
          row.pnl == null
            ? "text-[#6b7280]"
            : row.pnl >= 0
              ? "text-race-green"
              : "text-race-red"
        }`}
      >
        {formatUsd(row.pnl)}
      </span>
    ),
    sortValue: (row) => row.pnl ?? 0,
  },
  {
    key: "createdAt",
    header: "Created",
    render: (row) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {formatDateTimeShort(row.createdAt)}
      </span>
    ),
    sortValue: (row) => row.createdAt,
  },
];

const snapshotColumns: Column<SnapshotRow>[] = [
  {
    key: "snapshot",
    header: "Snapshot",
    render: (row) => (
      <div>
        <p className="font-medium text-white">{row.snapshotLabel}</p>
        <p className="mt-1 text-xs text-[#9ca3af]">{row.contextLabel}</p>
      </div>
    ),
    sortValue: (row) => row.snapshotLabel,
  },
  {
    key: "rows",
    header: "Rows",
    render: (row) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {row.rowCount ?? "—"}
      </span>
    ),
    sortValue: (row) => row.rowCount ?? -1,
  },
  {
    key: "version",
    header: "Version",
    render: (row) => (
      <span className="text-sm text-[#d1d5db]">{row.featureVersion}</span>
    ),
    sortValue: (row) => row.featureVersion,
  },
  {
    key: "asOf",
    header: "Captured",
    render: (row) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {formatDateTimeShort(row.asOfTs)}
      </span>
    ),
    sortValue: (row) => row.asOfTs,
  },
];

export function BacktestTableSection({
  backtestResults,
  snapshots,
  sessions,
  meetings,
  pnlLabels,
  pnlCumulative,
}: {
  backtestResults: BacktestResult[];
  snapshots: FeatureSnapshot[];
  sessions: F1Session[];
  meetings: F1Meeting[];
  pnlLabels: string[];
  pnlCumulative: number[];
}) {
  const sessionsById = new Map(
    sessions.map((session) => [session.id, session]),
  );
  const meetingsById = new Map(
    meetings.map((meeting) => [meeting.id, meeting]),
  );

  const resultRows: ResultRow[] = [...backtestResults]
    .map((result) => {
      const stage = describeStage(result.stage);
      const metrics = result.metricsJson;
      const roiPct =
        typeof metrics?.roi_pct === "number" ? metrics.roi_pct : null;

      return {
        id: result.id,
        strategyName: result.strategyName,
        stageLabel: stage.label,
        windowLabel: formatDateRangeShort(result.startAt, result.endAt),
        betCount: backtestBetCount(metrics),
        hitRate: backtestHitRate(metrics),
        roiPct,
        pnl: backtestPnl(metrics),
        createdAt: result.createdAt,
      };
    })
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));

  const snapshotRows: SnapshotRow[] = [...snapshots]
    .map((snapshot) => {
      const stage = describeStage(snapshot.snapshotType);
      const session = snapshot.sessionId
        ? sessionsById.get(snapshot.sessionId)
        : null;
      const meeting = session?.meetingId
        ? meetingsById.get(session.meetingId)
        : null;
      const contextBits = [meeting?.meetingName, session?.sessionName].filter(
        Boolean,
      );

      return {
        id: snapshot.id,
        snapshotLabel: stage.label,
        contextLabel:
          contextBits.join(" · ") ||
          "Linked to the stored feature dataset used by training and backtests.",
        rowCount: snapshot.rowCount,
        featureVersion: snapshot.featureVersion,
        asOfTs: snapshot.asOfTs,
      };
    })
    .sort((a, b) => b.asOfTs.localeCompare(a.asOfTs));

  return (
    <>
      {pnlCumulative.length > 1 && (
        <Panel title="Combined PnL trend" eyebrow="All settled backtests">
          <PnlAreaChart
            labels={pnlLabels}
            values={pnlCumulative}
            height={280}
          />
        </Panel>
      )}

      <Panel
        title="Backtest results"
        eyebrow={`${resultRows.length} settled runs`}
      >
        <DataTable
          columns={resultColumns}
          data={resultRows}
          rowKey={(row) => row.id}
          emptyMessage="No settled backtest runs are available yet."
        />
      </Panel>

      <Panel
        title="Feature snapshots"
        eyebrow={`${snapshotRows.length} stored datasets`}
      >
        <DataTable
          columns={snapshotColumns}
          data={snapshotRows}
          rowKey={(row) => row.id}
          emptyMessage="No feature snapshots are available yet."
        />
      </Panel>
    </>
  );
}
