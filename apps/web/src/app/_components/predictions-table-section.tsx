"use client";

import type {
  ModelPrediction,
  ModelRun,
  PolymarketMarket,
} from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";

import { backtestPnl } from "../../lib/backtest-metrics";
import type { CalibrationPoint } from "../../lib/calibration";
import {
  describePredictionSignal,
  describeStage,
  formatDateTimeShort,
  formatPriceCents,
  formatProbability,
  formatUsd,
} from "../../lib/display";
import { CalibrationChart } from "./charts/calibration-chart";
import { type Column, DataTable } from "./data-table";
import { EmptyState } from "./empty-state";

type RunRow = {
  id: string;
  modelName: string;
  modelFamily: string;
  stageLabel: string;
  rowCount: number | null;
  brierScore: number | null;
  paperPnl: number | null;
  createdAt: string;
};

type PredictionRow = {
  id: string;
  marketId: string | null;
  marketQuestion: string;
  marketPrice: number | null;
  modelName: string;
  stageLabel: string;
  probabilityYes: number | null;
  asOfTs: string;
  gap: number | null;
};

const runColumns: Column<RunRow>[] = [
  {
    key: "run",
    header: "Model",
    render: (row) => (
      <div>
        <p className="font-medium text-white">{row.modelName}</p>
        <p className="mt-1 text-xs text-[#9ca3af]">
          {row.modelFamily} · {row.stageLabel}
        </p>
      </div>
    ),
    sortValue: (row) => `${row.modelName}:${row.stageLabel}`,
  },
  {
    key: "coverage",
    header: "Forecasts",
    render: (row) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {row.rowCount ?? "—"} rows
      </span>
    ),
    sortValue: (row) => row.rowCount ?? -1,
  },
  {
    key: "brier",
    header: "Error",
    render: (row) =>
      row.brierScore != null ? (
        <span className="font-mono text-sm tabular-nums text-white">
          {row.brierScore.toFixed(4)}
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (row) => row.brierScore ?? 999,
  },
  {
    key: "paperPnl",
    header: "Paper PnL",
    render: (row) => (
      <span
        className={`font-semibold tabular-nums ${
          row.paperPnl == null
            ? "text-[#6b7280]"
            : row.paperPnl >= 0
              ? "text-race-green"
              : "text-race-red"
        }`}
      >
        {formatUsd(row.paperPnl)}
      </span>
    ),
    sortValue: (row) => row.paperPnl ?? 0,
  },
  {
    key: "createdAt",
    header: "Built",
    render: (row) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {formatDateTimeShort(row.createdAt)}
      </span>
    ),
    sortValue: (row) => row.createdAt,
  },
];

const predictionColumns: Column<PredictionRow>[] = [
  {
    key: "market",
    header: "Market",
    render: (row) => (
      <div>
        <p className="font-medium text-white">{row.marketQuestion}</p>
        <p className="mt-1 text-xs text-[#9ca3af]">
          {row.stageLabel} · {row.modelName}
        </p>
      </div>
    ),
    sortValue: (row) => row.marketQuestion,
  },
  {
    key: "modelChance",
    header: "Model says",
    render: (row) => (
      <div>
        <p className="font-semibold tabular-nums text-white">
          {formatProbability(row.probabilityYes)}
        </p>
        <p className="mt-1 text-xs text-[#6b7280]">
          Fair YES price {formatPriceCents(row.probabilityYes)}
        </p>
      </div>
    ),
    sortValue: (row) => row.probabilityYes ?? -1,
  },
  {
    key: "marketPrice",
    header: "Market price",
    render: (row) => (
      <span className="font-semibold tabular-nums text-white">
        {formatPriceCents(row.marketPrice)}
      </span>
    ),
    sortValue: (row) => row.marketPrice ?? -1,
  },
  {
    key: "gap",
    header: "Difference",
    render: (row) => {
      const signal = describePredictionSignal(row.probabilityYes);
      const gapLabel =
        row.gap == null
          ? "—"
          : `${row.gap >= 0 ? "+" : ""}${(row.gap * 100).toFixed(1)} pts`;

      return (
        <div>
          <Badge tone={signal.tone}>{signal.label}</Badge>
          <p
            className={`mt-1 text-xs font-medium tabular-nums ${
              row.gap == null
                ? "text-[#6b7280]"
                : row.gap >= 0
                  ? "text-race-green"
                  : "text-race-red"
            }`}
          >
            {gapLabel}
          </p>
        </div>
      );
    },
    sortValue: (row) => row.gap ?? 0,
  },
  {
    key: "asOf",
    header: "Updated",
    render: (row) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {formatDateTimeShort(row.asOfTs)}
      </span>
    ),
    sortValue: (row) => row.asOfTs,
  },
];

export function PredictionsTableSection({
  modelRuns,
  predictions,
  markets,
  calibrationPoints,
  calibrationMessage,
  title = "Current predictions",
  eyebrow,
  description,
  emptyTitle = "No predictions to show",
  emptyMessage = "There are no model predictions for this view yet.",
  showPredictions = true,
  showModelHealth = true,
}: {
  modelRuns: ModelRun[];
  predictions: ModelPrediction[];
  markets: PolymarketMarket[];
  calibrationPoints: CalibrationPoint[];
  calibrationMessage: string;
  title?: string;
  eyebrow?: string;
  description?: string;
  emptyTitle?: string;
  emptyMessage?: string;
  showPredictions?: boolean;
  showModelHealth?: boolean;
}) {
  const marketsById = new Map(markets.map((market) => [market.id, market]));
  const runsById = new Map(modelRuns.map((run) => [run.id, run]));

  const runRows: RunRow[] = [...modelRuns]
    .map((run) => {
      const metrics = run.metricsJson;
      const stage = describeStage(run.stage);
      const rowCount =
        typeof metrics?.row_count === "number" ? metrics.row_count : null;
      const brierScore =
        typeof metrics?.brier_score === "number" ? metrics.brier_score : null;

      return {
        id: run.id,
        modelName: run.modelName,
        modelFamily: run.modelFamily,
        stageLabel: stage.label,
        rowCount,
        brierScore,
        paperPnl: backtestPnl(metrics),
        createdAt: run.createdAt,
      };
    })
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));

  const predictionRows: PredictionRow[] = [...predictions]
    .map((prediction) => {
      const market = prediction.marketId
        ? marketsById.get(prediction.marketId)
        : null;
      const run = runsById.get(prediction.modelRunId);
      const stage = describeStage(run?.stage);

      return {
        id: prediction.id,
        marketId: prediction.marketId,
        marketQuestion:
          market?.question ?? prediction.marketId ?? "Unlinked market",
        marketPrice: market?.lastTradePrice ?? null,
        modelName: run?.modelName ?? "Unknown model",
        stageLabel: stage.label,
        probabilityYes: prediction.probabilityYes,
        asOfTs: prediction.asOfTs,
        gap:
          prediction.probabilityYes != null && market?.lastTradePrice != null
            ? prediction.probabilityYes - market.lastTradePrice
            : null,
      };
    })
    .sort((a, b) => {
      const timeCompare = b.asOfTs.localeCompare(a.asOfTs);
      if (timeCompare !== 0) {
        return timeCompare;
      }
      return (b.probabilityYes ?? 0) - (a.probabilityYes ?? 0);
    });

  return (
    <>
      {showPredictions && (
        <Panel
          title={title}
          eyebrow={eyebrow ?? `${predictionRows.length} rows`}
        >
          {description && (
            <p className="mb-4 max-w-3xl text-sm text-[#9ca3af]">
              {description}
            </p>
          )}
          {predictionRows.length > 0 ? (
            <DataTable
              columns={predictionColumns}
              data={predictionRows}
              rowKey={(row) => row.id}
              onRowClick={(row) =>
                row.marketId ? `/markets/${row.marketId}` : undefined
              }
              emptyMessage={emptyMessage}
            />
          ) : (
            <EmptyState title={emptyTitle} description={emptyMessage} />
          )}
        </Panel>
      )}

      {showModelHealth && (
        <div className="grid gap-4 xl:grid-cols-[1.25fr_1fr]">
          <Panel title="Model health" eyebrow={`${runRows.length} runs`}>
            <DataTable
              columns={runColumns}
              data={runRows}
              rowKey={(row) => row.id}
              emptyMessage="No model runs are available yet."
            />
          </Panel>

          <Panel title="Past accuracy" eyebrow="Settled markets">
            {calibrationPoints.length > 0 ? (
              <div className="space-y-3">
                <CalibrationChart points={calibrationPoints} />
                <p className="text-xs text-[#6b7280]">{calibrationMessage}</p>
              </div>
            ) : (
              <EmptyState
                title="Not enough joined outcomes yet"
                description={calibrationMessage}
              />
            )}
          </Panel>
        </div>
      )}
    </>
  );
}
