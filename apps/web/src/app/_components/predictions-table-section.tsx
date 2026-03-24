"use client";

import type { ModelPrediction, ModelRun } from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import { CalibrationChart } from "./charts/calibration-chart";
import { type Column, DataTable } from "./data-table";

const runColumns: Column<ModelRun>[] = [
  {
    key: "modelName",
    header: "Model",
    render: (r) => (
      <span className="font-medium text-white">{r.modelName}</span>
    ),
    sortValue: (r) => r.modelName,
  },
  {
    key: "family",
    header: "Family",
    render: (r) => (
      <span className="text-xs text-[#9ca3af]">{r.modelFamily}</span>
    ),
    sortValue: (r) => r.modelFamily,
  },
  {
    key: "stage",
    header: "Stage",
    render: (r) => <Badge>{r.stage}</Badge>,
    sortValue: (r) => r.stage,
  },
  {
    key: "brier",
    header: "Brier Score",
    render: (r) => {
      const brier = (r.metricsJson as Record<string, number> | null)
        ?.brier_score;
      return brier != null ? (
        <span className="font-mono text-sm tabular-nums text-white">
          {brier.toFixed(4)}
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      );
    },
    sortValue: (r) =>
      (r.metricsJson as Record<string, number> | null)?.brier_score ?? 999,
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

const predColumns: Column<ModelPrediction>[] = [
  {
    key: "market",
    header: "Market",
    render: (p) => (
      <span className="font-mono text-xs text-[#d1d5db]">
        {p.marketId?.slice(0, 16) ?? "—"}…
      </span>
    ),
    sortValue: (p) => p.marketId ?? "",
  },
  {
    key: "probYes",
    header: "P(YES)",
    render: (p) =>
      p.probabilityYes != null ? (
        <span className="font-semibold tabular-nums text-white">
          {(p.probabilityYes * 100).toFixed(1)}%
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (p) => p.probabilityYes ?? 0,
  },
  {
    key: "probNo",
    header: "P(NO)",
    render: (p) =>
      p.probabilityNo != null ? (
        <span className="tabular-nums text-sm text-[#9ca3af]">
          {(p.probabilityNo * 100).toFixed(1)}%
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (p) => p.probabilityNo ?? 0,
  },
  {
    key: "calibration",
    header: "Calibration",
    render: (p) =>
      p.calibrationVersion ? (
        <Badge tone="good">{p.calibrationVersion}</Badge>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (p) => p.calibrationVersion ?? "",
  },
  {
    key: "asOf",
    header: "As Of",
    render: (p) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(p.asOfTs).toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
        })}
      </span>
    ),
    sortValue: (p) => p.asOfTs,
  },
];

export function PredictionsTableSection({
  modelRuns,
  predictions,
  calibrationPoints,
}: {
  modelRuns: ModelRun[];
  predictions: ModelPrediction[];
  calibrationPoints: [number, number][];
}) {
  return (
    <>
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Model Runs" eyebrow={`${modelRuns.length} runs`}>
          <DataTable
            columns={runColumns}
            data={modelRuns}
            emptyMessage="No model runs yet."
          />
        </Panel>

        <Panel title="Calibration" eyebrow="Predicted vs Actual">
          <CalibrationChart points={calibrationPoints} height={300} />
        </Panel>
      </div>

      <Panel title="Prediction Log" eyebrow={`${predictions.length} forecasts`}>
        <DataTable
          columns={predColumns}
          data={predictions}
          emptyMessage="No predictions yet."
        />
      </Panel>
    </>
  );
}
