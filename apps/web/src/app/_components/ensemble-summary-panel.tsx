"use client";

import * as React from "react";

import type {
  EnsemblePrediction,
  ModelRun,
  SignalDiagnostic,
  SignalRegistryEntry,
  TradeDecision,
} from "@f1/shared-types";
import { Panel, StatCard } from "@f1/ui";

import type { CalibrationPoint } from "../../lib/calibration";
import {
  formatCompactUsd,
  formatMarketGroupLabel,
  formatProbability,
} from "../../lib/display";
import { CalibrationChart } from "./charts/calibration-chart";
import { SignalCorrelationGrid } from "./signal-correlation-grid";

function calibrationPointsFromBuckets(
  buckets: Record<string, unknown> | null | undefined,
): CalibrationPoint[] {
  return Object.entries(buckets ?? {}).flatMap(([bucketLabel, raw]) => {
    if (!raw || typeof raw !== "object") {
      return [];
    }
    const bucket = raw as Record<string, unknown>;
    const predicted = bucket.avg_predicted;
    const actual = bucket.avg_actual;
    const count = bucket.count;
    if (
      typeof predicted !== "number" ||
      typeof actual !== "number" ||
      typeof count !== "number"
    ) {
      return [];
    }
    return [
      {
        predicted,
        actual,
        count,
        bucketLabel,
      },
    ];
  });
}

export function EnsembleSummaryPanel({
  modelRuns,
  ensemblePredictions,
  signalDiagnostics,
  signalRegistry,
  tradeDecisions,
}: {
  modelRuns: ModelRun[];
  ensemblePredictions: EnsemblePrediction[];
  signalDiagnostics: SignalDiagnostic[];
  signalRegistry: SignalRegistryEntry[];
  tradeDecisions: TradeDecision[];
}) {
  const latestRun = [...modelRuns]
    .filter((run) => run.modelFamily === "signal_ensemble")
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];

  const latestRunPredictions = latestRun
    ? ensemblePredictions.filter(
        (prediction) => prediction.modelRunId === latestRun.id,
      )
    : [];
  const latestRunDecisions = latestRun
    ? tradeDecisions.filter((decision) => decision.modelRunId === latestRun.id)
    : [];
  const latestDiagnostics = latestRun
    ? signalDiagnostics.filter(
        (diagnostic) => diagnostic.modelRunId === latestRun.id,
      )
    : [];

  const averageDisagreement =
    latestRunPredictions.length > 0
      ? latestRunPredictions.reduce((sum, prediction) => {
          return sum + (prediction.disagreementScore ?? 0);
        }, 0) / latestRunPredictions.length
      : null;
  const averageEffectiveN =
    latestRunPredictions.length > 0
      ? latestRunPredictions.reduce((sum, prediction) => {
          return sum + (prediction.effectiveN ?? 0);
        }, 0) / latestRunPredictions.length
      : null;
  const executableCount = latestRunDecisions.filter(
    (decision) => decision.decisionStatus === "trade",
  ).length;
  const blockedCount = latestRunDecisions.filter(
    (decision) => decision.decisionStatus === "blocked",
  ).length;

  const calibrationPoints = calibrationPointsFromBuckets(
    (latestRun?.metricsJson?.calibration_buckets as Record<
      string,
      unknown
    > | null) ?? null,
  );
  const byMarketGroup = Object.entries(
    (latestRun?.metricsJson?.market_group_breakdown as Record<
      string,
      Record<string, unknown>
    >) ?? {},
  );
  const strongestDiagnostic = [...latestDiagnostics].sort((a, b) => {
    const left = a.skillVsMarket ?? Number.NEGATIVE_INFINITY;
    const right = b.skillVsMarket ?? Number.NEGATIVE_INFINITY;
    return right - left;
  })[0];
  const registryBySignal = [...signalRegistry].reduce((acc, entry) => {
    if (!acc.has(entry.signalCode)) {
      acc.set(entry.signalCode, entry);
    }
    return acc;
  }, new Map<string, SignalRegistryEntry>());
  const uniqueSignalRegistry = [...registryBySignal.values()];

  return (
    <Panel title="Signal Ensemble" eyebrow="Calibrated multi-signal stack">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Latest run"
          value={latestRun ? latestRun.modelName : "—"}
          hint={latestRun ? latestRun.stage : "No ensemble run stored yet"}
        />
        <StatCard
          label="Ensemble rows"
          value={latestRunPredictions.length}
          hint={`${executableCount} executable decisions`}
        />
        <StatCard
          label="Avg disagreement"
          value={
            averageDisagreement != null
              ? formatProbability(averageDisagreement, 0)
              : "—"
          }
          hint={
            blockedCount > 0
              ? `${blockedCount} blocked decisions`
              : "Lower is more redundant"
          }
        />
        <StatCard
          label="Effective N"
          value={averageEffectiveN != null ? averageEffectiveN.toFixed(2) : "—"}
          hint="Weight-adjusted independent signals"
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-[1.2fr_1fr]">
        <div className="space-y-4">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-white">Reliability</p>
                <p className="mt-1 text-xs text-[#9ca3af]">
                  The latest ensemble run is calibrated out of fold and then
                  anchored back to the executable market price.
                </p>
              </div>
              {latestRun?.metricsJson?.total_pnl != null && (
                <div className="text-right">
                  <p className="text-xs text-[#9ca3af]">Latest test PnL</p>
                  <p className="font-semibold text-white">
                    {formatCompactUsd(
                      latestRun.metricsJson.total_pnl as number | null,
                    )}
                  </p>
                </div>
              )}
            </div>
            <div className="mt-4">
              <CalibrationChart points={calibrationPoints} height={260} />
            </div>
          </div>

          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Market-group coverage
            </p>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              {byMarketGroup.length > 0 ? (
                byMarketGroup.map(([marketGroup, rawMetrics]) => {
                  const metrics = rawMetrics as Record<string, unknown>;
                  return (
                    <div
                      key={marketGroup}
                      className="rounded-lg border border-white/[0.05] bg-black/20 p-3"
                    >
                      <p className="text-xs uppercase tracking-[0.2em] text-[#9ca3af]">
                        {formatMarketGroupLabel(marketGroup)}
                      </p>
                      <p className="mt-2 text-lg font-semibold text-white">
                        {typeof metrics.row_count === "number"
                          ? metrics.row_count
                          : "—"}{" "}
                        rows
                      </p>
                      <p className="mt-1 text-xs text-[#9ca3af]">
                        Brier{" "}
                        {typeof metrics.brier_score === "number"
                          ? metrics.brier_score.toFixed(4)
                          : "—"}
                        {" · "}
                        ROI{" "}
                        {typeof metrics.roi_pct === "number"
                          ? `${metrics.roi_pct.toFixed(1)}%`
                          : "—"}
                      </p>
                    </div>
                  );
                })
              ) : (
                <p className="text-sm text-[#6b7280]">
                  No ensemble breakdown is available yet.
                </p>
              )}
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">Active signals</p>
            <div className="mt-3 space-y-2">
              {uniqueSignalRegistry.length > 0 ? (
                uniqueSignalRegistry.map((entry) => (
                  <div
                    key={entry.id}
                    className="rounded-lg border border-white/[0.05] bg-black/20 p-3"
                  >
                    <p className="text-sm font-medium text-white">
                      {entry.signalCode
                        .replace(/_signal$/, "")
                        .replace(/_/g, " ")}
                    </p>
                    <p className="mt-1 text-xs text-[#9ca3af]">
                      {entry.description ?? "No description available."}
                    </p>
                  </div>
                ))
              ) : (
                <p className="text-sm text-[#6b7280]">
                  No signal registry entries have been stored yet.
                </p>
              )}
            </div>
          </div>

          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-white">
                  Redundancy view
                </p>
                <p className="mt-1 text-xs text-[#9ca3af]">
                  Residual correlation comes from OOF signal residuals within
                  the latest trained market group.
                </p>
              </div>
              {strongestDiagnostic && (
                <div className="text-right">
                  <p className="text-xs text-[#9ca3af]">
                    Strongest skill vs market
                  </p>
                  <p className="font-semibold text-white">
                    {strongestDiagnostic.signalCode.replace(/_signal$/, "")}
                  </p>
                </div>
              )}
            </div>
            <div className="mt-4">
              <SignalCorrelationGrid
                matrix={strongestDiagnostic?.residualCorrelationJson}
              />
            </div>
          </div>
        </div>
      </div>
    </Panel>
  );
}
