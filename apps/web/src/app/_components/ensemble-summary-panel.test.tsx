// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type {
  EnsemblePrediction,
  ModelRun,
  SignalDiagnostic,
  SignalRegistryEntry,
  TradeDecision,
} from "@f1/shared-types";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("./charts/calibration-chart", () => ({
  CalibrationChart: () => <div>Calibration Chart</div>,
}));

import { EnsembleSummaryPanel } from "./ensemble-summary-panel";

const modelRuns: ModelRun[] = [
  {
    id: "ensemble-run-1",
    stage: "signal_ensemble_v1",
    modelFamily: "signal_ensemble",
    modelName: "anchor_ridge_stacking",
    datasetVersion: "features_v1",
    featureSnapshotId: "snapshot-1",
    configJson: null,
    metricsJson: {
      total_pnl: 12.5,
      calibration_buckets: {
        "40-50%": {
          avg_predicted: 0.44,
          avg_actual: 0.5,
          count: 12,
        },
      },
      market_group_breakdown: {
        driver_outright: { row_count: 10, brier_score: 0.18, roi_pct: 4.2 },
      },
    },
    artifactUri: null,
    registryRunId: null,
    promotionStatus: "inactive",
    promotedAt: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

const ensemblePredictions: EnsemblePrediction[] = [
  {
    id: "ensemble-prediction-1",
    modelRunId: "ensemble-run-1",
    featureSnapshotId: "snapshot-1",
    marketId: "market-1",
    tokenId: "token-1",
    eventId: "event-1",
    marketTaxonomy: "driver_pole_position",
    marketGroup: "driver_outright",
    meetingKey: 1281,
    asOfTs: "2026-04-08T10:00:00Z",
    pMarketRef: 0.31,
    pYesEnsemble: 0.44,
    zMarket: -0.8,
    zEnsemble: -0.2,
    intercept: 0.05,
    disagreementScore: 0.08,
    effectiveN: 2.2,
    uncertaintyScore: 0.24,
    contributionsJson: { pace_delta_signal: 0.41 },
    coverageJson: { supported: true },
    metadataJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

const signalDiagnostics: SignalDiagnostic[] = [
  {
    id: "signal-diagnostic-1",
    modelRunId: "ensemble-run-1",
    signalCode: "pace_delta_signal",
    marketTaxonomy: null,
    marketGroup: "driver_outright",
    phaseBucket: "overall",
    brier: 0.16,
    logLoss: 0.49,
    ece: 0.03,
    skillVsMarket: 0.02,
    coverageRate: 1,
    residualCorrelationJson: {
      pace_delta_signal: { pace_delta_signal: 1 },
    },
    stabilityJson: null,
    metricsJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

const signalRegistry: SignalRegistryEntry[] = [
  {
    id: "signal-registry-1",
    signalCode: "pace_delta_signal",
    signalFamily: "pace_delta",
    marketTaxonomy: null,
    marketGroup: null,
    description: "Latest pace softmax.",
    version: "v1",
    configJson: null,
    isActive: true,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

const tradeDecisions: TradeDecision[] = [
  {
    id: "trade-decision-1",
    modelRunId: "ensemble-run-1",
    ensemblePredictionId: "ensemble-prediction-1",
    featureSnapshotId: "snapshot-1",
    marketId: "market-1",
    tokenId: "token-1",
    eventId: "event-1",
    marketTaxonomy: "driver_pole_position",
    marketGroup: "driver_outright",
    meetingKey: 1281,
    asOfTs: "2026-04-08T10:00:00Z",
    side: "YES",
    edge: 0.07,
    threshold: 0.05,
    spread: 0.02,
    depth: 10,
    kellyFractionRaw: 0.11,
    disagreementPenalty: 0.75,
    liquidityFactor: 0.8,
    sizeFraction: 0.08,
    decisionStatus: "trade",
    decisionReason: "positive_yes_edge",
    metadataJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

describe("EnsembleSummaryPanel", () => {
  it("renders ensemble overview content", () => {
    render(
      <EnsembleSummaryPanel
        modelRuns={modelRuns}
        ensemblePredictions={ensemblePredictions}
        signalDiagnostics={signalDiagnostics}
        signalRegistry={signalRegistry}
        tradeDecisions={tradeDecisions}
      />,
    );

    expect(screen.getByText("Signal Ensemble")).toBeInTheDocument();
    expect(screen.getAllByText(/pace delta/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Driver outright")).toBeInTheDocument();
    expect(screen.getByText("Redundancy view")).toBeInTheDocument();
  });
});
