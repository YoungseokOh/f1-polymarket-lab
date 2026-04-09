// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type {
  EnsemblePrediction,
  PolymarketMarket,
  SignalSnapshot,
  TradeDecision,
} from "@f1/shared-types";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { MarketEnsemblePanel } from "./market-ensemble-panel";

const market: PolymarketMarket = {
  id: "market-1",
  eventId: "event-1",
  question: "Will Driver 0 win pole?",
  slug: "driver-0-pole",
  taxonomy: "driver_pole_position",
  taxonomyConfidence: 0.95,
  targetSessionCode: "Q",
  conditionId: "condition-1",
  questionId: "question-1",
  bestBid: 0.31,
  bestAsk: 0.35,
  lastTradePrice: 0.33,
  volume: 1200,
  liquidity: 900,
  active: true,
  closed: false,
};

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
    contributionsJson: {
      pace_delta_signal: 0.41,
      prior_signal: -0.1,
    },
    coverageJson: { supported: true },
    metadataJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

const signalSnapshots: SignalSnapshot[] = [
  {
    id: "signal-snapshot-1",
    modelRunId: "ensemble-run-1",
    featureSnapshotId: "snapshot-1",
    marketId: "market-1",
    tokenId: "token-1",
    eventId: "event-1",
    marketTaxonomy: "driver_pole_position",
    marketGroup: "driver_outright",
    meetingKey: 1281,
    asOfTs: "2026-04-08T10:00:00Z",
    signalCode: "pace_delta_signal",
    signalVersion: "v1",
    pYesRaw: 0.4,
    pYesCalibrated: 0.43,
    pMarketRef: 0.31,
    deltaLogit: 0.2,
    freshnessSec: 120,
    coverageFlag: true,
    metadataJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
  {
    id: "signal-snapshot-2",
    modelRunId: "ensemble-run-1",
    featureSnapshotId: "snapshot-1",
    marketId: "market-1",
    tokenId: "token-1",
    eventId: "event-1",
    marketTaxonomy: "driver_pole_position",
    marketGroup: "driver_outright",
    meetingKey: 1281,
    asOfTs: "2026-04-08T10:00:00Z",
    signalCode: "cross_market_consistency_signal",
    signalVersion: "v1",
    pYesRaw: 0.31,
    pYesCalibrated: 0.31,
    pMarketRef: 0.31,
    deltaLogit: 0,
    freshnessSec: 120,
    coverageFlag: false,
    metadataJson: null,
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
    depth: 20,
    kellyFractionRaw: 0.12,
    disagreementPenalty: 0.76,
    liquidityFactor: 0.82,
    sizeFraction: 0.08,
    decisionStatus: "trade",
    decisionReason: "positive_yes_edge",
    metadataJson: null,
    createdAt: "2026-04-08T10:00:00Z",
  },
];

describe("MarketEnsemblePanel", () => {
  it("renders probability, contributions, and signal coverage", () => {
    render(
      <MarketEnsemblePanel
        market={market}
        ensemblePredictions={ensemblePredictions}
        signalSnapshots={signalSnapshots}
        tradeDecisions={tradeDecisions}
      />,
    );

    expect(screen.getByText("Probability vs market")).toBeInTheDocument();
    expect(screen.getByText("Signal contributions")).toBeInTheDocument();
    expect(screen.getByText("Trade decision")).toBeInTheDocument();
    expect(screen.getByText("pace delta")).toBeInTheDocument();
    expect(screen.getByText("Active")).toBeInTheDocument();
  });
});
