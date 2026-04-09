"use client";

import * as React from "react";

import type {
  EnsemblePrediction,
  PolymarketMarket,
  SignalSnapshot,
  TradeDecision,
} from "@f1/shared-types";
import { Badge, Panel, StatCard } from "@f1/ui";

import {
  formatMarketGroupLabel,
  formatPriceCents,
  formatProbability,
} from "../../lib/display";

function latestByTimestamp<T extends { asOfTs: string }>(rows: T[]): T | null {
  return [...rows].sort((a, b) => b.asOfTs.localeCompare(a.asOfTs))[0] ?? null;
}

export function MarketEnsemblePanel({
  market,
  ensemblePredictions,
  signalSnapshots,
  tradeDecisions,
}: {
  market: PolymarketMarket;
  ensemblePredictions: EnsemblePrediction[];
  signalSnapshots: SignalSnapshot[];
  tradeDecisions: TradeDecision[];
}) {
  const latestPrediction = latestByTimestamp(ensemblePredictions);
  const latestDecision = latestByTimestamp(tradeDecisions);
  const latestSignals = latestPrediction
    ? signalSnapshots.filter(
        (snapshot) =>
          snapshot.modelRunId === latestPrediction.modelRunId &&
          snapshot.asOfTs === latestPrediction.asOfTs,
      )
    : [];

  if (!latestPrediction) {
    return (
      <Panel title="Signal Ensemble" eyebrow="No stored ensemble forecast">
        <p className="text-sm text-[#6b7280]">
          This market does not have a stored ensemble prediction yet.
        </p>
      </Panel>
    );
  }

  const contributions = Object.entries(latestPrediction.contributionsJson ?? {})
    .filter((entry): entry is [string, number] => typeof entry[1] === "number")
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));
  const activeSignals = latestSignals.filter((signal) => signal.coverageFlag);
  const unavailableSignals = latestSignals.filter(
    (signal) => !signal.coverageFlag,
  );
  const marketPrice = market.lastTradePrice ?? latestPrediction.pMarketRef;
  const edge =
    latestPrediction.pYesEnsemble != null && marketPrice != null
      ? latestPrediction.pYesEnsemble - marketPrice
      : null;

  return (
    <Panel
      title="Signal Ensemble"
      eyebrow={`${formatMarketGroupLabel(latestPrediction.marketGroup)} forecast`}
    >
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Market anchor"
          value={formatProbability(latestPrediction.pMarketRef)}
          hint={formatPriceCents(latestPrediction.pMarketRef)}
        />
        <StatCard
          label="Ensemble YES"
          value={formatProbability(latestPrediction.pYesEnsemble)}
          hint={formatPriceCents(latestPrediction.pYesEnsemble)}
        />
        <StatCard
          label="Disagreement"
          value={formatProbability(latestPrediction.disagreementScore, 0)}
          hint="Stddev of active signal deltas"
        />
        <StatCard
          label="Effective N"
          value={
            latestPrediction.effectiveN != null
              ? latestPrediction.effectiveN.toFixed(2)
              : "—"
          }
          hint={
            latestPrediction.uncertaintyScore != null
              ? `Uncertainty ${(latestPrediction.uncertaintyScore * 100).toFixed(0)}%`
              : "No uncertainty estimate"
          }
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <div className="space-y-4">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-white">
                  Probability vs market
                </p>
                <p className="mt-1 text-xs text-[#9ca3af]">
                  The final forecast is the market anchor plus the weighted
                  delta-logit contributions from the active signals.
                </p>
              </div>
              {edge != null && (
                <Badge tone={edge >= 0 ? "good" : "warn"}>
                  Edge {(edge * 100).toFixed(1)} pts
                </Badge>
              )}
            </div>
            <div className="mt-4 space-y-3">
              <div className="rounded-lg border border-white/[0.05] bg-black/20 p-3">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm text-[#9ca3af]">Current market</span>
                  <span className="font-semibold text-white">
                    {formatProbability(marketPrice)}
                  </span>
                </div>
                <div className="mt-2 h-2 overflow-hidden rounded-full bg-white/[0.06]">
                  <div
                    className="h-full rounded-full bg-white/60"
                    style={{
                      width: `${Math.max((marketPrice ?? 0) * 100, 2)}%`,
                    }}
                  />
                </div>
              </div>
              <div className="rounded-lg border border-[#e10600]/20 bg-[#e10600]/5 p-3">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm text-[#fca5a5]">Ensemble</span>
                  <span className="font-semibold text-white">
                    {formatProbability(latestPrediction.pYesEnsemble)}
                  </span>
                </div>
                <div className="mt-2 h-2 overflow-hidden rounded-full bg-white/[0.06]">
                  <div
                    className="h-full rounded-full bg-[#e10600]"
                    style={{
                      width: `${Math.max(
                        (latestPrediction.pYesEnsemble ?? 0) * 100,
                        2,
                      )}%`,
                    }}
                  />
                </div>
              </div>
            </div>
          </div>

          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Signal contributions
            </p>
            <div className="mt-3 space-y-3">
              {contributions.length > 0 ? (
                contributions.map(([signalCode, value]) => {
                  const width = Math.min(Math.abs(value) * 100, 100);
                  const positive = value >= 0;
                  return (
                    <div key={signalCode}>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-sm text-[#d1d5db]">
                          {signalCode
                            .replace(/_signal$/, "")
                            .replace(/_/g, " ")}
                        </span>
                        <span
                          className={`font-mono text-xs ${
                            positive ? "text-race-green" : "text-race-red"
                          }`}
                        >
                          {positive ? "+" : ""}
                          {value.toFixed(3)}
                        </span>
                      </div>
                      <div className="mt-2 h-2 overflow-hidden rounded-full bg-white/[0.06]">
                        <div
                          className={`h-full rounded-full ${
                            positive ? "bg-race-green" : "bg-race-red"
                          }`}
                          style={{ width: `${Math.max(width, 4)}%` }}
                        />
                      </div>
                    </div>
                  );
                })
              ) : (
                <p className="text-sm text-[#6b7280]">
                  No signal contribution breakdown was stored for this forecast.
                </p>
              )}
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3">
              <p className="text-sm font-medium text-white">Trade decision</p>
              {latestDecision && (
                <Badge
                  tone={
                    latestDecision.decisionStatus === "trade"
                      ? "good"
                      : latestDecision.decisionStatus === "blocked"
                        ? "warn"
                        : "default"
                  }
                >
                  {latestDecision.decisionStatus}
                </Badge>
              )}
            </div>
            {latestDecision ? (
              <div className="mt-3 space-y-2 text-sm text-[#d1d5db]">
                <div className="flex justify-between">
                  <span className="text-[#9ca3af]">Side</span>
                  <span>{latestDecision.side}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-[#9ca3af]">Edge</span>
                  <span>{formatProbability(latestDecision.edge)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-[#9ca3af]">Size fraction</span>
                  <span>{formatProbability(latestDecision.sizeFraction)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-[#9ca3af]">Spread</span>
                  <span>{formatPriceCents(latestDecision.spread)}</span>
                </div>
                <p className="rounded-lg border border-white/[0.05] bg-black/20 p-3 text-xs text-[#9ca3af]">
                  {latestDecision.decisionReason ??
                    "No decision reason recorded."}
                </p>
              </div>
            ) : (
              <p className="mt-3 text-sm text-[#6b7280]">
                No stored trade decision is available for this market.
              </p>
            )}
          </div>

          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">Signal coverage</p>
            <div className="mt-3 space-y-3">
              <div>
                <p className="text-xs uppercase tracking-[0.2em] text-[#9ca3af]">
                  Active
                </p>
                <div className="mt-2 flex flex-wrap gap-2">
                  {activeSignals.length > 0 ? (
                    activeSignals.map((signal) => (
                      <Badge key={signal.id} tone="good">
                        {signal.signalCode.replace(/_signal$/, "")}
                      </Badge>
                    ))
                  ) : (
                    <span className="text-sm text-[#6b7280]">None</span>
                  )}
                </div>
              </div>
              <div>
                <p className="text-xs uppercase tracking-[0.2em] text-[#9ca3af]">
                  Unavailable
                </p>
                <div className="mt-2 flex flex-wrap gap-2">
                  {unavailableSignals.length > 0 ? (
                    unavailableSignals.map((signal) => (
                      <Badge key={signal.id}>
                        {signal.signalCode.replace(/_signal$/, "")}
                      </Badge>
                    ))
                  ) : (
                    <span className="text-sm text-[#6b7280]">None</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Panel>
  );
}
