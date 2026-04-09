"use client";

import * as React from "react";

import type { BacktestResult } from "@f1/shared-types";
import { Panel, StatCard } from "@f1/ui";

import { backtestBetCount, backtestPnl } from "../../lib/backtest-metrics";
import {
  formatMarketGroupLabel,
  formatPercentValue,
  formatUsd,
} from "../../lib/display";

export function EnsembleBacktestSection({
  backtestResults,
}: {
  backtestResults: BacktestResult[];
}) {
  const ensembleResults = backtestResults.filter(
    (result) =>
      result.stage.includes("signal_ensemble") ||
      result.strategyName.includes("ensemble"),
  );

  const latestResult = [...ensembleResults].sort((a, b) =>
    b.createdAt.localeCompare(a.createdAt),
  )[0];

  if (!latestResult) {
    return (
      <Panel title="Ensemble comparison" eyebrow="No ensemble backtests yet">
        <p className="text-sm text-[#6b7280]">
          Run `run-signal-ensemble-backtest` after scoring a snapshot to compare
          the ensemble path with the existing baselines.
        </p>
      </Panel>
    );
  }

  const marketGroupBreakdown =
    (latestResult.metricsJson?.market_group_breakdown as
      | Record<string, Record<string, unknown>>
      | undefined) ?? {};
  const spreadBreakdown =
    (latestResult.metricsJson?.spread_regime_breakdown as
      | Record<string, Record<string, unknown>>
      | undefined) ?? {};

  return (
    <Panel title="Ensemble comparison" eyebrow="Executable decision path">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Ensemble runs"
          value={ensembleResults.length}
          hint="Backtests settled from trade_decisions"
        />
        <StatCard
          label="Latest bets"
          value={backtestBetCount(latestResult.metricsJson) ?? "—"}
          hint={latestResult.strategyName}
        />
        <StatCard
          label="Latest PnL"
          value={formatUsd(backtestPnl(latestResult.metricsJson))}
          hint={`Created ${new Date(latestResult.createdAt).toLocaleDateString("en-US")}`}
        />
        <StatCard
          label="Captured edge"
          value={
            typeof latestResult.metricsJson
              ?.average_executable_edge_captured === "number"
              ? formatPercentValue(
                  latestResult.metricsJson
                    .average_executable_edge_captured as number,
                  2,
                )
              : "—"
          }
          hint="Average executable edge on placed bets"
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-2">
        <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
          <p className="text-sm font-medium text-white">
            Breakdown by market group
          </p>
          <div className="mt-3 space-y-3">
            {Object.entries(marketGroupBreakdown).map(([marketGroup, raw]) => {
              const metrics = raw as Record<string, unknown>;
              return (
                <div
                  key={marketGroup}
                  className="rounded-lg border border-white/[0.05] bg-black/20 p-3"
                >
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-sm text-white">
                      {formatMarketGroupLabel(marketGroup)}
                    </span>
                    <span className="font-mono text-xs text-[#9ca3af]">
                      {typeof metrics.bet_count === "number"
                        ? `${metrics.bet_count} bets`
                        : "—"}
                    </span>
                  </div>
                  <p className="mt-1 text-xs text-[#9ca3af]">
                    PnL{" "}
                    {typeof metrics.total_pnl === "number"
                      ? formatUsd(metrics.total_pnl as number)
                      : "—"}
                    {" · "}
                    Avg edge{" "}
                    {typeof metrics.avg_edge === "number"
                      ? formatPercentValue(metrics.avg_edge as number, 2)
                      : "—"}
                  </p>
                </div>
              );
            })}
          </div>
        </div>

        <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
          <p className="text-sm font-medium text-white">
            Breakdown by spread regime
          </p>
          <div className="mt-3 space-y-3">
            {Object.entries(spreadBreakdown).map(([bucket, raw]) => {
              const metrics = raw as Record<string, unknown>;
              return (
                <div
                  key={bucket}
                  className="rounded-lg border border-white/[0.05] bg-black/20 p-3"
                >
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-sm text-white">{bucket}</span>
                    <span className="font-mono text-xs text-[#9ca3af]">
                      {typeof metrics.bet_count === "number"
                        ? `${metrics.bet_count} bets`
                        : "—"}
                    </span>
                  </div>
                  <p className="mt-1 text-xs text-[#9ca3af]">
                    PnL{" "}
                    {typeof metrics.total_pnl === "number"
                      ? formatUsd(metrics.total_pnl as number)
                      : "—"}
                  </p>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </Panel>
  );
}
