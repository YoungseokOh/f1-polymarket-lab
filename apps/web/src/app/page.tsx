import Link from "next/link";

import type { PolymarketMarket } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../lib/resource-state";
import { DashboardActions } from "./_components/dashboard-actions";
import { SessionTimeline } from "./_components/session-timeline";
import { StatusIndicator } from "./_components/status-indicator";

export const revalidate = 300;

const TAXONOMY_LABELS: Record<string, string> = {
  head_to_head_session: "Head-to-Head",
  head_to_head_practice: "Head-to-Head (Practice)",
  driver_pole_position: "Pole Position",
  constructor_pole_position: "Constructor Pole",
  race_winner: "Race Winner",
  sprint_winner: "Sprint Winner",
  qualifying_winner: "Qualifying Winner",
  driver_podium: "Podium",
  constructor_scores_first: "Constructor First",
  constructor_fastest_lap_practice: "Constructor FL",
  constructor_fastest_lap_session: "Constructor FL",
  driver_fastest_lap_practice: "Driver FL",
  driver_fastest_lap_session: "Driver FL",
  drivers_champion: "Drivers Champion",
  constructors_champion: "Constructors Champion",
  red_flag: "Red Flag",
  safety_car: "Safety Car",
  other: "Other",
};

function formatPrice(v: number | null) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}¢`;
}

function groupByTaxonomy(markets: PolymarketMarket[]) {
  const groups = new Map<string, PolymarketMarket[]>();
  for (const m of markets) {
    const key = m.taxonomy ?? "other";
    const list = groups.get(key) ?? [];
    list.push(m);
    groups.set(key, list);
  }
  return groups;
}

export default async function HomePage() {
  const [
    healthState,
    freshnessState,
    sessionsState,
    meetingsState,
    marketsState,
    mappingsState,
    modelRunsState,
    predictionsState,
    backtestResultsState,
  ] = await Promise.all([
    loadResource(
      sdk.health,
      {
        service: "api",
        status: "offline",
        now: new Date().toISOString(),
      },
      "API health",
    ),
    loadResource(() => sdk.freshness({ limit: 100 }), [], "Freshness feed"),
    loadResource(() => sdk.sessions({ limit: 250 }), [], "Session feed"),
    loadResource(() => sdk.meetings({ limit: 100 }), [], "Meeting feed"),
    loadResource(() => sdk.markets({ limit: 250 }), [], "Market feed"),
    loadResource(() => sdk.mappings({ limit: 250 }), [], "Mapping feed"),
    loadResource(sdk.modelRuns, [], "Model runs"),
    loadResource(sdk.predictions, [], "Predictions"),
    loadResource(sdk.backtestResults, [], "Backtest results"),
  ]);

  const health = healthState.data;
  const freshness = freshnessState.data;
  const sessions = sessionsState.data;
  const meetings = meetingsState.data;
  const markets = marketsState.data;
  const mappings = mappingsState.data;
  const modelRuns = modelRunsState.data;
  const predictions = predictionsState.data;
  const backtestResults = backtestResultsState.data;
  const degradedMessages = collectResourceErrors([
    healthState,
    freshnessState,
    sessionsState,
    meetingsState,
    marketsState,
    mappingsState,
    modelRunsState,
    predictionsState,
    backtestResultsState,
  ]);

  const now = new Date();
  const sorted = [...meetings].sort((a, b) =>
    (a.startDateUtc ?? "").localeCompare(b.startDateUtc ?? ""),
  );
  const upcoming = sorted.filter(
    (m) => m.startDateUtc && new Date(m.endDateUtc ?? m.startDateUtc) >= now,
  );
  const currentGP = upcoming[0] ?? sorted.at(-1);

  const gpSessions = currentGP
    ? sessions.filter((s) => s.meetingId === currentGP.id)
    : [];
  const completedCodes = gpSessions
    .filter((s) => s.dateEndUtc && new Date(s.dateEndUtc) < now)
    .map((s) => s.sessionCode ?? "")
    .filter(Boolean);
  const activeSession = gpSessions.find(
    (s) =>
      s.dateStartUtc &&
      new Date(s.dateStartUtc) <= now &&
      (!s.dateEndUtc || new Date(s.dateEndUtc) > now),
  );

  const activeMarkets = markets.filter((m) => m.active && !m.closed);
  const mappedCount = mappings.filter((m) => m.polymarketMarketId).length;
  const marketGroups = groupByTaxonomy(activeMarkets);

  const lastFetch = freshness
    .map((r) => r.lastFetchAt)
    .filter((v): v is string => Boolean(v))
    .sort()
    .at(-1);

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      {/* Hero — GP Weekend */}
      <section className="rounded-xl border border-white/[0.06] bg-gradient-to-r from-[#1e1e2e] to-[#15151e] p-6">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
          <div className="space-y-2">
            <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
              GP Weekend
            </p>
            {currentGP ? (
              <>
                <h1 className="text-2xl font-bold text-white">
                  {currentGP.meetingName}
                </h1>
                <p className="text-sm text-[#9ca3af]">
                  {currentGP.circuitShortName &&
                    `${currentGP.circuitShortName} · `}
                  {currentGP.location}, {currentGP.countryName}
                  {currentGP.startDateUtc && (
                    <span className="ml-2 tabular-nums">
                      R{currentGP.roundNumber} ·{" "}
                      {new Date(currentGP.startDateUtc).toLocaleDateString(
                        "en-US",
                        { month: "short", day: "numeric" },
                      )}
                      {currentGP.endDateUtc &&
                        `–${new Date(currentGP.endDateUtc).toLocaleDateString("en-US", { day: "numeric" })}`}
                    </span>
                  )}
                </p>
              </>
            ) : (
              <h1 className="text-2xl font-bold text-white">
                No Meetings Loaded
              </h1>
            )}
          </div>
          <div className="flex items-center gap-4">
            <StatusIndicator
              status={health.status === "ok" ? "ok" : "error"}
              label={`API ${health.status}`}
            />
            {lastFetch && (
              <span className="text-[11px] tabular-nums text-[#6b7280]">
                Last sync{" "}
                {new Date(lastFetch).toLocaleTimeString("en-US", {
                  hour: "2-digit",
                  minute: "2-digit",
                  hour12: false,
                })}
              </span>
            )}
          </div>
        </div>
        {gpSessions.length > 0 && (
          <div className="mt-6">
            <SessionTimeline
              completedCodes={completedCodes}
              activeCode={activeSession?.sessionCode ?? undefined}
            />
          </div>
        )}
      </section>

      {/* Quick Actions */}
      <DashboardActions />

      {/* Stats Row */}
      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <StatCard label="Meetings" value={meetings.length} hint="GP weekends" />
        <StatCard
          label="Sessions"
          value={sessions.length}
          hint="FP / Quali / Race"
        />
        <StatCard
          label="Active Markets"
          value={activeMarkets.length}
          hint="open on Polymarket"
        />
        <StatCard
          label="Model Runs"
          value={modelRuns.length}
          hint="trained models"
        />
        <StatCard
          label="Predictions"
          value={predictions.length}
          hint="probability forecasts"
        />
      </section>

      {/* Markets by Taxonomy */}
      <section className="grid gap-4 lg:grid-cols-2">
        {[...marketGroups.entries()].map(([taxonomy, group]) => (
          <Panel
            key={taxonomy}
            title={TAXONOMY_LABELS[taxonomy] ?? taxonomy}
            eyebrow={`${group.length} markets`}
          >
            <div className="space-y-2">
              {group.slice(0, 6).map((m) => (
                <Link
                  key={m.id}
                  href={`/markets/${m.id}`}
                  className="flex items-center justify-between rounded-lg border border-white/[0.04] px-3 py-2 transition-colors hover:border-[#e10600]/20 hover:bg-white/[0.02]"
                >
                  <span className="mr-4 line-clamp-1 text-sm text-[#d1d5db]">
                    {m.question}
                  </span>
                  <div className="flex shrink-0 items-center gap-2">
                    {m.lastTradePrice != null && (
                      <span className="text-sm font-semibold tabular-nums text-white">
                        {formatPrice(m.lastTradePrice)}
                      </span>
                    )}
                    <Badge tone={m.active ? "good" : "default"}>
                      {m.active ? "Active" : "Closed"}
                    </Badge>
                  </div>
                </Link>
              ))}
              {group.length > 6 && (
                <Link
                  href="/markets"
                  className="block text-center text-xs text-[#e10600] hover:underline"
                >
                  View all {group.length} →
                </Link>
              )}
            </div>
          </Panel>
        ))}
        {marketGroups.size === 0 && (
          <Panel title="Markets" eyebrow="Polymarket">
            <p className="text-sm text-[#6b7280]">No active markets loaded.</p>
          </Panel>
        )}
      </section>

      {/* Bottom Row — Activity + Platform */}
      <section className="grid gap-4 lg:grid-cols-3">
        <Panel title="Recent Model Runs" eyebrow="ML Pipeline">
          <div className="space-y-2 text-sm">
            {modelRuns.length === 0 ? (
              <p className="text-[#6b7280]">No model runs yet.</p>
            ) : (
              modelRuns.slice(0, 5).map((run) => (
                <div
                  key={run.id}
                  className="flex items-center justify-between rounded-lg border border-white/[0.04] px-3 py-2"
                >
                  <div>
                    <span className="text-white">{run.modelName}</span>
                    <span className="ml-1 text-[#6b7280]">
                      ({run.modelFamily})
                    </span>
                  </div>
                  <Badge>{run.stage}</Badge>
                </div>
              ))
            )}
          </div>
        </Panel>

        <Panel title="Backtest Summary" eyebrow="Performance">
          <div className="space-y-2 text-sm">
            {backtestResults.length === 0 ? (
              <p className="text-[#6b7280]">No backtests yet.</p>
            ) : (
              backtestResults.slice(0, 5).map((bt) => {
                const metrics = bt.metricsJson as Record<string, number> | null;
                const pnl = metrics?.realized_pnl_total;
                return (
                  <div
                    key={bt.id}
                    className="flex items-center justify-between rounded-lg border border-white/[0.04] px-3 py-2"
                  >
                    <span className="text-white">{bt.strategyName}</span>
                    {pnl != null && (
                      <Badge tone={pnl >= 0 ? "good" : "warn"}>
                        ${pnl.toFixed(2)}
                      </Badge>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </Panel>

        <Panel title="Data Freshness" eyebrow="Connectors">
          <div className="space-y-2 text-sm">
            {freshness.length === 0 ? (
              <p className="text-[#6b7280]">No data sources configured.</p>
            ) : (
              freshness.slice(0, 5).map((f) => (
                <div
                  key={`${f.source}:${f.dataset}`}
                  className="flex items-center justify-between rounded-lg border border-white/[0.04] px-3 py-2"
                >
                  <span className="text-[#d1d5db]">
                    {f.source}/{f.dataset}
                  </span>
                  <StatusIndicator
                    status={
                      f.status === "ok"
                        ? "ok"
                        : f.status === "pending"
                          ? "pending"
                          : "idle"
                    }
                    label={`${f.recordsFetched}`}
                  />
                </div>
              ))
            )}
          </div>
        </Panel>
      </section>

      {/* Platform Stats */}
      <section className="grid gap-3 sm:grid-cols-3">
        <StatCard
          label="Entity Mappings"
          value={mappedCount}
          hint="F1 ↔ Polymarket links"
        />
        <StatCard
          label="Data Sources"
          value={freshness.length}
          hint="connector datasets"
        />
        <StatCard
          label="Strategies"
          value={
            [...new Set(backtestResults.map((b) => b.strategyName))].length
          }
          hint="backtested strategies"
        />
      </section>
    </div>
  );
}
