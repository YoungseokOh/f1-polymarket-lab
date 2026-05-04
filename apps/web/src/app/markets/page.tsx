import Link from "next/link";

import type { PolymarketMarket } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import {
  formatCompactUsd,
  formatPriceCents,
  formatSessionCodeLabel,
  formatTaxonomyLabel,
  formatTaxonomySummary,
} from "../../lib/display";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";

export const revalidate = 300;

const TAXONOMY_ORDER: Record<string, number> = {
  race_winner: 0,
  driver_pole_position: 1,
  qualifying_winner: 2,
  sprint_winner: 3,
  driver_podium: 4,
  head_to_head_session: 5,
  head_to_head_practice: 6,
  constructor_pole_position: 7,
  constructor_scores_first: 8,
  constructor_fastest_lap_practice: 9,
  constructor_fastest_lap_session: 10,
  driver_fastest_lap_practice: 11,
  driver_fastest_lap_session: 12,
  drivers_champion: 13,
  constructors_champion: 14,
  red_flag: 15,
  safety_car: 16,
};

function groupByTaxonomy(markets: PolymarketMarket[]) {
  const groups = new Map<string, PolymarketMarket[]>();
  for (const market of markets) {
    const key = market.taxonomy ?? "other";
    const list = groups.get(key) ?? [];
    list.push(market);
    groups.set(key, list);
  }
  return groups;
}

function compactMeetingToken(meetingName: string | null | undefined): string {
  return (meetingName ?? "")
    .replace(/^Formula 1\s+/i, "")
    .replace(/^F1\s+/i, "")
    .replace(/\s+Grand Prix$/i, "")
    .trim()
    .toLowerCase();
}

function isCurrentMeetingMarket(
  market: PolymarketMarket,
  meetingName: string | null | undefined,
): boolean {
  const meetingToken = compactMeetingToken(meetingName);
  return Boolean(
    meetingToken && market.question.toLowerCase().includes(meetingToken),
  );
}

export default async function MarketsPage() {
  const [marketsState, readinessState] = await Promise.all([
    loadResource(
      () => sdk.markets({ limit: 1000, active: true, closed: false }),
      [],
      "Market feed",
    ),
    loadResource(
      () => sdk.currentWeekendReadiness({ season: 2026 }),
      null,
      "Current weekend",
    ),
  ]);
  const allMarkets = marketsState.data;
  const readiness = readinessState.data;
  const degradedMessages = collectResourceErrors([
    marketsState,
    readinessState,
  ]);
  const currentMeetingName =
    readiness?.meeting?.meetingName ?? readiness?.selectedConfig.name ?? null;
  const targetSessionCode =
    readiness?.selectedConfig.target_session_code ??
    readiness?.nextActiveSession?.sessionCode ??
    null;

  const markets = allMarkets.filter(
    (market) =>
      market.taxonomy !== "other" &&
      market.active &&
      !market.closed &&
      market.targetSessionCode === targetSessionCode &&
      isCurrentMeetingMarket(market, currentMeetingName),
  );
  const activeMarkets = markets.filter(
    (market) => market.active && !market.closed,
  );
  const pricedMarkets = markets.filter(
    (market) => market.lastTradePrice != null,
  );
  const totalVolume = markets.reduce(
    (sum, market) => sum + (market.volume ?? 0),
    0,
  );

  const groups = groupByTaxonomy(markets);
  const sortedCategories = [...groups.entries()].sort(
    ([keyA, listA], [keyB, listB]) => {
      const orderA = TAXONOMY_ORDER[keyA] ?? 50;
      const orderB = TAXONOMY_ORDER[keyB] ?? 50;
      if (orderA !== orderB) {
        return orderA - orderB;
      }
      return listB.length - listA.length;
    },
  );

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Market board</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          Current Grand Prix markets only. Finished races and old sessions are
          hidden from this default view.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Current race"
          value={currentMeetingName ?? "—"}
          hint="Default market scope"
        />
        <StatCard
          label="Session"
          value={formatSessionCodeLabel(targetSessionCode)}
          hint="Markets shown below"
        />
        <StatCard
          label="Open markets"
          value={activeMarkets.length}
          hint="Trading now"
        />
        <StatCard
          label="With prices"
          value={pricedMarkets.length}
          hint="Markets with a latest trade price"
        />
      </section>

      <Panel title="What you are seeing" eyebrow="Plain status">
        <div className="grid gap-3 md:grid-cols-2">
          <p className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4 text-sm text-[#9ca3af]">
            This board is filtered to open {currentMeetingName ?? "current GP"}{" "}
            {formatSessionCodeLabel(targetSessionCode)} markets.
          </p>
          <p className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4 text-sm text-[#9ca3af]">
            Price is the latest YES price. Volume and liquidity show how active
            each market is.
          </p>
        </div>
      </Panel>

      {markets.length === 0 ? (
        <div className="rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-8 text-center">
          <p className="text-sm text-[#6b7280]">
            No F1 markets are loaded yet.{" "}
            <Link
              href="/"
              prefetch={false}
              className="text-[#e10600] hover:underline"
            >
              Run Sync F1 Markets
            </Link>{" "}
            to populate the board.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          {sortedCategories.map(([taxonomy, list]) => {
            const categoryLabel = formatTaxonomyLabel(taxonomy);
            const categorySummary = formatTaxonomySummary(taxonomy);
            const activeCount = list.filter(
              (market) => market.active && !market.closed,
            ).length;
            const sortedList = [...list].sort((a, b) => {
              if (a.active && !a.closed && !(b.active && !b.closed)) return -1;
              if (!(a.active && !a.closed) && b.active && !b.closed) return 1;
              return (b.volume ?? 0) - (a.volume ?? 0);
            });

            return (
              <section
                key={taxonomy}
                className="relative overflow-hidden rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] shadow-xl shadow-black/30"
              >
                <div className="absolute left-0 top-0 h-full w-[3px] bg-[#e10600]" />
                <div className="absolute left-0 top-0 h-full w-[6px] bg-[#e10600]/20 blur-sm" />

                <div className="flex items-start justify-between gap-4 px-5 py-4 pl-7">
                  <div className="min-w-0">
                    <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
                      Market family
                    </p>
                    <h2 className="mt-1 text-lg font-semibold text-white">
                      {categoryLabel}
                    </h2>
                    <p className="mt-2 max-w-3xl text-sm text-[#9ca3af]">
                      {categorySummary}
                    </p>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    <Badge tone="good">{activeCount} open</Badge>
                    <Badge>{list.length} tracked</Badge>
                  </div>
                </div>

                <div className="border-t border-white/[0.04]">
                  {sortedList.map((market, index) => (
                    <Link
                      key={market.id}
                      href={`/markets/${market.id}`}
                      prefetch={false}
                      className={`flex items-center justify-between gap-4 px-5 py-4 pl-7 transition-colors hover:bg-white/[0.03] ${
                        index < sortedList.length - 1
                          ? "border-b border-white/[0.03]"
                          : ""
                      }`}
                    >
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-medium text-white">
                          {market.question}
                        </p>
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[#9ca3af]">
                          {market.targetSessionCode ? (
                            <Badge>
                              {formatSessionCodeLabel(market.targetSessionCode)}
                            </Badge>
                          ) : null}
                          {market.taxonomyConfidence != null ? (
                            <span>
                              {(market.taxonomyConfidence * 100).toFixed(0)}%
                              taxonomy confidence
                            </span>
                          ) : null}
                          {market.volume != null ? (
                            <span>
                              Volume {formatCompactUsd(market.volume)}
                            </span>
                          ) : null}
                          {market.liquidity != null ? (
                            <span>
                              Liquidity {formatCompactUsd(market.liquidity)}
                            </span>
                          ) : null}
                        </div>
                      </div>

                      <div className="flex shrink-0 items-center gap-3">
                        <div className="text-right">
                          <p className="text-sm font-semibold tabular-nums text-white">
                            {formatPriceCents(market.lastTradePrice)}
                          </p>
                          {(market.bestBid != null ||
                            market.bestAsk != null) && (
                            <p className="mt-1 text-[11px] tabular-nums text-[#6b7280]">
                              Bid {formatPriceCents(market.bestBid)} · Ask{" "}
                              {formatPriceCents(market.bestAsk)}
                            </p>
                          )}
                        </div>
                        <Badge
                          tone={
                            market.active && !market.closed ? "good" : "default"
                          }
                        >
                          {market.active && !market.closed ? "Open" : "Closed"}
                        </Badge>
                      </div>
                    </Link>
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}
    </div>
  );
}
