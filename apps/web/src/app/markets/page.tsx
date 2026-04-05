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

export default async function MarketsPage() {
  const marketsState = await loadResource(
    () => sdk.markets({ limit: 250 }),
    [],
    "Market feed",
  );
  const allMarkets = marketsState.data;
  const degradedMessages = collectResourceErrors([marketsState]);

  const markets = allMarkets.filter((market) => market.taxonomy !== "other");
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
          Browse the F1 questions currently tracked on Polymarket, grouped by
          market family and labeled with the session they are meant to settle
          against.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Tracked markets"
          value={markets.length}
          hint="F1-linked questions"
        />
        <StatCard
          label="Open now"
          value={activeMarkets.length}
          hint="Markets still trading"
        />
        <StatCard
          label="With prices"
          value={pricedMarkets.length}
          hint="Markets with a latest trade price"
        />
        <StatCard
          label="Total volume"
          value={formatCompactUsd(totalVolume)}
          hint="Combined traded volume"
        />
      </section>

      <Panel title="How to read this board" eyebrow="Quick guide">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Read the question first
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Every row starts with the exact Polymarket question, so you can
              understand the bet without decoding taxonomy labels.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Use the session badge as context
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              The session badge tells you whether the market should settle from
              practice, qualifying, sprint, or race results.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Price, volume, and liquidity are separate
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              The price shows the latest implied YES probability, while volume
              and liquidity show how actively the market is trading.
            </p>
          </div>
        </div>
      </Panel>

      {markets.length === 0 ? (
        <div className="rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-8 text-center">
          <p className="text-sm text-[#6b7280]">
            No F1 markets are loaded yet.{" "}
            <Link href="/" className="text-[#e10600] hover:underline">
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
