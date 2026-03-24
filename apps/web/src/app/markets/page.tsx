import Link from "next/link";

import type { PolymarketMarket } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, StatCard } from "@f1/ui";

export const revalidate = 300;

const TAXONOMY_LABELS: Record<string, string> = {
  head_to_head_session: "Head-to-Head (Session)",
  head_to_head_practice: "Head-to-Head (Practice)",
  driver_pole_position: "Driver Pole Position",
  constructor_pole_position: "Constructor Pole",
  race_winner: "Race Winner",
  sprint_winner: "Sprint Winner",
  qualifying_winner: "Qualifying Winner",
  driver_podium: "Driver Podium",
  constructor_scores_first: "Constructor First",
  constructor_fastest_lap_practice: "Constructor Fastest Lap",
  driver_fastest_lap_practice: "Driver Fastest Lap",
  drivers_champion: "Drivers Champion",
  constructors_champion: "Constructors Champion",
  red_flag: "Red Flag",
  safety_car: "Safety Car",
};

// Preferred display order for categories
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
  driver_fastest_lap_practice: 10,
  drivers_champion: 11,
  constructors_champion: 12,
  red_flag: 13,
  safety_car: 14,
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

export default async function MarketsPage() {
  const allMarkets = await sdk.markets().catch(() => []);
  const markets = allMarkets.filter((m) => (m.taxonomy ?? "other") !== "other");

  const activeMarkets = markets.filter((m) => m.active && !m.closed);
  const closedMarkets = markets.filter((m) => m.closed);
  const totalVolume = markets.reduce((s, m) => s + (m.volume ?? 0), 0);

  const groups = groupByTaxonomy(markets);

  // Sort categories by preferred order, then by active count desc
  const sortedCategories = [...groups.entries()].sort(
    ([keyA, listA], [keyB, listB]) => {
      const orderA = TAXONOMY_ORDER[keyA] ?? 50;
      const orderB = TAXONOMY_ORDER[keyB] ?? 50;
      if (orderA !== orderB) return orderA - orderB;
      const activeA = listA.filter((m) => m.active && !m.closed).length;
      const activeB = listB.filter((m) => m.active && !m.closed).length;
      return activeB - activeA;
    },
  );

  return (
    <div className="flex flex-col gap-6 p-6">
      <div>
        <h1 className="text-xl font-bold text-white">Markets</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Polymarket F1 prediction markets by category
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard label="Total" value={markets.length} hint="F1 markets" />
        <StatCard
          label="Active"
          value={activeMarkets.length}
          hint="currently trading"
        />
        <StatCard
          label="Closed"
          value={closedMarkets.length}
          hint="settled markets"
        />
        <StatCard
          label="Volume"
          value={`$${(totalVolume / 1000).toFixed(0)}k`}
          hint="total traded"
        />
      </section>

      {markets.length === 0 ? (
        <div className="rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-8 text-center">
          <p className="text-sm text-[#6b7280]">
            No F1 markets loaded.{" "}
            <Link href="/" className="text-[#e10600] hover:underline">
              Run Sync F1 Markets
            </Link>{" "}
            to populate data.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          {sortedCategories.map(([taxonomy, list]) => {
            const activeCount = list.filter(
              (m) => m.active && !m.closed,
            ).length;
            const closedCount = list.filter((m) => m.closed).length;
            const categoryVolume = list.reduce(
              (s, m) => s + (m.volume ?? 0),
              0,
            );

            // Sort within category: active first, then by volume desc
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
                {/* left accent */}
                <div className="absolute left-0 top-0 h-full w-[3px] bg-[#e10600]" />
                <div className="absolute left-0 top-0 h-full w-[6px] bg-[#e10600]/20 blur-sm" />

                {/* Category header */}
                <div className="flex items-center justify-between gap-4 px-5 py-4 pl-7">
                  <div className="flex min-w-0 flex-col gap-0.5">
                    <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
                      {taxonomy.replace(/_/g, "_").toUpperCase()}
                    </p>
                    <h2 className="text-sm font-semibold text-white">
                      {TAXONOMY_LABELS[taxonomy] ?? taxonomy}
                    </h2>
                  </div>
                  <div className="flex shrink-0 items-center gap-2 text-xs text-[#6b7280]">
                    {activeCount > 0 && (
                      <Badge tone="good">{activeCount} active</Badge>
                    )}
                    {closedCount > 0 && (
                      <Badge tone="default">{closedCount} closed</Badge>
                    )}
                    {categoryVolume > 0 && (
                      <span className="tabular-nums text-[11px]">
                        ${(categoryVolume / 1000).toFixed(0)}k vol
                      </span>
                    )}
                  </div>
                </div>

                {/* Market rows */}
                <div className="border-t border-white/[0.04]">
                  {sortedList.map((m, idx) => (
                    <Link
                      key={m.id}
                      href={`/markets/${m.id}`}
                      className={`flex items-center gap-3 px-5 py-3 pl-7 transition-colors hover:bg-white/[0.03] ${
                        idx < sortedList.length - 1
                          ? "border-b border-white/[0.03]"
                          : ""
                      }`}
                    >
                      {/* Question */}
                      <span className="min-w-0 flex-1 truncate text-[13px] text-[#d1d5db]">
                        {m.question}
                      </span>

                      {/* Price */}
                      <div className="flex shrink-0 items-center gap-3">
                        <div className="text-right">
                          <p className="tabular-nums text-sm font-semibold text-white">
                            {formatPrice(m.lastTradePrice)}
                          </p>
                          {m.bestBid != null && m.bestAsk != null && (
                            <p className="tabular-nums text-[10px] text-[#6b7280]">
                              <span className="text-emerald-400">
                                {formatPrice(m.bestBid)}
                              </span>
                              {" / "}
                              <span className="text-amber-400">
                                {formatPrice(m.bestAsk)}
                              </span>
                            </p>
                          )}
                        </div>
                        <Badge
                          tone={
                            m.active && !m.closed
                              ? "good"
                              : m.closed
                                ? "warn"
                                : "default"
                          }
                        >
                          {m.active && !m.closed
                            ? "Active"
                            : m.closed
                              ? "Closed"
                              : "Inactive"}
                        </Badge>
                        <span className="text-[#4b5563]">→</span>
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
