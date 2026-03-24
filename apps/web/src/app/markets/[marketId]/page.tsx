import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";
import { PriceLineChart } from "../../_components/charts/price-line-chart";

export const revalidate = 300;

type Props = { params: Promise<{ marketId: string }> };

export default async function MarketDetailPage({ params }: Props) {
  const { marketId } = await params;

  const [market, prices, predictions] = await Promise.all([
    sdk.market(marketId).catch(() => null),
    sdk.marketPrices(marketId).catch(() => []),
    sdk.predictions().catch(() => []),
  ]);

  if (!market) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center">
          <h1 className="text-xl font-bold text-white">Market Not Found</h1>
          <Link
            href="/markets"
            className="mt-2 block text-sm text-[#e10600] hover:underline"
          >
            ← Back to Markets
          </Link>
        </div>
      </div>
    );
  }

  const marketPredictions = predictions.filter((p) => p.marketId === market.id);
  const latestPrediction = marketPredictions.sort((a, b) =>
    b.asOfTs.localeCompare(a.asOfTs),
  )[0];

  const spread =
    market.bestBid != null && market.bestAsk != null
      ? ((market.bestAsk - market.bestBid) * 100).toFixed(1)
      : null;

  return (
    <div className="flex flex-col gap-6 p-6">
      {/* Breadcrumb */}
      <nav className="text-xs text-[#6b7280]">
        <Link href="/" className="text-[#e10600] hover:underline">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <Link href="/markets" className="text-[#e10600] hover:underline">
          Markets
        </Link>
        <span className="mx-2">/</span>
        <span className="line-clamp-1 inline text-[#d1d5db]">
          {market.question.slice(0, 50)}
        </span>
      </nav>

      {/* Market Header */}
      <section className="rounded-xl border border-white/[0.06] bg-gradient-to-r from-[#1e1e2e] to-[#15151e] p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 flex-1 space-y-2">
            <div className="flex items-center gap-2">
              <Badge tone={market.active ? "good" : "warn"}>
                {market.active ? "Active" : "Closed"}
              </Badge>
              <span className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                {market.taxonomy.replace(/_/g, " ")}
              </span>
            </div>
            <h1 className="text-xl font-bold text-white">{market.question}</h1>
            {market.targetSessionCode && (
              <p className="text-xs text-[#9ca3af]">
                Target Session:{" "}
                <span className="font-mono text-white">
                  {market.targetSessionCode}
                </span>
              </p>
            )}
          </div>
          {market.lastTradePrice != null && (
            <div className="text-right">
              <p className="text-3xl font-bold tabular-nums text-white">
                {(market.lastTradePrice * 100).toFixed(1)}¢
              </p>
              <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
                Last Trade
              </p>
            </div>
          )}
        </div>
      </section>

      {/* Stats */}
      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <StatCard
          label="Best Bid"
          value={
            market.bestBid != null
              ? `${(market.bestBid * 100).toFixed(1)}¢`
              : "—"
          }
          hint="highest buy"
        />
        <StatCard
          label="Best Ask"
          value={
            market.bestAsk != null
              ? `${(market.bestAsk * 100).toFixed(1)}¢`
              : "—"
          }
          hint="lowest sell"
        />
        <StatCard
          label="Spread"
          value={spread != null ? `${spread}¢` : "—"}
          hint="bid-ask gap"
        />
        <StatCard
          label="Volume"
          value={
            market.volume != null ? `$${market.volume.toLocaleString()}` : "—"
          }
          hint="total traded"
        />
        <StatCard
          label="Liquidity"
          value={
            market.liquidity != null
              ? `$${market.liquidity.toLocaleString()}`
              : "—"
          }
          hint="available depth"
        />
      </section>

      {/* Price Chart */}
      <Panel title="Price History" eyebrow={`${prices.length} data points`}>
        <PriceLineChart data={prices} height={360} />
      </Panel>

      {/* Model Predictions */}
      {marketPredictions.length > 0 && (
        <Panel
          title="Model Predictions"
          eyebrow={`${marketPredictions.length} forecasts`}
        >
          <div className="space-y-2">
            {latestPrediction && (
              <div className="mb-4 flex items-center gap-4 rounded-lg border border-[#e10600]/20 bg-[#e10600]/5 p-4">
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#e10600]">
                    Latest Prediction
                  </p>
                  <p className="mt-1 text-2xl font-bold tabular-nums text-white">
                    {latestPrediction.probabilityYes != null
                      ? `${(latestPrediction.probabilityYes * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
                <div className="flex-1 text-right">
                  <p className="text-xs text-[#9ca3af]">
                    as of{" "}
                    {new Date(latestPrediction.asOfTs).toLocaleString("en-US", {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                      hour12: false,
                    })}
                  </p>
                  {latestPrediction.calibrationVersion && (
                    <Badge>{latestPrediction.calibrationVersion}</Badge>
                  )}
                </div>
              </div>
            )}
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {marketPredictions.slice(0, 9).map((pred) => (
                <div
                  key={pred.id}
                  className="rounded-lg border border-white/[0.04] px-3 py-2"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-semibold tabular-nums text-white">
                      {pred.probabilityYes != null
                        ? `${(pred.probabilityYes * 100).toFixed(1)}%`
                        : "—"}
                    </span>
                    <span className="text-[10px] tabular-nums text-[#6b7280]">
                      {new Date(pred.asOfTs).toLocaleDateString("en-US", {
                        month: "short",
                        day: "numeric",
                      })}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Panel>
      )}

      {/* Market Metadata */}
      <Panel title="Details" eyebrow="Metadata">
        <div className="grid gap-x-8 gap-y-2 text-sm sm:grid-cols-2">
          <div className="flex justify-between border-b border-white/[0.04] py-2">
            <span className="text-[#6b7280]">Market ID</span>
            <span className="font-mono text-xs text-[#d1d5db]">
              {market.id.slice(0, 16)}…
            </span>
          </div>
          <div className="flex justify-between border-b border-white/[0.04] py-2">
            <span className="text-[#6b7280]">Condition ID</span>
            <span className="font-mono text-xs text-[#d1d5db]">
              {market.conditionId.slice(0, 16)}…
            </span>
          </div>
          <div className="flex justify-between border-b border-white/[0.04] py-2">
            <span className="text-[#6b7280]">Taxonomy</span>
            <span className="text-[#d1d5db]">
              {market.taxonomy.replace(/_/g, " ")}
            </span>
          </div>
          <div className="flex justify-between border-b border-white/[0.04] py-2">
            <span className="text-[#6b7280]">Confidence</span>
            <span className="text-[#d1d5db]">
              {market.taxonomyConfidence != null
                ? `${(market.taxonomyConfidence * 100).toFixed(0)}%`
                : "—"}
            </span>
          </div>
          {market.slug && (
            <div className="flex justify-between border-b border-white/[0.04] py-2 sm:col-span-2">
              <span className="text-[#6b7280]">Slug</span>
              <span className="text-[#d1d5db]">{market.slug}</span>
            </div>
          )}
        </div>
      </Panel>
    </div>
  );
}
