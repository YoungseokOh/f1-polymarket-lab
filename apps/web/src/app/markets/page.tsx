import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";

export default async function MarketsPage() {
  const markets = await sdk.markets().catch(() => []);

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <Panel title="Polymarket Explorer" eyebrow="Silver">
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="pb-3">Question</th>
                <th className="pb-3">Taxonomy</th>
                <th className="pb-3">Best Bid</th>
                <th className="pb-3">Best Ask</th>
              </tr>
            </thead>
            <tbody>
              {markets.map((market) => (
                <tr key={market.id} className="border-t border-white/10">
                  <td className="py-3">{market.question}</td>
                  <td className="py-3">{market.taxonomy}</td>
                  <td className="py-3">{market.bestBid ?? "n/a"}</td>
                  <td className="py-3">{market.bestAsk ?? "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </main>
  );
}
