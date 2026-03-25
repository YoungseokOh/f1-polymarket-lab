import { sdk } from "@f1/ts-sdk";
import { StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { LineageTableSection } from "../_components/lineage-table-section";

export const revalidate = 300;

export default async function LineagePage() {
  const [freshnessState, mappingsState] = await Promise.all([
    loadResource(() => sdk.freshness({ limit: 100 }), [], "Freshness feed"),
    loadResource(() => sdk.mappings({ limit: 250 }), [], "Mapping feed"),
  ]);
  const freshness = freshnessState.data;
  const mappings = mappingsState.data;
  const degradedMessages = collectResourceErrors([
    freshnessState,
    mappingsState,
  ]);

  const totalRecords = freshness.reduce((s, r) => s + r.recordsFetched, 0);
  const okSources = freshness.filter((r) => r.status === "ok").length;
  const mappedMarkets = mappings.filter((m) => m.polymarketMarketId).length;

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Lineage</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Data freshness, connector health, and entity mapping lineage
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Data Sources"
          value={freshness.length}
          hint="connector datasets"
        />
        <StatCard label="Healthy" value={okSources} hint="status = ok" />
        <StatCard
          label="Total Records"
          value={totalRecords.toLocaleString()}
          hint="fetched rows"
        />
        <StatCard
          label="Entity Mappings"
          value={mappings.length}
          hint="F1 ↔ Polymarket links"
        />
      </section>

      <LineageTableSection
        freshness={freshness}
        mappings={mappings}
        mappedMarkets={mappedMarkets}
      />
    </div>
  );
}
