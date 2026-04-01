import { sdk } from "@f1/ts-sdk";
import { StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { LineageTableSection } from "../_components/lineage-table-section";

export const revalidate = 300;

export default async function LineagePage() {
  const [
    freshnessState,
    mappingsState,
    jobsState,
    cursorStatesState,
    qualityState,
  ] = await Promise.all([
    loadResource(() => sdk.freshness({ limit: 100 }), [], "Freshness feed"),
    loadResource(() => sdk.mappings({ limit: 250 }), [], "Mapping feed"),
    loadResource(() => sdk.ingestionJobs({ limit: 50 }), [], "Ingestion jobs"),
    loadResource(() => sdk.cursorStates({ limit: 100 }), [], "Cursor state"),
    loadResource(() => sdk.qualityResults({ limit: 50 }), [], "Data quality"),
  ]);
  const freshness = freshnessState.data;
  const mappings = mappingsState.data;
  const jobs = jobsState.data;
  const cursorStates = cursorStatesState.data;
  const qualityResults = qualityState.data;
  const degradedMessages = collectResourceErrors([
    freshnessState,
    mappingsState,
    jobsState,
    cursorStatesState,
    qualityState,
  ]);

  const totalRecords = freshness.reduce((s, r) => s + r.recordsFetched, 0);
  const okSources = freshness.filter((r) => r.status === "ok").length;
  const mappedMarkets = mappings.filter((m) => m.polymarketMarketId).length;
  const failedJobs = jobs.filter((job) => job.status === "failed").length;
  const failingChecks = qualityResults.filter(
    (result) => result.status !== "pass",
  ).length;
  const statusMessages = [
    ...(failingChecks > 0
      ? [`${failingChecks} data quality check(s) are currently failing.`]
      : []),
    ...(failedJobs > 0
      ? [`${failedJobs} recent ingestion run(s) failed.`]
      : []),
  ];

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={[...degradedMessages, ...statusMessages]} />

      <div>
        <h1 className="text-xl font-bold text-white">Lineage</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Data freshness, ingestion health, cursor state, and mapping lineage
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
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
        <StatCard
          label="DQ Failures"
          value={failingChecks}
          hint="latest non-pass checks"
        />
        <StatCard
          label="Failed Jobs"
          value={failedJobs}
          hint="recent ingestion runs"
        />
        <StatCard
          label="Cursors"
          value={cursorStates.length}
          hint="tracked source cursors"
        />
      </section>

      <LineageTableSection
        freshness={freshness}
        jobs={jobs}
        cursorStates={cursorStates}
        qualityResults={qualityResults}
        mappings={mappings}
        mappedMarkets={mappedMarkets}
      />
    </div>
  );
}
