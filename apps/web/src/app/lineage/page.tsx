import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import {
  describeQualityAlert,
  describeQualityDataset,
} from "../../lib/display";
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

  const totalRecords = freshness.reduce(
    (sum, record) => sum + record.recordsFetched,
    0,
  );
  const healthySources = freshness.filter(
    (record) => record.status === "ok",
  ).length;
  const mappedMarkets = mappings.filter(
    (mapping) => mapping.polymarketMarketId,
  ).length;
  const failedJobs = jobs.filter((job) => job.status === "failed");
  const failingChecks = qualityResults.filter(
    (result) => result.status !== "pass",
  );
  const optionalFailingChecks = failingChecks.filter(
    (result) => describeQualityDataset(result.dataset).optional,
  );
  const coreFailingChecks = failingChecks.filter(
    (result) => !describeQualityDataset(result.dataset).optional,
  );
  const attentionMessages = [
    ...coreFailingChecks.map(describeQualityAlert),
    ...optionalFailingChecks.map(describeQualityAlert),
    ...failedJobs.map(
      (job) =>
        `Recent ingestion run failed for ${job.jobName}. Check the ingestion panel below for the run timestamp and source dataset.`,
    ),
  ];

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Pipeline health</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          Use this page to confirm what refreshed recently, which ingestion jobs
          ran, which quality checks need follow-up, and how F1 entities map onto
          Polymarket markets.
        </p>
      </div>

      {attentionMessages.length > 0 ? (
        <Panel
          title={
            coreFailingChecks.length > 0
              ? "Attention needed"
              : "Attention needed for optional live data"
          }
          eyebrow={`${attentionMessages.length} items`}
        >
          <div className="space-y-3">
            <p className="text-sm text-[#9ca3af]">
              {coreFailingChecks.length > 0
                ? "At least one core data-quality or ingestion issue needs review."
                : "The current issues are limited to optional live-data capture and do not block the main market, prediction, or backtest pages."}
            </p>
            <ul className="space-y-2">
              {attentionMessages.map((message) => (
                <li
                  key={message}
                  className="rounded-lg border border-white/[0.05] bg-white/[0.03] px-4 py-3 text-sm text-[#d1d5db]"
                >
                  {message}
                </li>
              ))}
            </ul>
          </div>
        </Panel>
      ) : null}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
        <StatCard
          label="Data sources"
          value={freshness.length}
          hint="Tracked connector datasets"
        />
        <StatCard
          label="Healthy feeds"
          value={healthySources}
          hint="Freshness status = ok"
        />
        <StatCard
          label="Checks passing"
          value={qualityResults.length - failingChecks.length}
          hint={`${failingChecks.length} need review`}
        />
        <StatCard
          label="Attention items"
          value={attentionMessages.length}
          hint={
            optionalFailingChecks.length > 0 && coreFailingChecks.length === 0
              ? "Optional live-data follow-up only"
              : "Quality and ingestion issues"
          }
        />
        <StatCard
          label="Entity mappings"
          value={mappings.length}
          hint={`${mappedMarkets} market links`}
        />
        <StatCard
          label="Total fetched rows"
          value={totalRecords.toLocaleString()}
          hint={`${cursorStates.length} tracked cursors`}
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
