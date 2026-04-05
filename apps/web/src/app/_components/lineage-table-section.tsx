"use client";

import type {
  CursorState,
  DataQualityResult,
  EntityMapping,
  FreshnessRecord,
  IngestionJobRun,
} from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import { describeQualityDataset } from "../../lib/display";
import { type Column, DataTable } from "./data-table";
import { StatusIndicator } from "./status-indicator";

function statusVariant(
  status: string,
): "live" | "ok" | "idle" | "error" | "pending" {
  if (status === "ok" || status === "completed" || status === "pass") {
    return "ok";
  }
  if (status === "pending" || status === "running" || status === "planned") {
    return "pending";
  }
  if (status === "idle") {
    return "idle";
  }
  return "error";
}

function summarizeRecord(
  value: Record<string, unknown> | null | undefined,
  emptyLabel = "—",
): string {
  if (!value || Object.keys(value).length === 0) {
    return emptyLabel;
  }

  return Object.entries(value)
    .slice(0, 3)
    .map(([key, raw]) => {
      if (typeof raw === "number") {
        return `${key}=${raw}`;
      }
      if (typeof raw === "string") {
        return `${key}=${raw}`;
      }
      if (typeof raw === "boolean") {
        return `${key}=${raw ? "true" : "false"}`;
      }
      return `${key}=…`;
    })
    .join(" · ");
}

const freshnessColumns: Column<FreshnessRecord>[] = [
  {
    key: "source",
    header: "Source",
    render: (r) => <span className="font-medium text-white">{r.source}</span>,
    sortValue: (r) => r.source,
  },
  {
    key: "dataset",
    header: "Dataset",
    render: (r) => <span className="text-sm text-[#d1d5db]">{r.dataset}</span>,
    sortValue: (r) => r.dataset,
  },
  {
    key: "status",
    header: "Status",
    render: (r) => (
      <StatusIndicator status={statusVariant(r.status)} label={r.status} />
    ),
    sortValue: (r) => r.status,
  },
  {
    key: "records",
    header: "Records",
    render: (r) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {r.recordsFetched.toLocaleString()}
      </span>
    ),
    sortValue: (r) => r.recordsFetched,
  },
  {
    key: "lastFetch",
    header: "Last Fetch",
    render: (r) =>
      r.lastFetchAt ? (
        <span className="tabular-nums text-xs text-[#6b7280]">
          {new Date(r.lastFetchAt).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
          })}
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">Never</span>
      ),
    sortValue: (r) => r.lastFetchAt ?? "",
  },
];

const jobColumns: Column<IngestionJobRun>[] = [
  {
    key: "jobName",
    header: "Job",
    render: (job) => (
      <span className="font-medium text-white">{job.jobName}</span>
    ),
    sortValue: (job) => job.jobName,
  },
  {
    key: "status",
    header: "Status",
    render: (job) => (
      <StatusIndicator status={statusVariant(job.status)} label={job.status} />
    ),
    sortValue: (job) => job.status,
  },
  {
    key: "source",
    header: "Source",
    render: (job) => (
      <span className="text-xs text-[#9ca3af]">
        {job.source} / {job.dataset}
      </span>
    ),
    sortValue: (job) => `${job.source}:${job.dataset}`,
  },
  {
    key: "records",
    header: "Records",
    render: (job) => (
      <span className="tabular-nums text-sm text-[#d1d5db]">
        {job.recordsWritten ?? "—"}
      </span>
    ),
    sortValue: (job) => job.recordsWritten ?? -1,
  },
  {
    key: "started",
    header: "Started",
    render: (job) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(job.startedAt).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        })}
      </span>
    ),
    sortValue: (job) => job.startedAt,
  },
];

const cursorColumns: Column<CursorState>[] = [
  {
    key: "source",
    header: "Source",
    render: (cursor) => (
      <span className="font-medium text-white">{cursor.source}</span>
    ),
    sortValue: (cursor) => cursor.source,
  },
  {
    key: "dataset",
    header: "Dataset",
    render: (cursor) => (
      <span className="text-sm text-[#d1d5db]">{cursor.dataset}</span>
    ),
    sortValue: (cursor) => cursor.dataset,
  },
  {
    key: "cursorKey",
    header: "Cursor Key",
    render: (cursor) => (
      <span className="font-mono text-xs text-[#9ca3af]">
        {cursor.cursorKey}
      </span>
    ),
    sortValue: (cursor) => cursor.cursorKey,
  },
  {
    key: "cursorValue",
    header: "Cursor Value",
    render: (cursor) => (
      <span className="text-xs text-[#9ca3af]">
        {summarizeRecord(cursor.cursorValue)}
      </span>
    ),
    sortValue: (cursor) => summarizeRecord(cursor.cursorValue, ""),
  },
  {
    key: "updatedAt",
    header: "Updated",
    render: (cursor) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(cursor.updatedAt).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        })}
      </span>
    ),
    sortValue: (cursor) => cursor.updatedAt,
  },
];

const qualityColumns: Column<DataQualityResult>[] = [
  {
    key: "check",
    header: "Check",
    render: (result) => {
      const info = describeQualityDataset(result.dataset);
      return (
        <div>
          <p className="font-medium text-white">{info.label}</p>
          <p className="mt-1 text-xs text-[#9ca3af]">{info.impact}</p>
        </div>
      );
    },
    sortValue: (result) => describeQualityDataset(result.dataset).label,
  },
  {
    key: "status",
    header: "Status",
    render: (result) => (
      <StatusIndicator
        status={statusVariant(result.status)}
        label={result.status}
      />
    ),
    sortValue: (result) => result.status,
  },
  {
    key: "metrics",
    header: "Metrics",
    render: (result) => (
      <span className="text-xs text-[#9ca3af]">
        {summarizeRecord(result.metricsJson)}
      </span>
    ),
    sortValue: (result) => summarizeRecord(result.metricsJson, ""),
  },
  {
    key: "observedAt",
    header: "Observed",
    render: (result) => (
      <span className="tabular-nums text-xs text-[#6b7280]">
        {new Date(result.observedAt).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        })}
      </span>
    ),
    sortValue: (result) => result.observedAt,
  },
];

const mappingColumns: Column<EntityMapping>[] = [
  {
    key: "type",
    header: "Type",
    render: (m) => <Badge>{m.mappingType}</Badge>,
    sortValue: (m) => m.mappingType,
  },
  {
    key: "f1Meeting",
    header: "F1 Meeting",
    render: (m) => (
      <span className="font-mono text-xs text-[#d1d5db]">
        {m.f1MeetingId?.slice(0, 12) ?? "—"}
      </span>
    ),
    sortValue: (m) => m.f1MeetingId ?? "",
  },
  {
    key: "polymarket",
    header: "Polymarket",
    render: (m) => (
      <span className="font-mono text-xs text-[#d1d5db]">
        {m.polymarketMarketId?.slice(0, 12) ??
          m.polymarketEventId?.slice(0, 12) ??
          "—"}
      </span>
    ),
    sortValue: (m) => m.polymarketMarketId ?? m.polymarketEventId ?? "",
  },
  {
    key: "confidence",
    header: "Confidence",
    render: (m) =>
      m.confidence != null ? (
        <span className="tabular-nums text-sm text-[#d1d5db]">
          {(m.confidence * 100).toFixed(0)}%
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (m) => m.confidence ?? 0,
  },
  {
    key: "matchedBy",
    header: "Matched By",
    render: (m) => (
      <span className="text-xs text-[#9ca3af]">{m.matchedBy ?? "—"}</span>
    ),
    sortValue: (m) => m.matchedBy ?? "",
  },
  {
    key: "override",
    header: "Override",
    render: (m) =>
      m.overrideFlag ? <Badge tone="warn">Override</Badge> : null,
    sortValue: (m) => (m.overrideFlag ? "a" : "z"),
  },
];

export function LineageTableSection({
  freshness,
  jobs,
  cursorStates,
  qualityResults,
  mappings,
  mappedMarkets,
}: {
  freshness: FreshnessRecord[];
  jobs: IngestionJobRun[];
  cursorStates: CursorState[];
  qualityResults: DataQualityResult[];
  mappings: EntityMapping[];
  mappedMarkets: number;
}) {
  const sortedQualityResults = [...qualityResults].sort((a, b) => {
    const aFail = a.status !== "pass";
    const bFail = b.status !== "pass";
    if (aFail !== bFail) {
      return aFail ? -1 : 1;
    }
    return b.observedAt.localeCompare(a.observedAt);
  });

  return (
    <>
      <Panel title="Data Freshness" eyebrow={`${freshness.length} sources`}>
        <DataTable
          columns={freshnessColumns}
          data={freshness}
          emptyMessage="No data sources configured."
        />
      </Panel>

      <div className="grid gap-4 xl:grid-cols-2">
        <Panel title="Recent Ingestion Runs" eyebrow={`${jobs.length} runs`}>
          <DataTable
            columns={jobColumns}
            data={jobs}
            emptyMessage="No ingestion runs recorded."
          />
        </Panel>

        <Panel title="Cursor State" eyebrow={`${cursorStates.length} cursors`}>
          <DataTable
            columns={cursorColumns}
            data={cursorStates}
            emptyMessage="No source cursors recorded."
          />
        </Panel>
      </div>

      <Panel
        title="Data Quality"
        eyebrow={`${qualityResults.length} latest checks`}
      >
        <DataTable
          columns={qualityColumns}
          data={sortedQualityResults}
          emptyMessage="No data quality checks recorded."
        />
      </Panel>

      <Panel
        title="Entity Mappings"
        eyebrow={`${mappings.length} mappings · ${mappedMarkets} market links`}
      >
        <DataTable
          columns={mappingColumns}
          data={mappings}
          emptyMessage="No entity mappings found."
        />
      </Panel>
    </>
  );
}
