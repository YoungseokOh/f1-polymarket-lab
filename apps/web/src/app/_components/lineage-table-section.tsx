"use client";

import type { EntityMapping, FreshnessRecord } from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import { type Column, DataTable } from "./data-table";
import { StatusIndicator } from "./status-indicator";

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
      <StatusIndicator
        status={
          r.status === "ok"
            ? "ok"
            : r.status === "pending"
              ? "pending"
              : "error"
        }
        label={r.status}
      />
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
  mappings,
  mappedMarkets,
}: {
  freshness: FreshnessRecord[];
  mappings: EntityMapping[];
  mappedMarkets: number;
}) {
  return (
    <>
      <Panel title="Data Freshness" eyebrow={`${freshness.length} sources`}>
        <DataTable
          columns={freshnessColumns}
          data={freshness}
          emptyMessage="No data sources configured."
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
