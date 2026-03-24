"use client";

import type { PolymarketMarket } from "@f1/shared-types";
import { Badge } from "@f1/ui";
import { type Column, DataTable } from "./data-table";

function formatPrice(v: number | null) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}¢`;
}

const columns: Column<PolymarketMarket>[] = [
  {
    key: "question",
    header: "Question",
    render: (m) => (
      <span className="line-clamp-1 text-[#d1d5db]">{m.question}</span>
    ),
    sortValue: (m) => m.question,
  },
  {
    key: "taxonomy",
    header: "Category",
    render: (m) => (
      <span className="text-[10px] uppercase tracking-wider text-[#9ca3af]">
        {m.taxonomy.replace(/_/g, " ")}
      </span>
    ),
    sortValue: (m) => m.taxonomy,
  },
  {
    key: "price",
    header: "Price",
    render: (m) => (
      <span className="font-semibold tabular-nums text-white">
        {formatPrice(m.lastTradePrice)}
      </span>
    ),
    sortValue: (m) => m.lastTradePrice ?? 0,
  },
  {
    key: "bid",
    header: "Bid",
    render: (m) => (
      <span className="tabular-nums text-xs text-race-green">
        {formatPrice(m.bestBid)}
      </span>
    ),
    sortValue: (m) => m.bestBid ?? 0,
  },
  {
    key: "ask",
    header: "Ask",
    render: (m) => (
      <span className="tabular-nums text-xs text-race-yellow">
        {formatPrice(m.bestAsk)}
      </span>
    ),
    sortValue: (m) => m.bestAsk ?? 0,
  },
  {
    key: "status",
    header: "Status",
    render: (m) => (
      <Badge tone={m.active ? "good" : m.closed ? "warn" : "default"}>
        {m.active ? "Active" : m.closed ? "Closed" : "Inactive"}
      </Badge>
    ),
    sortValue: (m) => (m.active ? "a" : m.closed ? "z" : "m"),
  },
];

export function MarketsTableSection({
  markets,
}: { markets: PolymarketMarket[] }) {
  return (
    <DataTable
      columns={columns}
      data={markets}
      onRowClick={(m) => `/markets/${m.id}`}
      emptyMessage="No markets loaded."
    />
  );
}
