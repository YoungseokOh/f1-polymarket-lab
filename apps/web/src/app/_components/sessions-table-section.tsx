"use client";

import type { F1Session } from "@f1/shared-types";
import { Badge } from "@f1/ui";
import { type Column, DataTable } from "./data-table";

const columns: Column<F1Session>[] = [
  {
    key: "sessionName",
    header: "Session",
    render: (s) => (
      <span className="font-medium text-white">{s.sessionName}</span>
    ),
    sortValue: (s) => s.sessionName,
  },
  {
    key: "sessionCode",
    header: "Code",
    render: (s) => (
      <span className="font-mono text-xs text-[#9ca3af]">
        {s.sessionCode ?? "—"}
      </span>
    ),
    sortValue: (s) => s.sessionCode ?? "",
  },
  {
    key: "type",
    header: "Type",
    render: (s) => (
      <Badge tone={s.isPractice ? "good" : "default"}>
        {s.isPractice ? "Practice" : (s.sessionType ?? "Other")}
      </Badge>
    ),
    sortValue: (s) => (s.isPractice ? "a" : "z"),
  },
  {
    key: "start",
    header: "Start",
    render: (s) =>
      s.dateStartUtc ? (
        <span className="tabular-nums text-xs text-[#d1d5db]">
          {new Date(s.dateStartUtc).toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
            year: "numeric",
          })}{" "}
          {new Date(s.dateStartUtc).toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
          })}
        </span>
      ) : (
        <span className="text-xs text-[#6b7280]">—</span>
      ),
    sortValue: (s) => s.dateStartUtc ?? "",
  },
];

export function SessionsTableSection({ sessions }: { sessions: F1Session[] }) {
  return (
    <DataTable
      columns={columns}
      data={sessions}
      emptyMessage="No sessions loaded."
    />
  );
}
