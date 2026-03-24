"use client";

import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";

export type Column<T> = {
  key: string;
  header: string;
  render: (row: T) => ReactNode;
  sortValue?: (row: T) => string | number | null;
};

type DataTableProps<T> = {
  columns: Column<T>[];
  data: T[];
  rowKey?: (row: T) => string;
  onRowClick?: (row: T) => string | undefined;
  emptyMessage?: string;
};

export function DataTable<T>({
  columns,
  data,
  rowKey,
  onRowClick,
  emptyMessage = "No data available",
}: DataTableProps<T>) {
  const router = useRouter();
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const sorted = useMemo(() => {
    if (!sortCol) return data;
    const col = columns.find((c) => c.key === sortCol);
    if (!col?.sortValue) return data;
    const getter = col.sortValue;
    return [...data].sort((a, b) => {
      const va = getter(a);
      const vb = getter(b);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const cmp =
        typeof va === "number" && typeof vb === "number"
          ? va - vb
          : String(va).localeCompare(String(vb));
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [data, sortCol, sortDir, columns]);

  function handleSort(key: string) {
    if (sortCol === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(key);
      setSortDir("asc");
    }
  }

  if (data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <p className="text-sm text-[#6b7280]">{emptyMessage}</p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-white/[0.06]">
            {columns.map((col) => (
              <th
                key={col.key}
                className={`pb-3 pr-4 text-[10px] font-bold uppercase tracking-[0.2em] text-[#6b7280] ${col.sortValue ? "cursor-pointer select-none hover:text-white" : ""}`}
                onClick={col.sortValue ? () => handleSort(col.key) : undefined}
                onKeyDown={
                  col.sortValue
                    ? (e) => {
                        if (e.key === "Enter" || e.key === " ")
                          handleSort(col.key);
                      }
                    : undefined
                }
                tabIndex={col.sortValue ? 0 : undefined}
                role={col.sortValue ? "button" : undefined}
              >
                <span className="inline-flex items-center gap-1">
                  {col.header}
                  {sortCol === col.key && (
                    <span className="text-f1-red">
                      {sortDir === "asc" ? "↑" : "↓"}
                    </span>
                  )}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={rowKey ? rowKey(row) : i}
              className={`border-b border-white/[0.03] transition-colors ${onRowClick ? "cursor-pointer hover:bg-white/[0.02]" : ""} ${i % 2 === 1 ? "bg-white/[0.01]" : ""}`}
              onClick={
                onRowClick
                  ? () => {
                      const result = onRowClick(row);
                      if (typeof result === "string") router.push(result);
                    }
                  : undefined
              }
              onKeyDown={
                onRowClick
                  ? (e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        const result = onRowClick(row);
                        if (typeof result === "string") router.push(result);
                      }
                    }
                  : undefined
              }
              tabIndex={onRowClick ? 0 : undefined}
              role={onRowClick ? "button" : undefined}
            >
              {columns.map((col) => (
                <td key={col.key} className="py-3 pr-4 text-[#d1d5db]">
                  {col.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
