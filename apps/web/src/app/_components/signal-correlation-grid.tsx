"use client";

import * as React from "react";

type SignalCorrelationGridProps = {
  matrix: Record<string, unknown> | null | undefined;
};

function toneForCorrelation(value: number): string {
  if (value >= 0.75) {
    return "rgba(225, 6, 0, 0.55)";
  }
  if (value >= 0.4) {
    return "rgba(249, 115, 22, 0.45)";
  }
  if (value <= -0.4) {
    return "rgba(22, 163, 74, 0.4)";
  }
  return "rgba(255,255,255,0.06)";
}

export function SignalCorrelationGrid({ matrix }: SignalCorrelationGridProps) {
  const entries = Object.entries(matrix ?? {}) as Array<
    [string, Record<string, number>]
  >;
  if (entries.length === 0) {
    return (
      <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4 text-sm text-[#6b7280]">
        Residual correlation estimates are not available yet.
      </div>
    );
  }

  const signals = entries.map(([signal]) => signal);

  return (
    <div className="overflow-x-auto rounded-lg border border-white/[0.05]">
      <table className="min-w-full border-collapse text-xs text-[#d1d5db]">
        <thead className="bg-white/[0.03]">
          <tr>
            <th className="px-3 py-2 text-left font-medium text-[#9ca3af]">
              Signal
            </th>
            {signals.map((signal) => (
              <th
                key={signal}
                className="px-2 py-2 text-right font-medium text-[#9ca3af]"
              >
                {signal.replace(/_signal$/, "").replace(/_/g, " ")}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {entries.map(([left, rightMatrix]) => (
            <tr key={left} className="border-t border-white/[0.05]">
              <td className="px-3 py-2 font-medium text-white">
                {left.replace(/_signal$/, "").replace(/_/g, " ")}
              </td>
              {signals.map((right) => {
                const raw = rightMatrix?.[right];
                const value =
                  typeof raw === "number" && Number.isFinite(raw) ? raw : 0;
                return (
                  <td key={`${left}:${right}`} className="px-2 py-2 text-right">
                    <span
                      className="inline-flex min-w-[3rem] justify-end rounded px-2 py-1 font-mono"
                      style={{ backgroundColor: toneForCorrelation(value) }}
                    >
                      {value.toFixed(2)}
                    </span>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
