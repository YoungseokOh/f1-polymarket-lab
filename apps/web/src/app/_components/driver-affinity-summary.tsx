import type {
  DriverAffinityReport,
  OperationReadiness,
} from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import React from "react";
import { getDriverAffinitySegments } from "../../lib/driver-affinity";

function formatDateTime(value: string | null | undefined) {
  if (!value) return "—";
  return new Date(value).toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function tone(
  report: DriverAffinityReport | null,
): "default" | "good" | "warn" {
  if (!report) return "default";
  return report.isFresh ? "good" : "warn";
}

function statusLabel(report: DriverAffinityReport | null) {
  if (!report) return "Unavailable";
  return report.isFresh ? "Fresh" : "Stale";
}

export function DriverAffinitySummary({
  report,
  refreshMessage,
  readiness,
}: {
  report: DriverAffinityReport | null;
  refreshMessage?: string | null;
  readiness?: OperationReadiness | null;
}) {
  const segments = report ? getDriverAffinitySegments(report) : [];
  return (
    <Panel title="Driver affinity" eyebrow="Three lenses">
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <Badge tone={tone(report)}>{statusLabel(report)}</Badge>
          <p className="text-xs text-[#6b7280]">
            {report ? formatDateTime(report.computedAtUtc) : "No report yet"}
          </p>
        </div>

        {readiness ? (
          <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                tone={
                  readiness.status === "ready"
                    ? "good"
                    : readiness.status === "blocked"
                      ? "warn"
                      : "default"
                }
              >
                {readiness.status}
              </Badge>
              <p className="text-xs text-[#9ca3af]">{readiness.message}</p>
            </div>
            {readiness.lastJobRun ? (
              <p className="mt-1 text-[11px] text-[#6b7280]">
                Last run: {readiness.lastJobRun.status} ·{" "}
                {formatDateTime(readiness.lastJobRun.finishedAt)}
              </p>
            ) : null}
          </div>
        ) : null}

        {report ? (
          <>
            <div className="space-y-1">
              <p className="text-[13px] font-semibold text-white">
                {report.meeting.meetingName}
              </p>
              <p className="text-[11px] text-[#6b7280]">
                {report.meeting.circuitShortName ?? "Circuit unavailable"} track
                view. Current GP, season-to-date, and 2024-2026 all-time lenses.
              </p>
            </div>

            <div className="space-y-2">
              {segments.slice(0, 3).map((segment) => (
                <div
                  key={segment.key}
                  className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[#d1d5db]">
                        {segment.title}
                      </p>
                      <p className="mt-1 text-[10px] text-[#6b7280]">
                        {segment.description}
                      </p>
                    </div>
                    <p className="text-[10px] text-[#6b7280]">
                      {segment.entryCount} drivers
                    </p>
                  </div>
                  <div className="mt-2 space-y-1.5">
                    {segment.entries.slice(0, 2).map((entry) => (
                      <div
                        key={`${segment.key}:${entry.canonicalDriverKey}`}
                        className="flex items-center justify-between gap-3"
                      >
                        <div>
                          <p className="text-[13px] font-medium text-white">
                            {entry.rank}. {entry.displayName}
                          </p>
                          <p className="text-[10px] text-[#6b7280]">
                            {entry.teamName ??
                              entry.teamId ??
                              "Team unavailable"}
                          </p>
                        </div>
                        <p className="text-[13px] font-semibold text-white">
                          {entry.affinityScore.toFixed(3)}
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>

            <p className="text-xs text-[#6b7280]">
              Latest ended session:{" "}
              {report.latestEndedRelevantSessionCode ?? "None"} ·{" "}
              {formatDateTime(report.latestEndedRelevantSessionEndUtc)}
            </p>
            {report.staleReason ? (
              <p className="text-xs text-[#fbbf24]">{report.staleReason}</p>
            ) : null}
          </>
        ) : (
          <p className="text-sm text-[#9ca3af]">
            No affinity report is available yet for the current meeting.
          </p>
        )}

        {refreshMessage ? (
          <p className="rounded-lg border border-[#e10600]/20 bg-[#e10600]/10 px-3 py-2 text-xs text-[#ffb4b1]">
            {refreshMessage}
          </p>
        ) : null}

        <a
          href="/driver-affinity"
          className="inline-flex text-sm font-medium text-[#ff6a63] hover:text-[#ff8b85]"
        >
          Open full leaderboard
        </a>
      </div>
    </Panel>
  );
}
