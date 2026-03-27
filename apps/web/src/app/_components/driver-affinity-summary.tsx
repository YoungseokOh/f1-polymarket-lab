import type { DriverAffinityReport } from "@f1/shared-types";
import { Badge, Panel } from "@f1/ui";
import React from "react";

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
}: {
  report: DriverAffinityReport | null;
  refreshMessage?: string | null;
}) {
  return (
    <Panel title="Driver affinity" eyebrow="Current meeting">
      <div className="space-y-4">
        <div className="flex items-center gap-2">
          <Badge tone={tone(report)}>{statusLabel(report)}</Badge>
          <p className="text-xs text-[#6b7280]">
            {report ? formatDateTime(report.computedAtUtc) : "No report yet"}
          </p>
        </div>

        {report ? (
          <>
            <div>
              <p className="text-sm font-semibold text-white">
                {report.meeting.meetingName}
              </p>
              <p className="text-xs text-[#6b7280]">
                {report.meeting.circuitShortName ?? "Circuit unavailable"} ·
                Coverage:{" "}
                {report.sourceSessionCodesIncluded.length > 0
                  ? report.sourceSessionCodesIncluded.join(", ")
                  : "historical only"}
              </p>
            </div>

            <div className="space-y-2">
              {report.entries.slice(0, 5).map((entry) => (
                <div
                  key={entry.canonicalDriverKey}
                  className="flex items-center justify-between rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2"
                >
                  <div>
                    <p className="text-sm font-medium text-white">
                      {entry.rank}. {entry.displayName}
                    </p>
                    <p className="text-[11px] text-[#6b7280]">
                      {entry.teamName ?? entry.teamId ?? "Team unavailable"} ·{" "}
                      {entry.contributingSessionCodes.join(", ")}
                    </p>
                  </div>
                  <p className="text-sm font-semibold text-white">
                    {entry.affinityScore.toFixed(3)}
                  </p>
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
