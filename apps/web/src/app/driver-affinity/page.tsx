import type {
  DriverAffinityEntry,
  DriverAffinityReport,
  RefreshDriverAffinityResponse,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";
import { PageStatusBanner } from "../../components/page-status-banner";
import {
  getDefaultDriverAffinitySegment,
  getDriverAffinitySegments,
} from "../../lib/driver-affinity";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";

export const revalidate = 60;

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

function refreshBannerMessage(response: RefreshDriverAffinityResponse | null) {
  if (!response) return null;
  return response.status === "blocked" ? response.message : null;
}

function SegmentLeaderboard({
  title,
  description,
  entryCount,
  entries,
}: {
  title: string;
  description: string;
  entryCount: number;
  entries: DriverAffinityEntry[];
}) {
  return (
    <Panel title={title}>
      <div className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className="text-sm text-[#9ca3af]">{description}</p>
          <p className="text-xs text-[#6b7280]">{entryCount} drivers ranked</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-[#6b7280]">
                <th className="pb-2 text-left font-medium">Rank</th>
                <th className="pb-2 text-left font-medium">Driver</th>
                <th className="pb-2 text-left font-medium">Team</th>
                <th className="pb-2 text-right font-medium">Affinity</th>
                <th className="pb-2 text-left font-medium">Support</th>
              </tr>
            </thead>
            <tbody>
              {entries.slice(0, 10).map((entry) => (
                <tr
                  key={`${title}:${entry.canonicalDriverKey}`}
                  className="border-b border-white/[0.04] last:border-0"
                >
                  <td className="py-2 text-white">{entry.rank}</td>
                  <td className="py-2 text-white">
                    <div className="font-medium">{entry.displayName}</div>
                    <div className="text-[11px] text-[#6b7280]">
                      {entry.displayDriverId ?? "driver unavailable"}
                    </div>
                  </td>
                  <td className="py-2 text-[#9ca3af]">
                    {entry.teamName ?? entry.teamId ?? "—"}
                  </td>
                  <td className="py-2 text-right font-semibold text-white">
                    {entry.affinityScore.toFixed(3)}
                  </td>
                  <td className="py-2 text-[#9ca3af]">
                    {entry.contributingSessionCount} sessions ·{" "}
                    {entry.contributingSessionCodes.join(", ")} ·{" "}
                    {formatDateTime(entry.latestContributingSessionEndUtc)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </Panel>
  );
}

export default async function DriverAffinityPage() {
  const refreshState = await loadResource(
    () => sdk.refreshDriverAffinity({ season: 2026 }),
    null as RefreshDriverAffinityResponse | null,
    "Driver affinity refresh",
  );
  const reportState = await loadResource(
    () => sdk.driverAffinity(2026),
    null as DriverAffinityReport | null,
    "Driver affinity",
  );
  const report = reportState.data ?? refreshState.data?.report ?? null;
  const degradedMessages = [
    ...(!report ? collectResourceErrors([refreshState, reportState]) : []),
    ...(refreshBannerMessage(refreshState.data)
      ? [refreshBannerMessage(refreshState.data) as string]
      : []),
    ...(report && !report.isFresh && report.staleReason
      ? [report.staleReason]
      : []),
  ];

  if (!report) {
    return (
      <div className="flex flex-col gap-6 p-6">
        <PageStatusBanner messages={degradedMessages} />
        <Panel title="No report available">
          <p className="text-sm text-[#9ca3af]">
            The driver affinity report could not be loaded for the current
            meeting.
          </p>
        </Panel>
      </div>
    );
  }

  const segments = getDriverAffinitySegments(report);
  const currentSegment = getDefaultDriverAffinitySegment(report);
  const seasonSegment =
    segments.find((segment) => segment.key === "season_to_date") ??
    currentSegment;
  const allHistorySegment =
    segments.find((segment) => segment.key === "all_history") ?? currentSegment;

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Driver Affinity</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Three views of pace strength for the current circuit: this weekend
          only, 2026 season to date, and the full 2024-2026 sample.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Meeting"
          value={report.meeting.meetingName}
          hint={report.meeting.circuitShortName ?? "circuit unavailable"}
        />
        <StatCard
          label="Freshness"
          value={report.isFresh ? "Fresh" : "Stale"}
          hint={formatDateTime(report.sourceMaxSessionEndUtc)}
        />
        <StatCard
          label="Current GP Leader"
          value={currentSegment.entries[0]?.displayName ?? "—"}
          hint={currentSegment.title}
        />
        <StatCard
          label="Season Leader"
          value={seasonSegment.entries[0]?.displayName ?? "—"}
          hint={seasonSegment.title}
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-3">
        <SegmentLeaderboard
          title={currentSegment.title}
          description={currentSegment.description}
          entryCount={currentSegment.entryCount}
          entries={currentSegment.entries}
        />
        <SegmentLeaderboard
          title={seasonSegment.title}
          description={seasonSegment.description}
          entryCount={seasonSegment.entryCount}
          entries={seasonSegment.entries}
        />
        <SegmentLeaderboard
          title={allHistorySegment.title}
          description={allHistorySegment.description}
          entryCount={allHistorySegment.entryCount}
          entries={allHistorySegment.entries}
        />
      </section>
    </div>
  );
}
