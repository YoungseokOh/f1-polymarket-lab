import type {
  DriverAffinityReport,
  RefreshDriverAffinityResponse,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";
import { PageStatusBanner } from "../../components/page-status-banner";
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
  const degradedMessages = [
    ...collectResourceErrors([refreshState, reportState]),
    ...(refreshBannerMessage(refreshState.data)
      ? [refreshBannerMessage(refreshState.data) as string]
      : []),
    ...(reportState.data &&
    !reportState.data.isFresh &&
    reportState.data.staleReason
      ? [reportState.data.staleReason]
      : []),
  ];
  const report = reportState.data;

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Driver Affinity</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Circuit-weighted driver pace strength for the current meeting,
          refreshed on page load.
        </p>
      </div>

      {report ? (
        <>
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
              label="Coverage"
              value={report.sourceSessionCodesIncluded.join(", ") || "None"}
              hint="ended sessions included"
            />
            <StatCard
              label="Drivers"
              value={report.entryCount}
              hint={`updated ${formatDateTime(report.computedAtUtc)}`}
            />
          </section>

          <Panel title="Leaderboard">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-[#6b7280]">
                    <th className="pb-2 text-left font-medium">Rank</th>
                    <th className="pb-2 text-left font-medium">Driver</th>
                    <th className="pb-2 text-left font-medium">Team</th>
                    <th className="pb-2 text-right font-medium">Affinity</th>
                    <th className="pb-2 text-right font-medium">S1</th>
                    <th className="pb-2 text-right font-medium">S2</th>
                    <th className="pb-2 text-right font-medium">S3</th>
                    <th className="pb-2 text-left font-medium">Support</th>
                  </tr>
                </thead>
                <tbody>
                  {report.entries.map((entry) => (
                    <tr
                      key={entry.canonicalDriverKey}
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
                      <td className="py-2 text-right text-[#9ca3af]">
                        {entry.s1Strength.toFixed(3)}
                      </td>
                      <td className="py-2 text-right text-[#9ca3af]">
                        {entry.s2Strength.toFixed(3)}
                      </td>
                      <td className="py-2 text-right text-[#9ca3af]">
                        {entry.s3Strength.toFixed(3)}
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
          </Panel>
        </>
      ) : (
        <Panel title="No report available">
          <p className="text-sm text-[#9ca3af]">
            The driver affinity report could not be loaded for the current
            meeting.
          </p>
        </Panel>
      )}
    </div>
  );
}
