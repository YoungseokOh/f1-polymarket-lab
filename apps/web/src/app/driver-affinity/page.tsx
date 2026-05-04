import type {
  CurrentWeekendOperationsReadiness,
  DriverAffinityEntry,
  DriverAffinityReport,
  RefreshDriverAffinityResponse,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";
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

function isRosterOnlyFallback(report: DriverAffinityReport | null) {
  return Boolean(
    report &&
      report.entryCount > 0 &&
      !report.sourceMaxSessionEndUtc &&
      report.entries.every((entry) => entry.contributingSessionCount === 0),
  );
}

function sameSegmentSample(
  left: DriverAffinityReport["segments"][number],
  right: DriverAffinityReport["segments"][number],
) {
  return (
    left.sourceSeasonsIncluded.join(",") ===
      right.sourceSeasonsIncluded.join(",") &&
    left.sourceSessionCodesIncluded.join(",") ===
      right.sourceSessionCodesIncluded.join(",") &&
    left.entries.map((entry) => entry.canonicalDriverKey).join(",") ===
      right.entries.map((entry) => entry.canonicalDriverKey).join(",")
  );
}

function SegmentLeaderboard({
  title,
  description,
  entryCount,
  sourceSessionCodesIncluded,
  sourceSeasonsIncluded,
  entries,
}: {
  title: string;
  description: string;
  entryCount: number;
  sourceSessionCodesIncluded: string[];
  sourceSeasonsIncluded: number[];
  entries: DriverAffinityEntry[];
}) {
  const sessionLabel =
    sourceSessionCodesIncluded.length > 0
      ? sourceSessionCodesIncluded.join(" / ")
      : "No session data";
  const seasonLabel =
    sourceSeasonsIncluded.length > 1
      ? `${sourceSeasonsIncluded[0]}-${sourceSeasonsIncluded.at(-1)}`
      : `${sourceSeasonsIncluded[0] ?? "season"}`;

  return (
    <Panel title={title}>
      <div className="space-y-4">
        <div className="space-y-3">
          <p className="text-sm text-[#9ca3af]">{description}</p>
          <div className="flex flex-wrap gap-2 text-[11px] uppercase tracking-wider">
            <span className="rounded-md border border-white/[0.08] bg-white/[0.03] px-2 py-1 text-[#d1d5db]">
              {entryCount} drivers
            </span>
            <span className="rounded-md border border-white/[0.08] bg-white/[0.03] px-2 py-1 text-[#d1d5db]">
              {sessionLabel}
            </span>
            <span className="rounded-md border border-white/[0.08] bg-white/[0.03] px-2 py-1 text-[#d1d5db]">
              {seasonLabel}
            </span>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-[#6b7280]">
                <th className="w-10 pb-2 text-left font-medium">#</th>
                <th className="pb-2 text-left font-medium">Driver</th>
                <th className="pb-2 text-right font-medium">Affinity</th>
                <th className="pb-2 text-right font-medium">Sectors</th>
              </tr>
            </thead>
            <tbody>
              {entries.slice(0, 10).map((entry) => (
                <tr
                  key={`${title}:${entry.canonicalDriverKey}`}
                  className="border-b border-white/[0.04] last:border-0"
                >
                  <td className="py-3 text-[#9ca3af]">{entry.rank}</td>
                  <td className="py-3 text-white">
                    <div className="font-medium">{entry.displayName}</div>
                    <div className="text-[11px] text-[#6b7280]">
                      {entry.teamName ?? entry.teamId ?? "Team unavailable"}
                    </div>
                  </td>
                  <td className="py-3 text-right text-base font-semibold text-white">
                    {entry.affinityScore.toFixed(3)}
                  </td>
                  <td className="py-3 text-right text-[11px] tabular-nums text-[#9ca3af]">
                    S1 {entry.s1Strength.toFixed(2)} · S2{" "}
                    {entry.s2Strength.toFixed(2)} · S3{" "}
                    {entry.s3Strength.toFixed(2)}
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
  const readinessState = await loadResource(
    () => sdk.currentWeekendReadiness({ season: 2026 }),
    null as CurrentWeekendOperationsReadiness | null,
    "Weekend operations readiness",
  );
  const report = reportState.data ?? refreshState.data?.report ?? null;
  const rosterOnlyFallback = isRosterOnlyFallback(report);
  const affinityReadiness =
    readinessState.data?.actions.find(
      (action) => action.key === "driver_affinity",
    ) ?? null;
  const readinessStatus =
    rosterOnlyFallback && affinityReadiness?.status === "blocked"
      ? "degraded"
      : affinityReadiness?.status;
  const readinessMessage = rosterOnlyFallback
    ? "FP1 lap data is not hydrated yet; showing the current season roster."
    : affinityReadiness?.message;
  const degradedMessages = [
    ...(!report
      ? collectResourceErrors([refreshState, reportState, readinessState])
      : collectResourceErrors([readinessState])),
    ...(!rosterOnlyFallback && refreshBannerMessage(refreshState.data)
      ? [refreshBannerMessage(refreshState.data) as string]
      : []),
    ...(report && !rosterOnlyFallback && !report.isFresh && report.staleReason
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
  const visibleSegments =
    rosterOnlyFallback || sameSegmentSample(seasonSegment, allHistorySegment)
      ? [currentSegment, seasonSegment]
      : [currentSegment, seasonSegment, allHistorySegment];

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Driver Affinity</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Pace strength for the current circuit. Historical views appear when
          distinct prior-season lap data is loaded.
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
          value={
            rosterOnlyFallback ? "Roster" : report.isFresh ? "Fresh" : "Stale"
          }
          hint={
            rosterOnlyFallback
              ? "FP1 data pending"
              : formatDateTime(report.sourceMaxSessionEndUtc)
          }
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

      {affinityReadiness ? (
        <Panel title="Refresh status" eyebrow="Current readiness">
          <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                tone={
                  readinessStatus === "ready"
                    ? "good"
                    : readinessStatus === "blocked"
                      ? "warn"
                      : "default"
                }
              >
                {readinessStatus}
              </Badge>
              <p className="text-sm text-[#9ca3af]">{readinessMessage}</p>
            </div>
            {affinityReadiness.lastJobRun ? (
              <p className="text-xs text-[#6b7280]">
                Last run: {affinityReadiness.lastJobRun.status} ·{" "}
                {formatDateTime(affinityReadiness.lastJobRun.finishedAt)}
              </p>
            ) : null}
            {affinityReadiness.lastReportPath ? (
              <p className="text-xs text-[#6b7280]">
                Latest report: <code>{affinityReadiness.lastReportPath}</code>
              </p>
            ) : null}
          </div>
        </Panel>
      ) : null}

      <section className="grid gap-4 xl:grid-cols-3">
        {visibleSegments.map((segment) => (
          <SegmentLeaderboard
            key={segment.key}
            title={segment.title}
            description={segment.description}
            entryCount={segment.entryCount}
            sourceSessionCodesIncluded={segment.sourceSessionCodesIncluded}
            sourceSeasonsIncluded={segment.sourceSeasonsIncluded}
            entries={segment.entries}
          />
        ))}
      </section>
    </div>
  );
}
