import Link from "next/link";

import type { F1Meeting, F1Session } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import {
  formatDateRangeShort,
  formatDateTimeShort,
  formatSessionCodeLabel,
} from "../../lib/display";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { selectScheduleMeetings } from "../../lib/schedule";
import { latestEndedSessionForMeeting } from "../../lib/session-refresh";
import { MeetingRefreshButton } from "../_components/meeting-refresh-button";

export const revalidate = 300;

const SESSION_ORDER: Record<string, number> = {
  FP1: 0,
  FP2: 1,
  FP3: 2,
  SQ: 3,
  S: 4,
  Q: 5,
  R: 6,
};

function meetingBucket(
  meeting: F1Meeting,
  nowMs: number,
): "live" | "upcoming" | "completed" {
  const startMs = meeting.startDateUtc
    ? new Date(meeting.startDateUtc).getTime()
    : 0;
  const endMs = meeting.endDateUtc
    ? new Date(meeting.endDateUtc).getTime()
    : startMs;

  if (startMs <= nowMs && endMs >= nowMs) {
    return "live";
  }
  if (startMs > nowMs) {
    return "upcoming";
  }
  return "completed";
}

function sortSessionsForMeeting(meetingSessions: F1Session[]): F1Session[] {
  return [...meetingSessions].sort((a, b) => {
    const orderA = SESSION_ORDER[a.sessionCode ?? ""] ?? 99;
    const orderB = SESSION_ORDER[b.sessionCode ?? ""] ?? 99;
    if (orderA !== orderB) {
      return orderA - orderB;
    }
    return (a.dateStartUtc ?? "").localeCompare(b.dateStartUtc ?? "");
  });
}

function sessionStatus(
  session: F1Session,
  nowMs: number,
): { label: string; tone: "default" | "good" | "warn" | "live" } {
  const startMs = session.dateStartUtc
    ? new Date(session.dateStartUtc).getTime()
    : null;
  const endMs = session.dateEndUtc
    ? new Date(session.dateEndUtc).getTime()
    : null;

  if (startMs != null && endMs != null && startMs <= nowMs && endMs >= nowMs) {
    return { label: "Live", tone: "live" };
  }
  if (endMs != null && endMs < nowMs) {
    return { label: "Finished", tone: "good" };
  }
  return { label: "Scheduled", tone: "default" };
}

function renderMeetingCard(
  meeting: F1Meeting,
  meetingSessions: F1Session[],
  now: Date,
) {
  const nowMs = now.getTime();
  const latestEndedSession = latestEndedSessionForMeeting(meetingSessions, now);
  const nextSession = meetingSessions.find((session) => {
    if (!session.dateStartUtc) {
      return false;
    }
    return new Date(session.dateStartUtc).getTime() > nowMs;
  });
  const bucket = meetingBucket(meeting, nowMs);
  const statusBadge =
    bucket === "live"
      ? { label: "Live weekend", tone: "live" as const }
      : bucket === "upcoming"
        ? { label: "Coming up", tone: "warn" as const }
        : { label: "Completed", tone: "good" as const };

  return (
    <section
      key={meeting.id}
      className="relative overflow-hidden rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-5 shadow-xl shadow-black/30"
    >
      <div className="absolute left-0 top-0 h-full w-[3px] bg-[#e10600]" />
      <div className="absolute left-0 top-0 h-full w-[6px] bg-[#e10600]/20 blur-sm" />

      <div className="mb-5 flex items-start justify-between gap-4 pl-2">
        <div className="min-w-0">
          <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
            {meeting.season} · Round {meeting.roundNumber ?? "?"}
          </p>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <h2 className="text-lg font-semibold text-white">
              {meeting.meetingName}
            </h2>
            <Badge tone={statusBadge.tone}>{statusBadge.label}</Badge>
          </div>
          <p className="mt-1 text-sm text-[#9ca3af]">
            {meeting.circuitShortName ??
              meeting.location ??
              "Location unavailable"}
            {meeting.countryName ? ` · ${meeting.countryName}` : ""}
          </p>
          <p className="mt-1 text-xs text-[#6b7280]">
            {formatDateRangeShort(meeting.startDateUtc, meeting.endDateUtc)}
          </p>
        </div>

        <div className="flex shrink-0 flex-col items-end gap-2">
          <MeetingRefreshButton
            meetingId={meeting.id}
            latestEndedSession={latestEndedSession}
          />
          <Link
            href={`/gp/${meeting.id}`}
            className="rounded-md border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-xs text-[#9ca3af] transition-colors hover:border-[#e10600]/30 hover:text-white"
          >
            Open Grand Prix detail
          </Link>
        </div>
      </div>

      <div className="mb-4 grid gap-3 pl-2 md:grid-cols-3">
        <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-3">
          <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
            Latest finished
          </p>
          <p className="mt-2 text-sm font-medium text-white">
            {latestEndedSession
              ? formatSessionCodeLabel(latestEndedSession.sessionCode)
              : "No finished session yet"}
          </p>
          <p className="mt-1 text-xs text-[#6b7280]">
            {latestEndedSession?.dateEndUtc
              ? `Ended ${formatDateTimeShort(latestEndedSession.dateEndUtc)}`
              : "Refresh becomes available after the first session ends."}
          </p>
        </div>

        <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-3">
          <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
            Next on schedule
          </p>
          <p className="mt-2 text-sm font-medium text-white">
            {nextSession
              ? formatSessionCodeLabel(nextSession.sessionCode)
              : "Weekend complete"}
          </p>
          <p className="mt-1 text-xs text-[#6b7280]">
            {nextSession?.dateStartUtc
              ? `Starts ${formatDateTimeShort(nextSession.dateStartUtc)}`
              : "No future sessions remain for this weekend."}
          </p>
        </div>

        <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-3">
          <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
            Session coverage
          </p>
          <p className="mt-2 text-sm font-medium text-white">
            {meetingSessions.length} sessions tracked
          </p>
          <p className="mt-1 text-xs text-[#6b7280]">
            Practice, sprint, qualifying, and race sessions appear here when
            available.
          </p>
        </div>
      </div>

      {meetingSessions.length === 0 ? (
        <p className="pl-2 text-sm text-[#6b7280]">
          No sessions are loaded for this Grand Prix yet.
        </p>
      ) : (
        <div className="grid grid-cols-1 gap-2 pl-2 sm:grid-cols-2 xl:grid-cols-3">
          {meetingSessions.map((session) => {
            const status = sessionStatus(session, nowMs);
            const label = formatSessionCodeLabel(session.sessionCode);

            return (
              <div
                key={session.id}
                className="rounded-lg border border-white/[0.05] bg-white/[0.02] px-3 py-3"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-white">{label}</p>
                    {session.sessionName !== label ? (
                      <p className="mt-0.5 text-xs text-[#9ca3af]">
                        {session.sessionName}
                      </p>
                    ) : null}
                  </div>
                  <Badge tone={status.tone}>{status.label}</Badge>
                </div>
                <p className="mt-3 text-xs text-[#6b7280]">
                  {formatDateTimeShort(session.dateStartUtc)}
                </p>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function renderMeetingSection(
  title: string,
  description: string,
  meetings: F1Meeting[],
  sessionsByMeetingId: Map<string, F1Session[]>,
  now: Date,
) {
  if (meetings.length === 0) {
    return null;
  }

  return (
    <section className="flex flex-col gap-4">
      <div>
        <h2 className="text-lg font-semibold text-white">{title}</h2>
        <p className="mt-1 text-sm text-[#6b7280]">{description}</p>
      </div>

      <div className="flex flex-col gap-4">
        {meetings.map((meeting) =>
          renderMeetingCard(
            meeting,
            sessionsByMeetingId.get(meeting.id) ?? [],
            now,
          ),
        )}
      </div>
    </section>
  );
}

export default async function SessionsPage() {
  const meetingsState = await loadResource(
    () => sdk.meetings({ limit: 100 }),
    [] as F1Meeting[],
    "Meeting feed",
  );
  const { season: scheduleSeason, meetings } = selectScheduleMeetings(
    meetingsState.data,
  );
  const sessionsState = await loadResource(
    () =>
      scheduleSeason == null
        ? Promise.resolve([] as F1Session[])
        : sdk.sessions({ limit: 1000, season: scheduleSeason }),
    [] as F1Session[],
    "Session feed",
  );
  const sessions = sessionsState.data;
  const degradedMessages = collectResourceErrors([
    sessionsState,
    meetingsState,
  ]);

  const now = new Date();
  const nowMs = now.getTime();
  const sessionsByMeetingId = new Map<string, F1Session[]>();
  for (const session of sessions) {
    if (!session.meetingId) {
      continue;
    }
    const list = sessionsByMeetingId.get(session.meetingId) ?? [];
    list.push(session);
    sessionsByMeetingId.set(session.meetingId, list);
  }
  for (const [meetingId, meetingSessions] of sessionsByMeetingId.entries()) {
    sessionsByMeetingId.set(meetingId, sortSessionsForMeeting(meetingSessions));
  }

  const liveMeetings = meetings
    .filter((meeting) => meetingBucket(meeting, nowMs) === "live")
    .sort((a, b) => (a.startDateUtc ?? "").localeCompare(b.startDateUtc ?? ""));
  const upcomingMeetings = meetings
    .filter((meeting) => meetingBucket(meeting, nowMs) === "upcoming")
    .sort((a, b) => (a.startDateUtc ?? "").localeCompare(b.startDateUtc ?? ""));
  const completedMeetings = meetings
    .filter((meeting) => meetingBucket(meeting, nowMs) === "completed")
    .sort((a, b) => (b.endDateUtc ?? "").localeCompare(a.endDateUtc ?? ""));

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Weekend schedule</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          Start here when you want to know which Grand Prix is live, what
          session finished last, and where to refresh the latest linked market
          data.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Live now"
          value={liveMeetings.length}
          hint={liveMeetings[0]?.meetingName ?? "No live weekend right now"}
        />
        <StatCard
          label="Coming up"
          value={upcomingMeetings.length}
          hint={upcomingMeetings[0]?.meetingName ?? "No future weekends loaded"}
        />
        <StatCard
          label="Completed"
          value={completedMeetings.length}
          hint="Archived weekends ready for review"
        />
        <StatCard
          label="Tracked sessions"
          value={sessions.length}
          hint="Practice, sprint, qualifying, and race"
        />
      </section>

      <Panel title="How to use this page" eyebrow="Operator guide">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Browse by weekend status
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Live, upcoming, and completed Grand Prix weekends are separated so
              you can scan the schedule without decoding raw session codes
              first.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Refresh the latest finished session
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Use the refresh button on a card after a session ends to pull the
              latest F1 and market data for that weekend.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Open the full Grand Prix detail
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              The detail page connects the schedule, mapped markets, and
              weekend-specific research context in one place.
            </p>
          </div>
        </div>
      </Panel>

      {meetings.length === 0 ? (
        <div className="rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-8 text-center">
          <p className="text-sm text-[#6b7280]">
            No Grand Prix weekends are loaded yet.{" "}
            <Link href="/" className="text-[#e10600] hover:underline">
              Run Sync F1 Calendar
            </Link>{" "}
            to populate the schedule.
          </p>
        </div>
      ) : (
        <>
          {renderMeetingSection(
            "Live now",
            "These weekends are currently in progress or within their scheduled event window.",
            liveMeetings,
            sessionsByMeetingId,
            now,
          )}
          {renderMeetingSection(
            "Coming up",
            "Use these cards to see what is next and prepare the weekend before sessions begin.",
            upcomingMeetings,
            sessionsByMeetingId,
            now,
          )}
          {renderMeetingSection(
            "Recently completed",
            "Completed weekends are the best place to inspect settled sessions and rerun research workflows.",
            completedMeetings,
            sessionsByMeetingId,
            now,
          )}
        </>
      )}
    </div>
  );
}
