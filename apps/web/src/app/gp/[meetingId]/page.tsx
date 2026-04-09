import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";
import { decodeRouteParam } from "../../../lib/route-param";
import { SessionTimeline } from "../../_components/session-timeline";

export const revalidate = 300;

type Props = { params: Promise<{ meetingId: string }> };

export default async function GPDetailPage({ params }: Props) {
  const { meetingId } = await params;
  const normalizedMeetingId = decodeRouteParam(meetingId);

  const [meeting, gpSessions, markets, mappings] = await Promise.all([
    sdk.meeting(normalizedMeetingId).catch(() => null),
    sdk.meetingSessions(normalizedMeetingId).catch(() => []),
    sdk.markets().catch(() => []),
    sdk.mappings().catch(() => []),
  ]);

  if (!meeting) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center">
          <h1 className="text-xl font-bold text-white">Meeting Not Found</h1>
          <Link
            href="/"
            className="mt-2 block text-sm text-[#e10600] hover:underline"
          >
            ← Back to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  const now = new Date();
  const completedCodes = gpSessions
    .filter((s) => s.dateEndUtc && new Date(s.dateEndUtc) < now)
    .map((s) => s.sessionCode ?? "")
    .filter(Boolean);
  const activeSession = gpSessions.find(
    (s) =>
      s.dateStartUtc &&
      new Date(s.dateStartUtc) <= now &&
      (!s.dateEndUtc || new Date(s.dateEndUtc) > now),
  );

  // Find markets mapped to this meeting
  const meetingMappings = mappings.filter((m) => m.f1MeetingId === meeting.id);
  const mappedMarketIds = new Set(
    meetingMappings.map((m) => m.polymarketMarketId).filter(Boolean),
  );
  const linkedMarkets = markets.filter((m) => mappedMarketIds.has(m.id));

  const practiceSessions = gpSessions.filter((s) => s.isPractice);

  return (
    <div className="flex flex-col gap-6 p-6">
      {/* Breadcrumb */}
      <nav className="text-xs text-[#6b7280]">
        <Link href="/" className="text-[#e10600] hover:underline">
          Dashboard
        </Link>
        <span className="mx-2">/</span>
        <span className="text-[#d1d5db]">{meeting.meetingName}</span>
      </nav>

      {/* Meeting Header */}
      <section className="rounded-xl border border-white/[0.06] bg-gradient-to-r from-[#1e1e2e] to-[#15151e] p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-1">
            <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
              Round {meeting.roundNumber ?? "—"} · {meeting.season}
            </p>
            <h1 className="text-2xl font-bold text-white">
              {meeting.meetingName}
            </h1>
            <p className="text-sm text-[#9ca3af]">
              {meeting.circuitShortName && `${meeting.circuitShortName} · `}
              {meeting.location}, {meeting.countryName}
            </p>
            {meeting.startDateUtc && (
              <p className="text-xs tabular-nums text-[#6b7280]">
                {new Date(meeting.startDateUtc).toLocaleDateString("en-US", {
                  weekday: "short",
                  month: "short",
                  day: "numeric",
                  year: "numeric",
                })}
                {meeting.endDateUtc &&
                  ` — ${new Date(meeting.endDateUtc).toLocaleDateString(
                    "en-US",
                    {
                      weekday: "short",
                      month: "short",
                      day: "numeric",
                    },
                  )}`}
              </p>
            )}
          </div>
          <div className="flex gap-3">
            <Badge tone={activeSession ? "live" : "default"}>
              {activeSession
                ? `LIVE: ${activeSession.sessionName}`
                : "No Active Session"}
            </Badge>
          </div>
        </div>
        {gpSessions.length > 0 && (
          <div className="mt-6">
            <SessionTimeline
              completedCodes={completedCodes}
              activeCode={activeSession?.sessionCode ?? undefined}
            />
          </div>
        )}
      </section>

      {/* Stats */}
      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Sessions"
          value={gpSessions.length}
          hint="total weekend sessions"
        />
        <StatCard
          label="Practice"
          value={practiceSessions.length}
          hint="FP1 / FP2 / FP3"
        />
        <StatCard
          label="Linked Markets"
          value={linkedMarkets.length}
          hint="Polymarket mappings"
        />
        <StatCard
          label="Completed"
          value={completedCodes.length}
          hint={`of ${gpSessions.length} sessions`}
        />
      </section>

      {/* Sessions Grid */}
      <Panel title="Sessions" eyebrow="Weekend Schedule">
        {gpSessions.length === 0 ? (
          <p className="text-sm text-[#6b7280]">
            No sessions for this meeting.
          </p>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {gpSessions.map((s) => {
              const isComplete = s.dateEndUtc && new Date(s.dateEndUtc) < now;
              const isActive =
                s.dateStartUtc &&
                new Date(s.dateStartUtc) <= now &&
                (!s.dateEndUtc || new Date(s.dateEndUtc) > now);
              return (
                <div
                  key={s.id}
                  className={`rounded-lg border px-4 py-3 ${
                    isActive
                      ? "border-[#e10600]/30 bg-[#e10600]/5"
                      : "border-white/[0.04] bg-white/[0.02]"
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-semibold text-white">
                      {s.sessionName}
                    </span>
                    <Badge
                      tone={isActive ? "live" : isComplete ? "good" : "default"}
                    >
                      {isActive ? "LIVE" : isComplete ? "Done" : "Upcoming"}
                    </Badge>
                  </div>
                  <div className="mt-1 text-xs text-[#6b7280]">
                    {s.sessionCode && (
                      <span className="mr-2 font-mono">{s.sessionCode}</span>
                    )}
                    {s.dateStartUtc &&
                      new Date(s.dateStartUtc).toLocaleString("en-US", {
                        month: "short",
                        day: "numeric",
                        hour: "2-digit",
                        minute: "2-digit",
                        hour12: false,
                      })}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Panel>

      {/* Linked Markets */}
      <Panel title="Linked Markets" eyebrow={`${linkedMarkets.length} markets`}>
        {linkedMarkets.length === 0 ? (
          <p className="text-sm text-[#6b7280]">
            No Polymarket markets mapped to this GP yet.
          </p>
        ) : (
          <div className="space-y-2">
            {linkedMarkets.map((m) => (
              <Link
                key={m.id}
                href={`/markets/${m.id}`}
                className="flex items-center justify-between rounded-lg border border-white/[0.04] px-4 py-3 transition-colors hover:border-[#e10600]/20 hover:bg-white/[0.02]"
              >
                <div className="mr-4 min-w-0 flex-1">
                  <p className="line-clamp-1 text-sm text-[#d1d5db]">
                    {m.question}
                  </p>
                  <p className="mt-0.5 text-[10px] uppercase tracking-wider text-[#6b7280]">
                    {m.taxonomy.replace(/_/g, " ")}
                    {m.targetSessionCode && ` · ${m.targetSessionCode}`}
                  </p>
                </div>
                <div className="flex shrink-0 items-center gap-3">
                  {m.lastTradePrice != null && (
                    <span className="text-sm font-semibold tabular-nums text-white">
                      {(m.lastTradePrice * 100).toFixed(1)}¢
                    </span>
                  )}
                  <Badge tone={m.active ? "good" : "warn"}>
                    {m.active ? "Active" : "Closed"}
                  </Badge>
                </div>
              </Link>
            ))}
          </div>
        )}
      </Panel>
    </div>
  );
}
