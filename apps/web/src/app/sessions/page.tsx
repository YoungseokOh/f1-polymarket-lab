import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, StatCard } from "@f1/ui";

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

export default async function SessionsPage() {
  const [sessions, meetings] = await Promise.all([
    sdk.sessions().catch(() => []),
    sdk.meetings().catch(() => []),
  ]);

  const now = new Date();

  // Upcoming first (ascending), then past (descending)
  const sortedMeetings = [...meetings].sort((a, b) => {
    const da = a.startDateUtc ? new Date(a.startDateUtc).getTime() : 0;
    const db = b.startDateUtc ? new Date(b.startDateUtc).getTime() : 0;
    const aFuture = da >= now.getTime();
    const bFuture = db >= now.getTime();
    if (aFuture && !bFuture) return -1;
    if (!aFuture && bFuture) return 1;
    if (aFuture && bFuture) return da - db;
    return db - da;
  });

  const practiceSessions = sessions.filter((s) => s.isPractice);
  const uniqueMeetings = new Set(
    sessions.map((s) => s.meetingId).filter(Boolean),
  );

  return (
    <div className="flex flex-col gap-6 p-6">
      <div>
        <h1 className="text-xl font-bold text-white">Sessions</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          F1 weekend sessions by Grand Prix — current and past seasons
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Total Sessions"
          value={sessions.length}
          hint="all session types"
        />
        <StatCard
          label="Practice"
          value={practiceSessions.length}
          hint="FP1 / FP2 / FP3"
        />
        <StatCard
          label="GP Weekends"
          value={uniqueMeetings.size}
          hint="distinct meetings"
        />
        <StatCard
          label="Seasons"
          value={[...new Set(meetings.map((m) => m.season))].length}
          hint="loaded seasons"
        />
      </section>

      {sortedMeetings.length === 0 ? (
        <div className="rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-8 text-center">
          <p className="text-sm text-[#6b7280]">
            No meetings loaded.{" "}
            <Link href="/" className="text-[#e10600] hover:underline">
              Run Sync F1 Calendar
            </Link>{" "}
            to populate data.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          {sortedMeetings.map((m) => {
            const mSessions = sessions
              .filter((s) => s.meetingId === m.id)
              .sort((a, b) => {
                const oa = SESSION_ORDER[a.sessionCode ?? ""] ?? 99;
                const ob = SESSION_ORDER[b.sessionCode ?? ""] ?? 99;
                if (oa !== ob) return oa - ob;
                return (a.dateStartUtc ?? "").localeCompare(
                  b.dateStartUtc ?? "",
                );
              });

            const startMs = m.startDateUtc
              ? new Date(m.startDateUtc).getTime()
              : 0;
            const endMs = m.endDateUtc
              ? new Date(m.endDateUtc).getTime()
              : startMs;
            const isLive = startMs <= now.getTime() && endMs >= now.getTime();
            const isUpcoming = startMs > now.getTime();

            return (
              <section
                key={m.id}
                className="relative overflow-hidden rounded-xl border border-white/[0.06] bg-gradient-to-br from-[#1e1e2e] to-[#1a1a28] p-5 shadow-xl shadow-black/30"
              >
                {/* left accent */}
                <div className="absolute left-0 top-0 h-full w-[3px] bg-[#e10600]" />
                <div className="absolute left-0 top-0 h-full w-[6px] bg-[#e10600]/20 blur-sm" />

                {/* Header row */}
                <div className="mb-4 flex items-start justify-between gap-4 pl-2">
                  <div className="min-w-0">
                    <p className="text-[10px] font-bold uppercase tracking-[0.35em] text-[#e10600]">
                      {m.season} · Round {m.roundNumber ?? "?"}
                    </p>
                    <h2 className="mt-0.5 truncate text-base font-semibold text-white">
                      {m.meetingName}
                    </h2>
                    <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1">
                      <span className="text-xs text-[#6b7280]">
                        {m.circuitShortName ?? m.location ?? "—"}
                        {m.countryName ? `, ${m.countryName}` : ""}
                      </span>
                      {m.startDateUtc && (
                        <span className="tabular-nums text-xs text-[#9ca3af]">
                          {new Date(m.startDateUtc).toLocaleDateString(
                            "en-US",
                            { month: "short", day: "numeric" },
                          )}
                          {m.endDateUtc &&
                            ` – ${new Date(m.endDateUtc).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}`}
                        </span>
                      )}
                      {isLive && <Badge tone="live">LIVE</Badge>}
                      {isUpcoming && !isLive && (
                        <Badge tone="warn">Upcoming</Badge>
                      )}
                    </div>
                  </div>
                  <Link
                    href={`/gp/${m.id}`}
                    className="shrink-0 rounded-md border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-xs text-[#9ca3af] transition-colors hover:border-[#e10600]/30 hover:text-white"
                  >
                    View detail →
                  </Link>
                </div>

                {/* Sessions tiles */}
                {mSessions.length === 0 ? (
                  <p className="pl-2 text-xs text-[#6b7280]">
                    No sessions loaded for this meeting.
                  </p>
                ) : (
                  <div className="grid grid-cols-2 gap-2 pl-2 sm:grid-cols-3 lg:grid-cols-5 xl:grid-cols-6">
                    {mSessions.map((s) => {
                      const ended = s.dateEndUtc
                        ? new Date(s.dateEndUtc).getTime() < now.getTime()
                        : false;
                      const sessionLive =
                        s.dateStartUtc && s.dateEndUtc
                          ? new Date(s.dateStartUtc).getTime() <=
                              now.getTime() &&
                            new Date(s.dateEndUtc).getTime() >= now.getTime()
                          : false;

                      return (
                        <div
                          key={s.id}
                          className="rounded-lg border border-white/[0.05] bg-white/[0.02] px-3 py-2.5"
                        >
                          <div className="flex items-center justify-between gap-1">
                            <span className="font-mono text-xs font-bold text-[#e10600]">
                              {s.sessionCode ?? "?"}
                            </span>
                            {sessionLive ? (
                              <Badge tone="live">LIVE</Badge>
                            ) : ended ? (
                              <Badge tone="good">Done</Badge>
                            ) : (
                              <Badge tone="default">Sched</Badge>
                            )}
                          </div>
                          <p className="mt-1 text-[11px] text-[#d1d5db]">
                            {s.sessionName}
                          </p>
                          {s.dateStartUtc && (
                            <p className="mt-0.5 tabular-nums text-[10px] text-[#6b7280]">
                              {new Date(s.dateStartUtc).toLocaleDateString(
                                "en-US",
                                { month: "short", day: "numeric" },
                              )}{" "}
                              {new Date(s.dateStartUtc).toLocaleTimeString(
                                "en-US",
                                {
                                  hour: "2-digit",
                                  minute: "2-digit",
                                  hour12: false,
                                },
                              )}
                            </p>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>
            );
          })}
        </div>
      )}
    </div>
  );
}
