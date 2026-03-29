import type {
  F1Meeting,
  F1Session,
  GPRegistryItem,
  RefreshedSessionSummary,
} from "@f1/shared-types";

export type MeetingRefreshTarget = {
  meetingId: string;
  latestEndedSession: RefreshedSessionSummary | null;
};

function toRefreshedSessionSummary(
  session: F1Session,
): RefreshedSessionSummary {
  return {
    id: session.id,
    sessionKey: session.sessionKey,
    sessionCode: session.sessionCode,
    sessionName: session.sessionName,
    dateEndUtc: session.dateEndUtc,
  };
}

export function latestEndedSessionForMeeting(
  sessions: F1Session[],
  now = new Date(),
): RefreshedSessionSummary | null {
  const latestSession =
    [...sessions]
      .filter(
        (session) =>
          session.dateEndUtc &&
          new Date(session.dateEndUtc).getTime() <= now.getTime(),
      )
      .sort((left, right) => {
        const leftEndMs = left.dateEndUtc
          ? new Date(left.dateEndUtc).getTime()
          : 0;
        const rightEndMs = right.dateEndUtc
          ? new Date(right.dateEndUtc).getTime()
          : 0;
        if (leftEndMs !== rightEndMs) return rightEndMs - leftEndMs;
        return right.sessionKey - left.sessionKey;
      })[0] ?? null;

  return latestSession ? toRefreshedSessionSummary(latestSession) : null;
}

export function meetingRefreshTargetForConfig(
  config: GPRegistryItem,
  meetings: F1Meeting[],
  sessions: F1Session[],
  now = new Date(),
): MeetingRefreshTarget | null {
  const meeting =
    meetings.find(
      (candidate) =>
        candidate.meetingKey === config.meeting_key &&
        candidate.season === config.season,
    ) ?? null;
  if (!meeting) return null;

  return {
    meetingId: meeting.id,
    latestEndedSession: latestEndedSessionForMeeting(
      sessions.filter((session) => session.meetingId === meeting.id),
      now,
    ),
  };
}
