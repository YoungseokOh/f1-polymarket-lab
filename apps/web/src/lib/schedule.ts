import type { F1Meeting } from "@f1/shared-types";

export function selectScheduleMeetings(meetings: F1Meeting[]): {
  season: number | null;
  meetings: F1Meeting[];
} {
  if (meetings.length === 0) {
    return { season: null, meetings: [] };
  }

  const season = Math.max(...meetings.map((meeting) => meeting.season));
  const seasonMeetings = meetings.filter((meeting) => meeting.season === season);
  const datedMeetings = seasonMeetings.filter(
    (meeting) => meeting.startDateUtc || meeting.endDateUtc,
  );

  return {
    season,
    meetings: datedMeetings.length > 0 ? datedMeetings : seasonMeetings,
  };
}
