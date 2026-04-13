import { describe, expect, it } from "vitest";

import type { F1Meeting } from "@f1/shared-types";

import { selectScheduleMeetings } from "./schedule";

function meeting(overrides: Partial<F1Meeting>): F1Meeting {
  return {
    id: "meeting:test",
    meetingKey: 1,
    season: 2026,
    roundNumber: null,
    meetingName: "Test Grand Prix",
    meetingSlug: null,
    eventFormat: null,
    circuitShortName: null,
    countryName: null,
    location: null,
    startDateUtc: null,
    endDateUtc: null,
    ...overrides,
  };
}

describe("selectScheduleMeetings", () => {
  it("returns an empty schedule when no meetings are loaded", () => {
    expect(selectScheduleMeetings([])).toEqual({
      season: null,
      meetings: [],
    });
  });

  it("keeps the latest season and drops undated placeholders when dated meetings exist", () => {
    const result = selectScheduleMeetings([
      meeting({
        id: "meeting:historical",
        meetingKey: 10,
        season: 2025,
        meetingName: "Historical Placeholder",
      }),
      meeting({
        id: "meeting:current-placeholder",
        meetingKey: 20,
        season: 2026,
        meetingName: "Current Placeholder",
      }),
      meeting({
        id: "meeting:1282",
        meetingKey: 1282,
        season: 2026,
        meetingName: "Bahrain Grand Prix",
        startDateUtc: "2026-04-10T10:30:00Z",
        endDateUtc: "2026-04-12T16:00:00Z",
      }),
    ]);

    expect(result.season).toBe(2026);
    expect(result.meetings.map((meeting) => meeting.id)).toEqual([
      "meeting:1282",
    ]);
  });
});
