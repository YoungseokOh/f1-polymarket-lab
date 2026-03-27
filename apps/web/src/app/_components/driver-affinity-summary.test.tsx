// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type { DriverAffinityReport } from "@f1/shared-types";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { DriverAffinitySummary } from "./driver-affinity-summary";

const baseReport: DriverAffinityReport = {
  season: 2026,
  meetingKey: 1281,
  meeting: {
    id: "meeting:1281",
    meetingKey: 1281,
    season: 2026,
    roundNumber: 3,
    meetingName: "Japanese Grand Prix",
    circuitShortName: "Suzuka",
    countryName: "Japan",
    location: "Suzuka",
    startDateUtc: "2026-03-27T02:30:00Z",
    endDateUtc: "2026-03-29T07:00:00Z",
  },
  computedAtUtc: "2026-03-27T08:45:00Z",
  asOfUtc: "2026-03-27T08:45:00Z",
  lookbackStartSeason: 2024,
  sessionCodeWeights: { Q: 1.0, FP3: 0.8, FP2: 0.6, FP1: 0.4 },
  seasonWeights: { 2026: 1.0, 2025: 0.65, 2024: 0.4 },
  trackWeights: {
    s1_fraction: 0.35,
    s2_fraction: 0.44,
    s3_fraction: 0.21,
  },
  sourceSessionCodesIncluded: ["FP1", "FP2"],
  sourceMaxSessionEndUtc: "2026-03-27T07:00:00Z",
  latestEndedRelevantSessionCode: "FP2",
  latestEndedRelevantSessionEndUtc: "2026-03-27T07:00:00Z",
  entryCount: 2,
  isFresh: true,
  staleReason: null,
  entries: [
    {
      canonicalDriverKey: "lando norris",
      displayDriverId: "driver:1",
      displayName: "Lando NORRIS",
      displayBroadcastName: "L NORRIS",
      driverNumber: 1,
      teamId: "team:mclaren",
      teamName: "McLaren",
      countryCode: "GBR",
      headshotUrl: null,
      rank: 1,
      affinityScore: 1.23,
      s1Strength: 1.0,
      s2Strength: 1.2,
      s3Strength: 1.1,
      trackS1Fraction: 0.35,
      trackS2Fraction: 0.44,
      trackS3Fraction: 0.21,
      contributingSessionCount: 6,
      contributingSessionCodes: ["Q", "FP2"],
      latestContributingSessionCode: "FP2",
      latestContributingSessionEndUtc: "2026-03-27T07:00:00Z",
    },
    {
      canonicalDriverKey: "george russell",
      displayDriverId: "driver:63",
      displayName: "George RUSSELL",
      displayBroadcastName: "G RUSSELL",
      driverNumber: 63,
      teamId: "team:mercedes",
      teamName: "Mercedes",
      countryCode: "GBR",
      headshotUrl: null,
      rank: 2,
      affinityScore: 0.91,
      s1Strength: 0.8,
      s2Strength: 0.9,
      s3Strength: 1.0,
      trackS1Fraction: 0.35,
      trackS2Fraction: 0.44,
      trackS3Fraction: 0.21,
      contributingSessionCount: 5,
      contributingSessionCodes: ["FP2"],
      latestContributingSessionCode: "FP2",
      latestContributingSessionEndUtc: "2026-03-27T07:00:00Z",
    },
  ],
};

describe("DriverAffinitySummary", () => {
  it("renders freshness, coverage, and the top drivers", () => {
    render(<DriverAffinitySummary report={baseReport} refreshMessage={null} />);

    expect(screen.getByText("Driver affinity")).toBeInTheDocument();
    expect(screen.getByText("Fresh")).toBeInTheDocument();
    expect(screen.getByText("Japanese Grand Prix")).toBeInTheDocument();
    expect(screen.getByText(/Coverage:/)).toHaveTextContent(
      "Coverage: FP1, FP2",
    );
    expect(screen.getByText("1. Lando NORRIS")).toBeInTheDocument();
    expect(screen.getByText("2. George RUSSELL")).toBeInTheDocument();
    expect(screen.getByText(/Latest ended session:/)).toHaveTextContent("FP2");
    expect(
      screen.getByRole("link", { name: "Open full leaderboard" }),
    ).toHaveAttribute("href", "/driver-affinity");
  });

  it("shows a blocked refresh message without hiding the last report", () => {
    render(
      <DriverAffinitySummary
        report={{
          ...baseReport,
          isFresh: false,
          staleReason: "FP3 has not been hydrated yet.",
        }}
        refreshMessage="Driver affinity needs newer ended session data."
      />,
    );

    expect(screen.getByText("Stale")).toBeInTheDocument();
    expect(
      screen.getByText("FP3 has not been hydrated yet."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Driver affinity needs newer ended session data."),
    ).toBeInTheDocument();
  });
});
