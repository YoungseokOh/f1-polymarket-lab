// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type { RefreshedSessionSummary } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MeetingRefreshButton } from "./meeting-refresh-button";

const refreshMock = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh: refreshMock,
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    refreshLatestSession: vi.fn(),
  },
}));

const latestSession: RefreshedSessionSummary = {
  id: "session:11249",
  sessionKey: 11249,
  sessionCode: "Q",
  sessionName: "Qualifying",
  dateEndUtc: "2026-03-28T07:00:00Z",
};

describe("MeetingRefreshButton", () => {
  afterEach(() => {
    vi.clearAllMocks();
    cleanup();
  });

  it("renders a disabled empty state when the GP has no ended session", () => {
    render(
      <MeetingRefreshButton
        meetingId="meeting:1281"
        latestEndedSession={null}
      />,
    );

    expect(
      screen.getByRole("button", { name: "No finished session yet" }),
    ).toBeDisabled();
    expect(
      screen.getByText("This Grand Prix does not have a finished session yet."),
    ).toBeInTheDocument();
  });

  it("posts the refresh action and refreshes the route on success", async () => {
    vi.mocked(sdk.refreshLatestSession).mockResolvedValue({
      action: "refresh-latest-session",
      status: "ok",
      message: "Updated latest ended session Q for Japanese Grand Prix.",
      meetingId: "meeting:1281",
      meetingName: "Japanese Grand Prix",
      refreshedSession: latestSession,
      f1RecordsWritten: 9,
      marketsDiscovered: 2,
      mappingsWritten: 1,
      marketsHydrated: 1,
      artifactsRefreshed: [],
    });

    render(
      <MeetingRefreshButton
        meetingId="meeting:1281"
        latestEndedSession={latestSession}
      />,
    );

    const button = screen.getByRole("button", {
      name: "Refresh Qualifying",
    });
    fireEvent.click(button);

    expect(sdk.refreshLatestSession).toHaveBeenCalledWith({
      meeting_id: "meeting:1281",
      search_fallback: false,
      discover_max_pages: 1,
      hydrate_market_history: false,
      sync_calendar: false,
      hydrate_f1_session_data: true,
      include_extended_f1_data: false,
      include_heavy_f1_data: false,
      refresh_artifacts: false,
    });
    expect(button).toBeDisabled();

    await waitFor(() => {
      expect(
        screen.getByText(
          "Updated latest ended session Q for Japanese Grand Prix.",
        ),
      ).toBeInTheDocument();
    });
    expect(refreshMock).toHaveBeenCalled();
  });
});
