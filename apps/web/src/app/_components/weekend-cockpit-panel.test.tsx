// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type { WeekendCockpitStatus } from "@f1/shared-types";
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

import { WeekendCockpitPanel } from "./weekend-cockpit-panel";

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh: vi.fn(),
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    weekendCockpitStatus: vi.fn(),
    runWeekendCockpit: vi.fn(),
  },
}));

const baseStatus: WeekendCockpitStatus = {
  now: "2026-03-27T05:13:00Z",
  autoSelectedGpShortCode: "japan_fp1_fp2",
  selectedGpShortCode: "japan_fp1_fp2",
  selectedConfig: {
    name: "Japanese Grand Prix",
    short_code: "japan_fp1_fp2",
    meeting_key: 1281,
    season: 2026,
    target_session_code: "FP2",
    variant: "fp1_to_fp2",
    source_session_code: "FP1",
    market_taxonomy: "driver_fastest_lap_practice",
    stage_rank: 1,
    stage_label: "FP1 -> FP2",
    display_label: "Use FP1 results to prepare FP2",
    display_description:
      "Use FP1 results to find FP2 markets and prepare paper trading.",
  },
  availableConfigs: [
    {
      name: "Japanese Grand Prix",
      short_code: "japan_fp1_fp2",
      meeting_key: 1281,
      season: 2026,
      target_session_code: "FP2",
      variant: "fp1_to_fp2",
      source_session_code: "FP1",
      market_taxonomy: "driver_fastest_lap_practice",
      stage_rank: 1,
      stage_label: "FP1 -> FP2",
      display_label: "Use FP1 results to prepare FP2",
      display_description:
        "Use FP1 results to find FP2 markets and prepare paper trading.",
    },
    {
      name: "Japanese Grand Prix",
      short_code: "japan_fp1",
      meeting_key: 1281,
      season: 2026,
      target_session_code: "Q",
      variant: "fp1_to_q",
      source_session_code: "FP1",
      market_taxonomy: "driver_pole_position",
      stage_rank: 2,
      stage_label: "FP1 -> Q",
      display_label: "Use FP1 results to prepare Qualifying",
      display_description:
        "Use FP1 results to find Qualifying markets and prepare paper trading.",
    },
  ],
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
  focusSession: {
    id: "session:11247",
    sessionKey: 11247,
    meetingId: "meeting:1281",
    sessionName: "Practice 2",
    sessionCode: "FP2",
    sessionType: "Practice",
    dateStartUtc: "2026-03-27T06:00:00Z",
    dateEndUtc: "2026-03-27T07:00:00Z",
    isPractice: true,
  },
  focusStatus: "upcoming",
  timelineCompletedCodes: ["FP1"],
  timelineActiveCode: "FP2",
  sourceSession: {
    id: "session:11246",
    sessionKey: 11246,
    meetingId: "meeting:1281",
    sessionName: "Practice 1",
    sessionCode: "FP1",
    sessionType: "Practice",
    dateStartUtc: "2026-03-27T02:30:00Z",
    dateEndUtc: "2026-03-27T03:30:00Z",
    isPractice: true,
  },
  targetSession: {
    id: "session:11247",
    sessionKey: 11247,
    meetingId: "meeting:1281",
    sessionName: "Practice 2",
    sessionCode: "FP2",
    sessionType: "Practice",
    dateStartUtc: "2026-03-27T06:00:00Z",
    dateEndUtc: "2026-03-27T07:00:00Z",
    isPractice: true,
  },
  latestPaperSession: null,
  steps: [
    {
      key: "sync_calendar",
      label: "Load weekend schedule",
      status: "completed",
      detail:
        "Loaded the Grand Prix schedule and the sessions required for this stage.",
      sessionCode: null,
      sessionKey: null,
      count: null,
      reasonCode: "already_loaded",
      actionableAfterUtc: null,
      resourceLabel: "Weekend schedule",
    },
    {
      key: "hydrate_source_session",
      label: "Load FP1 results",
      status: "completed",
      detail: "FP1 results are already available.",
      sessionCode: "FP1",
      sessionKey: 11246,
      count: 22,
      reasonCode: "already_loaded",
      actionableAfterUtc: null,
      resourceLabel: "FP1 results",
    },
    {
      key: "discover_target_markets",
      label: "Find FP2 markets",
      status: "ready",
      detail: "FP2 markets have not been found yet. Discovery can run now.",
      sessionCode: "FP2",
      sessionKey: 11247,
      count: null,
      reasonCode: "ready_to_discover",
      actionableAfterUtc: null,
      resourceLabel: "FP2 markets",
    },
    {
      key: "run_paper_trade",
      label: "Run paper trading",
      status: "ready",
      detail: "All prerequisites are complete. You can run paper trading now.",
      sessionCode: null,
      sessionKey: null,
      count: null,
      reasonCode: "ready_to_run",
      actionableAfterUtc: null,
      resourceLabel: "Paper trading",
    },
  ],
  blockers: [],
  readyToRun: true,
  primaryActionTitle: "Load FP1 results",
  primaryActionDescription:
    "This will load FP1 results first, then prepare FP2 markets.",
  primaryActionCta: "Load FP1 results",
  explanation:
    "This stage uses FP1 results to find FP2 markets and, when ready, continue into paper trading.",
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("WeekendCockpitPanel", () => {
  afterEach(() => {
    vi.clearAllMocks();
    cleanup();
  });

  it("shows beginner-friendly copy and hides raw technical labels by default", () => {
    render(<WeekendCockpitPanel initialStatus={baseStatus} />);

    expect(
      screen.getAllByText("Use FP1 results to prepare FP2").length,
    ).toBeGreaterThan(0);
    expect(screen.getByText("Next action")).toBeInTheDocument();
    expect(screen.getAllByText("Load FP1 results").length).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "Load FP1 results" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("japan_fp1_fp2")).not.toBeInTheDocument();
    expect(screen.queryByText("fp1_to_fp2")).not.toBeInTheDocument();
    expect(
      screen.queryByText("driver_fastest_lap_practice"),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("11246")).not.toBeInTheDocument();
  });

  it("reveals advanced identifiers only after expanding advanced details", () => {
    render(<WeekendCockpitPanel initialStatus={baseStatus} />);

    fireEvent.click(screen.getByText("Show advanced details"));

    expect(screen.getByText("japan_fp1_fp2")).toBeInTheDocument();
    expect(screen.getByText("fp1_to_fp2")).toBeInTheDocument();
    expect(screen.getByText("driver_fastest_lap_practice")).toBeInTheDocument();
    expect(screen.getByText(/11246/)).toBeInTheDocument();
  });

  it("shows pending state immediately and completes the CTA run flow", async () => {
    const runRequest = deferred<{
      action: string;
      status: string;
      message: string;
      gpShortCode: string;
      snapshotId: string | null;
      modelRunId: string | null;
      ptSessionId: string | null;
      executedSteps: [];
      details: null;
    }>();
    const updatedStatus: WeekendCockpitStatus = {
      ...baseStatus,
      latestPaperSession: {
        id: "pt-session-1",
        gpSlug: "japan_fp1_fp2",
        snapshotId: "snapshot-1",
        modelRunId: "model-run-1",
        configJson: null,
        logPath: null,
        startedAt: "2026-03-27T05:20:00Z",
        finishedAt: null,
        status: "open",
        summaryJson: null,
      },
    };

    vi.mocked(sdk.runWeekendCockpit).mockReturnValue(runRequest.promise);
    vi.mocked(sdk.weekendCockpitStatus).mockResolvedValue(updatedStatus);

    render(<WeekendCockpitPanel initialStatus={baseStatus} />);

    const button = screen.getByRole("button", { name: "Load FP1 results" });
    fireEvent.click(button);

    expect(sdk.runWeekendCockpit).toHaveBeenCalledWith({
      gp_short_code: "japan_fp1_fp2",
    });
    expect(button).toBeDisabled();

    runRequest.resolve({
      action: "run-weekend-cockpit",
      status: "ok",
      message: "Weekend cockpit complete",
      gpShortCode: "japan_fp1_fp2",
      snapshotId: "snapshot-1",
      modelRunId: "model-run-1",
      ptSessionId: "pt-session-1",
      executedSteps: [],
      details: null,
    });

    await waitFor(() => {
      expect(screen.getByText("Weekend cockpit complete")).toBeInTheDocument();
    });
    expect(sdk.weekendCockpitStatus).toHaveBeenCalledWith("japan_fp1_fp2");
  });

  it("disables the stage selector while the next status request is loading", async () => {
    const statusRequest = deferred<WeekendCockpitStatus>();
    const nextStatus: WeekendCockpitStatus = {
      ...baseStatus,
      selectedGpShortCode: "japan_fp1",
      selectedConfig: {
        ...baseStatus.selectedConfig,
        short_code: "japan_fp1",
        target_session_code: "Q",
        variant: "fp1_to_q",
        market_taxonomy: "driver_pole_position",
        stage_rank: 2,
        stage_label: "FP1 -> Q",
        display_label: "Use FP1 results to prepare Qualifying",
        display_description:
          "Use FP1 results to find Qualifying markets and prepare paper trading.",
      },
      availableConfigs: baseStatus.availableConfigs,
    };

    vi.mocked(sdk.weekendCockpitStatus).mockReturnValue(statusRequest.promise);

    render(<WeekendCockpitPanel initialStatus={baseStatus} />);

    const select = screen.getByLabelText("Stage");
    fireEvent.change(select, { target: { value: "japan_fp1" } });

    expect(sdk.weekendCockpitStatus).toHaveBeenCalledWith("japan_fp1");
    expect(select).toBeDisabled();

    statusRequest.resolve(nextStatus);

    await waitFor(() => {
      expect(
        screen.getAllByText("Use FP1 results to prepare Qualifying").length,
      ).toBeGreaterThan(0);
    });
  });
});
