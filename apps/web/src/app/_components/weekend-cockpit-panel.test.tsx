// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type {
  CaptureLiveWeekendResponse,
  CurrentWeekendOperationsReadiness,
  LiveTradeSignalBoard,
  RefreshedSessionSummary,
  RunWeekendCockpitResponse,
  WeekendCockpitStatus,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { WeekendCockpitPanel } from "./weekend-cockpit-panel";

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh: vi.fn(),
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    captureLiveWeekend: vi.fn(),
    currentWeekendReadiness: vi.fn(),
    cancelLiveTradeTicket: vi.fn(),
    createLiveTradeTicket: vi.fn(),
    liveTradeSignalBoard: vi.fn(),
    liveTradeExecutions: vi.fn(),
    liveTradeTickets: vi.fn(),
    recordLiveTradeFill: vi.fn(),
    refreshLatestSession: vi.fn(),
    weekendCockpitStatus: vi.fn(),
    runWeekendCockpit: vi.fn(),
  },
}));

const latestPracticeRefresh: RefreshedSessionSummary = {
  id: "session:11246",
  sessionKey: 11246,
  sessionCode: "FP1",
  sessionName: "Practice 1",
  dateEndUtc: "2026-03-27T03:30:00Z",
};

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
    required_model_stage: null,
    live_bet_size: 10,
    live_min_edge: 0.05,
    live_max_daily_loss: 100,
    live_max_spread: 0.03,
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
      required_model_stage: null,
      live_bet_size: 10,
      live_min_edge: 0.05,
      live_max_daily_loss: 100,
      live_max_spread: 0.03,
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
      required_model_stage: "multitask_qr",
      live_bet_size: 10,
      live_min_edge: 0.05,
      live_max_daily_loss: 100,
      live_max_spread: 0.03,
    },
  ],
  calendarStatus: "scheduled",
  meetingSlug: "japanese-grand-prix",
  sourceConflict: false,
  overrideSourceUrl: null,
  calendarMeetings: [
    {
      season: 2026,
      meetingKey: 1281,
      meetingSlug: "japanese-grand-prix",
      opsSlug: "japan",
      meetingName: "Japanese Grand Prix",
      roundNumber: 3,
      eventFormat: "conventional",
      startDateUtc: "2026-03-27T02:30:00Z",
      endDateUtc: "2026-03-29T07:00:00Z",
      countryName: "Japan",
      location: "Suzuka",
      status: "scheduled",
      sourceConflict: false,
      sourceLabel: null,
      sourceUrl: null,
      note: null,
    },
    {
      season: 2026,
      meetingKey: 1284,
      meetingSlug: "miami-grand-prix",
      opsSlug: "miami",
      meetingName: "Miami Grand Prix",
      roundNumber: 4,
      eventFormat: "sprint",
      startDateUtc: "2026-05-01T16:30:00Z",
      endDateUtc: "2026-05-03T21:00:00Z",
      countryName: "United States",
      location: "Miami",
      status: "scheduled",
      sourceConflict: false,
      sourceLabel: null,
      sourceUrl: null,
      note: null,
    },
  ],
  cancelledMeetings: [
    {
      season: 2026,
      meetingKey: 1282,
      meetingSlug: "bahrain-grand-prix",
      opsSlug: "bahrain",
      meetingName: "Bahrain Grand Prix",
      roundNumber: 2,
      eventFormat: "conventional",
      startDateUtc: "2026-04-10T10:30:00Z",
      endDateUtc: "2026-04-12T16:00:00Z",
      countryName: "Bahrain",
      location: "Sakhir",
      status: "cancelled",
      sourceConflict: true,
      sourceLabel: "Formula 1 official",
      sourceUrl:
        "https://www.formula1.com/en/latest/article/bahrain-and-saudi-arabian-grands-prix-will-not-take-place-in-april.1hnqllVG85RSt8pbFc5Ivx/",
      note: "Cancelled after official schedule update.",
    },
  ],
  meeting: {
    id: "meeting:1281",
    meetingKey: 1281,
    season: 2026,
    roundNumber: 3,
    meetingName: "Japanese Grand Prix",
    meetingSlug: "japanese-grand-prix",
    eventFormat: "conventional",
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
      key: "settle_finished_stage",
      label: "Settle finished stage",
      status: "skipped",
      detail: "No open tickets are waiting on FP1 results.",
      sessionCode: "FP1",
      sessionKey: 11246,
      count: null,
      reasonCode: "nothing_to_settle",
      actionableAfterUtc: null,
      resourceLabel: "FP1 tickets",
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
  modelReady: true,
  requiredStage: null,
  activeModelRunId: null,
  modelBlockers: [],
  sessionStageStatuses: [],
  liveTicketSummary: {
    ticketCount: 0,
    openTicketCount: 0,
    filledTicketCount: 0,
    cancelledTicketCount: 0,
  },
  liveExecutionSummary: {
    executionCount: 0,
    filledExecutionCount: 0,
  },
  primaryActionTitle: "Update to latest",
  primaryActionDescription:
    "This latest update will discover FP2 markets first, then continue into paper trading.",
  primaryActionCta: "Update to latest",
  explanation:
    "This stage uses FP1 results to settle finished FP1 tickets, find FP2 markets, and when ready continue into paper trading.",
};

const baseReadiness: CurrentWeekendOperationsReadiness = {
  now: "2026-03-27T05:13:00Z",
  selectedGpShortCode: "japan_fp1_fp2",
  selectedConfig: baseStatus.selectedConfig,
  meeting: baseStatus.meeting,
  latestEndedSession: baseStatus.sourceSession,
  nextActiveSession: baseStatus.focusSession,
  openf1CredentialsConfigured: true,
  actions: [
    {
      key: "weekend_cockpit",
      label: "Weekend cockpit",
      status: "ready",
      message:
        "This latest update will load FP1 results first and prepare FP2 markets.",
      blockers: [],
      warnings: [],
      meetingKey: 1281,
      meetingName: "Japanese Grand Prix",
      gpShortCode: "japan_fp1_fp2",
      sessionCode: "FP2",
      sessionKey: 11247,
      actionableAfterUtc: null,
      openf1CredentialsConfigured: true,
      lastJobRun: null,
      lastReportPath: "/tmp/run-weekend-cockpit.json",
    },
    {
      key: "driver_affinity",
      label: "Driver affinity refresh",
      status: "degraded",
      message:
        "Driver affinity is stale and can be refreshed with current ended session data.",
      blockers: [],
      warnings: ["Missing hydration for FP2."],
      meetingKey: 1281,
      meetingName: "Japanese Grand Prix",
      gpShortCode: "japan_fp1_fp2",
      sessionCode: "FP2",
      sessionKey: 11247,
      actionableAfterUtc: null,
      openf1CredentialsConfigured: true,
      lastJobRun: null,
      lastReportPath: "/tmp/driver-affinity.json",
    },
    {
      key: "live_capture",
      label: "Live capture",
      status: "blocked",
      message:
        "Practice 2 live capture becomes available at 2026-03-27T06:00:00Z.",
      blockers: [
        "Practice 2 live capture becomes available at 2026-03-27T06:00:00Z.",
      ],
      warnings: [],
      meetingKey: 1281,
      meetingName: "Japanese Grand Prix",
      gpShortCode: "japan_fp1_fp2",
      sessionCode: "FP2",
      sessionKey: 11247,
      actionableAfterUtc: "2026-03-27T06:00:00Z",
      openf1CredentialsConfigured: true,
      lastJobRun: null,
      lastReportPath: "/tmp/capture-live.json",
    },
    {
      key: "live_operator_ticket",
      label: "Live operator tickets",
      status: "ready",
      message:
        "Ready for manual operator tickets. Conservative limits are enforced at size <= 10.0, edge >= 0.050, spread <= 0.030, daily loss <= 100.00.",
      blockers: [],
      warnings: [],
      meetingKey: 1281,
      meetingName: "Japanese Grand Prix",
      gpShortCode: "japan_fp1_fp2",
      sessionCode: "FP2",
      sessionKey: 11247,
      actionableAfterUtc: null,
      openf1CredentialsConfigured: true,
      lastJobRun: null,
      lastReportPath: null,
    },
  ],
  blockers: [
    "Practice 2 live capture becomes available at 2026-03-27T06:00:00Z.",
  ],
  warnings: ["Missing hydration for FP2."],
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

function buildLiveCaptureResponse(
  jobRunId: string,
  openf1Messages: number,
  polymarketMessages: number,
  recordsWritten: number,
  marketQuotes: CaptureLiveWeekendResponse["summary"]["marketQuotes"] = [],
): CaptureLiveWeekendResponse {
  return {
    action: "capture-live-weekend",
    status: "ok",
    message: "Captured 20s of live data for Practice 2 across 12 market(s).",
    jobRunId,
    sessionKey: 11247,
    captureSeconds: 20,
    openf1Messages,
    polymarketMessages,
    marketCount: 12,
    polymarketMarketIds: ["market-1", "market-2"],
    recordsWritten,
    summary: {
      openf1Topics: [{ key: "v1/laps", count: openf1Messages }],
      polymarketEventTypes: [{ key: "book", count: polymarketMessages }],
      observedMarketCount: 1,
      observedTokenCount: 1,
      marketQuotes,
    },
  };
}

function buildLiveSignalBoard(): LiveTradeSignalBoard {
  return {
    gpShortCode: "japan_fp1_fp2",
    requiredStage: null,
    activeModelRunId: "model-run-1",
    modelRunId: "model-run-1",
    snapshotId: "snapshot-1",
    blockers: [],
    rows: [
      {
        marketId: "market-1",
        tokenId: "token-1",
        question: "Will George Russell top Practice 2?",
        sessionCode: "FP2",
        promotionStage: null,
        modelRunId: "model-run-1",
        snapshotId: "snapshot-1",
        modelProb: 0.6,
        marketPrice: 0.33,
        edge: 0.27,
        spread: 0.02,
        signalAction: "buy_yes",
        sideLabel: "YES",
        recommendedSize: 10,
        maxSpread: 0.03,
        observedAtUtc: "2026-03-27T06:18:00Z",
        eventType: null,
      },
    ],
  };
}

describe("WeekendCockpitPanel", () => {
  beforeEach(() => {
    vi.mocked(sdk.liveTradeSignalBoard).mockResolvedValue(
      buildLiveSignalBoard(),
    );
    vi.mocked(sdk.currentWeekendReadiness).mockResolvedValue(baseReadiness);
    vi.mocked(sdk.liveTradeTickets).mockResolvedValue([]);
    vi.mocked(sdk.liveTradeExecutions).mockResolvedValue([]);
    vi.mocked(sdk.createLiveTradeTicket).mockResolvedValue({
      action: "create-live-trade-ticket",
      status: "ok",
      message: "Created YES live ticket.",
      ticketId: "live-ticket-1",
      gpShortCode: "japan_fp1_fp2",
      marketId: "market-1",
      modelRunId: "model-run-1",
      snapshotId: "snapshot-1",
      promotionStage: null,
      signalAction: "buy_yes",
      sideLabel: "YES",
      recommendedSize: 10,
      marketPrice: 0.41,
      modelProb: 0.62,
      edge: 0.21,
      observedSpread: 0.02,
      maxSpread: 0.04,
      observedAtUtc: "2026-03-27T06:10:00Z",
      expiresAt: "2026-03-27T06:30:00Z",
    });
    vi.mocked(sdk.recordLiveTradeFill).mockResolvedValue({
      action: "record-live-trade-fill",
      status: "ok",
      message: "Recorded filled execution.",
      ticketId: "live-ticket-1",
      executionId: "live-execution-1",
      executionStatus: "filled",
      ticketStatus: "filled",
    });
    vi.mocked(sdk.cancelLiveTradeTicket).mockResolvedValue({
      action: "cancel-live-trade-ticket",
      status: "ok",
      message: "Cancelled live ticket.",
      ticketId: "live-ticket-1",
      ticketStatus: "cancelled",
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    cleanup();
  });

  it("shows beginner-friendly copy and hides raw technical labels by default", () => {
    render(
      <WeekendCockpitPanel
        initialStatus={baseStatus}
        initialReadiness={baseReadiness}
        refreshTargetsByGpShortCode={{
          japan_fp1_fp2: {
            meetingId: "meeting:1281",
            latestEndedSession: latestPracticeRefresh,
          },
        }}
      />,
    );

    expect(
      screen.getAllByText("Use FP1 results to prepare FP2").length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText("Latest update").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Update to latest").length).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "Update to latest" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Refresh Practice 1" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Capture 20s live sample" }),
    ).toBeDisabled();
    expect(screen.getByText("Operations readiness")).toBeInTheDocument();
    expect(screen.getByText("Driver affinity refresh")).toBeInTheDocument();
    expect(screen.queryByText("japan_fp1_fp2")).not.toBeInTheDocument();
    expect(screen.queryByText("fp1_to_fp2")).not.toBeInTheDocument();
    expect(
      screen.queryByText("driver_fastest_lap_practice"),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("11246")).not.toBeInTheDocument();
  });

  it("reveals advanced identifiers only after expanding advanced details", () => {
    render(
      <WeekendCockpitPanel
        initialStatus={baseStatus}
        initialReadiness={baseReadiness}
      />,
    );

    fireEvent.click(screen.getByText("Show advanced details"));

    expect(screen.getByText("japan_fp1_fp2")).toBeInTheDocument();
    expect(screen.getByText("fp1_to_fp2")).toBeInTheDocument();
    expect(screen.getByText("driver_fastest_lap_practice")).toBeInTheDocument();
    expect(screen.getByText(/11246/)).toBeInTheDocument();
  });

  it("shows model blockers separately from data blockers", () => {
    const blockedStatus: WeekendCockpitStatus = {
      ...baseStatus,
      readyToRun: false,
      modelReady: false,
      requiredStage: "multitask_qr",
      activeModelRunId: null,
      modelBlockers: [
        "A promoted multitask_qr champion is required before paper trading can run.",
      ],
      steps: baseStatus.steps.map((step) =>
        step.key === "run_paper_trade"
          ? {
              ...step,
              status: "blocked",
              detail:
                "A promoted multitask_qr champion is required before paper trading can run.",
            }
          : step,
      ),
      blockers: [
        "A promoted multitask_qr champion is required before paper trading can run.",
      ],
    };

    render(<WeekendCockpitPanel initialStatus={blockedStatus} />);

    expect(screen.getByText("Model blockers")).toBeInTheDocument();
    expect(
      screen.getAllByText(
        "A promoted multitask_qr champion is required before paper trading can run.",
      ).length,
    ).toBeGreaterThan(0);
    expect(screen.queryByText("Current blockers")).not.toBeInTheDocument();
  });

  it("shows pending state immediately and completes the CTA run flow", async () => {
    const runRequest = deferred<RunWeekendCockpitResponse>();
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

    render(
      <WeekendCockpitPanel
        initialStatus={baseStatus}
        initialReadiness={baseReadiness}
      />,
    );

    const button = screen.getByRole("button", { name: "Update to latest" });
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
      details: {
        snapshotId: "snapshot-1",
        modelRunId: "model-run-1",
        baseline: "hybrid",
        ptSessionId: "pt-session-1",
        logPath: null,
        totalSignals: 4,
        tradesExecuted: 1,
        openPositions: 1,
        settledPositions: 0,
        winCount: 0,
        lossCount: 0,
        winRate: null,
        totalPnl: 0,
        dailyPnl: 0,
        settlement: {
          settledSessionIds: ["pt-prev"],
          settledGpSlugs: ["japan_fp3"],
          settledPositions: 1,
          manualPositionsSettled: 1,
          unresolvedPositions: 0,
          unresolvedSessionIds: [],
          winnerDriverId: "driver:12",
        },
      },
    });

    await waitFor(() => {
      expect(
        screen.getByText("Weekend cockpit complete Settled 1 prior ticket."),
      ).toBeInTheDocument();
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

    render(
      <WeekendCockpitPanel
        initialStatus={baseStatus}
        initialReadiness={baseReadiness}
      />,
    );

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

  it("captures a short live sample when the target session is live", async () => {
    const liveStatus: WeekendCockpitStatus = {
      ...baseStatus,
      now: "2026-03-27T06:20:00Z",
      focusStatus: "live",
      latestPaperSession: {
        id: "pt-session-1",
        gpSlug: "japan_fp1_fp2",
        snapshotId: "snapshot-1",
        modelRunId: "model-run-1",
        configJson: null,
        logPath: null,
        startedAt: "2026-03-27T06:15:00Z",
        finishedAt: null,
        status: "open",
        summaryJson: null,
      },
      steps: baseStatus.steps.map((step) =>
        step.key === "discover_target_markets"
          ? {
              ...step,
              status: "completed",
              detail: "12 Practice 2 markets are already linked.",
              count: 12,
            }
          : step,
      ),
    };
    const captureRequest = deferred<CaptureLiveWeekendResponse>();
    vi.mocked(sdk.captureLiveWeekend).mockReturnValue(captureRequest.promise);
    vi.mocked(sdk.weekendCockpitStatus).mockResolvedValue(liveStatus);

    render(
      <WeekendCockpitPanel
        initialStatus={liveStatus}
        initialReadiness={baseReadiness}
      />,
    );

    const button = screen.getByRole("button", {
      name: "Capture 20s live sample",
    });
    fireEvent.click(button);

    expect(sdk.captureLiveWeekend).toHaveBeenCalledWith({
      session_key: 11247,
      capture_seconds: 20,
      start_buffer_min: 0,
      stop_buffer_min: 0,
      message_limit: 250,
    });
    expect(button).toBeDisabled();

    await act(async () => {
      captureRequest.resolve(
        buildLiveCaptureResponse("job-live-1", 14, 9, 31, [
          {
            marketId: "market-1",
            tokenId: "token-1",
            outcome: "Yes",
            eventType: "best_bid_ask",
            observedAtUtc: "2026-03-27T06:20:00Z",
            price: 0.41,
            bestBid: 0.4,
            bestAsk: 0.42,
            midpoint: 0.41,
            spread: 0.02,
            size: 12,
            side: "buy",
          },
        ]),
      );
    });

    await waitFor(() => {
      expect(
        screen.getByText(
          "Captured 20s of live data for Practice 2 across 12 market(s). OpenF1 14 · Polymarket 9 · Records 31.",
        ),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("Latest telemetry")).toBeInTheDocument();
    expect(screen.getByText("v1/laps · 14")).toBeInTheDocument();
    expect(screen.getByText("book · 9")).toBeInTheDocument();
    expect(screen.getByText("Signal board")).toBeInTheDocument();
    expect(
      screen.getByText("Will George Russell top Practice 2?"),
    ).toBeInTheDocument();
    expect(screen.getAllByText("+19.0 pts").length).toBeGreaterThan(0);
    expect(
      screen.getByText("Live best_bid_ask sample 06:20:00 UTC"),
    ).toBeInTheDocument();
    expect(screen.getByText("Live delta +8.0¢")).toBeInTheDocument();
    expect(sdk.weekendCockpitStatus).toHaveBeenCalledWith("japan_fp1_fp2");
  });

  it("creates a live ticket from a live signal row", async () => {
    const liveStatus: WeekendCockpitStatus = {
      ...baseStatus,
      now: "2026-03-27T06:20:00Z",
      focusStatus: "live",
      latestPaperSession: {
        id: "pt-session-1",
        gpSlug: "japan_fp1_fp2",
        snapshotId: "snapshot-1",
        modelRunId: "model-run-1",
        configJson: null,
        logPath: null,
        startedAt: "2026-03-27T06:15:00Z",
        finishedAt: null,
        status: "open",
        summaryJson: null,
      },
      steps: baseStatus.steps.map((step) =>
        step.key === "discover_target_markets"
          ? {
              ...step,
              status: "completed",
              detail: "12 Practice 2 markets are already linked.",
              count: 12,
            }
          : step,
      ),
    };
    const captureRequest = deferred<CaptureLiveWeekendResponse>();
    vi.mocked(sdk.captureLiveWeekend).mockReturnValue(captureRequest.promise);
    vi.mocked(sdk.weekendCockpitStatus).mockResolvedValue(liveStatus);

    render(
      <WeekendCockpitPanel
        initialStatus={liveStatus}
        initialReadiness={baseReadiness}
      />,
    );

    fireEvent.click(
      screen.getByRole("button", {
        name: "Capture 20s live sample",
      }),
    );

    await act(async () => {
      captureRequest.resolve(
        buildLiveCaptureResponse("job-live-1", 14, 9, 31, [
          {
            marketId: "market-1",
            tokenId: "token-1",
            outcome: "Yes",
            eventType: "best_bid_ask",
            observedAtUtc: "2026-03-27T06:20:00Z",
            price: 0.41,
            bestBid: 0.4,
            bestAsk: 0.42,
            midpoint: 0.41,
            spread: 0.02,
            size: 12,
            side: "buy",
          },
        ]),
      );
    });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Create ticket" }),
      ).toBeEnabled();
    });

    fireEvent.change(screen.getByRole("spinbutton", { name: "Shares" }), {
      target: { value: "10" },
    });
    fireEvent.change(screen.getByRole("spinbutton", { name: "Min edge pts" }), {
      target: { value: "7" },
    });
    fireEvent.change(screen.getByRole("spinbutton", { name: "Max spread ¢" }), {
      target: { value: "3" },
    });

    fireEvent.click(screen.getByRole("button", { name: "Create ticket" }));

    await waitFor(() => {
      expect(sdk.createLiveTradeTicket).toHaveBeenCalledWith({
        gp_short_code: "japan_fp1_fp2",
        market_id: "market-1",
        observed_market_price: 0.41,
        observed_at_utc: "2026-03-27T06:20:00Z",
        observed_spread: 0.02,
        source_event_type: "best_bid_ask",
        min_edge: 0.07,
        max_spread: 0.03,
        bet_size: 10,
      });
    });
    await waitFor(() => {
      expect(screen.getByText("Created YES live ticket.")).toBeInTheDocument();
    });
  });

  it("blocks live ticket creation when the readiness gate is closed", async () => {
    const liveStatus: WeekendCockpitStatus = {
      ...baseStatus,
      now: "2026-03-27T06:20:00Z",
      focusStatus: "live",
      steps: baseStatus.steps.map((step) =>
        step.key === "discover_target_markets"
          ? {
              ...step,
              status: "completed",
              detail: "12 Practice 2 markets are already linked.",
              count: 12,
            }
          : step,
      ),
    };
    const blockedReadiness: CurrentWeekendOperationsReadiness = {
      ...baseReadiness,
      actions: baseReadiness.actions.map((action) =>
        action.key === "live_operator_ticket"
          ? {
              ...action,
              status: "blocked",
              message:
                "Live operator tickets require LIVE_TRADING_READINESS_CONFIRMED=true after jurisdiction, account, and Miami rehearsal checks are complete.",
              blockers: [
                "Live operator tickets require LIVE_TRADING_READINESS_CONFIRMED=true after jurisdiction, account, and Miami rehearsal checks are complete.",
              ],
            }
          : action,
      ),
      blockers: [
        ...baseReadiness.blockers,
        "Live operator tickets require LIVE_TRADING_READINESS_CONFIRMED=true after jurisdiction, account, and Miami rehearsal checks are complete.",
      ],
    };

    vi.mocked(sdk.captureLiveWeekend).mockResolvedValue(
      buildLiveCaptureResponse("job-live-1", 14, 9, 31, [
        {
          marketId: "market-1",
          tokenId: "token-1",
          outcome: "Yes",
          eventType: "best_bid_ask",
          observedAtUtc: "2026-03-27T06:20:00Z",
          price: 0.41,
          bestBid: 0.4,
          bestAsk: 0.42,
          midpoint: 0.41,
          spread: 0.02,
          size: 12,
          side: "buy",
        },
      ]),
    );
    vi.mocked(sdk.weekendCockpitStatus).mockResolvedValue(liveStatus);

    render(
      <WeekendCockpitPanel
        initialStatus={liveStatus}
        initialReadiness={blockedReadiness}
      />,
    );

    fireEvent.click(
      screen.getByRole("button", {
        name: "Capture 20s live sample",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getAllByText(/LIVE_TRADING_READINESS_CONFIRMED=true/).length,
      ).toBeGreaterThan(0);
    });

    expect(
      screen.getByRole("button", { name: "Create ticket" }),
    ).toBeDisabled();
  });

  it("repeats live capture until stopped", async () => {
    const liveStatus: WeekendCockpitStatus = {
      ...baseStatus,
      now: "2026-03-27T06:20:00Z",
      focusStatus: "live",
      steps: baseStatus.steps.map((step) =>
        step.key === "discover_target_markets"
          ? {
              ...step,
              status: "completed",
              detail: "12 Practice 2 markets are already linked.",
              count: 12,
            }
          : step,
      ),
    };
    const firstCapture = deferred<CaptureLiveWeekendResponse>();
    const secondCapture = deferred<CaptureLiveWeekendResponse>();

    vi.mocked(sdk.captureLiveWeekend)
      .mockReturnValueOnce(firstCapture.promise)
      .mockReturnValueOnce(secondCapture.promise);
    vi.mocked(sdk.weekendCockpitStatus).mockResolvedValue(liveStatus);

    render(
      <WeekendCockpitPanel
        initialStatus={liveStatus}
        initialReadiness={baseReadiness}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Start live watch" }));

    expect(
      screen.getByRole("button", { name: "Stop live watch" }),
    ).toBeEnabled();
    expect(sdk.captureLiveWeekend).toHaveBeenCalledTimes(1);

    await act(async () => {
      firstCapture.resolve(buildLiveCaptureResponse("job-live-1", 14, 9, 31));
    });

    await waitFor(() => {
      expect(sdk.captureLiveWeekend).toHaveBeenCalledTimes(2);
    });
    expect(screen.getByText("Running")).toBeInTheDocument();
    const samplesCard = screen.getByText("Samples").closest("div");
    expect(samplesCard).not.toBeNull();
    expect(
      within(samplesCard as HTMLElement).getByText("1"),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Stop live watch" }));

    await waitFor(() => {
      expect(
        screen.getByText("Stopping after the current live sample finishes."),
      ).toBeInTheDocument();
    });

    await act(async () => {
      secondCapture.resolve(buildLiveCaptureResponse("job-live-2", 10, 7, 24));
    });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "Start live watch" }),
      ).toBeInTheDocument();
    });
    expect(screen.getByText("Idle")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.getByText("1.00 msg/s")).toBeInTheDocument();
    expect(sdk.captureLiveWeekend).toHaveBeenCalledTimes(2);
  });
});
