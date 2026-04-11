// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type { IngestionJobRun } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DashboardActions } from "./dashboard-actions";

const refreshMock = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh: refreshMock,
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    ingestionJobs: vi.fn(),
    ingestDemo: vi.fn(),
    syncCalendar: vi.fn(),
    syncF1Markets: vi.fn(),
  },
}));

describe("DashboardActions", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    cleanup();
  });

  it("renders the latest demo ingest summary", () => {
    render(
      <DashboardActions
        latestDemoIngestJob={{
          id: "job-demo-1",
          jobName: "ingest-demo",
          source: "demo",
          dataset: "demo_ingest",
          status: "completed",
          executeMode: "execute",
          recordsWritten: 321,
          startedAt: "2026-04-11T10:00:00Z",
          finishedAt: "2026-04-11T10:02:00Z",
        }}
      />,
    );

    expect(screen.getByText("Latest Demo Ingest")).toBeInTheDocument();
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("321 records written")).toBeInTheDocument();
  });

  it("polls a running demo ingest until it completes", async () => {
    vi.useFakeTimers();
    vi.mocked(sdk.ingestionJobs).mockResolvedValue([
      {
        id: "job-demo-2",
        jobName: "ingest-demo",
        source: "demo",
        dataset: "demo_ingest",
        status: "completed",
        executeMode: "execute",
        recordsWritten: 456,
        startedAt: "2026-04-11T10:05:00Z",
        finishedAt: "2026-04-11T10:06:00Z",
      } satisfies IngestionJobRun,
    ]);

    render(
      <DashboardActions
        latestDemoIngestJob={{
          id: "job-demo-2",
          jobName: "ingest-demo",
          source: "demo",
          dataset: "demo_ingest",
          status: "running",
          executeMode: "execute",
          recordsWritten: null,
          startedAt: "2026-04-11T10:05:00Z",
          finishedAt: null,
        }}
      />,
    );

    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(
      screen.getByText("Refreshing automatically while this ingest is running."),
    ).toBeInTheDocument();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });

    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("456 records written")).toBeInTheDocument();
    expect(sdk.ingestionJobs).toHaveBeenCalledWith({ limit: 25 });
    expect(refreshMock).toHaveBeenCalled();
  });

  it("updates the status card immediately after starting a demo ingest", async () => {
    vi.mocked(sdk.ingestDemo).mockResolvedValue({
      action: "ingest-demo",
      status: "ok",
      message: "Demo ingestion started.",
      details: { job_run_id: "job-demo-3" },
    });
    vi.mocked(sdk.ingestionJobs).mockResolvedValue([
      {
        id: "job-demo-3",
        jobName: "ingest-demo",
        source: "demo",
        dataset: "demo_ingest",
        status: "running",
        executeMode: "execute",
        recordsWritten: null,
        startedAt: "2026-04-11T10:10:00Z",
        finishedAt: null,
      } satisfies IngestionJobRun,
    ]);
    vi.mocked(sdk.syncCalendar).mockResolvedValue({
      action: "sync-calendar",
      status: "ok",
      message: "Calendar sync complete.",
      details: null,
    });
    vi.mocked(sdk.syncF1Markets).mockResolvedValue({
      action: "sync-f1-markets",
      status: "ok",
      message: "Market sync complete.",
      details: null,
    });

    render(<DashboardActions latestDemoIngestJob={null} />);

    fireEvent.click(screen.getByRole("button", { name: "Ingest Demo Data" }));

    await waitFor(() => {
      expect(screen.getByText("Running")).toBeInTheDocument();
    });
    expect(sdk.ingestDemo).toHaveBeenCalled();
    expect(sdk.ingestionJobs).toHaveBeenCalledWith({ limit: 25 });
    expect(refreshMock).toHaveBeenCalled();
  });
});
