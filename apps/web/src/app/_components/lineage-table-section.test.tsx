// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { LineageTableSection } from "./lineage-table-section";

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    push: vi.fn(),
  }),
}));

globalThis.React = React;

describe("LineageTableSection", () => {
  it("renders ingest-demo run details from lineage metadata", () => {
    render(
      <LineageTableSection
        freshness={[]}
        jobs={[
          {
            id: "job-demo-1",
            jobName: "ingest-demo",
            source: "demo",
            dataset: "demo_ingest",
            status: "completed",
            executeMode: "execute",
            plannedInputs: {
              season: 2026,
              weekends: 1,
              market_batches: 1,
            },
            cursorAfter: {
              f1_sessions: 120,
              polymarket_markets: 67,
              entity_mappings: 86,
              feature_registry: 54,
              records_written: 4174,
            },
            recordsWritten: 4174,
            errorMessage: null,
            queuedAt: "2026-04-11T13:01:00Z",
            availableAt: "2026-04-11T13:01:00Z",
            attemptCount: 1,
            maxAttempts: 3,
            lockedBy: null,
            lockedAt: null,
            startedAt: "2026-04-11T13:02:00Z",
            finishedAt: "2026-04-11T13:03:00Z",
          },
        ]}
        cursorStates={[]}
        qualityResults={[]}
        mappings={[]}
        mappedMarkets={0}
      />,
    );

    expect(screen.getByText("ingest-demo")).toBeInTheDocument();
    expect(
      screen.getByText(/Inputs season=2026 · weekends=1 · market_batches=1/),
    ).toBeInTheDocument();
    expect(screen.getByText("F1 sessions")).toBeInTheDocument();
    expect(screen.getByText("120")).toBeInTheDocument();
    expect(screen.getByText("Polymarket markets")).toBeInTheDocument();
    expect(screen.getByText("67")).toBeInTheDocument();
    expect(screen.getByText("Entity mappings")).toBeInTheDocument();
    expect(screen.getByText("86")).toBeInTheDocument();
    expect(screen.getByText("Feature registry")).toBeInTheDocument();
    expect(screen.getByText("54")).toBeInTheDocument();
  });

  it("shows a legacy fallback when older ingest-demo rows have no summary payload", () => {
    render(
      <LineageTableSection
        freshness={[]}
        jobs={[
          {
            id: "job-demo-legacy",
            jobName: "ingest-demo",
            source: "demo",
            dataset: "demo_ingest",
            status: "completed",
            executeMode: "execute",
            plannedInputs: {
              season: 2026,
            },
            cursorAfter: null,
            recordsWritten: 812,
            errorMessage: null,
            queuedAt: "2026-04-10T13:01:00Z",
            availableAt: "2026-04-10T13:01:00Z",
            attemptCount: 1,
            maxAttempts: 3,
            lockedBy: null,
            lockedAt: null,
            startedAt: "2026-04-10T13:02:00Z",
            finishedAt: "2026-04-10T13:03:00Z",
          },
        ]}
        cursorStates={[]}
        qualityResults={[]}
        mappings={[]}
        mappedMarkets={0}
      />,
    );

    expect(
      screen.getByText("Detailed summary not recorded for this run."),
    ).toBeInTheDocument();
  });
});
