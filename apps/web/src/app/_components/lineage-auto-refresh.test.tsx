// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { act, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { LineageAutoRefresh } from "./lineage-auto-refresh";

const refreshMock = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh: refreshMock,
  }),
}));

describe("LineageAutoRefresh", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("polls while active jobs are present", async () => {
    vi.useFakeTimers();

    render(<LineageAutoRefresh hasActiveJobs />);

    expect(
      screen.getByText("Active jobs detected · auto-refreshing every 5s"),
    ).toBeInTheDocument();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });

    expect(refreshMock).toHaveBeenCalledTimes(1);
  });

  it("stops polling once no active jobs remain", async () => {
    vi.useFakeTimers();

    const { rerender } = render(<LineageAutoRefresh hasActiveJobs />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });

    await act(async () => {
      rerender(<LineageAutoRefresh hasActiveJobs={false} />);
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });

    expect(refreshMock).toHaveBeenCalledTimes(1);
  });
});
