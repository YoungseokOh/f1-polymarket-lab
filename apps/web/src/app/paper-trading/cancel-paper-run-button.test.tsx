// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { sdk } from "@f1/ts-sdk";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CancelPaperRunButton } from "./cancel-paper-run-button";

const refresh = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh,
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    cancelPaperTradeSession: vi.fn(),
  },
}));

describe("CancelPaperRunButton", () => {
  beforeEach(() => {
    vi.mocked(sdk.cancelPaperTradeSession).mockResolvedValue({
      id: "pt-1",
      gpSlug: "miami_fp1_sq",
      snapshotId: null,
      modelRunId: null,
      status: "cancelled",
      configJson: null,
      summaryJson: null,
      logPath: null,
      startedAt: "2026-05-01T19:00:00Z",
      finishedAt: "2026-05-01T19:01:00Z",
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    refresh.mockReset();
  });

  it("cancels a paper run after confirmation and refreshes the page", async () => {
    render(<CancelPaperRunButton sessionId="pt-1" />);

    fireEvent.click(screen.getByRole("button", { name: "Cancel run" }));

    expect(window.confirm).toHaveBeenCalled();
    await waitFor(() => {
      expect(sdk.cancelPaperTradeSession).toHaveBeenCalledWith("pt-1");
    });
    expect(refresh).toHaveBeenCalled();
  });

  it("does not cancel when the confirmation is rejected", () => {
    vi.mocked(window.confirm).mockReturnValue(false);
    render(<CancelPaperRunButton sessionId="pt-1" />);

    fireEvent.click(screen.getByRole("button", { name: "Cancel run" }));

    expect(sdk.cancelPaperTradeSession).not.toHaveBeenCalled();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("cancels multiple paper runs with one click", async () => {
    render(
      <CancelPaperRunButton
        sessionIds={["pt-1", "pt-2"]}
        label="Cancel runs"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Cancel runs" }));

    expect(window.confirm).toHaveBeenCalledWith(
      "Cancel 2 paper runs? Their simulated open tickets will be removed from current results.",
    );
    await waitFor(() => {
      expect(sdk.cancelPaperTradeSession).toHaveBeenCalledWith("pt-1");
      expect(sdk.cancelPaperTradeSession).toHaveBeenCalledWith("pt-2");
    });
    expect(refresh).toHaveBeenCalled();
  });
});
