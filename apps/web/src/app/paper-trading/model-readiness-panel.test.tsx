// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import type { ModelRun, WeekendCockpitStatus } from "@f1/shared-types";
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
import { ModelReadinessPanel } from "./model-readiness-panel";

const refresh = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({
    refresh,
  }),
}));

vi.mock("@f1/ts-sdk", () => ({
  sdk: {
    buildMultitaskSnapshots: vi.fn(),
    promoteBestModelRun: vi.fn(),
    promoteModelRun: vi.fn(),
    refreshLatestSession: vi.fn(),
    runWeekendCockpit: vi.fn(),
    trainMultitaskModel: vi.fn(),
  },
}));

const status = {
  requiredStage: "multitask_qr",
  activeModelRunId: null,
  selectedConfig: {
    season: 2026,
    meeting_key: 1284,
    short_code: "miami_q_r",
    required_model_stage: "multitask_qr",
  },
  selectedGpShortCode: "miami_q_r",
  meeting: {
    id: "meeting:1284",
  },
} as WeekendCockpitStatus;

function modelRun(overrides: Partial<ModelRun>): ModelRun {
  return {
    id: "model-run-1",
    stage: "multitask_qr",
    modelFamily: "torch_multitask",
    modelName: "shared_encoder_multitask_v2",
    datasetVersion: "multitask_v1",
    featureSnapshotId: "snapshot-1",
    configJson: null,
    metricsJson: {
      total_pnl: 12,
      roi_pct: 0.14,
      bet_count: 42,
      ece: 0.04,
      family_pnl_share_max: 0.4,
    },
    artifactUri: "data/artifacts/model-runs/model-run-1",
    registryRunId: null,
    promotionStatus: "inactive",
    promotedAt: null,
    createdAt: "2026-05-01T00:00:00Z",
    ...overrides,
  };
}

describe("ModelReadinessPanel", () => {
  afterEach(() => {
    vi.clearAllMocks();
    refresh.mockClear();
    cleanup();
  });

  it("shows eligible model candidates and promotes the best candidate", async () => {
    vi.mocked(sdk.promoteBestModelRun).mockResolvedValue({
      action: "promote-best-model-run",
      status: "ok",
      message: "Promoted best model run for stage multitask_qr.",
      stage: "multitask_qr",
      promotionId: "promotion-1",
      modelRunId: "model-run-1",
      candidateCount: 1,
    });

    render(<ModelReadinessPanel status={status} modelRuns={[modelRun({})]} />);

    expect(screen.getByText("Model readiness")).toBeInTheDocument();
    expect(screen.getByText("Candidate ready")).toBeInTheDocument();
    expect(screen.getByText("1 eligible / 1 total")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Promote best" }));

    await waitFor(() => {
      expect(sdk.promoteBestModelRun).toHaveBeenCalledWith({
        stage: "multitask_qr",
      });
    });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(
      await screen.findByText(
        "Promoted best model run for stage multitask_qr.",
      ),
    ).toBeInTheDocument();
  });

  it("explains when no eligible candidates exist", () => {
    render(
      <ModelReadinessPanel
        status={status}
        modelRuns={[
          modelRun({
            id: "model-run-bad",
            metricsJson: {
              total_pnl: -2,
              roi_pct: -0.05,
              bet_count: 8,
              ece: 0.12,
              family_pnl_share_max: 0.8,
            },
          }),
        ]}
      />,
    );

    expect(screen.getByText("Needs model")).toBeInTheDocument();
    expect(screen.getByText("0 eligible / 1 total")).toBeInTheDocument();
    expect(screen.getByText(/PnL must be positive/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Promote best" })).toBeDisabled();
  });

  it("runs the model workflow from paper trading", async () => {
    vi.mocked(sdk.buildMultitaskSnapshots).mockResolvedValue({
      action: "build-multitask-snapshots",
      status: "completed",
      message: "Built 12 model snapshots for 3 GP(s).",
      stage: "multitask_qr",
      season: 2026,
      throughMeetingKey: 1284,
      meetingKeys: [1281, 1282, 1284],
      completedMeetings: [],
      snapshotIds: [],
      snapshotCount: 12,
      rowCount: 84,
      manifestPath: "/tmp/manifest.json",
      jobRunIds: [],
      warnings: [],
    });
    vi.mocked(sdk.trainMultitaskModel).mockResolvedValue({
      action: "train-multitask-model",
      status: "completed",
      message: "Created 1 model run(s) for multitask_qr.",
      stage: "multitask_qr",
      season: 2026,
      manifestPath: "/tmp/manifest.json",
      meetingKeys: [1281, 1282, 1284],
      splitCount: 1,
      modelRunIds: ["model-run-1"],
      modelRunCount: 1,
      runs: [],
      skipped: [],
    });

    render(<ModelReadinessPanel status={status} modelRuns={[]} />);

    vi.mocked(sdk.refreshLatestSession).mockResolvedValue({
      action: "refresh-latest-session",
      status: "ok",
      message: "Updated latest ended session.",
      meetingId: "meeting:1284",
      meetingName: "Miami Grand Prix",
      refreshedSession: {
        id: "session-q",
        sessionKey: 1,
        sessionCode: "Q",
        sessionName: "Qualifying",
        dateEndUtc: "2026-05-02T21:00:00Z",
      },
      f1RecordsWritten: 10,
      marketsDiscovered: 0,
      mappingsWritten: 0,
      marketsHydrated: 0,
      artifactsRefreshed: [],
    });

    fireEvent.click(screen.getByRole("button", { name: "Refresh local data" }));
    await waitFor(() => {
      expect(sdk.refreshLatestSession).toHaveBeenCalledWith({
        meeting_id: "meeting:1284",
        search_fallback: true,
        discover_max_pages: 5,
        hydrate_market_history: true,
        sync_calendar: false,
        hydrate_f1_session_data: false,
        include_extended_f1_data: false,
        include_heavy_f1_data: false,
        refresh_artifacts: false,
      });
    });
    expect(
      screen.getByText("Step 1: Refresh local GP data"),
    ).toBeInTheDocument();
    await waitFor(
      () => {
        expect(
          screen.getByText("20% complete. 1 of 5 steps done."),
        ).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    expect(
      screen.getByRole("progressbar", { name: "Model workflow progress" }),
    ).toHaveAttribute("aria-valuenow", "20");

    fireEvent.click(screen.getByRole("button", { name: "Build data" }));
    await waitFor(() => {
      expect(sdk.buildMultitaskSnapshots).toHaveBeenCalledWith({
        season: 2026,
        through_meeting_key: 1284,
        stage: "multitask_qr",
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "Train model" }));
    await waitFor(() => {
      expect(sdk.trainMultitaskModel).toHaveBeenCalledWith({
        season: 2026,
        stage: "multitask_qr",
        min_train_gps: 2,
      });
    });

    expect(
      screen.getByText(
        "No model runs yet. Build training data first, then train the model.",
      ),
    ).toBeInTheDocument();
  });

  it("shows blocked workflow progress without the raw HTTP status", async () => {
    vi.mocked(sdk.trainMultitaskModel).mockRejectedValue(
      new Error(
        "API request failed: 409 Training needs at least 3 GPs with non-empty snapshot rows.",
      ),
    );

    render(<ModelReadinessPanel status={status} modelRuns={[]} />);

    fireEvent.click(screen.getByRole("button", { name: "Train model" }));

    expect(
      await screen.findByText("Step 3: Train model runs"),
    ).toBeInTheDocument();
    await waitFor(
      () => {
        expect(
          screen.getByText(
            "Training needs at least 3 GPs with non-empty snapshot rows.",
          ),
        ).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    expect(screen.queryByText(/409 Training needs/)).not.toBeInTheDocument();
    expect(
      screen.getByText("40% complete. Blocked at step 3."),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("progressbar", { name: "Model workflow progress" }),
    ).toHaveAttribute("aria-valuenow", "40");
  });

  it("shows OpenF1 auth failures once with a readable message", async () => {
    vi.mocked(sdk.refreshLatestSession).mockRejectedValue(
      new Error(
        "API request failed: 401 Client error '401 Unauthorized' for url 'https://api.openf1.org/v1/sessions?year=2026' For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/401",
      ),
    );

    render(<ModelReadinessPanel status={status} modelRuns={[]} />);

    fireEvent.click(screen.getByRole("button", { name: "Refresh local data" }));

    expect(
      await screen.findByText(
        "OpenF1 rejected the request. Use local GP refresh here, or fix OpenF1 credentials before a full refresh.",
      ),
    ).toBeInTheDocument();
    expect(
      screen.getAllByText(
        "OpenF1 rejected the request. Use local GP refresh here, or fix OpenF1 credentials before a full refresh.",
      ),
    ).toHaveLength(1);
    expect(screen.queryByText(/api.openf1.org/)).not.toBeInTheDocument();
    expect(screen.queryByText(/developer.mozilla.org/)).not.toBeInTheDocument();
    expect(screen.queryByText(/401 Unauthorized/)).not.toBeInTheDocument();
  });
});
