import { describe, expect, it } from "vitest";

import type { ModelRun } from "@f1/shared-types";

import { calibrationSummaryFromModelRuns } from "./calibration";

describe("calibration", () => {
  it("aggregates calibration buckets across model runs", () => {
    const runs = [
      {
        id: "run-1",
        stage: "stage-a",
        modelFamily: "baseline",
        modelName: "hybrid",
        datasetVersion: null,
        featureSnapshotId: null,
        configJson: null,
        metricsJson: {
          calibration_buckets: {
            "0-10%": { count: 2, avg_predicted: 0.04, avg_actual: 0 },
            "40-50%": { count: 3, avg_predicted: 0.45, avg_actual: 1 / 3 },
          },
        },
        artifactUri: null,
        registryRunId: null,
        promotionStatus: "inactive",
        promotedAt: null,
        createdAt: "2026-03-28T00:00:00Z",
      },
      {
        id: "run-2",
        stage: "stage-b",
        modelFamily: "baseline",
        modelName: "market_implied",
        datasetVersion: null,
        featureSnapshotId: null,
        configJson: null,
        metricsJson: {
          calibration_buckets: {
            "40-50%": { count: 1, avg_predicted: 0.48, avg_actual: 1 },
            "90-100%": { count: 2, avg_predicted: 0.92, avg_actual: 1 },
          },
        },
        artifactUri: null,
        registryRunId: null,
        promotionStatus: "inactive",
        promotedAt: null,
        createdAt: "2026-03-28T01:00:00Z",
      },
    ] satisfies ModelRun[];

    const summary = calibrationSummaryFromModelRuns(runs);

    expect(summary.runCount).toBe(2);
    expect(summary.sampleCount).toBe(8);
    expect(summary.points).toEqual([
      expect.objectContaining({
        bucketLabel: "0-10%",
        count: 2,
        predicted: 0.04,
        actual: 0,
      }),
      expect.objectContaining({
        bucketLabel: "40-50%",
        count: 4,
        predicted: 0.4575,
        actual: 0.5,
      }),
      expect.objectContaining({
        bucketLabel: "90-100%",
        count: 2,
        predicted: 0.92,
        actual: 1,
      }),
    ]);
  });
});
