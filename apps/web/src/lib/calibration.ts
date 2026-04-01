import type { ModelRun } from "@f1/shared-types";

export type CalibrationPoint = {
  actual: number;
  bucketLabel: string;
  count: number;
  predicted: number;
};

type RawBucket = Record<string, unknown>;

function metricNumber(bucket: RawBucket, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = bucket[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function parseBucketLower(bucketLabel: string): number {
  const match = /^(\d+)-(\d+)%$/.exec(bucketLabel);
  return match ? Number(match[1]) / 100 : Number.POSITIVE_INFINITY;
}

export function calibrationSummaryFromModelRuns(modelRuns: ModelRun[]): {
  points: CalibrationPoint[];
  runCount: number;
  sampleCount: number;
} {
  const aggregated = new Map<
    string,
    {
      actualWeighted: number;
      count: number;
      predictedWeighted: number;
    }
  >();
  let runCount = 0;

  for (const run of modelRuns) {
    const metrics = run.metricsJson as Record<string, unknown> | null;
    const rawBuckets = metrics?.calibration_buckets;
    if (!rawBuckets || typeof rawBuckets !== "object") {
      continue;
    }

    let contributed = false;
    for (const [bucketLabel, rawValue] of Object.entries(rawBuckets)) {
      if (!rawValue || typeof rawValue !== "object") {
        continue;
      }
      const bucket = rawValue as RawBucket;
      const count = metricNumber(bucket, "count");
      const predicted = metricNumber(bucket, "avg_predicted", "avg_prob");
      const actual = metricNumber(bucket, "avg_actual", "actual_rate");
      if (
        count == null ||
        count <= 0 ||
        predicted == null ||
        actual == null
      ) {
        continue;
      }

      const existing = aggregated.get(bucketLabel) ?? {
        actualWeighted: 0,
        count: 0,
        predictedWeighted: 0,
      };
      existing.count += count;
      existing.predictedWeighted += predicted * count;
      existing.actualWeighted += actual * count;
      aggregated.set(bucketLabel, existing);
      contributed = true;
    }

    if (contributed) {
      runCount += 1;
    }
  }

  const points = [...aggregated.entries()]
    .map(([bucketLabel, value]) => ({
      actual: value.actualWeighted / value.count,
      bucketLabel,
      count: value.count,
      predicted: value.predictedWeighted / value.count,
    }))
    .sort((left, right) => {
      return parseBucketLower(left.bucketLabel) - parseBucketLower(right.bucketLabel);
    });

  return {
    points,
    runCount,
    sampleCount: points.reduce((sum, point) => sum + point.count, 0),
  };
}
