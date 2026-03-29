import type {
  DriverAffinityReport,
  DriverAffinitySegment,
} from "@f1/shared-types";

export function getDriverAffinitySegments(
  report: DriverAffinityReport,
): DriverAffinitySegment[] {
  if (report.segments.length > 0) {
    return report.segments;
  }

  return [
    {
      key: report.defaultSegmentKey ?? "current_gp",
      title: "Current Grand Prix",
      description: "Current meeting ended sessions only.",
      sourceSessionCodesIncluded: report.sourceSessionCodesIncluded,
      sourceSeasonsIncluded: [report.season],
      entryCount: report.entryCount,
      entries: report.entries,
    },
  ];
}

export function getDefaultDriverAffinitySegment(
  report: DriverAffinityReport,
): DriverAffinitySegment {
  const segments = getDriverAffinitySegments(report);
  return (
    segments.find((segment) => segment.key === report.defaultSegmentKey) ??
    segments[0]
  );
}
