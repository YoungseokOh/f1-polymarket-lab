import { describe, expect, it } from "vitest";

import {
  describePredictionSignal,
  describeQualityAlert,
  describeStage,
  formatSessionCodeLabel,
  formatTaxonomyLabel,
} from "./display";

describe("display helpers", () => {
  it("formats session and taxonomy labels for humans", () => {
    expect(formatSessionCodeLabel("FP3")).toBe("Practice 3");
    expect(formatTaxonomyLabel("race_winner")).toBe("Race winner");
    expect(formatTaxonomyLabel("q_pole")).toBe("Qualifying pole position");
  });

  it("parses model and snapshot stage names into readable labels", () => {
    expect(describeStage("japan_q_race_winner_quicktest")).toEqual({
      label: "Japanese Grand Prix · Qualifying to Race winner",
      context: "Forecast stage",
    });

    expect(describeStage("japan_fp3_to_q_pole_snapshot")).toEqual({
      label: "Japanese Grand Prix · Practice 3 to Qualifying pole position",
      context: "Feature snapshot",
    });

    expect(describeStage("pole_position_backtest")).toEqual({
      label: "Pole Position backtest",
      context: "Backtest stage",
    });
  });

  it("explains optional live-only quality failures", () => {
    expect(
      describeQualityAlert({
        id: "dq-1",
        dataset: "polymarket_ws_message_manifest",
        status: "fail",
        metricsJson: { row_count: 0 },
        observedAt: "2026-04-05T12:00:00Z",
      }),
    ).toContain("mainly affects live-only monitoring");
  });

  it("classifies probability signals into readable buckets", () => {
    expect(describePredictionSignal(0.81)).toEqual({
      label: "Strong YES",
      tone: "good",
    });
    expect(describePredictionSignal(0.39)).toEqual({
      label: "Lean NO",
      tone: "warn",
    });
    expect(describePredictionSignal(0.5)).toEqual({
      label: "Near even",
      tone: "default",
    });
  });
});
