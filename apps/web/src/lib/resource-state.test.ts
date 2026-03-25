import { describe, expect, it } from "vitest";

import { collectResourceErrors, loadResource } from "./resource-state";

describe("resource-state", () => {
  it("returns data without degradation on success", async () => {
    const state = await loadResource(async () => ["ok"], [], "Session feed");

    expect(state).toEqual({
      data: ["ok"],
      degraded: false,
      error: null,
    });
  });

  it("captures a degraded fallback on failure", async () => {
    const state = await loadResource(
      async () => {
        throw new Error("boom");
      },
      [],
      "Market feed",
    );

    expect(state.degraded).toBe(true);
    expect(state.data).toEqual([]);
    expect(collectResourceErrors([state])).toEqual([
      "Market feed unavailable: boom",
    ]);
  });
});
