import { describe, expect, it } from "vitest";

import { decodeRouteParam } from "./route-param";

describe("decodeRouteParam", () => {
  it("decodes an encoded dynamic segment once", () => {
    expect(decodeRouteParam("meeting%3A1279")).toBe("meeting:1279");
  });

  it("leaves an already-decoded segment unchanged", () => {
    expect(decodeRouteParam("meeting:1279")).toBe("meeting:1279");
  });

  it("falls back to the raw value when decoding fails", () => {
    expect(decodeRouteParam("meeting%ZZ1279")).toBe("meeting%ZZ1279");
  });
});
