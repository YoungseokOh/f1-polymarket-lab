import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { StatCard } from "./index";

describe("StatCard", () => {
  it("renders label and value", () => {
    render(
      <StatCard label="Sessions" value="12" hint="latest demo backfill" />,
    );

    expect(screen.getByText("Sessions")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
    expect(screen.getByText("latest demo backfill")).toBeInTheDocument();
  });
});
