// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { PageStatusBanner } from "./page-status-banner";

describe("PageStatusBanner", () => {
  it("renders nothing when there are no messages", () => {
    const { container } = render(<PageStatusBanner messages={[]} />);

    expect(container).toBeEmptyDOMElement();
  });

  it("renders degraded status messages", () => {
    render(<PageStatusBanner messages={["Session feed unavailable: boom"]} />);

    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText("Some API data is degraded.")).toBeInTheDocument();
    expect(
      screen.getByText("Session feed unavailable: boom"),
    ).toBeInTheDocument();
  });
});
