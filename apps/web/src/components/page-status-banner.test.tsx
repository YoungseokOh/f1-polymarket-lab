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
    expect(
      screen.getByText("Some data is not fully available yet."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Session feed unavailable: boom"),
    ).toBeInTheDocument();
  });

  it("renders duplicate degraded messages without collapsing them", () => {
    render(
      <PageStatusBanner
        messages={[
          "Recent ingestion run failed for refresh-driver-affinity.",
          "Recent ingestion run failed for refresh-driver-affinity.",
        ]}
      />,
    );

    expect(
      screen.getAllByText(
        "Recent ingestion run failed for refresh-driver-affinity.",
      ),
    ).toHaveLength(2);
  });
});
