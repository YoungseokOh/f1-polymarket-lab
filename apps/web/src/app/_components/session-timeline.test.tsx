import { render } from "@testing-library/react";
import React from "react";
import { expect, it } from "vitest";

import { SessionTimeline } from "./session-timeline";

it("renders the sprint timeline sequence in order", () => {
  const { container } = render(
    <SessionTimeline
      completedCodes={["FP1", "SQ"]}
      activeCode="S"
      sessionCodes={["FP1", "SQ", "S", "Q", "R"]}
    />,
  );

  const labels = Array.from(
    container.querySelectorAll(
      "div.flex.items-center.justify-center.rounded-md",
    ),
  ).map((el) => el.textContent?.trim() ?? "");

  expect(labels).toEqual(["FP1", "SQ", "S", "QUALI", "RACE"]);
});

it("renders the default conventional sequence", () => {
  const { container } = render(
    <SessionTimeline completedCodes={["FP1"]} activeCode="FP2" />,
  );

  const labels = Array.from(
    container.querySelectorAll(
      "div.flex.items-center.justify-center.rounded-md",
    ),
  ).map((el) => el.textContent?.trim() ?? "");

  expect(labels).toEqual(["FP1", "FP2", "FP3", "QUALI", "RACE"]);
});
