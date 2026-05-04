// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { Sidebar } from "./sidebar";

vi.mock("next/link", () => ({
  default: ({
    children,
    href,
    prefetch: _prefetch,
    ...props
  }: React.AnchorHTMLAttributes<HTMLAnchorElement> & {
    href: string;
    children: React.ReactNode;
    prefetch?: boolean;
  }) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

vi.mock("next/navigation", () => ({
  usePathname: () => "/paper-trading",
}));

describe("Sidebar", () => {
  it("shows Driver Affinity in the main navigation", () => {
    render(React.createElement(Sidebar));

    expect(
      screen.getByRole("link", { name: "Driver Affinity" }),
    ).toHaveAttribute("href", "/driver-affinity");
  });
});
