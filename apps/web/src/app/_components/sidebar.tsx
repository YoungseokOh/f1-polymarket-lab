"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import React from "react";
import { useState } from "react";

const navItems = [
  { href: "/", label: "Dashboard", icon: DashboardIcon },
  { href: "/sessions", label: "Sessions", icon: SessionIcon },
  { href: "/markets", label: "Markets", icon: MarketIcon },
  { href: "/predictions", label: "Predictions", icon: PredictionIcon },
  { href: "/backtest", label: "Backtest", icon: BacktestIcon },
  { href: "/paper-trading", label: "Paper Trading", icon: PaperTradeIcon },
  {
    href: "/driver-affinity",
    label: "Driver Affinity",
    icon: DriverAffinityIcon,
  },
  { href: "/lineage", label: "Lineage", icon: LineageIcon },
];

export function Sidebar() {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <>
      {/* Mobile top bar */}
      <div className="fixed left-0 top-0 z-50 flex h-14 w-full items-center justify-between border-b border-white/[0.06] bg-f1-dark/95 px-4 backdrop-blur-xl lg:hidden">
        <div className="flex items-center gap-2.5">
          <div className="flex h-7 w-7 items-center justify-center rounded-md bg-f1-red">
            <span className="text-[11px] font-black text-white">F1</span>
          </div>
          <span className="text-sm font-bold tracking-wide text-white">
            POLYMARKET LAB
          </span>
        </div>
        <button
          type="button"
          onClick={() => setMobileOpen(!mobileOpen)}
          className="rounded-lg border border-white/10 bg-white/[0.04] p-2 transition-colors hover:bg-white/[0.08]"
          aria-label="Toggle menu"
        >
          <svg
            className="h-5 w-5 text-white"
            fill="none"
            aria-hidden="true"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            {mobileOpen ? (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            ) : (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 6h16M4 12h16M4 18h16"
              />
            )}
          </svg>
        </button>
      </div>

      {/* Overlay */}
      {mobileOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm lg:hidden"
          onClick={() => setMobileOpen(false)}
          onKeyDown={() => {}}
          role="presentation"
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed left-0 top-0 z-40 flex h-screen w-[220px] flex-col border-r border-white/[0.06] bg-f1-dark transition-transform lg:translate-x-0 ${mobileOpen ? "translate-x-0" : "-translate-x-full"}`}
      >
        {/* Logo */}
        <div className="flex h-16 items-center gap-2.5 border-b border-white/[0.06] px-5">
          <div className="flex h-7 w-7 items-center justify-center rounded-md bg-f1-red">
            <span className="text-[11px] font-black text-white">F1</span>
          </div>
          <span className="text-sm font-bold tracking-wide text-white">
            POLYMARKET LAB
          </span>
        </div>

        {/* Nav */}
        <nav className="flex-1 space-y-0.5 overflow-y-auto px-3 py-4">
          {navItems.map((item) => {
            const isActive =
              item.href === "/"
                ? pathname === "/"
                : pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                href={item.href}
                prefetch={false}
                onClick={() => setMobileOpen(false)}
                className={`flex items-center gap-3 rounded-lg px-3 py-2.5 text-[13px] font-medium transition-colors ${
                  isActive
                    ? "bg-f1-red/10 text-f1-red"
                    : "text-[#9ca3af] hover:bg-white/[0.04] hover:text-white"
                }`}
              >
                <item.icon active={isActive} />
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Footer */}
        <div className="border-t border-white/[0.06] px-5 py-4">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            Research Lab
          </p>
          <p className="mt-1 text-[11px] text-[#4b5563]">v0.1.0 — Phase 6</p>
        </div>
      </aside>
    </>
  );
}

function DashboardIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M4 5a1 1 0 011-1h4a1 1 0 011 1v5a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM14 5a1 1 0 011-1h4a1 1 0 011 1v2a1 1 0 01-1 1h-4a1 1 0 01-1-1V5zM4 16a1 1 0 011-1h4a1 1 0 011 1v3a1 1 0 01-1 1H5a1 1 0 01-1-1v-3zM14 13a1 1 0 011-1h4a1 1 0 011 1v6a1 1 0 01-1 1h-4a1 1 0 01-1-1v-6z"
      />
    </svg>
  );
}

function SessionIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
      />
    </svg>
  );
}

function MarketIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"
      />
    </svg>
  );
}

function PredictionIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"
      />
    </svg>
  );
}

function BacktestIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
      />
    </svg>
  );
}

function DriverAffinityIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"
      />
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M19.5 10.5c0 6.5-7.5 10.5-7.5 10.5S4.5 17 4.5 10.5a7.5 7.5 0 1115 0z"
      />
    </svg>
  );
}

function PaperTradeIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
      />
    </svg>
  );
}

function LineageIcon({ active }: { active: boolean }) {
  return (
    <svg
      className={`h-4 w-4 ${active ? "text-f1-red" : "text-current"}`}
      fill="none"
      aria-hidden="true"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.8}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"
      />
    </svg>
  );
}
