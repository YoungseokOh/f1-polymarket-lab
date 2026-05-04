import type { PaperTradePosition, PaperTradeSession } from "@f1/shared-types";
import { describe, expect, it } from "vitest";

import { calculatePaperTradingStats } from "./stats";

function session(
  id: string,
  gpSlug: string,
  trades: number,
  pnl: number,
  status = "settled",
): PaperTradeSession {
  return {
    id,
    gpSlug,
    snapshotId: null,
    modelRunId: null,
    status,
    configJson: null,
    summaryJson: {
      trades_executed: trades,
      total_pnl: pnl,
    },
    logPath: null,
    startedAt: "2026-05-01T18:00:00Z",
    finishedAt: "2026-05-01T18:01:00Z",
  };
}

function position(
  id: string,
  sessionId: string,
  realizedPnl: number | null,
): PaperTradePosition {
  return {
    id,
    sessionId,
    marketId: `market-${id}`,
    tokenId: null,
    side: "buy_yes",
    quantity: 10,
    entryPrice: 0.4,
    entryTime: "2026-05-01T18:00:00Z",
    modelProb: 0.55,
    marketProb: 0.4,
    edge: 0.15,
    status: "settled",
    exitPrice: realizedPnl != null && realizedPnl > 0 ? 1 : 0,
    exitTime: "2026-05-01T19:00:00Z",
    realizedPnl,
  };
}

describe("calculatePaperTradingStats", () => {
  it("keeps current GP stats separate from all-time totals", () => {
    const sessions = [
      session("miami-1", "miami_fp1_sq", 2, 8),
      session("japan-1", "japan_fp1_fp2", 3, -4),
    ];
    const positionsBySessionId = new Map([
      [
        "miami-1",
        [
          position("miami-win", "miami-1", 6),
          position("miami-loss", "miami-1", -2),
        ],
      ],
      ["japan-1", [position("japan-loss", "japan-1", -4)]],
    ]);

    expect(
      calculatePaperTradingStats(
        sessions,
        positionsBySessionId,
        "miami_fp1_sq",
      ),
    ).toEqual({
      runs: 1,
      trades: 2,
      totalPnl: 8,
      settledPositions: 2,
      winRate: 0.5,
    });
    expect(calculatePaperTradingStats(sessions, positionsBySessionId)).toEqual({
      runs: 2,
      trades: 5,
      totalPnl: 4,
      settledPositions: 3,
      winRate: 1 / 3,
    });
  });

  it("does not mix prior GP runs into an empty current summary", () => {
    const sessions = [session("japan-1", "japan_fp1_fp2", 3, -4)];
    const positionsBySessionId = new Map([
      ["japan-1", [position("japan-loss", "japan-1", -4)]],
    ]);

    expect(
      calculatePaperTradingStats(
        sessions,
        positionsBySessionId,
        "miami_fp1_sq",
      ),
    ).toEqual({
      runs: 0,
      trades: 0,
      totalPnl: 0,
      settledPositions: 0,
      winRate: null,
    });
  });

  it("excludes cancelled runs from current and all-time stats", () => {
    const sessions = [
      session("miami-1", "miami_fp1_sq", 2, 8),
      session("miami-cancelled", "miami_fp1_sq", 5, 0, "cancelled"),
    ];
    const positionsBySessionId = new Map([
      ["miami-1", [position("miami-win", "miami-1", 8)]],
      [
        "miami-cancelled",
        [
          {
            ...position("miami-cancelled-ticket", "miami-cancelled", null),
            status: "cancelled",
          },
        ],
      ],
    ]);

    expect(
      calculatePaperTradingStats(
        sessions,
        positionsBySessionId,
        "miami_fp1_sq",
      ),
    ).toEqual({
      runs: 1,
      trades: 2,
      totalPnl: 8,
      settledPositions: 1,
      winRate: 1,
    });
    expect(calculatePaperTradingStats(sessions, positionsBySessionId)).toEqual({
      runs: 1,
      trades: 2,
      totalPnl: 8,
      settledPositions: 1,
      winRate: 1,
    });
  });
});
