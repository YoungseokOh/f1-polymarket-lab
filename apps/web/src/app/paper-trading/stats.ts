import type { PaperTradePosition, PaperTradeSession } from "@f1/shared-types";

export type PaperTradingStats = {
  runs: number;
  trades: number;
  totalPnl: number;
  settledPositions: number;
  winRate: number | null;
};

function numericSummaryValue(
  summary: Record<string, unknown> | null,
  key: string,
) {
  const value = summary?.[key];
  return typeof value === "number" ? value : 0;
}

export function calculatePaperTradingStats(
  sessions: PaperTradeSession[],
  positionsBySessionId: Map<string, PaperTradePosition[]>,
  gpSlug?: string | null,
): PaperTradingStats {
  const selectedSessions = gpSlug
    ? sessions.filter(
        (session) =>
          session.gpSlug === gpSlug && session.status !== "cancelled",
      )
    : sessions.filter((session) => session.status !== "cancelled");
  const selectedSessionIds = new Set(
    selectedSessions.map((session) => session.id),
  );
  const positions = [...positionsBySessionId.entries()]
    .filter(([sessionId]) => selectedSessionIds.has(sessionId))
    .flatMap(([, sessionPositions]) => sessionPositions);
  const settledPositions = positions.filter(
    (position) => position.status === "settled",
  );
  const wins = settledPositions.filter(
    (position) => (position.realizedPnl ?? 0) > 0,
  );

  return {
    runs: selectedSessions.length,
    trades: selectedSessions.reduce(
      (sum, session) =>
        sum + numericSummaryValue(session.summaryJson, "trades_executed"),
      0,
    ),
    totalPnl: selectedSessions.reduce(
      (sum, session) =>
        sum + numericSummaryValue(session.summaryJson, "total_pnl"),
      0,
    ),
    settledPositions: settledPositions.length,
    winRate:
      settledPositions.length > 0
        ? wins.length / settledPositions.length
        : null,
  };
}
