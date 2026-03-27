import type { PaperTradePosition, PaperTradeSession } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";
import { PageStatusBanner } from "../../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { WeekendCockpitPanel } from "../_components/weekend-cockpit-panel";

export const revalidate = 60;

function pnlColor(value: number | null | undefined) {
  if (value == null) return "text-[#9ca3af]";
  return value >= 0 ? "text-green-400" : "text-red-400";
}

function fmtPct(value: number | null | undefined) {
  if (value == null) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

function fmtPnl(value: number | null | undefined) {
  if (value == null) return "—";
  return `${value >= 0 ? "+" : ""}$${value.toFixed(2)}`;
}

function fmtDateTime(value: string) {
  return new Date(value).toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function StatusBadge({ status }: { status: string }) {
  const color =
    status === "settled"
      ? "bg-green-500/10 text-green-400 border-green-500/20"
      : status === "open"
        ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
        : "bg-[#374151] text-[#9ca3af] border-white/10";
  const label =
    status === "settled" ? "Settled" : status === "open" ? "Open" : status;
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${color}`}
    >
      {label}
    </span>
  );
}

function SessionRow({
  session,
  positions,
}: {
  session: PaperTradeSession;
  positions: PaperTradePosition[];
}) {
  const summary = session.summaryJson as Record<string, number> | null;
  const trades = summary?.trades_executed ?? 0;
  const pnl = summary?.total_pnl ?? null;
  const winRate = summary?.win_rate ?? null;

  return (
    <div className="rounded-xl border border-white/[0.06] bg-[#1a1a28] p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold tracking-wider text-white">
              Paper-trading run
            </span>
            <StatusBadge status={session.status} />
          </div>
          <p className="text-[11px] text-[#6b7280]">
            {fmtDateTime(session.startedAt)}
          </p>
        </div>
        <div className="flex gap-6 text-right">
          <div>
            <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
              Trades
            </p>
            <p className="text-lg font-bold text-white">{trades}</p>
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
              Win rate
            </p>
            <p className="text-lg font-bold text-white">{fmtPct(winRate)}</p>
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
              PnL
            </p>
            <p className={`text-lg font-bold tabular-nums ${pnlColor(pnl)}`}>
              {fmtPnl(pnl)}
            </p>
          </div>
        </div>
      </div>

      <details className="mt-4 rounded-lg border border-white/[0.06] bg-[#11131d] px-4 py-3">
        <summary className="cursor-pointer text-sm font-medium text-white">
          Show run details
        </summary>
        <div className="mt-4 space-y-4">
          <div className="grid gap-2 text-xs text-[#9ca3af] md:grid-cols-2">
            <p>Stage code: {session.gpSlug}</p>
            <p>Run ID: {session.id}</p>
            <p>Snapshot ID: {session.snapshotId ?? "None"}</p>
            <p>Model run ID: {session.modelRunId ?? "None"}</p>
          </div>

          {positions.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-[#6b7280]">
                    <th className="pb-2 text-left font-medium">Market</th>
                    <th className="pb-2 text-right font-medium">Model</th>
                    <th className="pb-2 text-right font-medium">Entry price</th>
                    <th className="pb-2 text-right font-medium">Edge</th>
                    <th className="pb-2 text-right font-medium">Exit price</th>
                    <th className="pb-2 text-right font-medium">PnL</th>
                    <th className="pb-2 text-right font-medium">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.map((pos) => (
                    <tr
                      key={pos.id}
                      className="border-b border-white/[0.04] last:border-0"
                    >
                      <td className="py-2 pr-4 font-mono text-[11px] text-[#9ca3af]">
                        {pos.marketId.slice(0, 10)}…
                      </td>
                      <td className="py-2 text-right text-white">
                        {fmtPct(pos.modelProb)}
                      </td>
                      <td className="py-2 text-right text-[#9ca3af]">
                        {fmtPct(pos.entryPrice)}
                      </td>
                      <td className="py-2 text-right text-white">
                        {fmtPct(pos.edge)}
                      </td>
                      <td className="py-2 text-right text-[#9ca3af]">
                        {pos.exitPrice != null ? fmtPct(pos.exitPrice) : "—"}
                      </td>
                      <td
                        className={`py-2 text-right font-bold tabular-nums ${pnlColor(pos.realizedPnl)}`}
                      >
                        {fmtPnl(pos.realizedPnl)}
                      </td>
                      <td className="py-2 text-right">
                        <StatusBadge status={pos.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-xs text-[#6b7280]">No positions recorded.</p>
          )}
        </div>
      </details>
    </div>
  );
}

export default async function PaperTradingPage() {
  const cockpitState = await loadResource(
    () => sdk.weekendCockpitStatus(),
    null,
    "Weekend cockpit",
  );
  const sessionsState = await loadResource(
    () => sdk.paperTradeSessions(),
    [] as PaperTradeSession[],
    "Paper trading sessions",
  );
  const sessions = sessionsState.data;
  const positionsState = await loadResource(
    () =>
      Promise.all(
        sessions
          .slice(0, 10)
          .map((session) => sdk.paperTradePositions(session.id)),
      ),
    [] as PaperTradePosition[][],
    "Paper trading positions",
  );
  const positionsBySession = positionsState.data;
  const degradedMessages = collectResourceErrors([
    cockpitState,
    sessionsState,
    positionsState,
  ]);

  const allPositions = positionsBySession.flat();

  const totalTrades = sessions.reduce((sum, s) => {
    const summary = s.summaryJson as Record<string, number> | null;
    return sum + (summary?.trades_executed ?? 0);
  }, 0);

  const totalPnl = sessions.reduce((sum, s) => {
    const summary = s.summaryJson as Record<string, number> | null;
    return sum + (summary?.total_pnl ?? 0);
  }, 0);

  const settledPositions = allPositions.filter((p) => p.status === "settled");
  const wins = settledPositions.filter((p) => (p.realizedPnl ?? 0) > 0);
  const winRate =
    settledPositions.length > 0 ? wins.length / settledPositions.length : null;

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Paper Trading</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          This page shows the next action first, then the current run readiness
          and history.
        </p>
      </div>

      <WeekendCockpitPanel initialStatus={cockpitState.data} />

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Runs"
          value={sessions.length}
          hint="total paper-trading runs"
        />
        <StatCard
          label="Total trades"
          value={totalTrades}
          hint="executed positions"
        />
        <StatCard
          label="Win rate"
          value={winRate != null ? `${(winRate * 100).toFixed(1)}%` : "—"}
          hint="based on settled positions"
        />
        <StatCard
          label="Total PnL"
          value={<span className={pnlColor(totalPnl)}>{fmtPnl(totalPnl)}</span>}
          hint="realized P&L"
        />
      </section>

      {sessions.length === 0 ? (
        <Panel title="No runs yet">
          <p className="text-sm text-[#6b7280]">
            Run the current stage from the cockpit above and the results will
            appear here.
          </p>
        </Panel>
      ) : (
        <section className="flex flex-col gap-4">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6b7280]">
            Run history ({sessions.length})
          </h2>
          {sessions.map((session, i) => (
            <SessionRow
              key={session.id}
              session={session}
              positions={positionsBySession[i] ?? []}
            />
          ))}
        </section>
      )}
    </div>
  );
}
