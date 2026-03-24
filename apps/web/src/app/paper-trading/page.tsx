import type { PaperTradePosition, PaperTradeSession } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";

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

function StatusBadge({ status }: { status: string }) {
  const color =
    status === "settled"
      ? "bg-green-500/10 text-green-400 border-green-500/20"
      : status === "open"
        ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
        : "bg-[#374151] text-[#9ca3af] border-white/10";
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${color}`}
    >
      {status}
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
            <span className="text-sm font-semibold uppercase tracking-wider text-white">
              {session.gpSlug.replace(/_/g, " ")}
            </span>
            <StatusBadge status={session.status} />
          </div>
          <p className="mt-0.5 font-mono text-[11px] text-[#6b7280]">
            {session.id.slice(0, 8)}… · baseline model
          </p>
          <p className="text-[11px] text-[#6b7280]">
            {new Date(session.startedAt).toLocaleString()}
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
              Win Rate
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

      {positions.length > 0 && (
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-white/[0.06] text-[10px] uppercase tracking-wider text-[#6b7280]">
                <th className="pb-2 text-left font-medium">Market</th>
                <th className="pb-2 text-right font-medium">Model</th>
                <th className="pb-2 text-right font-medium">Price</th>
                <th className="pb-2 text-right font-medium">Edge</th>
                <th className="pb-2 text-right font-medium">Exit</th>
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
      )}
    </div>
  );
}

export default async function PaperTradingPage() {
  const sessions = await sdk
    .paperTradeSessions()
    .catch(() => [] as PaperTradeSession[]);

  const positionsBySession = await Promise.all(
    sessions
      .slice(0, 10)
      .map((s) =>
        sdk.paperTradePositions(s.id).catch(() => [] as PaperTradePosition[]),
      ),
  );

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
      <div>
        <h1 className="text-xl font-bold text-white">Paper Trading</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Simulated trade sessions — FP1 snapshot → baseline model → paper
          execution
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Sessions"
          value={sessions.length}
          hint="total paper trade runs"
        />
        <StatCard
          label="Total Trades"
          value={totalTrades}
          hint="executed positions"
        />
        <StatCard
          label="Win Rate"
          value={winRate != null ? `${(winRate * 100).toFixed(1)}%` : "—"}
          hint="settled positions"
        />
        <StatCard
          label="Total PnL"
          value={<span className={pnlColor(totalPnl)}>{fmtPnl(totalPnl)}</span>}
          hint="realized P&L"
        />
      </section>

      {sessions.length === 0 ? (
        <Panel title="No sessions yet">
          <p className="text-sm text-[#6b7280]">
            Run the paper trading pipeline after FP1 to see results here.
          </p>
          <div className="mt-4 rounded-lg bg-[#0f0f1a] p-4 font-mono text-xs text-[#9ca3af]">
            <p className="text-[#e10600]"># After FP1 completes:</p>
            <p className="mt-1">
              uv run python -m f1_polymarket_worker.cli
              run-japan-fp1-paper-trade \
            </p>
            <p className="ml-4">--execute</p>
          </div>
        </Panel>
      ) : (
        <section className="flex flex-col gap-4">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6b7280]">
            Sessions ({sessions.length})
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

      <Panel title="How to run" eyebrow="Japan GP — FP1 workflow">
        <div className="space-y-3 text-sm text-[#9ca3af]">
          <p>
            <span className="font-semibold text-white">Step 1</span> — after FP1
            ends, run the one-shot pipeline:
          </p>
          <div className="rounded-lg bg-[#0f0f1a] p-4 font-mono text-xs">
            <p className="text-[#e10600]">
              # One-shot: snapshot + model + paper trade
            </p>
            <p className="mt-1">uv run python -m f1_polymarket_worker.cli</p>
            <p className="ml-4">run-japan-fp1-paper-trade --execute</p>
          </div>
          <p>
            <span className="font-semibold text-white">Step 2</span> — refresh
            this page to see signals and positions.
          </p>
          <p>
            <span className="font-semibold text-white">Step 3</span> — after
            qualifying, positions will be settled automatically against
            Polymarket outcomes.
          </p>
        </div>
      </Panel>
    </div>
  );
}
