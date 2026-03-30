import type {
  CurrentWeekendOperationsReadiness,
  DriverAffinityReport,
  PaperTradePosition,
  PaperTradeSession,
  PolymarketMarket,
  RefreshDriverAffinityResponse,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";
import { PageStatusBanner } from "../../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { meetingRefreshTargetForConfig } from "../../lib/session-refresh";
import { DriverAffinitySummary } from "../_components/driver-affinity-summary";
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

function fmtUsd(value: number | null | undefined) {
  if (value == null) return "—";
  return `$${value.toFixed(2)}`;
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

function affinityRefreshMessage(
  response: RefreshDriverAffinityResponse | null,
) {
  if (!response) return null;
  return response.status === "blocked" ? response.message : null;
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

function numberFromConfig(
  config: Record<string, unknown> | null,
  key: string,
): number | null {
  const value = config?.[key];
  return typeof value === "number" ? value : null;
}

function sessionStrategySummary(session: PaperTradeSession) {
  const config = session.configJson as Record<string, unknown> | null;
  const summary = session.summaryJson as Record<string, unknown> | null;
  const minEdge = numberFromConfig(config, "min_edge");
  const betSize = numberFromConfig(config, "bet_size");
  const feeRate = numberFromConfig(config, "fee_rate");
  const maxOpenPositions = numberFromConfig(config, "max_open_positions");

  if (config?.manual_trade === true) {
    const driver =
      typeof config.driver === "string" ? config.driver : "selected driver";
    const marketQuestion =
      typeof config.market_question === "string"
        ? config.market_question
        : null;
    const basis =
      typeof config.analysis_basis === "string" ? config.analysis_basis : null;
    return {
      title: "Manual analyst thesis",
      description:
        basis && marketQuestion
          ? `${driver} view. ${basis}. Ticket: ${marketQuestion}`
          : `${driver} view entered manually for this run.`,
      minEdge,
      betSize,
      feeRate,
      maxOpenPositions,
      selectedDriver:
        typeof summary?.selected_driver === "string"
          ? summary.selected_driver
          : null,
    };
  }

  return {
    title: "Rule-based stage portfolio",
    description:
      "Compares model YES probability against market YES price and buys the cheaper side when the edge clears the threshold.",
    minEdge,
    betSize,
    feeRate,
    maxOpenPositions,
    selectedDriver: null,
  };
}

function positionTicketLabel(side: string) {
  return side === "buy_no" ? "Bought NO" : "Bought YES";
}

function formatShares(quantity: number) {
  return `${quantity.toFixed(Number.isInteger(quantity) ? 0 : 2)} shares`;
}

function PositionCard({
  position,
  market,
}: {
  position: PaperTradePosition;
  market: PolymarketMarket | null;
}) {
  const stake = position.quantity * position.entryPrice;
  const maxPayout = position.quantity;
  return (
    <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <a
            href={`/markets/${position.marketId}`}
            className="text-sm font-medium text-white hover:text-[#ff8b85]"
          >
            {market?.question ?? `Market ${position.marketId}`}
          </a>
          <p className="text-xs text-[#6b7280]">
            {positionTicketLabel(position.side)} ·{" "}
            {formatShares(position.quantity)} @ {fmtPct(position.entryPrice)} ·
            stake {fmtUsd(stake)}
          </p>
        </div>
        <StatusBadge status={position.status} />
      </div>

      <div className="mt-3 grid gap-2 text-xs text-[#9ca3af] md:grid-cols-2 xl:grid-cols-5">
        <p>Model YES {fmtPct(position.modelProb)}</p>
        <p>Market YES {fmtPct(position.marketProb)}</p>
        <p>Edge {fmtPct(position.edge)}</p>
        <p>Max payout {fmtUsd(maxPayout)}</p>
        <p className={pnlColor(position.realizedPnl)}>
          PnL {fmtPnl(position.realizedPnl)}
        </p>
      </div>
    </div>
  );
}

function SessionRow({
  session,
  positions,
  marketsById,
}: {
  session: PaperTradeSession;
  positions: PaperTradePosition[];
  marketsById: Map<string, PolymarketMarket>;
}) {
  const summary = session.summaryJson as Record<string, number> | null;
  const strategy = sessionStrategySummary(session);
  const trades = summary?.trades_executed ?? 0;
  const pnl = summary?.total_pnl ?? null;
  const winRate = summary?.win_rate ?? null;

  return (
    <div className="rounded-xl border border-white/[0.06] bg-[#1a1a28] p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold tracking-wider text-white">
              Paper-trading run
            </span>
            <StatusBadge status={session.status} />
          </div>
          <p className="text-[11px] text-[#6b7280]">
            {fmtDateTime(session.startedAt)}
          </p>
          <div>
            <p className="text-xs font-medium text-white">{strategy.title}</p>
            <p className="mt-1 max-w-2xl text-xs text-[#9ca3af]">
              {strategy.description}
            </p>
          </div>
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
          <div className="grid gap-2 text-xs text-[#9ca3af] md:grid-cols-2 xl:grid-cols-4">
            <p>Stage code: {session.gpSlug}</p>
            <p>Run ID: {session.id}</p>
            <p>Snapshot ID: {session.snapshotId ?? "None"}</p>
            <p>Model run ID: {session.modelRunId ?? "None"}</p>
          </div>

          <div className="grid gap-2 text-xs text-[#9ca3af] md:grid-cols-2 xl:grid-cols-4">
            <p>
              Edge trigger:{" "}
              {strategy.minEdge != null ? fmtPct(strategy.minEdge) : "—"}
            </p>
            <p>
              Ticket size:{" "}
              {strategy.betSize != null ? formatShares(strategy.betSize) : "—"}
            </p>
            <p>
              Fee rate:{" "}
              {strategy.feeRate != null ? fmtPct(strategy.feeRate) : "—"}
            </p>
            <p>
              Max open tickets:{" "}
              {strategy.maxOpenPositions != null
                ? strategy.maxOpenPositions.toFixed(0)
                : "—"}
            </p>
          </div>

          {positions.length > 0 ? (
            <div className="space-y-3">
              {positions.map((position) => (
                <PositionCard
                  key={position.id}
                  position={position}
                  market={marketsById.get(position.marketId) ?? null}
                />
              ))}
            </div>
          ) : (
            <p className="text-xs text-[#6b7280]">No positions recorded.</p>
          )}
        </div>
      </details>
    </div>
  );
}

function HowPaperTradingWorks({
  latestSession,
}: {
  latestSession: PaperTradeSession | null;
}) {
  const strategy = latestSession ? sessionStrategySummary(latestSession) : null;

  return (
    <Panel title="What Paper Trading Does" eyebrow="Quick read">
      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            1. Compare
          </p>
          <p className="mt-2 text-sm text-white">
            Reads the current stage&apos;s F1 markets and compares model YES
            probability to the market&apos;s YES price.
          </p>
        </div>
        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            2. Buy Tickets
          </p>
          <p className="mt-2 text-sm text-white">
            Simulates buying YES or NO shares only when the edge clears the
            configured trigger.
          </p>
          <p className="mt-1 text-xs text-[#6b7280]">
            Current default: edge{" "}
            {strategy?.minEdge != null ? fmtPct(strategy.minEdge) : "—"} · size{" "}
            {strategy?.betSize != null ? formatShares(strategy.betSize) : "—"}
          </p>
        </div>
        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            3. Track Outcome
          </p>
          <p className="mt-2 text-sm text-white">
            Keeps open and settled tickets, stake, payout profile, and PnL so
            you can see exactly what the model would have bought.
          </p>
        </div>
      </div>
    </Panel>
  );
}

export default async function PaperTradingPage() {
  const affinityRefreshState = await loadResource(
    () => sdk.refreshDriverAffinity({ season: 2026 }),
    null as RefreshDriverAffinityResponse | null,
    "Driver affinity refresh",
  );
  const affinityState = await loadResource(
    () => sdk.driverAffinity(2026),
    null as DriverAffinityReport | null,
    "Driver affinity",
  );
  const affinityReport =
    affinityState.data ?? affinityRefreshState.data?.report ?? null;
  const cockpitState = await loadResource(
    () => sdk.weekendCockpitStatus(),
    null,
    "Weekend cockpit",
  );
  const readinessState = await loadResource(
    () => sdk.currentWeekendReadiness(),
    null as CurrentWeekendOperationsReadiness | null,
    "Weekend operations readiness",
  );
  const f1SessionsState = await loadResource(
    () => sdk.sessions({ limit: 250 }),
    [],
    "Session feed",
  );
  const meetingsState = await loadResource(
    () => sdk.meetings({ limit: 100 }),
    [],
    "Meeting feed",
  );
  const paperSessionsState = await loadResource(
    () => sdk.paperTradeSessions(),
    [] as PaperTradeSession[],
    "Paper trading sessions",
  );
  const sessions = paperSessionsState.data;
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
  const allPositions = positionsBySession.flat();
  const uniqueMarketIds = [
    ...new Set(allPositions.map((position) => position.marketId)),
  ];
  const marketsState = await loadResource(
    () =>
      uniqueMarketIds.length === 0
        ? Promise.resolve([] as PolymarketMarket[])
        : sdk.markets({
            ids: uniqueMarketIds,
            limit: uniqueMarketIds.length,
          }),
    [] as PolymarketMarket[],
    "Polymarket markets",
  );
  const marketsById = new Map(
    marketsState.data.map((market) => [market.id, market] as const),
  );

  const degradedMessages = collectResourceErrors([
    cockpitState,
    readinessState,
    f1SessionsState,
    meetingsState,
    paperSessionsState,
    positionsState,
    marketsState,
  ])
    .concat(
      !affinityReport
        ? collectResourceErrors([affinityRefreshState, affinityState])
        : [],
    )
    .concat(
      affinityRefreshMessage(affinityRefreshState.data)
        ? [affinityRefreshMessage(affinityRefreshState.data) as string]
        : [],
      affinityReport && !affinityReport.isFresh && affinityReport.staleReason
        ? [affinityReport.staleReason]
        : [],
    );

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
  const latestSession = sessions[0] ?? null;
  const refreshTargetsByGpShortCode = Object.fromEntries(
    (cockpitState.data?.availableConfigs ?? []).map((config) => [
      config.short_code,
      meetingRefreshTargetForConfig(
        config,
        meetingsState.data,
        f1SessionsState.data,
      ),
    ]),
  );

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Paper Trading</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Simulated YES and NO ticket buying for the current F1 stage. The
          cockpit prepares the stage, this page shows what the model bought, how
          much it staked, and how the run is performing.
        </p>
      </div>

      <HowPaperTradingWorks latestSession={latestSession} />

      <section className="grid gap-4 xl:grid-cols-[1.34fr_0.66fr]">
        <WeekendCockpitPanel
          initialStatus={cockpitState.data}
          initialReadiness={readinessState.data}
          refreshTargetsByGpShortCode={refreshTargetsByGpShortCode}
        />
        <DriverAffinitySummary
          report={affinityReport}
          refreshMessage={affinityRefreshMessage(affinityRefreshState.data)}
          readiness={
            readinessState.data?.actions.find(
              (action) => action.key === "driver_affinity",
            ) ?? null
          }
        />
      </section>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Runs"
          value={sessions.length}
          hint="total paper-trading runs"
        />
        <StatCard
          label="Total tickets"
          value={totalTrades}
          hint="executed YES/NO tickets"
        />
        <StatCard
          label="Win rate"
          value={winRate != null ? `${(winRate * 100).toFixed(1)}%` : "—"}
          hint="based on settled tickets"
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
            Run the current stage from the cockpit above and the simulated
            tickets will appear here.
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
              marketsById={marketsById}
            />
          ))}
        </section>
      )}
    </div>
  );
}
