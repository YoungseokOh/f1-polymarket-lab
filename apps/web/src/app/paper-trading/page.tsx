import type {
  CurrentWeekendOperationsReadiness,
  DriverAffinityReport,
  F1Meeting,
  F1Session,
  LiveTradeSignalBoard,
  ModelRun,
  PaperTradePosition,
  PaperTradeSession,
  PolymarketMarket,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";
import { PageStatusBanner } from "../../components/page-status-banner";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { selectScheduleMeetings } from "../../lib/schedule";
import { meetingRefreshTargetForConfig } from "../../lib/session-refresh";
import { DriverAffinitySummary } from "../_components/driver-affinity-summary";
import { WeekendCockpitPanel } from "../_components/weekend-cockpit-panel";
import { CancelPaperRunButton } from "./cancel-paper-run-button";
import { ModelReadinessPanel } from "./model-readiness-panel";
import { calculatePaperTradingStats } from "./stats";

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

function StatusBadge({ status }: { status: string }) {
  const color =
    status === "settled"
      ? "bg-green-500/10 text-green-400 border-green-500/20"
      : status === "open"
        ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
        : status === "cancelled"
          ? "bg-[#e10600]/10 text-[#ffb4b1] border-[#e10600]/20"
          : "bg-[#374151] text-[#9ca3af] border-white/10";
  const label =
    status === "settled"
      ? "Settled"
      : status === "open"
        ? "Open"
        : status === "cancelled"
          ? "Cancelled"
          : status;
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

function isManualPickBundleSession(session: PaperTradeSession) {
  const config = session.configJson;
  return config?.manual_trade === true && config.manual_trade_batch === true;
}

function buildGpSessionGroupLabel(
  gpSlug: string,
  availableConfigs: {
    short_code: string;
    name: string;
  }[],
) {
  const config = availableConfigs.find((item) => item.short_code === gpSlug);
  return (
    config?.name ??
    gpSlug.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase())
  );
}

function pickSideForPosition(position: PaperTradePosition) {
  return position.side === "buy_no" ? "NO" : "YES";
}

function manualPickConfigByMarket(session: PaperTradeSession) {
  const configPicks = session.configJson?.picks;
  if (!Array.isArray(configPicks))
    return new Map<string, Record<string, unknown>>();
  return new Map(
    configPicks
      .filter(
        (pick): pick is Record<string, unknown> =>
          typeof pick === "object" && pick !== null,
      )
      .flatMap((pick) => {
        const marketId = pick.market_id;
        return typeof marketId === "string" ? [[marketId, pick] as const] : [];
      }),
  );
}

type GpPaperSessionGroup = {
  gpSlug: string;
  manualSessions: PaperTradeSession[];
  modelSessions: PaperTradeSession[];
};

function PaperRunGroup({
  kind,
  sessions,
  positionsBySessionId,
  marketsById,
}: {
  kind: "manual" | "model";
  sessions: PaperTradeSession[];
  positionsBySessionId: Map<string, PaperTradePosition[]>;
  marketsById: Map<string, PolymarketMarket>;
}) {
  const runCount = sessions.length;
  const isManual = kind === "manual";
  const positionsWithSession = sessions.flatMap((session) =>
    (positionsBySessionId.get(session.id) ?? []).map((position) => ({
      session,
      position,
    })),
  );
  const totalTrades = sessions.reduce(
    (sum, session) =>
      sum +
      (numberFromConfig(session.summaryJson, "trades_executed") ||
        positionsBySessionId.get(session.id)?.length ||
        0),
    0,
  );
  const totalPnl = sessions.reduce(
    (sum, session) =>
      sum + (numberFromConfig(session.summaryJson, "total_pnl") ?? 0),
    0,
  );
  const openSessions = sessions.filter((session) => session.status === "open");
  const settledSessions = sessions.filter(
    (session) => session.status === "settled",
  );
  const groupStatus =
    openSessions.length > 0
      ? "open"
      : settledSessions.length > 0 && settledSessions.length === sessions.length
        ? "settled"
        : sessions.every((session) => session.status === "cancelled")
          ? "cancelled"
          : "mixed";
  const openSessionIds = openSessions.map((session) => session.id);

  return (
    <div
      className={`rounded-xl border p-5 ${
        isManual
          ? "border-[#e10600]/20 bg-[#1a1118]"
          : "border-white/[0.06] bg-[#1a1a28]"
      }`}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold tracking-wider text-white">
              {isManual ? "Your picks" : "Model runs"}
            </span>
            <StatusBadge status={groupStatus} />
          </div>
          <p className="text-[11px] text-[#6b7280]">
            {runCount} {isManual ? "manual" : "model"} run
            {runCount === 1 ? "" : "s"} shown together
          </p>
          <p className="max-w-2xl text-xs text-[#9ca3af]">
            {isManual
              ? "These are the YES/NO picks you chose from Trade candidates. They are grouped here so one-click attempts stay readable against the model runs."
              : "These are the YES/NO positions created by the model workflow. They are grouped here so the model side is easy to compare against your picks."}
          </p>
        </div>
        <div className="flex flex-wrap items-start justify-end gap-4 text-right">
          <div>
            <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
              Picks
            </p>
            <p className="text-lg font-bold text-white">{totalTrades}</p>
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-[#6b7280]">
              PnL
            </p>
            <p
              className={`text-lg font-bold tabular-nums ${pnlColor(totalPnl)}`}
            >
              {fmtPnl(totalPnl)}
            </p>
          </div>
        </div>
      </div>

      {positionsWithSession.length > 0 ? (
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full table-fixed divide-y divide-white/[0.06] text-left text-sm">
            <colgroup>
              {isManual ? (
                <>
                  <col className="w-[36%]" />
                  <col className="w-[10%]" />
                  <col className="w-[10%]" />
                  <col className="w-[10%]" />
                  <col className="w-[11%]" />
                  <col className="w-[11%]" />
                  <col className="w-[13%]" />
                </>
              ) : (
                <>
                  <col className="w-[38%]" />
                  <col className="w-[12%]" />
                  <col className="w-[11%]" />
                  <col className="w-[11%]" />
                  <col className="w-[11%]" />
                  <col className="w-[17%]" />
                </>
              )}
            </colgroup>
            <thead className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
              <tr>
                <th className="min-w-0 py-2 pr-4">Market</th>
                <th className="px-4 py-2 whitespace-nowrap">
                  {isManual ? "Your pick" : "Model pick"}
                </th>
                {isManual ? (
                  <th className="px-4 py-2 whitespace-nowrap">Model pick</th>
                ) : null}
                <th className="px-4 py-2 whitespace-nowrap">Model chance</th>
                <th className="px-4 py-2 whitespace-nowrap">Market price</th>
                <th className="px-4 py-2 whitespace-nowrap">Edge</th>
                <th className="py-2 pl-4 whitespace-nowrap">Run</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/[0.06]">
              {positionsWithSession.map(({ session, position }) => {
                const market = marketsById.get(position.marketId) ?? null;
                const pickConfig =
                  manualPickConfigByMarket(session).get(position.marketId) ??
                  null;
                const modelPick =
                  typeof pickConfig?.model_pick_side === "string"
                    ? pickConfig.model_pick_side
                    : "Review";
                return (
                  <tr key={position.id}>
                    <td className="min-w-0 py-3 pr-4">
                      <a
                        href={`/markets/${position.marketId}`}
                        className="block w-full min-w-0 break-words font-medium text-white transition-colors hover:text-[#ffb4b1]"
                      >
                        {market?.question ?? `Market ${position.marketId}`}
                      </a>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap font-semibold text-white">
                      {pickSideForPosition(position)}
                    </td>
                    {isManual ? (
                      <td className="px-4 py-3 whitespace-nowrap text-[#d1d5db]">
                        {modelPick}
                      </td>
                    ) : null}
                    <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                      {fmtPct(position.modelProb)}
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                      {fmtPct(position.marketProb)}
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                      {fmtPct(position.edge)}
                    </td>
                    <td className="min-w-0 py-3 pl-4 whitespace-nowrap">
                      <div className="inline-flex items-center gap-2 whitespace-nowrap">
                        <span className="min-w-0 shrink-0 text-xs text-[#9ca3af]">
                          {fmtDateTime(session.startedAt)}
                        </span>
                        <StatusBadge status={position.status} />
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="mt-4 text-xs text-[#6b7280]">
          No positions loaded for this group.
        </p>
      )}

      {openSessions.length > 0 ? (
        <div className="mt-4 flex flex-wrap gap-2 border-t border-white/[0.06] pt-3">
          <CancelPaperRunButton
            sessionIds={openSessionIds}
            label={openSessionIds.length === 1 ? "Cancel run" : "Cancel runs"}
          />
        </div>
      ) : null}
    </div>
  );
}

function PaperRunGpGroup({
  gpLabel,
  sessionsByType,
  positionsBySessionId,
  marketsById,
  isCurrent,
}: {
  gpLabel: string;
  sessionsByType: {
    manual: PaperTradeSession[];
    model: PaperTradeSession[];
  };
  positionsBySessionId: Map<string, PaperTradePosition[]>;
  marketsById: Map<string, PolymarketMarket>;
  isCurrent: boolean;
}) {
  const totalRuns = sessionsByType.manual.length + sessionsByType.model.length;
  const openSessionIds = [...sessionsByType.manual, ...sessionsByType.model]
    .filter((session) => session.status === "open")
    .map((session) => session.id);
  if (totalRuns === 0) {
    return (
      <details className="rounded-xl border border-white/[0.06] bg-[#11131d] px-4 py-3">
        <summary className="cursor-pointer text-sm font-medium text-white">
          {gpLabel} (0)
        </summary>
        <p className="mt-4 text-xs text-[#6b7280]">
          No paper-trading runs for this GP.
        </p>
      </details>
    );
  }

  return (
    <details
      open={isCurrent}
      className="rounded-xl border border-white/[0.06] bg-[#11131d] px-4 py-3"
    >
      <summary className="cursor-pointer text-sm font-medium text-white">
        {gpLabel} ({totalRuns} runs)
      </summary>
      <div className="mt-4 flex flex-wrap gap-2">
        {openSessionIds.length > 0 ? (
          <CancelPaperRunButton
            sessionIds={openSessionIds}
            label={
              openSessionIds.length === 1
                ? "Cancel run"
                : "Cancel all open runs"
            }
          />
        ) : null}
      </div>
      <div className="mt-4 flex flex-col gap-4">
        {sessionsByType.manual.length > 0 ? (
          <PaperRunGroup
            kind="manual"
            sessions={sessionsByType.manual}
            positionsBySessionId={positionsBySessionId}
            marketsById={marketsById}
          />
        ) : null}
        {sessionsByType.model.length > 0 ? (
          <PaperRunGroup
            kind="model"
            sessions={sessionsByType.model}
            positionsBySessionId={positionsBySessionId}
            marketsById={marketsById}
          />
        ) : null}
      </div>
    </details>
  );
}

export default async function PaperTradingPage() {
  const [
    affinityState,
    cockpitState,
    readinessState,
    meetingsState,
    modelRunsState,
    paperSessionsState,
  ] = await Promise.all([
    loadResource(
      () => sdk.driverAffinity(2026),
      null as DriverAffinityReport | null,
      "Driver affinity",
    ),
    loadResource(() => sdk.weekendCockpitStatus(), null, "Weekend cockpit"),
    loadResource(
      () => sdk.currentWeekendReadiness(),
      null as CurrentWeekendOperationsReadiness | null,
      "Weekend operations readiness",
    ),
    loadResource(
      () => sdk.meetings({ limit: 100 }),
      [] as F1Meeting[],
      "Meeting feed",
    ),
    loadResource(sdk.modelRuns, [] as ModelRun[], "Model runs"),
    loadResource(
      () => sdk.paperTradeSessions(),
      [] as PaperTradeSession[],
      "Paper trading sessions",
    ),
  ]);
  const affinityReport = affinityState.data;
  const { season: scheduleSeason, meetings } = selectScheduleMeetings(
    meetingsState.data,
  );
  const f1SessionsState = await loadResource(
    () =>
      scheduleSeason == null
        ? Promise.resolve([] as F1Session[])
        : sdk.sessions({ limit: 1000, season: scheduleSeason }),
    [] as F1Session[],
    "Session feed",
  );
  const sessions = paperSessionsState.data;
  const activeSessions = sessions.filter(
    (session) => session.status !== "cancelled",
  );
  const cancelledSessions = sessions.filter(
    (session) => session.status === "cancelled",
  );
  const visibleSessions = sessions.slice(0, 10);
  const positionsState = await loadResource(
    () =>
      Promise.all(
        visibleSessions.map((session) => sdk.paperTradePositions(session.id)),
      ),
    [] as PaperTradePosition[][],
    "Paper trading positions",
  );
  const positionsBySession = positionsState.data;
  const positionsBySessionId = new Map(
    visibleSessions.map(
      (session, index) =>
        [session.id, positionsBySession[index] ?? []] as const,
    ),
  );
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
  const selectedGpShortCode = cockpitState.data?.selectedGpShortCode ?? null;
  const liveSignalBoardState = await loadResource(
    () =>
      selectedGpShortCode == null
        ? Promise.resolve(null as LiveTradeSignalBoard | null)
        : sdk.liveTradeSignalBoard(selectedGpShortCode),
    null as LiveTradeSignalBoard | null,
    "Trade candidates",
  );

  const degradedMessages = collectResourceErrors([
    cockpitState,
    readinessState,
    f1SessionsState,
    meetingsState,
    modelRunsState,
    paperSessionsState,
    positionsState,
    marketsState,
    liveSignalBoardState,
  ]).concat(!affinityReport ? collectResourceErrors([affinityState]) : []);
  const currentGpStats = calculatePaperTradingStats(
    activeSessions,
    positionsBySessionId,
    selectedGpShortCode,
  );
  const allTimeStats = calculatePaperTradingStats(
    activeSessions,
    positionsBySessionId,
  );
  const availableConfigs = cockpitState.data?.availableConfigs ?? [];
  const activeSessionsWithGp = [...activeSessions, ...cancelledSessions].sort(
    (a, b) => +new Date(b.startedAt) - +new Date(a.startedAt),
  );
  const groupedRunsByGp = new Map<string, PaperTradeSession[]>();
  for (const session of activeSessionsWithGp) {
    const bucket = groupedRunsByGp.get(session.gpSlug) ?? [];
    bucket.push(session);
    groupedRunsByGp.set(session.gpSlug, bucket);
  }
  const gpSessionGroups: GpPaperSessionGroup[] = Array.from(
    groupedRunsByGp.entries(),
  )
    .map(([gpSlug, sessions]) => ({
      gpSlug,
      sessions,
      manualSessions: sessions.filter(isManualPickBundleSession),
      modelSessions: sessions.filter(
        (session) => !isManualPickBundleSession(session),
      ),
    }))
    .sort((a, b) => {
      if (a.gpSlug === selectedGpShortCode && b.gpSlug !== selectedGpShortCode)
        return -1;
      if (a.gpSlug !== selectedGpShortCode && b.gpSlug === selectedGpShortCode)
        return 1;
      return 0;
    });
  const refreshTargetsByGpShortCode = Object.fromEntries(
    availableConfigs.map((config) => [
      config.short_code,
      meetingRefreshTargetForConfig(config, meetings, f1SessionsState.data),
    ]),
  );

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Paper Trading Console</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Current F1 stage status, trade candidates, and simulated YES/NO ticket
          results.
        </p>
      </div>

      <section className="grid gap-4 xl:grid-cols-[1.34fr_0.66fr]">
        <WeekendCockpitPanel
          initialStatus={cockpitState.data}
          initialReadiness={readinessState.data}
          initialSignalBoard={liveSignalBoardState.data}
          refreshTargetsByGpShortCode={refreshTargetsByGpShortCode}
        />
        <div className="flex flex-col gap-4">
          <ModelReadinessPanel
            status={cockpitState.data}
            modelRuns={modelRunsState.data}
          />
          <DriverAffinitySummary
            report={affinityReport}
            readiness={
              readinessState.data?.actions.find(
                (action) => action.key === "driver_affinity",
              ) ?? null
            }
          />
        </div>
      </section>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Current GP runs"
          value={currentGpStats.runs}
          hint={selectedGpShortCode ?? "No GP selected"}
        />
        <StatCard
          label="Current tickets"
          value={currentGpStats.trades}
          hint="this GP only"
        />
        <StatCard
          label="Current win rate"
          value={
            currentGpStats.winRate != null
              ? `${(currentGpStats.winRate * 100).toFixed(1)}%`
              : "—"
          }
          hint={`${currentGpStats.settledPositions} settled tickets`}
        />
        <StatCard
          label="Current PnL"
          value={
            <span className={pnlColor(currentGpStats.totalPnl)}>
              {fmtPnl(currentGpStats.totalPnl)}
            </span>
          }
          hint="realized P&L"
        />
      </section>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="All-time runs"
          value={allTimeStats.runs}
          hint="all paper-trading runs"
        />
        <StatCard
          label="All-time tickets"
          value={allTimeStats.trades}
          hint="all executed tickets"
        />
        <StatCard
          label="All-time win rate"
          value={
            allTimeStats.winRate != null
              ? `${(allTimeStats.winRate * 100).toFixed(1)}%`
              : "—"
          }
          hint={`${allTimeStats.settledPositions} settled tickets loaded`}
        />
        <StatCard
          label="All-time PnL"
          value={
            <span className={pnlColor(allTimeStats.totalPnl)}>
              {fmtPnl(allTimeStats.totalPnl)}
            </span>
          }
          hint="realized P&L"
        />
      </section>

      {selectedGpShortCode && gpSessionGroups.length === 0 ? (
        <Panel title="Current GP results" eyebrow="This GP">
          <p className="text-sm text-[#6b7280]">
            No paper-trading run exists for the current GP yet. Past GP runs
            stay in the history below.
          </p>
        </Panel>
      ) : null}

      {gpSessionGroups.length > 0 ? (
        <section className="flex flex-col gap-3">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6b7280]">
            Runs by GP ({gpSessionGroups.length})
          </h2>
          <div className="flex flex-col gap-3">
            {gpSessionGroups.map((group) => (
              <PaperRunGpGroup
                key={group.gpSlug}
                gpLabel={buildGpSessionGroupLabel(
                  group.gpSlug,
                  availableConfigs,
                )}
                sessionsByType={{
                  manual: group.manualSessions,
                  model: group.modelSessions,
                }}
                positionsBySessionId={positionsBySessionId}
                marketsById={marketsById}
                isCurrent={group.gpSlug === selectedGpShortCode}
              />
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
