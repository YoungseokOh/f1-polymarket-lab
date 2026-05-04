import type { ModelPrediction, PolymarketMarket } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import { calibrationSummaryFromModelRuns } from "../../lib/calibration";
import {
  formatDateTimeShort,
  formatProbability,
  formatSessionCodeLabel,
} from "../../lib/display";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { PredictionsTableSection } from "../_components/predictions-table-section";

export const revalidate = 300;

function uniqueMarketIds(predictions: ModelPrediction[]): string[] {
  return [
    ...new Set(
      predictions
        .map((prediction) => prediction.marketId)
        .filter((marketId): marketId is string => Boolean(marketId)),
    ),
  ];
}

function compactMeetingToken(meetingName: string | null | undefined): string {
  return (meetingName ?? "")
    .replace(/^Formula 1\s+/i, "")
    .replace(/^F1\s+/i, "")
    .replace(/\s+Grand Prix$/i, "")
    .trim()
    .toLowerCase();
}

function isCurrentMeetingMarket(
  market: PolymarketMarket | null,
  meetingName: string | null | undefined,
): boolean {
  if (!market || market.closed || !market.active) {
    return false;
  }

  const question = market.question.toLowerCase();
  const meetingToken = compactMeetingToken(meetingName);
  return Boolean(meetingToken && question.includes(meetingToken));
}

function isOpenF1Market(market: PolymarketMarket): boolean {
  if (market.closed || !market.active) {
    return false;
  }

  const question = market.question.toLowerCase();
  return (
    question.includes(" f1 ") ||
    question.includes("formula 1") ||
    question.includes("grand prix")
  );
}

export default async function PredictionsPage() {
  const [modelRunsState, predictionsState, readinessState, openMarketsState] =
    await Promise.all([
      loadResource(sdk.modelRuns, [], "Model runs"),
      loadResource(sdk.predictions, [], "Predictions"),
      loadResource(
        () => sdk.currentWeekendReadiness({ season: 2026 }),
        null,
        "Current weekend",
      ),
      loadResource(
        () => sdk.markets({ limit: 1000, active: true, closed: false }),
        [],
        "Open markets",
      ),
    ]);

  const modelRuns = modelRunsState.data;
  const predictions = predictionsState.data;
  const readiness = readinessState.data;
  const openMarkets = openMarketsState.data;
  const predictionMarketIds = uniqueMarketIds(predictions);
  const predictionMarketsState = await loadResource(
    () =>
      predictionMarketIds.length > 0
        ? sdk.markets({
            ids: predictionMarketIds,
            limit: Math.min(predictionMarketIds.length, 1000),
          })
        : Promise.resolve([]),
    [],
    "Prediction markets",
  );
  const predictionMarkets = predictionMarketsState.data;
  const marketsById = new Map(
    predictionMarkets.map((market) => [market.id, market]),
  );
  const currentMeetingName =
    readiness?.meeting?.meetingName ?? readiness?.selectedConfig.name ?? null;
  const currentRaceLabel = currentMeetingName ?? "current race";
  const currentSessionLabel = formatSessionCodeLabel(
    readiness?.nextActiveSession?.sessionCode ??
      readiness?.selectedConfig.target_session_code,
  );
  const currentPredictions = predictions.filter((prediction) =>
    isCurrentMeetingMarket(
      prediction.marketId
        ? (marketsById.get(prediction.marketId) ?? null)
        : null,
      currentMeetingName,
    ),
  );
  const currentPredictionMarketIds = new Set(
    currentPredictions
      .map((prediction) => prediction.marketId)
      .filter((marketId): marketId is string => Boolean(marketId)),
  );
  const currentPredictionModelRunIds = new Set(
    currentPredictions.map((prediction) => prediction.modelRunId),
  );
  const currentPredictionMarkets = predictionMarkets.filter((market) =>
    currentPredictionMarketIds.has(market.id),
  );
  const currentModelRuns = modelRuns.filter((run) =>
    currentPredictionModelRunIds.has(run.id),
  );
  const hiddenOldPredictions = predictions.length - currentPredictions.length;
  const currentOpenMarkets = openMarkets.filter(
    (market) =>
      isOpenF1Market(market) &&
      isCurrentMeetingMarket(market, currentMeetingName),
  );
  const closedPredictionMarkets = predictionMarkets.filter(
    (market) => market.closed,
  ).length;
  const weekendAction = readiness?.actions.find(
    (action) => action.key === "weekend_cockpit",
  );
  const degradedMessages = collectResourceErrors([
    modelRunsState,
    predictionsState,
    readinessState,
    openMarketsState,
    predictionMarketsState,
  ]);

  const calibration = calibrationSummaryFromModelRuns(modelRuns);
  const averageConfidence =
    currentPredictions.length > 0
      ? currentPredictions.reduce((sum, prediction) => {
          const yes = prediction.probabilityYes ?? 0;
          const no = prediction.probabilityNo ?? 0;
          return sum + Math.max(yes, no);
        }, 0) / currentPredictions.length
      : null;
  const latestForecastTs = currentPredictions
    .map((prediction) => prediction.asOfTs)
    .sort()
    .at(-1);
  const coveredMarkets = new Set(
    currentPredictions.map((prediction) => prediction.marketId).filter(Boolean),
  ).size;
  const nextStep =
    currentPredictions.length > 0
      ? "Review model price vs market price."
      : currentOpenMarkets.length === 0
        ? `Sync ${currentRaceLabel} markets before scoring.`
        : (weekendAction?.message ?? "Run the weekend model before trading.");
  const calibrationMessage =
    calibration.points.length > 0
      ? `${calibration.sampleCount} settled forecasts from ${calibration.runCount} model run(s).`
      : "Not enough settled outcomes are linked yet.";

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Predictions</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          This page now starts with the current Grand Prix only. Finished races
          are hidden unless they are needed for model checks.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Current race"
          value={currentMeetingName ?? "—"}
          hint={
            readiness?.latestEndedSession
              ? `${formatSessionCodeLabel(
                  readiness.latestEndedSession.sessionCode,
                )} finished`
              : "Waiting for weekend data"
          }
        />
        <StatCard
          label="Next session"
          value={currentSessionLabel}
          hint={
            readiness?.nextActiveSession?.dateStartUtc
              ? formatDateTimeShort(readiness.nextActiveSession.dateStartUtc)
              : "No next session found"
          }
        />
        <StatCard
          label="Live predictions"
          value={currentPredictions.length}
          hint={`${coveredMarkets} current markets`}
        />
        <StatCard
          label="Average strength"
          value={
            averageConfidence != null
              ? formatProbability(averageConfidence)
              : "—"
          }
          hint={
            latestForecastTs
              ? `Updated ${formatDateTimeShort(latestForecastTs)}`
              : "No current prediction yet"
          }
        />
      </section>

      <Panel title="What to do now" eyebrow="Plain status">
        <div className="grid gap-3 lg:grid-cols-[1.1fr_0.9fr]">
          <div className="rounded-xl border border-white/[0.08] bg-white/[0.04] p-4">
            <p className="text-sm font-semibold text-white">
              {currentPredictions.length > 0
                ? "Current predictions are ready."
                : `No current ${currentRaceLabel} prediction is ready yet.`}
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">{nextStep}</p>
            <div className="mt-4 flex flex-wrap gap-2 text-xs text-[#9ca3af]">
              <span className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1">
                {hiddenOldPredictions} old rows hidden
              </span>
              <span className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1">
                {closedPredictionMarkets} closed markets linked
              </span>
              <span className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1">
                {currentOpenMarkets.length} open {currentRaceLabel} markets
                saved
              </span>
            </div>
          </div>
          <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
            <p className="text-sm font-semibold text-white">Read the table</p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Model says is our fair YES chance. Market price is the live YES
              price. Difference is the gap between them.
            </p>
            <a
              className="mt-4 inline-flex rounded-lg border border-white/[0.12] px-3 py-2 text-sm font-medium text-white hover:bg-white/[0.06]"
              href="/paper-trading"
            >
              Open Weekend Cockpit
            </a>
          </div>
        </div>
      </Panel>

      <PredictionsTableSection
        modelRuns={currentModelRuns}
        predictions={currentPredictions}
        markets={currentPredictionMarkets}
        calibrationPoints={calibration.points}
        calibrationMessage={calibrationMessage}
        title="Current market calls"
        eyebrow={`${currentPredictions.length} current rows`}
        description="Only open markets for the current Grand Prix are shown here. Finished races are filtered out."
        emptyTitle="No current predictions"
        emptyMessage="The stored prediction rows point to finished or closed markets, so they are hidden from the main view."
        showModelHealth={false}
      />
    </div>
  );
}
