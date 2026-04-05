import { sdk } from "@f1/ts-sdk";
import { Panel, StatCard } from "@f1/ui";

import { PageStatusBanner } from "../../components/page-status-banner";
import { calibrationSummaryFromModelRuns } from "../../lib/calibration";
import { formatProbability, formatTaxonomyLabel } from "../../lib/display";
import { collectResourceErrors, loadResource } from "../../lib/resource-state";
import { PredictionsTableSection } from "../_components/predictions-table-section";

export const revalidate = 300;

export default async function PredictionsPage() {
  const [modelRunsState, predictionsState, marketsState] = await Promise.all([
    loadResource(sdk.modelRuns, [], "Model runs"),
    loadResource(sdk.predictions, [], "Predictions"),
    loadResource(() => sdk.markets({ limit: 250 }), [], "Market feed"),
  ]);

  const modelRuns = modelRunsState.data;
  const predictions = predictionsState.data;
  const markets = marketsState.data;
  const degradedMessages = collectResourceErrors([
    modelRunsState,
    predictionsState,
    marketsState,
  ]);

  const calibration = calibrationSummaryFromModelRuns(modelRuns);
  const averageConfidence =
    predictions.length > 0
      ? predictions.reduce((sum, prediction) => {
          const yes = prediction.probabilityYes ?? 0;
          const no = prediction.probabilityNo ?? 0;
          return sum + Math.max(yes, no);
        }, 0) / predictions.length
      : null;
  const latestForecastTs = predictions
    .map((prediction) => prediction.asOfTs)
    .sort()
    .at(-1);
  const coveredMarkets = new Set(
    predictions.map((prediction) => prediction.marketId).filter(Boolean),
  ).size;
  const marketFamilies = new Set(
    markets.map((market) => formatTaxonomyLabel(market.taxonomy)),
  ).size;
  const calibrationMessage =
    calibration.points.length > 0
      ? `${calibration.sampleCount} settled forecasts from ${calibration.runCount} run(s) are available for calibration review.`
      : "The current API exposes forecast probabilities, but not enough joined outcomes to draw a predicted-versus-actual curve yet.";

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageStatusBanner messages={degradedMessages} />

      <div>
        <h1 className="text-xl font-bold text-white">Model forecasts</h1>
        <p className="mt-1 max-w-3xl text-sm text-[#6b7280]">
          Read this page as the model&apos;s view of current F1 market
          questions. Start with the run scoreboard, then inspect the latest
          market-level forecasts below.
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Model runs"
          value={modelRuns.length}
          hint="Trained forecasting runs"
        />
        <StatCard
          label="Forecast rows"
          value={predictions.length}
          hint={`${coveredMarkets} unique markets`}
        />
        <StatCard
          label="Average confidence"
          value={
            averageConfidence != null
              ? formatProbability(averageConfidence)
              : "—"
          }
          hint="Higher of YES / NO for each forecast"
        />
        <StatCard
          label="Market families"
          value={marketFamilies}
          hint={
            latestForecastTs
              ? `Latest forecast ${new Date(
                  latestForecastTs,
                ).toLocaleDateString("en-US", {
                  month: "short",
                  day: "numeric",
                })}`
              : "No forecasts available"
          }
        />
      </section>

      <Panel title="How to read a forecast" eyebrow="Quick guide">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">Question first</p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Each forecast row is tied to a live market question so you can
              judge the prediction in the same language the market uses.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              YES chance is the fair price
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              A model YES probability of 67% implies a fair YES price of about
              67 cents before any market spread or execution rules.
            </p>
          </div>
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              Signal strength is directional
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              Strong YES and Strong NO are confidence buckets, not trading
              instructions by themselves. Always compare them with market price
              and liquidity.
            </p>
          </div>
        </div>
      </Panel>

      <PredictionsTableSection
        modelRuns={modelRuns}
        predictions={predictions}
        markets={markets}
        calibrationPoints={calibration.points}
        calibrationMessage={calibrationMessage}
      />
    </div>
  );
}
