import { sdk } from "@f1/ts-sdk";
import { StatCard } from "@f1/ui";

import { calibrationSummaryFromModelRuns } from "../../lib/calibration";
import { PredictionsTableSection } from "../_components/predictions-table-section";

export const revalidate = 300;

export default async function PredictionsPage() {
  const [modelRuns, predictions] = await Promise.all([
    sdk.modelRuns().catch(() => []),
    sdk.predictions().catch(() => []),
  ]);

  const uniqueStages = [...new Set(modelRuns.map((r) => r.stage))];
  const uniqueFamilies = [...new Set(modelRuns.map((r) => r.modelFamily))];
  const calibration = calibrationSummaryFromModelRuns(modelRuns);
  const calibrationMessage =
    calibration.points.length > 0
      ? `${calibration.sampleCount} labeled forecasts bucketed across ${calibration.runCount} model run(s).`
      : "The current API only exposes forecast probabilities, not joined outcomes, so the dashboard cannot draw a real predicted-vs-actual chart yet.";

  return (
    <div className="flex flex-col gap-6 p-6">
      <div>
        <h1 className="text-xl font-bold text-white">Predictions</h1>
        <p className="mt-1 text-sm text-[#6b7280]">
          Model training runs and probability forecasts
        </p>
      </div>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Model Runs"
          value={modelRuns.length}
          hint="total training runs"
        />
        <StatCard
          label="Predictions"
          value={predictions.length}
          hint="market forecasts"
        />
        <StatCard
          label="Stages"
          value={uniqueStages.length}
          hint="modeling stages"
        />
        <StatCard
          label="Families"
          value={uniqueFamilies.length}
          hint="model architectures"
        />
      </section>

      <PredictionsTableSection
        modelRuns={modelRuns}
        predictions={predictions}
        calibrationPoints={calibration.points}
        calibrationMessage={calibrationMessage}
      />
    </div>
  );
}
