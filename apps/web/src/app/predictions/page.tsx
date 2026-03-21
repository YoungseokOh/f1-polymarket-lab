import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

export default async function PredictionsPage() {
  const [modelRuns, predictions] = await Promise.all([
    sdk.modelRuns().catch(() => []),
    sdk.predictions().catch(() => []),
  ]);

  const uniqueStages = [...new Set(modelRuns.map((r) => r.stage))];
  const uniqueFamilies = [...new Set(modelRuns.map((r) => r.modelFamily))];

  return (
    <main className="mx-auto flex min-h-screen max-w-7xl flex-col gap-8 px-6 py-10">
      <header>
        <Link href="/" className="text-xs text-cyan-300/80 hover:text-cyan-200">
          ← Home
        </Link>
        <h1 className="mt-2 text-3xl font-semibold text-white">
          Model Predictions
        </h1>
      </header>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Model Runs"
          value={modelRuns.length}
          hint="total training runs"
        />
        <StatCard
          label="Predictions"
          value={predictions.length}
          hint="market probability forecasts"
        />
        <StatCard
          label="Stages"
          value={uniqueStages.length}
          hint="distinct modeling stages"
        />
        <StatCard
          label="Model Families"
          value={uniqueFamilies.length}
          hint="xgboost, lightgbm, etc."
        />
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Panel title="Recent Model Runs" eyebrow="Training">
          <div className="space-y-3 text-sm">
            {modelRuns.length === 0 ? (
              <p className="text-slate-400">No model runs yet.</p>
            ) : (
              modelRuns.slice(0, 10).map((run) => (
                <div
                  key={run.id}
                  className="flex items-center justify-between rounded border border-white/5 px-3 py-2"
                >
                  <div>
                    <span className="text-white">{run.modelName}</span>
                    <span className="ml-2 text-slate-400">({run.modelFamily})</span>
                  </div>
                  <div className="flex gap-2">
                    <Badge>{run.stage}</Badge>
                    {run.metricsJson && (
                      <Badge tone="good">
                        brier: {Number(run.metricsJson.brier_score ?? 0).toFixed(4)}
                      </Badge>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </Panel>

        <Panel title="Latest Predictions" eyebrow="Forecasts">
          <div className="space-y-3 text-sm">
            {predictions.length === 0 ? (
              <p className="text-slate-400">No predictions yet.</p>
            ) : (
              predictions.slice(0, 10).map((pred) => (
                <div
                  key={pred.id}
                  className="flex items-center justify-between rounded border border-white/5 px-3 py-2"
                >
                  <span className="font-mono text-xs text-slate-300">
                    {pred.marketId?.slice(0, 12)}...
                  </span>
                  <div className="flex gap-2">
                    <Badge tone="good">
                      YES: {((pred.probabilityYes ?? 0) * 100).toFixed(1)}%
                    </Badge>
                    {pred.calibrationVersion && (
                      <Badge>{pred.calibrationVersion}</Badge>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </Panel>
      </section>
    </main>
  );
}
