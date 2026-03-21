import Link from "next/link";

import { sdk } from "@f1/ts-sdk";
import { Badge, Panel, StatCard } from "@f1/ui";

function lastFetched(records: Awaited<ReturnType<typeof sdk.freshness>>) {
  const values = records
    .map((record) => record.lastFetchAt)
    .filter((value): value is string => Boolean(value))
    .sort();

  return values.at(-1) ?? "not fetched yet";
}

export default async function HomePage() {
  const [health, freshness, sessions, markets, mappings, modelRuns, predictions] = await Promise.all([
    sdk.health().catch(() => ({
      service: "api",
      status: "offline",
      now: new Date().toISOString(),
    })),
    sdk.freshness().catch(() => []),
    sdk.sessions().catch(() => []),
    sdk.markets().catch(() => []),
    sdk.mappings().catch(() => []),
    sdk.modelRuns().catch(() => []),
    sdk.predictions().catch(() => []),
  ]);

  const practiceSessions = sessions.filter((session) => session.isPractice);
  const mappedMarkets = mappings.filter(
    (mapping) => mapping.polymarketMarketId,
  );

  return (
    <main className="mx-auto flex min-h-screen max-w-7xl flex-col gap-8 px-6 py-10">
      <header className="grid gap-6 lg:grid-cols-[1.3fr_0.7fr]">
        <div className="rounded-[2rem] border border-white/10 bg-slate-900/70 p-8 shadow-2xl shadow-black/30">
          <p className="text-xs uppercase tracking-[0.45em] text-cyan-300/80">
            Research Platform
          </p>
          <h1 className="mt-4 max-w-3xl text-5xl font-semibold leading-tight text-white">
            Build leakage-safe F1 prediction intuition before serious models.
          </h1>
          <p className="mt-4 max-w-2xl text-lg text-slate-300">
            Bronze-to-Gold storage, canonical F1 and Polymarket entities,
            realistic execution logic, and a dashboard designed for
            practice-session markets first.
          </p>
          <div className="mt-8 flex flex-wrap gap-3">
            <Badge tone={health.status === "ok" ? "good" : "warn"}>
              {health.status}
            </Badge>
            <Badge>{practiceSessions.length} practice sessions loaded</Badge>
            <Badge>{markets.length} markets in demo slice</Badge>
          </div>
        </div>
        <Panel title="Current Slice" eyebrow="Phase 0 / 1">
          <div className="space-y-4 text-sm text-slate-200">
            <p>API health: {health.status}</p>
            <p>Last fetch: {lastFetched(freshness)}</p>
            <p>Mapped F1 markets: {mappedMarkets.length}</p>
            <p>
              Schema: Bronze raw envelopes + Silver normalized entities + Gold
              placeholders.
            </p>
          </div>
        </Panel>
      </header>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Freshness Records"
          value={freshness.length}
          hint="connector fetch summaries"
        />
        <StatCard
          label="Practice Sessions"
          value={practiceSessions.length}
          hint="FP1/FP2/FP3 rows"
        />
        <StatCard
          label="Markets"
          value={markets.length}
          hint="official Polymarket API payloads"
        />
        <StatCard
          label="Mappings"
          value={mappedMarkets.length}
          hint="deterministic join attempts"
        />
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Model Runs"
          value={modelRuns.length}
          hint="training runs"
        />
        <StatCard
          label="Predictions"
          value={predictions.length}
          hint="probability forecasts"
        />
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Panel title="Explorers" eyebrow="Browse">
          <div className="grid gap-3 text-sm">
            <Link href="/sessions">F1 Session Explorer</Link>
            <Link href="/markets">Polymarket Explorer</Link>
            <Link href="/lineage">Lineage & Freshness</Link>
            <Link href="/predictions">Model Predictions</Link>
            <Link href="/backtest">Backtest Results</Link>
          </div>
        </Panel>
        <Panel title="Modeling Order" eyebrow="Non-Negotiable">
          <ol className="list-decimal space-y-2 pl-5 text-sm text-slate-200">
            <li>FP2 / FP3 head-to-head</li>
            <li>Constructor / team fastest lap</li>
            <li>Driver outright fastest lap</li>
            <li>Red flag / safety car</li>
          </ol>
        </Panel>
      </section>
    </main>
  );
}
