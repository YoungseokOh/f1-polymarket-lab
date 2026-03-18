import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";

export default async function LineagePage() {
  const freshness = await sdk.freshness().catch(() => []);

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <Panel title="Data Lineage & Freshness" eyebrow="Bronze / Silver">
        <div className="space-y-4">
          {freshness.map((record) => (
            <div
              key={`${record.source}:${record.dataset}`}
              className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
            >
              <p className="text-sm font-medium text-white">
                {record.source} / {record.dataset}
              </p>
              <p className="mt-1 text-sm text-slate-300">
                status={record.status} records={record.recordsFetched}{" "}
                lastFetchAt=
                {record.lastFetchAt ?? "n/a"}
              </p>
            </div>
          ))}
        </div>
      </Panel>
    </main>
  );
}
