"use client";

import Link from "next/link";

import type { IngestionJobRun } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import {
  buildDashboardDemoIngestRequest,
  buildDashboardMarketSyncRequest,
} from "../../lib/action-defaults";
import { ActionButton } from "./action-button";

function jobTone(status: string) {
  if (status === "completed") return "text-race-green";
  if (status === "failed") return "text-[#e10600]";
  return "text-[#f59e0b]";
}

export function DashboardActions({
  latestDemoIngestJob,
}: {
  latestDemoIngestJob: IngestionJobRun | null;
}) {
  const router = useRouter();

  return (
    <Panel title="Quick Actions" eyebrow="Pipeline Controls">
      <div className="flex flex-wrap items-start gap-3">
        <ActionButton
          label="Sync F1 Calendar"
          onAction={async () => {
            const res = await sdk.syncCalendar({ season: 2026 });
            router.refresh();
            return res.message;
          }}
        />
        <ActionButton
          label="Sync F1 Markets"
          variant="secondary"
          onAction={async () => {
            const res = await sdk.syncF1Markets(
              buildDashboardMarketSyncRequest(),
            );
            router.refresh();
            return res.message;
          }}
        />
        <ActionButton
          label="Ingest Demo Data"
          variant="secondary"
          onAction={async () => {
            const res = await sdk.ingestDemo(buildDashboardDemoIngestRequest());
            router.refresh();
            return res.message;
          }}
        />
      </div>
      {latestDemoIngestJob ? (
        <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3">
          <div className="min-w-0">
            <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-[#6b7280]">
              Latest Demo Ingest
            </p>
            <p className={`mt-1 text-sm font-medium ${jobTone(latestDemoIngestJob.status)}`}>
              {latestDemoIngestJob.status === "running"
                ? "Running"
                : latestDemoIngestJob.status === "completed"
                  ? "Completed"
                  : "Failed"}
            </p>
            <p className="mt-1 truncate text-xs text-[#9ca3af]">
              Job {latestDemoIngestJob.id} started{" "}
              {new Date(latestDemoIngestJob.startedAt).toLocaleTimeString("en-US", {
                hour: "2-digit",
                minute: "2-digit",
                hour12: false,
              })}
            </p>
          </div>
          <Link
            href="/lineage"
            className="shrink-0 rounded-md border border-white/10 px-3 py-1.5 text-xs text-[#d1d5db] transition-colors hover:bg-white/[0.04] hover:text-white"
          >
            Open Lineage
          </Link>
        </div>
      ) : null}
    </Panel>
  );
}
