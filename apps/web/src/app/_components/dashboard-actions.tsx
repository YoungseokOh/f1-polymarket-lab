"use client";

import Link from "next/link";

import type { IngestionJobRun } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import React, { startTransition, useEffect, useRef, useState } from "react";
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

function isActiveJob(status: string) {
  return status === "queued" || status === "running";
}

function selectLatestDemoIngestJob(
  jobs: IngestionJobRun[],
  runId?: string,
): IngestionJobRun | null {
  return (
    jobs.find((job) => job.id === runId) ??
    jobs.find((job) => job.jobName === "ingest-demo") ??
    null
  );
}

export function DashboardActions({
  latestDemoIngestJob,
}: {
  latestDemoIngestJob: IngestionJobRun | null;
}) {
  const router = useRouter();
  const [demoJob, setDemoJob] = useState(latestDemoIngestJob);
  const pollLatestDemoIngestJobRef = useRef<
    ((runId?: string) => Promise<IngestionJobRun | null>) | null
  >(null);

  useEffect(() => {
    setDemoJob(latestDemoIngestJob);
  }, [latestDemoIngestJob]);

  pollLatestDemoIngestJobRef.current = async (runId?: string) => {
    const jobs = await sdk.ingestionJobs({ limit: 25 });
    const nextJob = selectLatestDemoIngestJob(jobs, runId);
    setDemoJob(nextJob);
    if (nextJob && !isActiveJob(nextJob.status)) {
      startTransition(() => {
        router.refresh();
      });
    }
    return nextJob;
  };

  async function refreshLatestDemoIngestJob(runId?: string) {
    const jobs = await sdk.ingestionJobs({ limit: 25 });
    const nextJob = selectLatestDemoIngestJob(jobs, runId);
    setDemoJob(nextJob);
    if (nextJob && !isActiveJob(nextJob.status)) {
      startTransition(() => {
        router.refresh();
      });
    }
    return nextJob;
  }

  const activeDemoJobId =
    demoJob && isActiveJob(demoJob.status) ? demoJob.id : null;

  useEffect(() => {
    if (!activeDemoJobId) {
      return;
    }

    let cancelled = false;
    let timer: number | null = null;

    const poll = async () => {
      let shouldContinue = true;
      try {
        const nextJob =
          await pollLatestDemoIngestJobRef.current?.(activeDemoJobId);
        shouldContinue = nextJob ? isActiveJob(nextJob.status) : true;
      } catch {
        shouldContinue = true;
      } finally {
        if (!cancelled && shouldContinue) {
          timer = window.setTimeout(poll, 5000);
        }
      }
    };

    timer = window.setTimeout(poll, 5000);
    return () => {
      cancelled = true;
      if (timer != null) {
        window.clearTimeout(timer);
      }
    };
  }, [activeDemoJobId]);

  return (
    <Panel title="Quick Actions" eyebrow="Pipeline Controls">
      <div className="flex flex-wrap items-start gap-3">
        <ActionButton
          label="Sync F1 Calendar"
          onAction={async () => {
            const res = await sdk.syncCalendar({ season: 2026 });
            startTransition(() => {
              router.refresh();
            });
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
            startTransition(() => {
              router.refresh();
            });
            return res.message;
          }}
        />
        <ActionButton
          label="Ingest Demo Data"
          variant="secondary"
          onAction={async () => {
            const res = await sdk.ingestDemo(buildDashboardDemoIngestRequest());
            const jobRunId =
              typeof res.job_run_id === "string"
                ? res.job_run_id
                : res.details &&
                    typeof res.details === "object" &&
                    typeof res.details.job_run_id === "string"
                  ? res.details.job_run_id
                  : undefined;
            await refreshLatestDemoIngestJob(jobRunId);
            startTransition(() => {
              router.refresh();
            });
            return res.message;
          }}
        />
      </div>
      {demoJob ? (
        <div className="mt-4 flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3">
          <div className="min-w-0">
            <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-[#6b7280]">
              Latest Demo Ingest
            </p>
            <p
              className={`mt-1 text-sm font-medium ${jobTone(demoJob.status)}`}
            >
              {demoJob.status === "queued"
                ? "Queued"
                : demoJob.status === "running"
                  ? "Running"
                  : demoJob.status === "completed"
                    ? "Completed"
                    : "Failed"}
            </p>
            <p className="mt-1 truncate text-xs text-[#9ca3af]">
              Job {demoJob.id} started{" "}
              {new Date(demoJob.startedAt).toLocaleTimeString("en-US", {
                hour: "2-digit",
                minute: "2-digit",
                hour12: false,
              })}
            </p>
            <p className="mt-1 text-xs text-[#9ca3af]">
              {isActiveJob(demoJob.status)
                ? "Refreshing automatically while this ingest is active."
                : demoJob.recordsWritten != null
                  ? `${demoJob.recordsWritten} records written`
                  : "No row-count summary recorded."}
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
