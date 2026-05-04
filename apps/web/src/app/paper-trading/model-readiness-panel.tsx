"use client";

import type { ModelRun, WeekendCockpitStatus } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import React from "react";
import { useMemo, useState } from "react";
import { buildPaperTradingLocalRefreshRequest } from "../../lib/action-defaults";

const PROMOTION_THRESHOLDS = {
  totalPnlMin: 0,
  roiPctMin: 0,
  betCountMin: 20,
  eceMax: 0.08,
  familyPnlShareMax: 0.65,
};

function metricNumber(
  metrics: Record<string, unknown> | null,
  key: string,
  fallback: number,
) {
  const value = metrics?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function promotionGate(run: ModelRun) {
  const actuals = {
    totalPnl: metricNumber(run.metricsJson, "total_pnl", 0),
    roiPct: metricNumber(run.metricsJson, "roi_pct", 0),
    betCount: metricNumber(run.metricsJson, "bet_count", 0),
    ece: metricNumber(run.metricsJson, "ece", 1),
    familyPnlShareMax: metricNumber(run.metricsJson, "family_pnl_share_max", 1),
  };
  const failedRules: string[] = [];
  if (!run.artifactUri) {
    failedRules.push("artifact is missing");
  }
  if (actuals.totalPnl <= PROMOTION_THRESHOLDS.totalPnlMin) {
    failedRules.push("PnL must be positive");
  }
  if (actuals.roiPct <= PROMOTION_THRESHOLDS.roiPctMin) {
    failedRules.push("ROI must be positive");
  }
  if (actuals.betCount < PROMOTION_THRESHOLDS.betCountMin) {
    failedRules.push(`needs ${PROMOTION_THRESHOLDS.betCountMin}+ bets`);
  }
  if (actuals.ece > PROMOTION_THRESHOLDS.eceMax) {
    failedRules.push("calibration error is too high");
  }
  if (actuals.familyPnlShareMax > PROMOTION_THRESHOLDS.familyPnlShareMax) {
    failedRules.push("PnL is too concentrated");
  }
  return {
    actuals,
    eligible: failedRules.length === 0,
    failedRules,
  };
}

function formatPct(value: number | null | undefined) {
  if (value == null) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatUsd(value: number | null | undefined) {
  if (value == null) return "-";
  return `${value >= 0 ? "+" : ""}$${value.toFixed(2)}`;
}

function shortId(value: string | null | undefined) {
  if (!value) return "-";
  return value.length > 10 ? `${value.slice(0, 8)}...` : value;
}

function stageLabel(stage: string | null) {
  if (!stage) return "No model gate";
  if (stage === "multitask_qr") return "Qualifying/Race model";
  if (stage === "sq_pole_live_v1") return "Sprint Qualifying model";
  if (stage === "sprint_winner_live_v1") return "Sprint model";
  return stage;
}

function cleanActionMessage(error: unknown, fallback: string) {
  if (!(error instanceof Error) || !error.message) {
    return fallback;
  }
  const message = error.message
    .replace(/^API request failed:\s*/, "")
    .replace(/^\d{3}\s+/, "");

  if (
    message.includes("401 Unauthorized") &&
    message.includes("api.openf1.org")
  ) {
    return "OpenF1 rejected the request. Use local GP refresh here, or fix OpenF1 credentials before a full refresh.";
  }

  return message
    .replace(/Client error '\d{3} [^']+' for url '[^']+'\s*/g, "")
    .replace(/For more information check:\s*https?:\/\/\S+/g, "")
    .trim();
}

type WorkflowActionStatus = "error" | "running" | "success";

type WorkflowProgress = {
  step: number;
  title: string;
  status: WorkflowActionStatus;
  message: string;
};

const WORKFLOW_STEP_COUNT = 5;

function workflowStatusBadge(progress: WorkflowProgress | null) {
  if (!progress) {
    return <Badge tone="default">Not started</Badge>;
  }
  if (progress.status === "running") {
    return <Badge tone="live">Running</Badge>;
  }
  if (progress.status === "success") {
    return <Badge tone="good">Done</Badge>;
  }
  return <Badge tone="warn">Blocked</Badge>;
}

function workflowProgressTone(status: WorkflowActionStatus | null) {
  if (status === "success") return "bg-race-green";
  if (status === "error") return "bg-amber-400";
  if (status === "running") return "bg-[#e10600]";
  return "bg-white/[0.12]";
}

function workflowProgressStats(progress: WorkflowProgress | null) {
  const completedSteps =
    progress?.status === "success"
      ? progress.step
      : progress
        ? Math.max(progress.step - 1, 0)
        : 0;
  const percent = Math.round((completedSteps / WORKFLOW_STEP_COUNT) * 100);
  const summary =
    progress?.status === "running"
      ? `${percent}% complete. Step ${progress.step} is running.`
      : progress?.status === "error"
        ? `${percent}% complete. Blocked at step ${progress.step}.`
        : `${percent}% complete. ${completedSteps} of ${WORKFLOW_STEP_COUNT} steps done.`;

  return {
    completedSteps,
    percent,
    summary,
  };
}

function WorkflowProgressBar({
  progress,
  recommendedStep,
}: {
  progress: WorkflowProgress | null;
  recommendedStep: number;
}) {
  const stats = workflowProgressStats(progress);
  const fillClass =
    progress?.status === "error"
      ? "bg-amber-400"
      : progress?.status === "running"
        ? "bg-[#e10600]"
        : "bg-race-green";

  return (
    <div className="rounded-md border border-white/[0.06] bg-[#0b0d14] p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
            Progress
          </p>
          <p className="mt-1 text-xs font-semibold text-white">
            {progress
              ? `Step ${progress.step}: ${progress.title}`
              : `Next: step ${recommendedStep}`}
          </p>
          <p
            className={`mt-1 text-xs leading-5 ${
              progress?.status === "error"
                ? "text-amber-200"
                : progress?.status === "success"
                  ? "text-race-green"
                  : "text-[#9ca3af]"
            }`}
          >
            {progress
              ? progress.message
              : "Start with Refresh local data. The next step will be highlighted after each action."}
          </p>
          <p className="mt-1 text-xs font-semibold text-white">
            {stats.summary}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <p className="text-2xl font-semibold tabular-nums text-white">
            {stats.percent}%
          </p>
          {workflowStatusBadge(progress)}
        </div>
      </div>
      <div
        className="mt-3 h-2 overflow-hidden rounded-full bg-white/[0.08]"
        role="progressbar"
        aria-label="Model workflow progress"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={stats.percent}
        tabIndex={0}
      >
        <div
          className={`h-full rounded-full transition-all duration-300 ${fillClass}`}
          style={{ width: `${stats.percent}%` }}
        />
      </div>
      <div className="mt-3 grid grid-cols-5 gap-1.5">
        {Array.from(
          { length: WORKFLOW_STEP_COUNT },
          (_, index) => index + 1,
        ).map((step) => {
          const isCurrent = progress?.step === step;
          const isRecommended = recommendedStep === step && !isCurrent;
          const isComplete = step <= stats.completedSteps;
          const color = isCurrent
            ? workflowProgressTone(progress.status)
            : isComplete
              ? "bg-race-green"
              : isRecommended
                ? "bg-[#e10600]"
                : "bg-white/[0.12]";
          return (
            <div
              key={step}
              className={`h-1.5 rounded-full ${color} ${
                isCurrent || isRecommended ? "opacity-100" : "opacity-60"
              }`}
            />
          );
        })}
      </div>
    </div>
  );
}

function WorkflowButton({
  disabled,
  onRun,
  progressStep,
  onProgress,
  variant = "secondary",
  children,
}: {
  disabled?: boolean;
  onRun: () => Promise<string>;
  progressStep: { index: number; title: string };
  onProgress: (progress: WorkflowProgress) => void;
  variant?: "primary" | "secondary";
  children: ReactNode;
}) {
  const router = useRouter();
  const [isRunning, setIsRunning] = useState(false);

  async function handleClick() {
    setIsRunning(true);
    onProgress({
      step: progressStep.index,
      title: progressStep.title,
      status: "running",
      message: `${progressStep.title} is running.`,
    });
    try {
      const nextMessage = await onRun();
      onProgress({
        step: progressStep.index,
        title: progressStep.title,
        status: "success",
        message: nextMessage,
      });
      router.refresh();
    } catch (error) {
      const errorMessage = cleanActionMessage(error, "Action failed.");
      onProgress({
        step: progressStep.index,
        title: progressStep.title,
        status: "error",
        message: errorMessage,
      });
    } finally {
      setIsRunning(false);
    }
  }

  const buttonClass =
    variant === "primary"
      ? "border-[#e10600] bg-[#e10600] text-white hover:bg-[#b80500]"
      : "border-white/[0.12] bg-white/[0.04] text-[#d1d5db] hover:border-white/[0.2] hover:bg-white/[0.08]";

  return (
    <div className="flex min-w-0 flex-col items-stretch gap-1 md:items-end">
      <button
        type="button"
        onClick={() => {
          void handleClick();
        }}
        disabled={disabled || isRunning}
        className={`inline-flex h-9 min-w-[132px] items-center justify-center gap-2 rounded-md border px-3 text-xs font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${buttonClass}`}
      >
        {isRunning ? (
          <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
        ) : null}
        {children}
      </button>
    </div>
  );
}

function WorkflowStep({
  index,
  title,
  description,
  state,
  children,
}: {
  index: number;
  title: string;
  description: string;
  state: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="grid gap-3 rounded-md border border-white/[0.06] bg-[#0b0d14] p-3 md:grid-cols-[minmax(0,1fr)_auto] md:items-center">
      <div className="flex min-w-0 gap-3">
        <span className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full border border-white/[0.12] bg-white/[0.04] text-[11px] font-semibold text-[#d1d5db]">
          {index}
        </span>
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-sm font-semibold text-white">{title}</p>
            {state}
          </div>
          <p className="mt-1 text-xs leading-5 text-[#9ca3af]">{description}</p>
        </div>
      </div>
      {children}
    </div>
  );
}

function PromotionButton({
  stage,
  modelRunId,
  disabled,
  progressStep,
  onProgress,
  variant = "primary",
  children,
}: {
  stage: string;
  modelRunId?: string;
  disabled?: boolean;
  progressStep?: { index: number; title: string };
  onProgress?: (progress: WorkflowProgress) => void;
  variant?: "primary" | "compact";
  children: ReactNode;
}) {
  const router = useRouter();
  const [isRunning, setIsRunning] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageTone, setMessageTone] = useState<"error" | "success">(
    "success",
  );

  async function handleClick() {
    setIsRunning(true);
    setMessage(null);
    if (progressStep && onProgress) {
      onProgress({
        step: progressStep.index,
        title: progressStep.title,
        status: "running",
        message: `${progressStep.title} is running.`,
      });
    }
    try {
      const result = modelRunId
        ? await sdk.promoteModelRun({ stage, model_run_id: modelRunId })
        : await sdk.promoteBestModelRun({ stage });
      setMessage(result.message);
      setMessageTone("success");
      if (progressStep && onProgress) {
        onProgress({
          step: progressStep.index,
          title: progressStep.title,
          status: "success",
          message: result.message,
        });
      }
      router.refresh();
    } catch (error) {
      const errorMessage = cleanActionMessage(error, "Promotion failed.");
      setMessage(errorMessage);
      setMessageTone("error");
      if (progressStep && onProgress) {
        onProgress({
          step: progressStep.index,
          title: progressStep.title,
          status: "error",
          message: errorMessage,
        });
      }
    } finally {
      setIsRunning(false);
    }
  }

  const buttonClass =
    variant === "compact"
      ? "min-w-[92px] border-white/[0.12] bg-white/[0.04] text-[#d1d5db] hover:bg-white/[0.08]"
      : "min-w-[132px] border-[#e10600] bg-[#e10600] text-white hover:bg-[#b80500]";

  return (
    <div className="flex flex-col items-stretch gap-1 md:items-end">
      <button
        type="button"
        onClick={() => {
          void handleClick();
        }}
        disabled={disabled || isRunning}
        className={`inline-flex h-9 items-center justify-center gap-2 rounded-md border px-3 text-xs font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${buttonClass}`}
      >
        {isRunning ? (
          <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
        ) : null}
        {children}
      </button>
      {message && !onProgress ? (
        <p
          className={`max-w-[26rem] text-xs leading-5 md:text-right ${
            messageTone === "error" ? "text-amber-200" : "text-race-green"
          }`}
        >
          {message}
        </p>
      ) : null}
    </div>
  );
}

export function ModelReadinessPanel({
  status,
  modelRuns,
}: {
  status: WeekendCockpitStatus | null;
  modelRuns: ModelRun[];
}) {
  const [workflowProgress, setWorkflowProgress] =
    useState<WorkflowProgress | null>(null);
  const requiredStage =
    status?.requiredStage ??
    status?.selectedConfig.required_model_stage ??
    null;
  const stageRuns = useMemo(
    () =>
      requiredStage
        ? modelRuns
            .filter((run) => run.stage === requiredStage)
            .sort(
              (left, right) =>
                +new Date(right.createdAt) - +new Date(left.createdAt),
            )
        : [],
    [modelRuns, requiredStage],
  );
  const gateRows = stageRuns.map((run) => ({
    run,
    gate: promotionGate(run),
  }));
  const rows = gateRows.slice(0, 5);
  const eligibleCount = gateRows.filter((row) => row.gate.eligible).length;
  const activeRun =
    stageRuns.find((run) => run.id === status?.activeModelRunId) ??
    stageRuns.find((run) => run.promotionStatus === "active") ??
    null;
  const badgeTone = !requiredStage
    ? "default"
    : activeRun
      ? "good"
      : eligibleCount > 0
        ? "live"
        : "warn";
  const season = status?.selectedConfig.season ?? 2026;
  const throughMeetingKey = status?.selectedConfig.meeting_key ?? null;
  const gpShortCode =
    status?.selectedGpShortCode ?? status?.selectedConfig.short_code ?? null;
  const meetingId = status?.meeting?.id ?? null;
  const hasCandidates = stageRuns.length > 0;
  const canPromote = eligibleCount > 0 && !activeRun;
  const canRunStage = Boolean(activeRun && gpShortCode);
  const recommendedStep = canRunStage
    ? 5
    : canPromote
      ? 4
      : hasCandidates
        ? 4
        : workflowProgress?.step === 1 && workflowProgress.status === "success"
          ? 2
          : workflowProgress?.step === 2 &&
              workflowProgress.status === "success"
            ? 3
            : workflowProgress?.step === 3 &&
                workflowProgress.status === "error"
              ? 1
              : 1;

  return (
    <Panel title="Model readiness" eyebrow="Paper trading">
      <div className="space-y-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-sm font-semibold text-white">
              {stageLabel(requiredStage)}
            </p>
            <p className="mt-1 text-xs text-[#6b7280]">
              {requiredStage
                ? "This stage needs one promoted model before paper trading can run."
                : "This stage does not require a promoted model."}
            </p>
          </div>
          <Badge tone={badgeTone}>
            {!requiredStage
              ? "No gate"
              : activeRun
                ? "Promoted"
                : eligibleCount > 0
                  ? "Candidate ready"
                  : "Needs model"}
          </Badge>
        </div>

        {requiredStage ? (
          <>
            <div className="flex flex-wrap gap-x-6 gap-y-2 rounded-md border border-white/[0.06] bg-[#0f1119] px-3 py-2 text-xs">
              <p className="text-[#9ca3af]">
                Stage{" "}
                <span className="font-medium text-white">{requiredStage}</span>
              </p>
              <p className="text-[#9ca3af]">
                Active{" "}
                <span className="font-medium text-white">
                  {shortId(activeRun?.id)}
                </span>
              </p>
              <p className="text-[#9ca3af]">
                Candidates{" "}
                <span className="font-medium text-white">
                  {eligibleCount} eligible / {stageRuns.length} total
                </span>
              </p>
            </div>

            <div className="rounded-md border border-white/[0.06] bg-[#0f1119] p-3">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-white">
                    Prepare this model
                  </p>
                  <p className="mt-1 text-xs leading-5 text-[#9ca3af]">
                    Work top to bottom. Disabled steps are waiting for the step
                    above.
                  </p>
                </div>
                <Badge tone={activeRun ? "good" : "warn"}>
                  {activeRun ? "Ready for paper run" : "Needs promoted model"}
                </Badge>
              </div>
              <div className="mt-3 space-y-2">
                <WorkflowProgressBar
                  progress={workflowProgress}
                  recommendedStep={recommendedStep}
                />
                <WorkflowStep
                  index={1}
                  title="Refresh local GP data"
                  description="Use saved session data, relink markets, and update prices without calling OpenF1."
                  state={
                    <Badge tone={meetingId ? "good" : "warn"}>
                      {meetingId ? "Available" : "No GP"}
                    </Badge>
                  }
                >
                  <WorkflowButton
                    disabled={!meetingId}
                    variant={recommendedStep === 1 ? "primary" : "secondary"}
                    progressStep={{ index: 1, title: "Refresh local GP data" }}
                    onProgress={setWorkflowProgress}
                    onRun={async () => {
                      const result = await sdk.refreshLatestSession(
                        buildPaperTradingLocalRefreshRequest(meetingId ?? ""),
                      );
                      return result.message;
                    }}
                  >
                    Refresh local data
                  </WorkflowButton>
                </WorkflowStep>
                <WorkflowStep
                  index={2}
                  title="Build training data"
                  description="Create as-of snapshots for this season through the selected GP."
                  state={<Badge tone="good">Available</Badge>}
                >
                  <WorkflowButton
                    variant={recommendedStep === 2 ? "primary" : "secondary"}
                    progressStep={{ index: 2, title: "Build training data" }}
                    onProgress={setWorkflowProgress}
                    onRun={async () => {
                      const result = await sdk.buildMultitaskSnapshots({
                        season,
                        through_meeting_key: throughMeetingKey,
                        stage: requiredStage,
                      });
                      const message = `${result.message} ${result.rowCount} training row(s).`;
                      return result.warnings.length > 0
                        ? `${message} ${result.warnings.length} warning(s).`
                        : message;
                    }}
                  >
                    Build data
                  </WorkflowButton>
                </WorkflowStep>
                <WorkflowStep
                  index={3}
                  title="Train model runs"
                  description="Run walk-forward training and create promotion candidates."
                  state={
                    <Badge tone={hasCandidates ? "good" : "warn"}>
                      {hasCandidates ? "Runs exist" : "No runs"}
                    </Badge>
                  }
                >
                  <WorkflowButton
                    variant={recommendedStep === 3 ? "primary" : "secondary"}
                    progressStep={{ index: 3, title: "Train model runs" }}
                    onProgress={setWorkflowProgress}
                    onRun={async () => {
                      const result = await sdk.trainMultitaskModel({
                        season,
                        stage: requiredStage,
                        min_train_gps: 2,
                      });
                      return result.message;
                    }}
                  >
                    Train model
                  </WorkflowButton>
                </WorkflowStep>
                <WorkflowStep
                  index={4}
                  title="Promote candidate"
                  description="Choose the best eligible model as the active model for this stage."
                  state={
                    <Badge tone={canPromote || activeRun ? "good" : "warn"}>
                      {activeRun
                        ? "Promoted"
                        : canPromote
                          ? "Ready"
                          : "No eligible run"}
                    </Badge>
                  }
                >
                  <PromotionButton
                    stage={requiredStage}
                    disabled={!canPromote}
                    progressStep={{ index: 4, title: "Promote candidate" }}
                    onProgress={setWorkflowProgress}
                  >
                    Promote best
                  </PromotionButton>
                </WorkflowStep>
                <WorkflowStep
                  index={5}
                  title="Run paper trading"
                  description="Generate model trade calls for the selected stage."
                  state={
                    <Badge tone={canRunStage ? "good" : "warn"}>
                      {canRunStage ? "Ready" : "Waiting"}
                    </Badge>
                  }
                >
                  <WorkflowButton
                    disabled={!canRunStage}
                    variant={recommendedStep === 5 ? "primary" : "secondary"}
                    progressStep={{ index: 5, title: "Run paper trading" }}
                    onProgress={setWorkflowProgress}
                    onRun={async () => {
                      const result = await sdk.runWeekendCockpit({
                        gp_short_code: gpShortCode ?? "",
                      });
                      return result.message;
                    }}
                  >
                    Run paper trading
                  </WorkflowButton>
                </WorkflowStep>
              </div>
            </div>

            {rows.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="min-w-full table-fixed divide-y divide-white/[0.06] text-left text-sm">
                  <colgroup>
                    <col className="w-[28%]" />
                    <col className="w-[13%]" />
                    <col className="w-[13%]" />
                    <col className="w-[13%]" />
                    <col className="w-[18%]" />
                    <col className="w-[15%]" />
                  </colgroup>
                  <thead className="text-[10px] font-bold uppercase tracking-[0.2em] text-[#6b7280]">
                    <tr>
                      <th className="py-2 pr-4">Run</th>
                      <th className="px-4 py-2 whitespace-nowrap">PnL</th>
                      <th className="px-4 py-2 whitespace-nowrap">ROI</th>
                      <th className="px-4 py-2 whitespace-nowrap">Bets</th>
                      <th className="px-4 py-2 whitespace-nowrap">Gate</th>
                      <th className="py-2 pl-4 whitespace-nowrap">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.06]">
                    {rows.map(({ run, gate }) => (
                      <tr key={run.id}>
                        <td className="py-3 pr-4">
                          <p className="font-medium text-white">
                            {shortId(run.id)}
                          </p>
                          <p className="mt-1 text-xs text-[#6b7280]">
                            {run.modelName}
                          </p>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                          {formatUsd(gate.actuals.totalPnl)}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                          {formatPct(gate.actuals.roiPct)}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap tabular-nums text-[#d1d5db]">
                          {gate.actuals.betCount.toFixed(0)}
                        </td>
                        <td className="px-4 py-3 text-xs text-[#d1d5db]">
                          {gate.eligible
                            ? "Eligible"
                            : gate.failedRules.slice(0, 2).join(", ")}
                        </td>
                        <td className="py-3 pl-4">
                          <PromotionButton
                            stage={requiredStage}
                            modelRunId={run.id}
                            variant="compact"
                            disabled={
                              !gate.eligible || run.id === activeRun?.id
                            }
                          >
                            Promote
                          </PromotionButton>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="rounded-md border border-amber-500/20 bg-amber-500/10 px-3 py-2 text-sm text-amber-100">
                No model runs yet. Build training data first, then train the
                model.
              </p>
            )}
          </>
        ) : null}
      </div>
    </Panel>
  );
}
