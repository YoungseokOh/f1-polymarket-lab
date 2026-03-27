"use client";

import type { WeekendCockpitStatus } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import React from "react";
import { useEffect, useState } from "react";
import { SessionTimeline } from "./session-timeline";

function formatDateTime(value: string | null | undefined) {
  if (!value) return "—";
  return new Date(value).toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function stepTone(status: string): "default" | "good" | "warn" | "live" {
  if (status === "completed" || status === "skipped") return "good";
  if (status === "blocked") return "warn";
  if (status === "ready") return "live";
  return "default";
}

function focusTone(status: WeekendCockpitStatus["focusStatus"]) {
  if (status === "live") return "live";
  if (status === "ended") return "good";
  return "default";
}

function focusStatusLabel(status: WeekendCockpitStatus["focusStatus"]) {
  if (status === "live") return "Live";
  if (status === "ended") return "Ended";
  return "Up next";
}

function stepStatusLabel(status: string) {
  return (
    {
      blocked: "Blocked",
      completed: "Done",
      pending: "Pending",
      ready: "Ready",
      skipped: "Skipped",
    }[status] ?? status
  );
}

function pluralize(value: number, singular: string, plural = `${singular}s`) {
  return `${value} ${value === 1 ? singular : plural}`;
}

function formatRelativeWindow(target: string | null | undefined, now: string) {
  if (!target) return null;
  const deltaMs = new Date(target).getTime() - new Date(now).getTime();
  const absMinutes = Math.round(Math.abs(deltaMs) / 60000);
  const days = Math.floor(absMinutes / (60 * 24));
  const hours = Math.floor((absMinutes % (60 * 24)) / 60);
  const minutes = absMinutes % 60;
  const parts = [
    days > 0 ? pluralize(days, "day") : null,
    hours > 0 ? pluralize(hours, "hour") : null,
    pluralize(minutes, "minute"),
  ].filter(Boolean);
  return parts.join(" ");
}

function focusDetail(status: WeekendCockpitStatus) {
  const focus = status.focusSession;
  if (!focus) return "The weekend session flow is unavailable right now.";
  if (status.focusStatus === "live") {
    const remaining = formatRelativeWindow(focus.dateEndUtc, status.now);
    return remaining
      ? `Ends in ${remaining}.`
      : "This session is currently live.";
  }
  if (status.focusStatus === "upcoming") {
    const untilStart = formatRelativeWindow(focus.dateStartUtc, status.now);
    return untilStart
      ? `Starts in ${untilStart}.`
      : "The next session starts soon.";
  }
  const sinceEnd = formatRelativeWindow(focus.dateEndUtc, status.now);
  return sinceEnd ? `Ended ${sinceEnd} ago.` : "This session has ended.";
}

function feedbackTone(status: "ok" | "error") {
  return status === "ok"
    ? "border-emerald-500/20 bg-emerald-500/10 text-emerald-200"
    : "border-[#e10600]/20 bg-[#e10600]/10 text-[#ffb4b1]";
}

function feedbackMessage(error: unknown) {
  if (error instanceof Error && error.message) return error.message;
  return "An unknown error occurred.";
}

function sessionDisplayName(sessionCode: string | null | undefined) {
  return (
    {
      FP1: "FP1",
      FP2: "FP2",
      FP3: "FP3",
      Q: "Qualifying",
      R: "Race",
    }[sessionCode ?? ""] ??
    sessionCode ??
    "Session"
  );
}

export function WeekendCockpitPanel({
  initialStatus,
}: {
  initialStatus: WeekendCockpitStatus | null;
}) {
  const router = useRouter();
  const [status, setStatus] = useState(initialStatus);
  const [selectedGp, setSelectedGp] = useState(
    initialStatus?.selectedGpShortCode ?? "",
  );
  const [feedback, setFeedback] = useState<{
    status: "ok" | "error";
    message: string;
  } | null>(null);
  const [isLoadingStatus, setIsLoadingStatus] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);

  useEffect(() => {
    setStatus(initialStatus);
    setSelectedGp(initialStatus?.selectedGpShortCode ?? "");
    setFeedback(null);
    setIsLoadingStatus(false);
    setIsRunning(false);
    setShowAdvanced(false);
  }, [initialStatus]);

  async function loadStatus(gpShortCode?: string) {
    const next = await sdk.weekendCockpitStatus(gpShortCode);
    setStatus(next);
    return next;
  }

  async function handleSelectionChange(nextGp: string) {
    setSelectedGp(nextGp);
    setFeedback(null);
    setIsLoadingStatus(true);
    try {
      await loadStatus(nextGp);
    } catch (error) {
      setFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      setIsLoadingStatus(false);
    }
  }

  async function handleRun() {
    if (!selectedGp) return;
    setFeedback(null);
    setIsRunning(true);
    try {
      const result = await sdk.runWeekendCockpit({
        gp_short_code: selectedGp,
      });
      setFeedback({ status: "ok", message: result.message });
      await loadStatus(selectedGp);
      router.refresh();
    } catch (error) {
      setFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      setIsRunning(false);
    }
  }

  function toggleAdvanced() {
    setShowAdvanced((current) => !current);
  }

  if (!status) {
    return (
      <Panel title="Weekend cockpit" eyebrow="Paper trading">
        <p className="text-sm text-[#9ca3af]">
          Unable to load the current cockpit status.
        </p>
      </Panel>
    );
  }

  const selectedConfig = status.selectedConfig;
  const autoConfig = status.availableConfigs.find(
    (config) => config.short_code === status.autoSelectedGpShortCode,
  );
  const sourceLabel = selectedConfig.source_session_code
    ? `${sessionDisplayName(selectedConfig.source_session_code)} results`
    : "Pre-weekend data";
  const runDisabled = !status.readyToRun || isLoadingStatus || isRunning;
  const blockedSteps = status.steps.filter((step) => step.status === "blocked");

  return (
    <Panel title="Weekend cockpit" eyebrow="Paper trading">
      <div className="space-y-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <p className="text-sm font-semibold text-white">
              {status.meeting?.meetingName ?? selectedConfig.name}
            </p>
            <h3 className="text-xl font-semibold text-white">
              {selectedConfig.display_label}
            </h3>
            <p className="text-sm text-[#9ca3af]">
              {selectedConfig.display_description}
            </p>
            <p className="text-xs text-[#6b7280]">
              Recommended stage:{" "}
              {autoConfig?.display_label ?? status.autoSelectedGpShortCode}
            </p>
          </div>

          <div className="w-full max-w-sm space-y-2">
            <label
              htmlFor="weekend-cockpit-gp"
              className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]"
            >
              Stage
            </label>
            <select
              id="weekend-cockpit-gp"
              value={selectedGp}
              onChange={(event) => {
                void handleSelectionChange(event.target.value);
              }}
              className="w-full rounded-lg border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
              disabled={isLoadingStatus || isRunning}
            >
              {status.availableConfigs.map((config) => (
                <option key={config.short_code} value={config.short_code}>
                  {config.display_label}
                </option>
              ))}
            </select>
            <p className="text-xs text-[#6b7280]">
              {selectedConfig.display_description}
            </p>
          </div>
        </div>

        <div className="grid gap-3 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
            <div className="space-y-3">
              <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                Current focus
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-lg font-semibold text-white">
                  {status.focusSession?.sessionCode ?? "—"}
                </p>
                <Badge tone={focusTone(status.focusStatus)}>
                  {focusStatusLabel(status.focusStatus)}
                </Badge>
              </div>
              <p className="text-sm text-[#9ca3af]">
                {status.focusSession?.sessionName ??
                  "No session information available"}
              </p>
              <p className="text-xs text-[#6b7280]">
                {focusDetail(status)}
                {status.focusSession && (
                  <>
                    {" · "}
                    {formatDateTime(status.focusSession.dateStartUtc)}
                    {" → "}
                    {formatDateTime(status.focusSession.dateEndUtc)}
                  </>
                )}
              </p>
              <SessionTimeline
                completedCodes={status.timelineCompletedCodes}
                activeCode={status.timelineActiveCode}
              />
            </div>
          </div>

          <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
            <div className="space-y-3">
              <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                Next action
              </p>
              <h4 className="text-lg font-semibold text-white">
                {status.primaryActionTitle}
              </h4>
              <p className="text-sm text-[#9ca3af]">
                {status.primaryActionDescription}
              </p>
              <button
                type="button"
                onClick={() => {
                  void handleRun();
                }}
                disabled={runDisabled}
                aria-busy={isRunning}
                className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-[#e10600] px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-[#b80500] disabled:cursor-not-allowed disabled:opacity-50"
              >
                {(isLoadingStatus || isRunning) && (
                  <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
                )}
                {status.primaryActionCta}
              </button>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            What this stage does
          </p>
          <p className="mt-2 text-sm text-[#9ca3af]">{status.explanation}</p>
        </div>

        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
            <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
              Grand Prix
            </p>
            <p className="mt-2 text-sm font-semibold text-white">
              {status.meeting?.meetingName ?? "Weekend details unavailable"}
            </p>
            <p className="mt-1 text-xs text-[#6b7280]">
              {status.meeting
                ? `${status.meeting.location ?? "—"}, ${status.meeting.countryName ?? "—"}`
                : "Load the weekend schedule to see this information."}
            </p>
          </div>
          <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
            <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
              Source data
            </p>
            <p className="mt-2 text-sm font-semibold text-white">
              {sourceLabel}
            </p>
            <p className="mt-1 text-xs text-[#6b7280]">
              {formatDateTime(status.sourceSession?.dateEndUtc)}
            </p>
          </div>
          <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
            <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
              Target market session
            </p>
            <p className="mt-2 text-sm font-semibold text-white">
              {status.targetSession?.sessionName ??
                sessionDisplayName(selectedConfig.target_session_code)}
            </p>
            <p className="mt-1 text-xs text-[#6b7280]">
              {formatDateTime(status.targetSession?.dateStartUtc)}
            </p>
          </div>
        </div>

        {feedback && (
          <div
            className={`rounded-xl border px-4 py-3 text-sm ${feedbackTone(feedback.status)}`}
          >
            {feedback.message}
          </div>
        )}

        {blockedSteps.length > 0 && (
          <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
            <p className="font-medium">Current blockers</p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              {blockedSteps.map((step) => (
                <li key={step.key}>{step.detail}</li>
              ))}
            </ul>
          </div>
        )}

        <div className="space-y-2">
          {status.steps.map((step) => (
            <div
              key={step.key}
              className="rounded-xl border border-white/[0.06] bg-[#11131d] px-4 py-3"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-sm font-medium text-white">{step.label}</p>
                  <p className="mt-1 text-xs text-[#9ca3af]">{step.detail}</p>
                </div>
                <div className="flex items-center gap-2">
                  {step.resourceLabel && (
                    <span className="text-[11px] text-[#6b7280]">
                      {step.resourceLabel}
                    </span>
                  )}
                  <Badge tone={stepTone(step.status)}>
                    {stepStatusLabel(step.status)}
                  </Badge>
                </div>
              </div>
            </div>
          ))}
        </div>

        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] px-4 py-3">
          <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
            Recent run
          </p>
          {status.latestPaperSession ? (
            <div className="mt-2 flex flex-wrap items-center justify-between gap-3 text-sm">
              <div>
                <p className="font-medium text-white">
                  A previous run already exists.
                </p>
                <p className="text-xs text-[#6b7280]">
                  {formatDateTime(status.latestPaperSession.startedAt)}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Badge tone="good">
                  {status.latestPaperSession.status === "settled"
                    ? "Settled"
                    : status.latestPaperSession.status}
                </Badge>
              </div>
            </div>
          ) : (
            <p className="mt-2 text-sm text-[#9ca3af]">
              No paper-trading run exists for this stage yet.
            </p>
          )}
        </div>

        <details
          open={showAdvanced}
          className="rounded-xl border border-white/[0.06] bg-[#11131d] px-4 py-3"
        >
          <summary
            className="cursor-pointer text-sm font-medium text-white"
            onClick={(event) => {
              event.preventDefault();
              toggleAdvanced();
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                toggleAdvanced();
              }
            }}
          >
            Show advanced details
          </summary>
          {showAdvanced && (
            <div className="mt-4 grid gap-3 text-sm text-[#9ca3af] md:grid-cols-2">
              <div className="space-y-1">
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Selected stage
                </p>
                <p>{selectedConfig.short_code}</p>
                <p>{selectedConfig.variant}</p>
                <p>{selectedConfig.market_taxonomy}</p>
                <p>{selectedConfig.stage_label}</p>
              </div>
              <div className="space-y-1">
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Session identifiers
                </p>
                <p>
                  Source session:{" "}
                  {status.sourceSession
                    ? `${status.sourceSession.sessionCode} · ${status.sourceSession.sessionKey}`
                    : "None"}
                </p>
                <p>
                  Target session:{" "}
                  {status.targetSession
                    ? `${status.targetSession.sessionCode} · ${status.targetSession.sessionKey}`
                    : "None"}
                </p>
                <p>Auto-selected code: {status.autoSelectedGpShortCode}</p>
                {status.latestPaperSession && (
                  <p>Latest run ID: {status.latestPaperSession.id}</p>
                )}
              </div>
            </div>
          )}
        </details>
      </div>
    </Panel>
  );
}
