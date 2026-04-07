"use client";

import type {
  CaptureLiveWeekendResponse,
  LiveTradeExecution,
  LiveSignalRow as LiveTradeSignalRow,
  LiveTradeTicket,
  RunWeekendCockpitResponse,
  WeekendCockpitStatus,
} from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Badge, Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import React from "react";
import { useEffect, useRef, useState } from "react";
import type { MeetingRefreshTarget } from "../../lib/session-refresh";
import { MeetingRefreshButton } from "./meeting-refresh-button";
import { SessionTimeline } from "./session-timeline";

function formatDateTime(value: string | null | undefined) {
  if (!value) return "—";
  return `${new Date(value).toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
  })} UTC`;
}

function formatCalendarDateRange(
  meeting: WeekendCockpitStatus["calendarMeetings"][number],
) {
  if (!meeting.startDateUtc && !meeting.endDateUtc) return "Dates unavailable";
  const formatter = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const start = meeting.startDateUtc
    ? formatter.format(new Date(meeting.startDateUtc))
    : "TBD";
  const end = meeting.endDateUtc
    ? formatter.format(new Date(meeting.endDateUtc))
    : "TBD";
  return start === end ? start : `${start} → ${end}`;
}

function formatEventFormat(value: string | null | undefined) {
  if (!value) return "Format pending";
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
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

function buildRunFeedbackMessage(result: RunWeekendCockpitResponse) {
  if (result.message.toLowerCase().includes("settled")) {
    return result.message;
  }
  const settlement = result.details?.settlement;
  if (!settlement) return result.message;

  const parts: string[] = [result.message];
  if (settlement.settledPositions > 0) {
    parts.push(
      `Settled ${settlement.settledPositions} prior ticket${settlement.settledPositions === 1 ? "" : "s"}.`,
    );
  }
  if (settlement.unresolvedPositions > 0) {
    parts.push(
      `${settlement.unresolvedPositions} manual ticket${settlement.unresolvedPositions === 1 ? "" : "s"} still need a driver match.`,
    );
  }
  return parts.join(" ");
}

function buildLiveCaptureFeedbackMessage(result: CaptureLiveWeekendResponse) {
  return (
    `${result.message} ` +
    `OpenF1 ${result.openf1Messages} · ` +
    `Polymarket ${result.polymarketMessages} · ` +
    `Records ${result.recordsWritten}.`
  );
}

function formatCaptureTime(value: string) {
  return `${new Date(value).toLocaleString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "UTC",
  })} UTC`;
}

function formatCents(value: number | null | undefined) {
  if (value == null) return "—";
  return `${(value * 100).toFixed(1)}¢`;
}

function formatEdgePoints(value: number | null | undefined) {
  if (value == null) return "—";
  return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)} pts`;
}

function formatPriceMove(value: number | null | undefined) {
  if (value == null) return "—";
  return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}¢`;
}

function parsePositiveNumberInput(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return parsed;
}

function formatMessageRate(messages: number, seconds: number) {
  if (seconds <= 0) return "0.00 msg/s";
  return `${(messages / seconds).toFixed(2)} msg/s`;
}

function topLiveCount(
  items: CaptureLiveWeekendResponse["summary"]["openf1Topics"],
) {
  return items[0] ?? null;
}

function currentLiveQuoteProbability(
  quote: CaptureLiveWeekendResponse["summary"]["marketQuotes"][number],
) {
  if (quote.price != null) return quote.price;
  if (quote.midpoint != null) return quote.midpoint;
  if (quote.bestBid != null && quote.bestAsk != null) {
    return (quote.bestBid + quote.bestAsk) / 2;
  }
  return quote.bestAsk ?? quote.bestBid ?? null;
}

function currentLiveQuoteSpread(
  quote: CaptureLiveWeekendResponse["summary"]["marketQuotes"][number],
) {
  if (quote.spread != null) return quote.spread;
  if (quote.bestBid == null || quote.bestAsk == null) return null;
  return quote.bestAsk - quote.bestBid;
}

function sessionDisplayName(sessionCode: string | null | undefined) {
  return (
    {
      FP1: "FP1",
      FP2: "FP2",
      FP3: "FP3",
      SQ: "Sprint Qualifying",
      S: "Sprint",
      Q: "Qualifying",
      R: "Race",
    }[sessionCode ?? ""] ??
    sessionCode ??
    "Session"
  );
}

function isSessionLive(
  session: WeekendCockpitStatus["targetSession"],
  now: string,
) {
  if (!session?.dateStartUtc || !session.dateEndUtc) return false;
  const nowMs = new Date(now).getTime();
  const startMs = new Date(session.dateStartUtc).getTime();
  const endMs = new Date(session.dateEndUtc).getTime();
  return nowMs >= startMs && nowMs <= endMs;
}

function liveMonitorState(status: WeekendCockpitStatus) {
  const targetSession = status.targetSession;
  const targetName =
    targetSession?.sessionName ??
    sessionDisplayName(status.selectedConfig.target_session_code);
  const discoverStep = status.steps.find(
    (step) => step.key === "discover_target_markets",
  );
  const marketCount =
    discoverStep?.status === "completed" ? (discoverStep.count ?? 0) : 0;

  if (!targetSession) {
    return {
      enabled: false,
      detail: `${targetName} session details are unavailable.`,
      marketCount,
    };
  }
  if (!targetSession.dateStartUtc || !targetSession.dateEndUtc) {
    return {
      enabled: false,
      detail: `${targetName} timing is unavailable for live watch.`,
      marketCount,
    };
  }
  const now = status.now;
  if (!isSessionLive(targetSession, now)) {
    const nowMs = new Date(now).getTime();
    const startMs = new Date(targetSession.dateStartUtc).getTime();
    if (nowMs < startMs) {
      return {
        enabled: false,
        detail: `Available once ${targetName} goes live at ${formatDateTime(targetSession.dateStartUtc)}.`,
        marketCount,
      };
    }
    return {
      enabled: false,
      detail: `${targetName} ended at ${formatDateTime(targetSession.dateEndUtc)}.`,
      marketCount,
    };
  }
  if (discoverStep?.status !== "completed" || marketCount < 1) {
    return {
      enabled: false,
      detail: `Run ${targetName} market discovery before starting live watch.`,
      marketCount,
    };
  }
  return {
    enabled: true,
    detail: `Watches ${marketCount} linked market${marketCount === 1 ? "" : "s"} for ${targetName} for 20 seconds.`,
    marketCount,
  };
}

export function WeekendCockpitPanel({
  initialStatus,
  refreshTargetsByGpShortCode = {},
}: {
  initialStatus: WeekendCockpitStatus | null;
  refreshTargetsByGpShortCode?: Record<string, MeetingRefreshTarget | null>;
}) {
  type LiveCaptureSample = {
    id: string;
    capturedAt: string;
    result: CaptureLiveWeekendResponse;
  };
  type LiveSignalRow = LiveTradeSignalRow & {
    priceMove: number | null;
    priceMoveLabel: string;
    priceSource: "live" | "stored";
  };

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
  const [isCapturingLive, setIsCapturingLive] = useState(false);
  const [isLiveWatchActive, setIsLiveWatchActive] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [liveFeedback, setLiveFeedback] = useState<{
    status: "ok" | "error";
    message: string;
  } | null>(null);
  const [liveSamples, setLiveSamples] = useState<LiveCaptureSample[]>([]);
  const [liveSignalRows, setLiveSignalRows] = useState<LiveSignalRow[]>([]);
  const [isLoadingLiveSignals, setIsLoadingLiveSignals] = useState(false);
  const [liveSignalError, setLiveSignalError] = useState<string | null>(null);
  const [liveSignalBlockers, setLiveSignalBlockers] = useState<string[]>([]);
  const [manualTradeMarketId, setManualTradeMarketId] = useState<string | null>(
    null,
  );
  const [liveTickets, setLiveTickets] = useState<LiveTradeTicket[]>([]);
  const [liveExecutions, setLiveExecutions] = useState<LiveTradeExecution[]>(
    [],
  );
  const [selectedLiveTicketId, setSelectedLiveTicketId] = useState<
    string | null
  >(null);
  const [liveFillSize, setLiveFillSize] = useState("");
  const [liveFillPrice, setLiveFillPrice] = useState("");
  const [liveFillNote, setLiveFillNote] = useState("");
  const [liveFillReference, setLiveFillReference] = useState("");
  const [isRecordingLiveFill, setIsRecordingLiveFill] = useState(false);
  const [manualTradeShares, setManualTradeShares] = useState("10");
  const [manualTradeMinEdgePts, setManualTradeMinEdgePts] = useState("5");
  const [manualTradeMaxSpreadCents, setManualTradeMaxSpreadCents] =
    useState("");
  const liveWatchStopRef = useRef(false);
  const liveSignalRequestRef = useRef(0);
  const selectedGpRef = useRef(initialStatus?.selectedGpShortCode ?? "");
  const statusRef = useRef(initialStatus);
  const latestLiveSample = liveSamples[0] ?? null;
  const latestLiveQuotes =
    latestLiveSample?.result.summary.marketQuotes ?? null;

  useEffect(() => {
    setStatus(initialStatus);
    setSelectedGp(initialStatus?.selectedGpShortCode ?? "");
    selectedGpRef.current = initialStatus?.selectedGpShortCode ?? "";
    statusRef.current = initialStatus;
    setFeedback(null);
    setIsLoadingStatus(false);
    setIsRunning(false);
    liveWatchStopRef.current = true;
    setIsCapturingLive(false);
    setIsLiveWatchActive(false);
    setShowAdvanced(false);
    setLiveFeedback(null);
    setLiveSamples([]);
    setLiveSignalRows([]);
    setIsLoadingLiveSignals(false);
    setLiveSignalError(null);
    setLiveSignalBlockers([]);
    setManualTradeMarketId(null);
    setLiveTickets([]);
    setLiveExecutions([]);
    setSelectedLiveTicketId(null);
    setLiveFillSize("");
    setLiveFillPrice("");
    setLiveFillNote("");
    setLiveFillReference("");
    setIsRecordingLiveFill(false);
  }, [initialStatus]);

  useEffect(() => {
    selectedGpRef.current = selectedGp;
  }, [selectedGp]);

  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  useEffect(() => {
    return () => {
      liveWatchStopRef.current = true;
    };
  }, []);

  useEffect(() => {
    const gpShortCode = status?.selectedGpShortCode ?? null;
    if (gpShortCode === null) {
      setLiveSignalRows([]);
      setLiveSignalError(null);
      setLiveSignalBlockers([]);
      setIsLoadingLiveSignals(false);
      return;
    }
    const activeGpShortCode: string = gpShortCode;

    let cancelled = false;
    const requestId = liveSignalRequestRef.current + 1;
    liveSignalRequestRef.current = requestId;

    async function loadLiveSignals() {
      setIsLoadingLiveSignals(true);
      setLiveSignalError(null);
      setLiveSignalBlockers([]);

      try {
        const liveQuoteByMarketId = new Map(
          (latestLiveQuotes ?? []).map((quote) => [quote.marketId, quote]),
        );
        const signalBoard = await sdk.liveTradeSignalBoard(activeGpShortCode);
        const rows = signalBoard.rows
          .map((row) => {
            const liveQuote = liveQuoteByMarketId.get(row.marketId) ?? null;
            const liveMarketPrice = liveQuote
              ? currentLiveQuoteProbability(liveQuote)
              : null;
            const liveSpread = liveQuote
              ? currentLiveQuoteSpread(liveQuote)
              : null;
            const marketPrice = liveMarketPrice ?? row.marketPrice;
            return {
              ...row,
              tokenId: liveQuote?.tokenId ?? row.tokenId,
              marketPrice,
              edge:
                marketPrice != null
                  ? Number(row.modelProb) - marketPrice
                  : null,
              spread: liveSpread ?? row.spread,
              observedAtUtc: liveQuote?.observedAtUtc ?? row.observedAtUtc,
              eventType: liveQuote?.eventType ?? row.eventType,
              priceMove:
                liveQuote && row.marketPrice != null && liveMarketPrice != null
                  ? liveMarketPrice - row.marketPrice
                  : null,
              priceMoveLabel: liveQuote ? "Live delta" : "Stored gap",
              priceSource: liveQuote ? ("live" as const) : ("stored" as const),
            };
          })
          .sort((left, right) => {
            const edgeDelta =
              Math.abs(right.edge ?? -999) - Math.abs(left.edge ?? -999);
            if (edgeDelta !== 0) return edgeDelta;
            return (right.marketPrice ?? 0) - (left.marketPrice ?? 0);
          });

        if (!cancelled && requestId === liveSignalRequestRef.current) {
          setLiveSignalRows(rows);
          setLiveSignalBlockers(signalBoard.blockers);
        }
      } catch (error) {
        if (!cancelled && requestId === liveSignalRequestRef.current) {
          setLiveSignalRows([]);
          setLiveSignalError(feedbackMessage(error));
          setLiveSignalBlockers([]);
        }
      } finally {
        if (!cancelled && requestId === liveSignalRequestRef.current) {
          setIsLoadingLiveSignals(false);
        }
      }
    }

    void loadLiveSignals();

    return () => {
      cancelled = true;
    };
  }, [latestLiveQuotes, status?.selectedGpShortCode]);

  async function loadStatus(gpShortCode?: string) {
    const next = await sdk.weekendCockpitStatus(gpShortCode);
    setStatus(next);
    return next;
  }

  const loadLiveTradeState = React.useCallback(async (gpShortCode?: string) => {
    if (!gpShortCode) {
      setLiveTickets([]);
      setLiveExecutions([]);
      return;
    }
    const [tickets, executions] = await Promise.all([
      sdk.liveTradeTickets({ gpSlug: gpShortCode, limit: 20 }),
      sdk.liveTradeExecutions({ gpSlug: gpShortCode, limit: 20 }),
    ]);
    setLiveTickets(tickets);
    setLiveExecutions(executions);
  }, []);

  useEffect(() => {
    const gpShortCode = status?.selectedGpShortCode;
    if (!gpShortCode) {
      setLiveTickets([]);
      setLiveExecutions([]);
      return;
    }
    let cancelled = false;
    void loadLiveTradeState(gpShortCode).catch((error) => {
      if (!cancelled) {
        setLiveFeedback({
          status: "error",
          message: feedbackMessage(error),
        });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [loadLiveTradeState, status?.selectedGpShortCode]);

  async function handleSelectionChange(nextGp: string) {
    liveWatchStopRef.current = true;
    setIsLiveWatchActive(false);
    setSelectedGp(nextGp);
    setFeedback(null);
    setLiveFeedback(null);
    setLiveSamples([]);
    setLiveSignalRows([]);
    setLiveSignalError(null);
    setManualTradeMarketId(null);
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

  function appendLiveSample(result: CaptureLiveWeekendResponse) {
    const capturedAt = new Date().toISOString();
    setLiveSamples((current) =>
      [
        {
          id: `${result.jobRunId}:${capturedAt}`,
          capturedAt,
          result,
        },
        ...current,
      ].slice(0, 6),
    );
  }

  async function runLiveCaptureCycle() {
    const currentStatus = statusRef.current;
    if (!currentStatus?.targetSession) {
      throw new Error("Target session is unavailable for live capture.");
    }
    const currentLiveState = liveMonitorState(currentStatus);
    if (!currentLiveState.enabled) {
      throw new Error(currentLiveState.detail);
    }

    const result = await sdk.captureLiveWeekend({
      session_key: currentStatus.targetSession.sessionKey,
      capture_seconds: 20,
      start_buffer_min: 0,
      stop_buffer_min: 0,
      message_limit: 250,
    });
    appendLiveSample(result);
    setLiveFeedback({
      status: "ok",
      message: buildLiveCaptureFeedbackMessage(result),
    });
    await loadStatus(selectedGpRef.current);
    return result;
  }

  async function handleRun() {
    if (!selectedGp) return;
    setFeedback(null);
    setIsRunning(true);
    try {
      const result = await sdk.runWeekendCockpit({
        gp_short_code: selectedGp,
      });
      setFeedback({ status: "ok", message: buildRunFeedbackMessage(result) });
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

  async function handleLiveCapture() {
    if (!status?.targetSession || isLiveWatchActive) return;
    setLiveFeedback(null);
    setIsCapturingLive(true);
    try {
      await runLiveCaptureCycle();
    } catch (error) {
      setLiveFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      setIsCapturingLive(false);
    }
  }

  async function handleToggleLiveWatch() {
    if (isLiveWatchActive) {
      liveWatchStopRef.current = true;
      setIsLiveWatchActive(false);
      setLiveFeedback({
        status: "ok",
        message: "Stopping after the current live sample finishes.",
      });
      return;
    }

    liveWatchStopRef.current = false;
    setLiveFeedback(null);
    setIsLiveWatchActive(true);

    try {
      while (!liveWatchStopRef.current) {
        setIsCapturingLive(true);
        await runLiveCaptureCycle();
        setIsCapturingLive(false);
      }
    } catch (error) {
      setLiveFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      liveWatchStopRef.current = false;
      setIsCapturingLive(false);
      setIsLiveWatchActive(false);
    }
  }

  function toggleAdvanced() {
    setShowAdvanced((current) => !current);
  }

  async function handleCreateLiveTicket(row: LiveSignalRow) {
    const gpShortCode =
      selectedGpRef.current || statusRef.current?.selectedGpShortCode;
    if (!gpShortCode || row.priceSource !== "live") return;
    if (row.marketPrice == null) return;

    const shares = parsePositiveNumberInput(manualTradeShares);
    const minEdgePts = parsePositiveNumberInput(manualTradeMinEdgePts);
    const maxSpreadCents = parsePositiveNumberInput(manualTradeMaxSpreadCents);
    if (shares == null || shares <= 0) {
      setLiveFeedback({
        status: "error",
        message: "Manual shares must be a positive number.",
      });
      return;
    }
    if (minEdgePts == null) {
      setLiveFeedback({
        status: "error",
        message: "Manual min edge must be zero or greater.",
      });
      return;
    }

    setManualTradeMarketId(row.marketId);
    setLiveFeedback(null);
    try {
      const result = await sdk.createLiveTradeTicket({
        gp_short_code: gpShortCode,
        market_id: row.marketId,
        observed_market_price: row.marketPrice,
        observed_at_utc: row.observedAtUtc,
        observed_spread: row.spread,
        source_event_type: row.eventType,
        min_edge: minEdgePts / 100,
        max_spread: maxSpreadCents == null ? null : maxSpreadCents / 100,
        bet_size: shares,
      });
      setLiveFeedback({ status: "ok", message: result.message });
      setSelectedLiveTicketId(result.ticketId);
      setLiveFillSize(String(result.recommendedSize));
      setLiveFillPrice("");
      setLiveFillNote("");
      setLiveFillReference("");
      await loadStatus(gpShortCode);
      await loadLiveTradeState(gpShortCode);
      router.refresh();
    } catch (error) {
      setLiveFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      setManualTradeMarketId(null);
    }
  }

  function handlePrepareLiveFill(ticket: LiveTradeTicket) {
    setSelectedLiveTicketId(ticket.id);
    setLiveFillSize(String(ticket.recommendedSize));
    setLiveFillPrice("");
    setLiveFillNote("");
    setLiveFillReference("");
  }

  async function handleRecordLiveFill() {
    if (!selectedLiveTicketId) return;
    const gpShortCode =
      selectedGpRef.current || statusRef.current?.selectedGpShortCode;
    const submittedSize = parsePositiveNumberInput(liveFillSize);
    const fillPrice = parsePositiveNumberInput(liveFillPrice);
    if (submittedSize == null || submittedSize <= 0) {
      setLiveFeedback({
        status: "error",
        message: "Fill size must be a positive number.",
      });
      return;
    }
    if (fillPrice == null || fillPrice <= 0) {
      setLiveFeedback({
        status: "error",
        message: "Fill price must be a positive number.",
      });
      return;
    }

    setIsRecordingLiveFill(true);
    setLiveFeedback(null);
    try {
      const result = await sdk.recordLiveTradeFill({
        ticket_id: selectedLiveTicketId,
        submitted_size: submittedSize,
        actual_fill_size: submittedSize,
        actual_fill_price: fillPrice,
        operator_note: liveFillNote || null,
        external_reference: liveFillReference || null,
        status: "filled",
      });
      setLiveFeedback({ status: "ok", message: result.message });
      setSelectedLiveTicketId(null);
      setLiveFillSize("");
      setLiveFillPrice("");
      setLiveFillNote("");
      setLiveFillReference("");
      await loadStatus(gpShortCode);
      await loadLiveTradeState(gpShortCode);
      router.refresh();
    } catch (error) {
      setLiveFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    } finally {
      setIsRecordingLiveFill(false);
    }
  }

  async function handleCancelLiveTicket(ticketId: string) {
    const gpShortCode =
      selectedGpRef.current || statusRef.current?.selectedGpShortCode;
    setLiveFeedback(null);
    try {
      const result = await sdk.cancelLiveTradeTicket({ ticket_id: ticketId });
      setLiveFeedback({ status: "ok", message: result.message });
      if (selectedLiveTicketId === ticketId) {
        setSelectedLiveTicketId(null);
      }
      await loadStatus(gpShortCode);
      await loadLiveTradeState(gpShortCode);
      router.refresh();
    } catch (error) {
      setLiveFeedback({
        status: "error",
        message: feedbackMessage(error),
      });
    }
  }

  if (!status) {
    return (
      <Panel title="Weekend cockpit" eyebrow="Latest update">
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
  const refreshTarget =
    refreshTargetsByGpShortCode[selectedGp || status.selectedGpShortCode] ??
    null;
  const liveState = liveMonitorState(status);
  const liveCaptureCount = liveSamples.length;
  const liveOpenf1Total = liveSamples.reduce(
    (total, sample) => total + sample.result.openf1Messages,
    0,
  );
  const livePolymarketTotal = liveSamples.reduce(
    (total, sample) => total + sample.result.polymarketMessages,
    0,
  );
  const liveTotalCaptureSeconds = liveSamples.reduce(
    (total, sample) => total + sample.result.captureSeconds,
    0,
  );
  const liveTotalMessages = liveOpenf1Total + livePolymarketTotal;
  const liveAverageRate = formatMessageRate(
    liveTotalMessages,
    liveTotalCaptureSeconds,
  );
  const latestLiveTotalMessages = latestLiveSample
    ? latestLiveSample.result.openf1Messages +
      latestLiveSample.result.polymarketMessages
    : 0;
  const latestOpenf1Topic = latestLiveSample
    ? topLiveCount(latestLiveSample.result.summary.openf1Topics)
    : null;
  const latestPolymarketEvent = latestLiveSample
    ? topLiveCount(latestLiveSample.result.summary.polymarketEventTypes)
    : null;
  const pricedSignalCount = liveSignalRows.filter(
    (row) => row.marketPrice != null,
  ).length;
  const modeledSignalCount = liveSignalRows.filter(
    (row) => row.modelProb != null,
  ).length;
  const topEdgeSignal = liveSignalRows.find((row) => row.edge != null) ?? null;
  const sourceLabel = selectedConfig.source_session_code
    ? `${sessionDisplayName(selectedConfig.source_session_code)} results`
    : "Pre-weekend data";
  const runDisabled =
    !status.readyToRun ||
    isLoadingStatus ||
    isRunning ||
    isCapturingLive ||
    isLiveWatchActive ||
    manualTradeMarketId !== null;
  const blockedSteps = status.steps.filter(
    (step) =>
      step.status === "blocked" &&
      (step.key !== "run_paper_trade" || status.modelBlockers.length === 0),
  );

  return (
    <Panel title="Weekend cockpit" eyebrow="Latest update">
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
            {status.requiredStage && (
              <p className="text-xs text-[#6b7280]">
                Model stage {status.requiredStage} ·{" "}
                {status.modelReady
                  ? `champion ${status.activeModelRunId?.slice(0, 8) ?? "unknown"} active`
                  : "promotion required"}
              </p>
            )}
            <p className="text-xs text-[#6b7280]">
              Calendar status {status.calendarStatus}
              {status.sourceConflict ? " · override active" : ""}
              {status.overrideSourceUrl ? (
                <>
                  {" · "}
                  <a
                    href={status.overrideSourceUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-[#ffb4b1] underline decoration-[#e10600]/40 underline-offset-2"
                  >
                    source
                  </a>
                </>
              ) : null}
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
              disabled={
                isLoadingStatus ||
                isRunning ||
                isCapturingLive ||
                isLiveWatchActive
              }
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

        <div className="rounded-xl border border-white/[0.06] bg-[#11131d] p-4">
          <div className="space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Ops calendar
                </p>
                <p className="mt-1 text-sm text-[#9ca3af]">
                  Active meetings follow the effective calendar. Cancelled Grands Prix
                  stay in history with their override source.
                </p>
              </div>
              {status.sourceConflict && (
                <Badge tone="warn">Override active</Badge>
              )}
            </div>

            <div className="grid gap-3 lg:grid-cols-[1.3fr_0.7fr]">
              <div className="space-y-2">
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Upcoming
                </p>
                <div className="space-y-2">
                  {status.calendarMeetings.map((meeting) => {
                    const isSelected = meeting.meetingSlug === status.meetingSlug;
                    return (
                      <div
                        key={`${meeting.season}-${meeting.meetingSlug}`}
                        className={`rounded-lg border px-3 py-2 ${
                          isSelected
                            ? "border-[#e10600]/40 bg-[#1a0f14]"
                            : "border-white/10 bg-[#0f1119]"
                        }`}
                      >
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="text-sm font-semibold text-white">
                            {meeting.meetingName}
                          </p>
                          {isSelected && <Badge tone="live">Selected</Badge>}
                          <Badge tone="default">
                            {formatEventFormat(meeting.eventFormat)}
                          </Badge>
                        </div>
                        <p className="mt-1 text-xs text-[#9ca3af]">
                          Round {meeting.roundNumber ?? "—"} ·{" "}
                          {formatCalendarDateRange(meeting)}
                        </p>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="space-y-2">
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Cancelled
                </p>
                <div className="space-y-2">
                  {status.cancelledMeetings.length === 0 ? (
                    <p className="rounded-lg border border-white/10 bg-[#0f1119] px-3 py-2 text-xs text-[#6b7280]">
                      No cancelled meetings in the effective calendar.
                    </p>
                  ) : (
                    status.cancelledMeetings.map((meeting) => (
                      <div
                        key={`${meeting.season}-${meeting.meetingSlug}`}
                        className="rounded-lg border border-[#e10600]/20 bg-[#1a0f14] px-3 py-2"
                      >
                        <p className="text-sm font-semibold text-white">
                          {meeting.meetingName}
                        </p>
                        <p className="mt-1 text-xs text-[#9ca3af]">
                          {formatCalendarDateRange(meeting)}
                        </p>
                        <p className="mt-1 text-xs text-[#ffb4b1]">
                          {meeting.sourceLabel ?? "Override"} {meeting.sourceUrl ? "·" : ""}
                          {meeting.sourceUrl ? (
                            <>
                              {" "}
                              <a
                                href={meeting.sourceUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="underline decoration-[#e10600]/40 underline-offset-2"
                              >
                                source
                              </a>
                            </>
                          ) : null}
                        </p>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
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
                Latest update
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
              {refreshTarget && (
                <div className="border-t border-white/[0.06] pt-3">
                  <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                    Data only
                  </p>
                  <p className="mt-1 text-xs text-[#6b7280]">
                    Refreshes the latest ended session for this GP without
                    running paper trading.
                  </p>
                  <div className="mt-2">
                    <MeetingRefreshButton
                      meetingId={refreshTarget.meetingId}
                      latestEndedSession={refreshTarget.latestEndedSession}
                      align="start"
                    />
                  </div>
                </div>
              )}
              <div className="border-t border-white/[0.06] pt-3">
                <p className="text-[10px] font-bold uppercase tracking-[0.3em] text-[#6b7280]">
                  Live monitor
                </p>
                <p className="mt-1 text-xs text-[#6b7280]">
                  Captures a short OpenF1 + Polymarket live sample for this
                  stage&apos;s target session while it is live.
                </p>
                <div className="mt-2 flex flex-col gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      void handleLiveCapture();
                    }}
                    disabled={
                      !liveState.enabled ||
                      isCapturingLive ||
                      isRunning ||
                      isLiveWatchActive
                    }
                    aria-busy={isCapturingLive}
                    className="inline-flex w-full items-center justify-center gap-2 rounded-lg border border-white/10 bg-[#171a25] px-4 py-2.5 text-sm font-medium text-white transition-colors hover:border-[#e10600]/40 hover:bg-[#1b2030] disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {isCapturingLive && (
                      <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
                    )}
                    Capture 20s live sample
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      void handleToggleLiveWatch();
                    }}
                    disabled={
                      (!liveState.enabled && !isLiveWatchActive) || isRunning
                    }
                    className={`inline-flex w-full items-center justify-center gap-2 rounded-lg px-4 py-2.5 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
                      isLiveWatchActive
                        ? "border border-emerald-500/30 bg-emerald-500/10 text-emerald-200 hover:bg-emerald-500/15"
                        : "border border-white/10 bg-[#171a25] text-white hover:border-[#e10600]/40 hover:bg-[#1b2030]"
                    }`}
                  >
                    {isLiveWatchActive ? "Stop live watch" : "Start live watch"}
                  </button>
                  <p className="text-xs text-[#6b7280]">{liveState.detail}</p>
                  <div className="grid gap-2 sm:grid-cols-4">
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-2">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Watch
                      </p>
                      <p className="mt-1 text-sm font-medium text-white">
                        {isLiveWatchActive ? "Running" : "Idle"}
                      </p>
                    </div>
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-2">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Samples
                      </p>
                      <p className="mt-1 text-sm font-medium text-white">
                        {liveCaptureCount}
                      </p>
                    </div>
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-2">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Avg rate
                      </p>
                      <p className="mt-1 text-sm font-medium text-white">
                        {liveAverageRate}
                      </p>
                    </div>
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-2">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Markets seen
                      </p>
                      <p className="mt-1 text-sm font-medium text-white">
                        {latestLiveSample
                          ? `${latestLiveSample.result.summary.observedMarketCount}/${latestLiveSample.result.marketCount}`
                          : "—"}
                      </p>
                    </div>
                  </div>
                  {latestLiveSample && (
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-3">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Latest telemetry
                      </p>
                      <div className="mt-2 grid gap-2 sm:grid-cols-2">
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                            Throughput
                          </p>
                          <p className="mt-1 text-sm font-medium text-white">
                            {formatMessageRate(
                              latestLiveTotalMessages,
                              latestLiveSample.result.captureSeconds,
                            )}
                          </p>
                          <p className="mt-1 text-xs text-[#9ca3af]">
                            {latestLiveTotalMessages} messages over{" "}
                            {latestLiveSample.result.captureSeconds}s
                          </p>
                        </div>
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                            Feed coverage
                          </p>
                          <p className="mt-1 text-sm font-medium text-white">
                            {latestLiveSample.result.summary.observedTokenCount}{" "}
                            tokens across{" "}
                            {
                              latestLiveSample.result.summary
                                .observedMarketCount
                            }{" "}
                            markets
                          </p>
                          <p className="mt-1 text-xs text-[#9ca3af]">
                            Linked target set:{" "}
                            {latestLiveSample.result.marketCount} markets
                          </p>
                        </div>
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                            Top OpenF1
                          </p>
                          <p className="mt-1 text-sm font-medium text-white">
                            {latestOpenf1Topic
                              ? `${latestOpenf1Topic.key} · ${latestOpenf1Topic.count}`
                              : "—"}
                          </p>
                        </div>
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                            Top market event
                          </p>
                          <p className="mt-1 text-sm font-medium text-white">
                            {latestPolymarketEvent
                              ? `${latestPolymarketEvent.key} · ${latestPolymarketEvent.count}`
                              : "—"}
                          </p>
                        </div>
                      </div>
                    </div>
                  )}
                  <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-3">
                    <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                      Signal board
                    </p>
                    <p className="mt-1 text-xs text-[#6b7280]">
                      Linked markets for this target session, ranked by current
                      model edge when a stage run exists.
                    </p>
                    <p className="mt-1 text-xs text-[#6b7280]">
                      Click `Create ticket` to generate one operator ticket from
                      the current live quote.
                    </p>
                    <div className="mt-3 grid gap-2 md:grid-cols-3">
                      <label className="space-y-1">
                        <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                          Shares
                        </span>
                        <input
                          type="number"
                          min="0"
                          step="1"
                          value={manualTradeShares}
                          onChange={(event) => {
                            setManualTradeShares(event.target.value);
                          }}
                          className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                        />
                      </label>
                      <label className="space-y-1">
                        <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                          Min edge pts
                        </span>
                        <input
                          type="number"
                          min="0"
                          step="0.1"
                          value={manualTradeMinEdgePts}
                          onChange={(event) => {
                            setManualTradeMinEdgePts(event.target.value);
                          }}
                          className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                        />
                      </label>
                      <label className="space-y-1">
                        <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                          Max spread ¢
                        </span>
                        <input
                          type="number"
                          min="0"
                          step="0.1"
                          value={manualTradeMaxSpreadCents}
                          onChange={(event) => {
                            setManualTradeMaxSpreadCents(event.target.value);
                          }}
                          placeholder="Off"
                          className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white placeholder:text-[#6b7280] focus:border-[#e10600] focus:outline-none"
                        />
                      </label>
                    </div>
                    <p className="mt-2 text-xs text-[#6b7280]">
                      Empty max spread means no spread cap. `5` edge pts equals
                      `0.05`, and `4` spread cents equals `0.04`.
                    </p>
                    <p className="mt-1 text-xs text-[#9ca3af]">
                      {status.requiredStage
                        ? `Required stage: ${status.requiredStage}`
                        : "This stage can generate live tickets without a promoted model gate."}
                    </p>
                    {liveSignalBlockers.length > 0 && (
                      <div className="mt-3 rounded-xl border border-[#f59e0b]/20 bg-[#f59e0b]/10 px-4 py-3 text-sm text-[#fcd34d]">
                        {liveSignalBlockers.join(" ")}
                      </div>
                    )}
                    {isLoadingLiveSignals ? (
                      <p className="mt-3 text-sm text-[#9ca3af]">
                        Loading linked market signals…
                      </p>
                    ) : liveSignalError ? (
                      <div
                        className={`mt-3 rounded-xl border px-4 py-3 text-sm ${feedbackTone("error")}`}
                      >
                        {liveSignalError}
                      </div>
                    ) : liveSignalRows.length > 0 ? (
                      <>
                        <div className="mt-3 grid gap-2 sm:grid-cols-4">
                          <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                            <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                              Linked
                            </p>
                            <p className="mt-1 text-sm font-medium text-white">
                              {liveSignalRows.length}
                            </p>
                          </div>
                          <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                            <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                              Priced
                            </p>
                            <p className="mt-1 text-sm font-medium text-white">
                              {pricedSignalCount}
                            </p>
                          </div>
                          <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                            <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                              Modeled
                            </p>
                            <p className="mt-1 text-sm font-medium text-white">
                              {modeledSignalCount}
                            </p>
                          </div>
                          <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                            <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                              Top edge
                            </p>
                            <p className="mt-1 text-sm font-medium text-white">
                              {topEdgeSignal
                                ? formatEdgePoints(topEdgeSignal.edge)
                                : "—"}
                            </p>
                          </div>
                        </div>
                        <div className="mt-3 space-y-2">
                          {liveSignalRows.slice(0, 6).map((row) => (
                            <div
                              key={row.marketId}
                              className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-3 transition-colors hover:border-[#e10600]/30 hover:bg-[#151925]"
                            >
                              <div className="flex flex-wrap items-start justify-between gap-3">
                                <div className="space-y-1">
                                  <a
                                    href={`/markets/${row.marketId}`}
                                    className="text-sm font-medium text-white transition-colors hover:text-[#ffb4b1]"
                                  >
                                    {row.question}
                                  </a>
                                  <p className="text-[11px] text-[#6b7280]">
                                    {row.observedAtUtc
                                      ? row.priceSource === "live"
                                        ? `Live ${row.eventType ?? "market"} sample ${formatCaptureTime(row.observedAtUtc)}`
                                        : `Latest stored price sample ${formatCaptureTime(row.observedAtUtc)}`
                                      : "Using latest stored market snapshot"}
                                  </p>
                                </div>
                                <div className="text-right">
                                  <p
                                    className={`text-sm font-semibold tabular-nums ${
                                      row.edge != null && row.edge >= 0
                                        ? "text-emerald-300"
                                        : "text-[#d1d5db]"
                                    }`}
                                  >
                                    {formatEdgePoints(row.edge)}
                                  </p>
                                  <p className="text-[10px] uppercase tracking-[0.18em] text-[#6b7280]">
                                    Edge
                                  </p>
                                  <button
                                    type="button"
                                    onClick={(event) => {
                                      event.preventDefault();
                                      void handleCreateLiveTicket(row);
                                    }}
                                    disabled={
                                      manualTradeMarketId !== null ||
                                      row.priceSource !== "live" ||
                                      row.marketPrice == null
                                    }
                                    className="mt-2 inline-flex items-center justify-center rounded-md border border-white/10 bg-[#171a25] px-3 py-1.5 text-[11px] font-medium text-white transition-colors hover:border-[#e10600]/40 hover:bg-[#1b2030] disabled:cursor-not-allowed disabled:opacity-50"
                                  >
                                    {manualTradeMarketId === row.marketId
                                      ? "Creating..."
                                      : "Create ticket"}
                                  </button>
                                </div>
                              </div>
                              <div className="mt-3 grid gap-2 text-xs text-[#9ca3af] sm:grid-cols-4">
                                <p>Market YES {formatCents(row.marketPrice)}</p>
                                <p>Model YES {formatCents(row.modelProb)}</p>
                                <p>Spread {formatCents(row.spread)}</p>
                                <p>
                                  {row.priceMoveLabel}{" "}
                                  {formatPriceMove(row.priceMove)}
                                </p>
                              </div>
                            </div>
                          ))}
                        </div>
                      </>
                    ) : (
                      <p className="mt-3 text-sm text-[#9ca3af]">
                        No linked markets are available for this target session
                        yet.
                      </p>
                    )}
                  </div>
                  <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-3">
                    <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                      Live tickets
                    </p>
                    <p className="mt-1 text-xs text-[#6b7280]">
                      Operator tickets are separate from paper trades. Record
                      the actual browser fill after each order.
                    </p>
                    <div className="mt-3 grid gap-2 sm:grid-cols-4">
                      <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Tickets
                        </p>
                        <p className="mt-1 text-sm font-medium text-white">
                          {status.liveTicketSummary.ticketCount}
                        </p>
                      </div>
                      <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Open
                        </p>
                        <p className="mt-1 text-sm font-medium text-white">
                          {status.liveTicketSummary.openTicketCount}
                        </p>
                      </div>
                      <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Executions
                        </p>
                        <p className="mt-1 text-sm font-medium text-white">
                          {status.liveExecutionSummary.executionCount}
                        </p>
                      </div>
                      <div className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-2">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Filled
                        </p>
                        <p className="mt-1 text-sm font-medium text-white">
                          {status.liveExecutionSummary.filledExecutionCount}
                        </p>
                      </div>
                    </div>
                    {liveTickets.length > 0 ? (
                      <div className="mt-3 space-y-2">
                        {liveTickets.slice(0, 4).map((ticket) => (
                          <div
                            key={ticket.id}
                            className="rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-3"
                          >
                            <div className="flex flex-wrap items-start justify-between gap-3">
                              <div className="space-y-1">
                                <p className="text-sm font-medium text-white">
                                  {ticket.question}
                                </p>
                                <p className="text-[11px] text-[#6b7280]">
                                  {ticket.sideLabel} · edge{" "}
                                  {formatEdgePoints(ticket.edge)} · size{" "}
                                  {ticket.recommendedSize}
                                </p>
                                <p className="text-[11px] text-[#6b7280]">
                                  Created {formatCaptureTime(ticket.createdAt)}
                                  {ticket.expiresAt
                                    ? ` · expires ${formatCaptureTime(ticket.expiresAt)}`
                                    : ""}
                                </p>
                              </div>
                              <div className="flex gap-2">
                                <button
                                  type="button"
                                  onClick={() => {
                                    handlePrepareLiveFill(ticket);
                                  }}
                                  disabled={ticket.status !== "open"}
                                  className="inline-flex items-center justify-center rounded-md border border-white/10 bg-[#171a25] px-3 py-1.5 text-[11px] font-medium text-white transition-colors hover:border-[#e10600]/40 hover:bg-[#1b2030] disabled:cursor-not-allowed disabled:opacity-50"
                                >
                                  Record fill
                                </button>
                                <button
                                  type="button"
                                  onClick={() => {
                                    void handleCancelLiveTicket(ticket.id);
                                  }}
                                  disabled={ticket.status !== "open"}
                                  className="inline-flex items-center justify-center rounded-md border border-white/10 bg-[#171a25] px-3 py-1.5 text-[11px] font-medium text-white transition-colors hover:border-[#e10600]/40 hover:bg-[#1b2030] disabled:cursor-not-allowed disabled:opacity-50"
                                >
                                  Cancel
                                </button>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-3 text-sm text-[#9ca3af]">
                        No live tickets have been created for this stage yet.
                      </p>
                    )}
                    {selectedLiveTicketId && (
                      <div className="mt-3 rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-3">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Record browser fill
                        </p>
                        <div className="mt-3 grid gap-2 md:grid-cols-2">
                          <label className="space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                              Filled size
                            </span>
                            <input
                              type="number"
                              min="0"
                              step="0.1"
                              value={liveFillSize}
                              onChange={(event) => {
                                setLiveFillSize(event.target.value);
                              }}
                              className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                            />
                          </label>
                          <label className="space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                              Fill price
                            </span>
                            <input
                              type="number"
                              min="0"
                              step="0.001"
                              value={liveFillPrice}
                              onChange={(event) => {
                                setLiveFillPrice(event.target.value);
                              }}
                              className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                            />
                          </label>
                          <label className="space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                              Note
                            </span>
                            <input
                              type="text"
                              value={liveFillNote}
                              onChange={(event) => {
                                setLiveFillNote(event.target.value);
                              }}
                              className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                            />
                          </label>
                          <label className="space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#6b7280]">
                              External ref
                            </span>
                            <input
                              type="text"
                              value={liveFillReference}
                              onChange={(event) => {
                                setLiveFillReference(event.target.value);
                              }}
                              className="w-full rounded-md border border-white/10 bg-[#11131d] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
                            />
                          </label>
                        </div>
                        <button
                          type="button"
                          onClick={() => {
                            void handleRecordLiveFill();
                          }}
                          disabled={isRecordingLiveFill}
                          className="mt-3 inline-flex items-center justify-center rounded-lg bg-[#e10600] px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-[#b80500] disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          {isRecordingLiveFill ? "Recording..." : "Record fill"}
                        </button>
                      </div>
                    )}
                    {liveExecutions.length > 0 && (
                      <div className="mt-3 rounded-lg border border-white/[0.06] bg-[#11131d] px-3 py-3">
                        <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                          Recent executions
                        </p>
                        <div className="mt-2 space-y-2">
                          {liveExecutions.slice(0, 4).map((execution) => (
                            <div
                              key={execution.id}
                              className="flex flex-wrap items-center justify-between gap-2 text-xs"
                            >
                              <p className="text-white">
                                {execution.ticketId.slice(0, 8)} ·{" "}
                                {execution.status}
                              </p>
                              <p className="text-[#9ca3af]">
                                {execution.actualFillSize ??
                                  execution.submittedSize}{" "}
                                @ {execution.actualFillPrice?.toFixed(3) ?? "—"}
                              </p>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                  {liveSamples.length > 0 && (
                    <div className="rounded-lg border border-white/[0.06] bg-[#0f1119] px-3 py-3">
                      <p className="text-[10px] font-bold uppercase tracking-[0.24em] text-[#6b7280]">
                        Recent samples
                      </p>
                      <div className="mt-2 space-y-2">
                        {liveSamples.map((sample) => (
                          <div
                            key={sample.id}
                            className="flex flex-wrap items-center justify-between gap-2 text-xs"
                          >
                            <p className="text-white">
                              {formatCaptureTime(sample.capturedAt)}
                            </p>
                            <p className="text-[#9ca3af]">
                              OpenF1 {sample.result.openf1Messages} · Polymarket{" "}
                              {sample.result.polymarketMessages} ·{" "}
                              {formatMessageRate(
                                sample.result.openf1Messages +
                                  sample.result.polymarketMessages,
                                sample.result.captureSeconds,
                              )}
                            </p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                {liveFeedback && (
                  <div
                    className={`mt-3 rounded-xl border px-4 py-3 text-sm ${feedbackTone(liveFeedback.status)}`}
                  >
                    {liveFeedback.message}
                  </div>
                )}
              </div>
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

        {status.modelBlockers.length > 0 && (
          <div className="rounded-xl border border-[#e10600]/20 bg-[#e10600]/10 px-4 py-3 text-sm text-[#ffd9d6]">
            <p className="font-medium">Model blockers</p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              {status.modelBlockers.map((blocker) => (
                <li key={blocker}>{blocker}</li>
              ))}
            </ul>
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
