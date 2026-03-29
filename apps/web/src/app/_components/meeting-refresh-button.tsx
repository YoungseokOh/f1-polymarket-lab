"use client";

import type { RefreshedSessionSummary } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { useRouter } from "next/navigation";
import React, { startTransition, useState } from "react";

type ActionState = "idle" | "loading" | "success" | "error";

function sessionLabel(session: RefreshedSessionSummary | null) {
  if (!session) return null;
  return session.sessionCode ?? session.sessionName;
}

export function MeetingRefreshButton({
  meetingId,
  latestEndedSession,
  align = "end",
}: {
  meetingId: string;
  latestEndedSession: RefreshedSessionSummary | null;
  align?: "start" | "end";
}) {
  const router = useRouter();
  const [state, setState] = useState<ActionState>("idle");
  const [message, setMessage] = useState("");

  const label = sessionLabel(latestEndedSession);
  const disabled = !latestEndedSession || state === "loading";
  const alignClass = align === "start" ? "items-start" : "items-end";
  const textAlignClass = align === "start" ? "text-left" : "text-right";

  async function handleClick() {
    if (!latestEndedSession) return;

    setState("loading");
    setMessage("");
    try {
      const result = await sdk.refreshLatestSession({
        meeting_id: meetingId,
      });
      setMessage(result.message);
      setState("success");
      startTransition(() => {
        router.refresh();
      });
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unknown error");
      setState("error");
    }
  }

  return (
    <div className={`flex flex-col gap-1.5 ${alignClass}`}>
      <button
        type="button"
        onClick={handleClick}
        disabled={disabled}
        className="inline-flex min-w-[148px] items-center justify-center gap-2 rounded-md bg-[#e10600] px-3 py-1.5 text-xs font-medium text-white transition-colors hover:bg-[#b80500] disabled:cursor-not-allowed disabled:opacity-50"
      >
        {state === "loading" && (
          <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
        )}
        {label ? `Update latest: ${label}` : "No ended session yet"}
      </button>
      {message ? (
        <p
          className={`max-w-[220px] text-[11px] ${textAlignClass} ${
            state === "error" ? "text-[#e10600]" : "text-race-green"
          }`}
        >
          {message}
        </p>
      ) : (
        !latestEndedSession && (
          <p
            className={`max-w-[220px] text-[11px] ${textAlignClass} text-[#6b7280]`}
          >
            This GP does not have an ended session yet.
          </p>
        )
      )}
    </div>
  );
}
