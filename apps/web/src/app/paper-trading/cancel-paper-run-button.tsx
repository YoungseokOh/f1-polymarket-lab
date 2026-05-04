"use client";

import { sdk } from "@f1/ts-sdk";
import { useRouter } from "next/navigation";
import React from "react";
import { useState } from "react";

export function CancelPaperRunButton({
  sessionId,
  sessionIds,
  label,
}: {
  sessionId?: string;
  sessionIds?: string[];
  label?: string;
}) {
  const router = useRouter();
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const ids = sessionIds ?? (sessionId ? [sessionId] : []);
  const isBundle = ids.length > 1;

  async function handleCancel() {
    if (ids.length === 0) return;
    const confirmed = window.confirm(
      isBundle
        ? `Cancel ${ids.length} paper runs? Their simulated open tickets will be removed from current results.`
        : "Cancel this paper run? Its simulated open tickets will be removed from current results.",
    );
    if (!confirmed) return;
    setError(null);
    setIsCancelling(true);
    try {
      await Promise.all(ids.map((id) => sdk.cancelPaperTradeSession(id)));
      router.refresh();
    } catch (cancelError) {
      setError(
        cancelError instanceof Error
          ? cancelError.message
          : "Could not cancel paper run.",
      );
    } finally {
      setIsCancelling(false);
    }
  }

  return (
    <div className="flex flex-col items-end gap-1">
      <button
        type="button"
        onClick={() => {
          void handleCancel();
        }}
        disabled={isCancelling}
        className="rounded-lg border border-[#e10600]/30 px-3 py-1.5 text-xs font-medium text-[#ffb4b1] transition-colors hover:border-[#e10600]/60 hover:bg-[#e10600]/10 disabled:cursor-not-allowed disabled:opacity-50"
      >
        {isCancelling ? "Cancelling..." : (label ?? "Cancel run")}
      </button>
      {error ? (
        <p className="max-w-64 text-right text-xs text-[#ffb4b1]">{error}</p>
      ) : null}
    </div>
  );
}
