"use client";

import { useState } from "react";

type ActionState = "idle" | "loading" | "success" | "error";

export function ActionButton({
  label,
  onAction,
  variant = "primary",
}: {
  label: string;
  onAction: () => Promise<string>;
  variant?: "primary" | "secondary";
}) {
  const [state, setState] = useState<ActionState>("idle");
  const [message, setMessage] = useState("");

  async function handleClick() {
    setState("loading");
    setMessage("");
    try {
      const msg = await onAction();
      setMessage(msg);
      setState("success");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Unknown error");
      setState("error");
    }
  }

  const base =
    "inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors disabled:opacity-50";
  const styles =
    variant === "primary"
      ? `${base} bg-[#e10600] text-white hover:bg-[#b80500]`
      : `${base} border border-white/10 text-[#d1d5db] hover:bg-white/[0.04]`;

  return (
    <div className="flex flex-col gap-1.5">
      <button
        type="button"
        className={styles}
        onClick={handleClick}
        disabled={state === "loading"}
      >
        {state === "loading" && (
          <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
        )}
        {label}
      </button>
      {message && (
        <p
          className={`text-xs ${state === "error" ? "text-[#e10600]" : "text-race-green"}`}
        >
          {message}
        </p>
      )}
    </div>
  );
}
