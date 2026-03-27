import React from "react";

type PageStatusBannerProps = {
  messages: string[];
};

export function PageStatusBanner({ messages }: PageStatusBannerProps) {
  if (messages.length === 0) {
    return null;
  }

  return (
    <div
      role="alert"
      className="rounded-2xl border border-amber-400/30 bg-amber-300/10 px-4 py-3 text-sm text-amber-100"
    >
      <p className="font-medium">Some data is not fully available yet.</p>
      <ul className="mt-2 list-disc space-y-1 pl-5">
        {messages.map((message) => (
          <li key={message}>{message}</li>
        ))}
      </ul>
    </div>
  );
}
