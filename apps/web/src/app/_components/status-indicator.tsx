type StatusIndicatorProps = {
  status: "live" | "ok" | "idle" | "error" | "pending";
  label?: string;
};

const statusConfig = {
  live: { color: "bg-f1-red", pulse: true, text: "text-f1-red" },
  ok: { color: "bg-race-green", pulse: false, text: "text-race-green" },
  idle: { color: "bg-[#6b7280]", pulse: false, text: "text-[#6b7280]" },
  error: { color: "bg-amber-500", pulse: false, text: "text-amber-500" },
  pending: { color: "bg-race-yellow", pulse: true, text: "text-race-yellow" },
};

export function StatusIndicator({ status, label }: StatusIndicatorProps) {
  const cfg = statusConfig[status];
  return (
    <span className="inline-flex items-center gap-2">
      <span className="relative flex h-2.5 w-2.5">
        {cfg.pulse && (
          <span
            className={`absolute inline-flex h-full w-full animate-ping rounded-full ${cfg.color} opacity-40`}
          />
        )}
        <span
          className={`relative inline-flex h-2.5 w-2.5 rounded-full ${cfg.color}`}
        />
      </span>
      {label && (
        <span className={`text-xs font-medium ${cfg.text}`}>{label}</span>
      )}
    </span>
  );
}
