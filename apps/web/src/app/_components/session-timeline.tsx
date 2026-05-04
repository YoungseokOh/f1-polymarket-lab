import React from "react";

const stageLabels: Record<string, string> = {
  FP1: "FP1",
  FP2: "FP2",
  FP3: "FP3",
  SQ: "SQ",
  S: "S",
  Q: "QUALI",
  R: "RACE",
};

const DEFAULT_SESSION_TIMELINE = ["FP1", "FP2", "FP3", "Q", "R"] as const;

type SessionTimelineProps = {
  completedCodes: string[];
  activeCode?: string | null;
  sessionCodes?: readonly string[];
};

export function SessionTimeline({
  completedCodes,
  activeCode,
  sessionCodes = DEFAULT_SESSION_TIMELINE,
}: SessionTimelineProps) {
  return (
    <div className="flex items-center gap-1">
      {sessionCodes.map((sessionCode, index) => {
        const isCompleted = completedCodes.includes(sessionCode);
        const isActive = activeCode === sessionCode;
        const label = stageLabels[sessionCode] ?? sessionCode;

        return (
          <div key={sessionCode} className="flex items-center">
            {index > 0 && (
              <div
                className={`mx-0.5 h-[2px] w-4 ${
                  isCompleted ? "bg-race-green" : "bg-white/10"
                }`}
              />
            )}
            <div
              className={`flex items-center justify-center rounded-md px-2.5 py-1 text-[10px] font-bold tracking-wider ${
                isActive
                  ? "bg-f1-red text-white shadow-md shadow-f1-red/30"
                  : isCompleted
                    ? "bg-race-green/15 text-race-green"
                    : "bg-white/[0.04] text-[#6b7280]"
              }`}
            >
              {label}
            </div>
          </div>
        );
      })}
    </div>
  );
}
