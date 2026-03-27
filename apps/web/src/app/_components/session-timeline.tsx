import React from "react";

const stages = [
  { code: "FP1", label: "FP1" },
  { code: "FP2", label: "FP2" },
  { code: "FP3", label: "FP3" },
  { code: "Q", label: "QUALI" },
  { code: "R", label: "RACE" },
] as const;

type SessionTimelineProps = {
  completedCodes: string[];
  activeCode?: string | null;
};

export function SessionTimeline({
  completedCodes,
  activeCode,
}: SessionTimelineProps) {
  return (
    <div className="flex items-center gap-1">
      {stages.map((stage, i) => {
        const isCompleted = completedCodes.includes(stage.code);
        const isActive = activeCode === stage.code;

        return (
          <div key={stage.code} className="flex items-center">
            {i > 0 && (
              <div
                className={`mx-0.5 h-[2px] w-4 ${isCompleted ? "bg-race-green" : "bg-white/10"}`}
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
              {stage.label}
            </div>
          </div>
        );
      })}
    </div>
  );
}
