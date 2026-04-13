"use client";

import { useRouter } from "next/navigation";
import React, { startTransition, useEffect, useRef } from "react";

export function LineageAutoRefresh({
  hasActiveJobs,
  intervalMs = 5000,
}: {
  hasActiveJobs: boolean;
  intervalMs?: number;
}) {
  const router = useRouter();
  const refreshLineageRef = useRef<() => void>(() => {});

  refreshLineageRef.current = () => {
    startTransition(() => {
      router.refresh();
    });
  };

  useEffect(() => {
    if (!hasActiveJobs) {
      return;
    }

    const timer = window.setInterval(() => {
      refreshLineageRef.current();
    }, intervalMs);

    return () => {
      window.clearInterval(timer);
    };
  }, [hasActiveJobs, intervalMs]);

  if (!hasActiveJobs) {
    return null;
  }

  return (
    <p className="mt-2 text-xs text-[#9ca3af]">
      Active jobs detected · auto-refreshing every 5s
    </p>
  );
}
