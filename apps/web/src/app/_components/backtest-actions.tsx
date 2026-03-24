"use client";

import type { GPRegistryItem } from "@f1/shared-types";
import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { ActionButton } from "./action-button";

export function BacktestActions() {
  const router = useRouter();
  const [registry, setRegistry] = useState<GPRegistryItem[]>([]);
  const [selected, setSelected] = useState("");

  useEffect(() => {
    sdk
      .gpRegistry()
      .then((items) => {
        setRegistry(items);
        if (items.length > 0) setSelected(items[0].short_code);
      })
      .catch(() => {});
  }, []);

  return (
    <Panel title="Run Backtest" eyebrow="Actions">
      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1">
          <label
            htmlFor="gp-select"
            className="text-[10px] font-bold uppercase tracking-wider text-[#6b7280]"
          >
            Grand Prix
          </label>
          <select
            id="gp-select"
            value={selected}
            onChange={(e) => setSelected(e.target.value)}
            className="rounded-lg border border-white/10 bg-[#1a1a2e] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
          >
            {registry.map((gp) => (
              <option key={gp.short_code} value={gp.short_code}>
                {gp.name} ({gp.target_session_code}) — {gp.variant}
              </option>
            ))}
          </select>
        </div>
        <ActionButton
          label="Run Backtest"
          onAction={async () => {
            if (!selected) throw new Error("Select a GP first");
            const res = await sdk.runBacktest({ gp_short_code: selected });
            router.refresh();
            return res.message;
          }}
        />
      </div>
    </Panel>
  );
}
