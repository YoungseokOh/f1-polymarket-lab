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

  const selectedItem =
    registry.find((item) => item.short_code === selected) ?? null;

  return (
    <Panel title="Run or refresh a backtest" eyebrow="Actions">
      <div className="flex flex-col gap-4">
        <div className="flex flex-wrap items-end gap-3">
          <div className="flex flex-col gap-1">
            <label
              htmlFor="gp-select"
              className="text-[10px] font-bold uppercase tracking-wider text-[#6b7280]"
            >
              Experiment
            </label>
            <select
              id="gp-select"
              value={selected}
              onChange={(e) => setSelected(e.target.value)}
              className="min-w-[320px] rounded-lg border border-white/10 bg-[#1a1a2e] px-3 py-2 text-sm text-white focus:border-[#e10600] focus:outline-none"
            >
              {registry.map((gp) => (
                <option key={gp.short_code} value={gp.short_code}>
                  {gp.display_label}
                </option>
              ))}
            </select>
          </div>
          <ActionButton
            label="Run selected backtest"
            onAction={async () => {
              if (!selected) throw new Error("Select an experiment first");
              const res = await sdk.runBacktest({ gp_short_code: selected });
              router.refresh();
              return res.message;
            }}
          />
          <ActionButton
            label="Rebuild latest snapshot"
            variant="secondary"
            onAction={async () => {
              if (!selected) throw new Error("Select an experiment first");
              const res = await sdk.backfillBacktests({
                gp_short_code: selected,
                rebuild_missing: true,
              });
              router.refresh();
              return res.message;
            }}
          />
        </div>

        {selectedItem ? (
          <div className="rounded-lg border border-white/[0.05] bg-white/[0.03] p-4">
            <p className="text-sm font-medium text-white">
              {selectedItem.display_label}
            </p>
            <p className="mt-2 text-sm text-[#9ca3af]">
              {selectedItem.display_description}
            </p>
          </div>
        ) : null}
      </div>
    </Panel>
  );
}
