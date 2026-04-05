"use client";

import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";
import { useRouter } from "next/navigation";
import { buildDashboardMarketSyncRequest } from "../../lib/action-defaults";
import { ActionButton } from "./action-button";

export function DashboardActions() {
  const router = useRouter();

  return (
    <Panel title="Quick Actions" eyebrow="Pipeline Controls">
      <div className="flex flex-wrap items-start gap-3">
        <ActionButton
          label="Sync F1 Calendar"
          onAction={async () => {
            const res = await sdk.syncCalendar({ season: 2026 });
            router.refresh();
            return res.message;
          }}
        />
        <ActionButton
          label="Sync F1 Markets"
          variant="secondary"
          onAction={async () => {
            const res = await sdk.syncF1Markets(
              buildDashboardMarketSyncRequest(),
            );
            router.refresh();
            return res.message;
          }}
        />
        <ActionButton
          label="Ingest Demo Data"
          variant="secondary"
          onAction={async () => {
            const res = await sdk.ingestDemo({ season: 2026 });
            router.refresh();
            return res.message;
          }}
        />
      </div>
    </Panel>
  );
}
