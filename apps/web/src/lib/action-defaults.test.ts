import { describe, expect, it } from "vitest";

import {
  buildDashboardDemoIngestRequest,
  buildDashboardMarketSyncRequest,
  buildLatestSessionRefreshRequest,
  buildPaperTradingLocalRefreshRequest,
} from "./action-defaults";

describe("buildDashboardMarketSyncRequest", () => {
  it("limits dashboard catalog syncs to recent seasons", () => {
    expect(
      buildDashboardMarketSyncRequest(new Date("2026-04-05T00:00:00Z")),
    ).toEqual({
      max_pages: 2,
      search_fallback: false,
      start_year: 2026,
      end_year: 2026,
    });
  });
});

describe("buildDashboardDemoIngestRequest", () => {
  it("keeps the dashboard demo ingest lightweight", () => {
    expect(buildDashboardDemoIngestRequest()).toEqual({
      season: 2026,
      weekends: 1,
      market_batches: 1,
    });
  });
});

describe("buildLatestSessionRefreshRequest", () => {
  it("keeps latest-session button refreshes off OpenF1 by default", () => {
    expect(
      buildLatestSessionRefreshRequest("meeting:1281", {
        id: "session:11249",
        sessionKey: 11249,
        sessionCode: "Q",
        sessionName: "Qualifying",
        dateEndUtc: "2026-03-28T07:00:00Z",
      }),
    ).toEqual({
      meeting_id: "meeting:1281",
      search_fallback: false,
      discover_max_pages: 1,
      hydrate_market_history: false,
      sync_calendar: false,
      hydrate_f1_session_data: false,
      include_extended_f1_data: false,
      include_heavy_f1_data: false,
      refresh_artifacts: false,
    });
  });

  it("skips heavy F1 hydration for race-session button refreshes", () => {
    expect(
      buildLatestSessionRefreshRequest("meeting:1279", {
        id: "session:11234",
        sessionKey: 11234,
        sessionCode: "R",
        sessionName: "Race",
        dateEndUtc: "2026-03-08T06:00:00Z",
      }),
    ).toEqual({
      meeting_id: "meeting:1279",
      search_fallback: false,
      discover_max_pages: 1,
      hydrate_market_history: false,
      sync_calendar: false,
      hydrate_f1_session_data: false,
      include_extended_f1_data: false,
      include_heavy_f1_data: false,
      refresh_artifacts: false,
    });
  });
});

describe("buildPaperTradingLocalRefreshRequest", () => {
  it("keeps paper trading model workflow refreshes off OpenF1 by default", () => {
    expect(buildPaperTradingLocalRefreshRequest("meeting:1284")).toEqual({
      meeting_id: "meeting:1284",
      search_fallback: true,
      discover_max_pages: 5,
      hydrate_market_history: true,
      sync_calendar: false,
      hydrate_f1_session_data: false,
      include_extended_f1_data: false,
      include_heavy_f1_data: false,
      refresh_artifacts: false,
    });
  });
});
