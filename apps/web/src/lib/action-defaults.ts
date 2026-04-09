import type {
  RefreshLatestSessionRequest,
  RefreshedSessionSummary,
  SyncF1MarketsRequest,
} from "@f1/shared-types";

export function buildDashboardMarketSyncRequest(
  now: Date = new Date(),
): SyncF1MarketsRequest {
  const currentSeason = now.getUTCFullYear();
  return {
    max_pages: 2,
    search_fallback: false,
    start_year: currentSeason,
    end_year: currentSeason,
  };
}

export function buildLatestSessionRefreshRequest(
  meetingId: string,
  latestEndedSession: RefreshedSessionSummary | null,
): RefreshLatestSessionRequest {
  const isRaceSession = latestEndedSession?.sessionCode === "R";
  return {
    meeting_id: meetingId,
    search_fallback: false,
    discover_max_pages: 1,
    hydrate_market_history: false,
    sync_calendar: false,
    hydrate_f1_session_data: !isRaceSession,
    include_extended_f1_data: false,
    include_heavy_f1_data: false,
    refresh_artifacts: false,
  };
}
