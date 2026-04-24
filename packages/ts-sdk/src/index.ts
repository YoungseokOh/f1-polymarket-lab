import { getWebEnv } from "@f1/config";
import type {
  ActionStatusResponse,
  ApiHealth,
  ArtifactRefreshSummary,
  BackfillBacktestsRequest,
  BacktestResult,
  CancelLiveTradeTicketRequest,
  CancelLiveTradeTicketResponse,
  CaptureLiveWeekendRequest,
  CaptureLiveWeekendResponse,
  ClearCalendarOverrideRequest,
  CreateLiveTradeTicketRequest,
  CreateLiveTradeTicketResponse,
  CurrentWeekendOperationsReadiness,
  CursorState,
  DataQualityResult,
  DriverAffinityEntry,
  DriverAffinityReport,
  DriverAffinitySegment,
  EnsemblePrediction,
  EntityMapping,
  ExecuteManualLivePaperTradeRequest,
  ExecuteManualLivePaperTradeResponse,
  F1Driver,
  F1Meeting,
  F1Session,
  F1Team,
  FeatureSnapshot,
  FreshnessRecord,
  GPRegistryItem,
  IngestDemoRequest,
  IngestionJobRun,
  IngestionJobRunSummary,
  LiveTradeExecution,
  LiveTradeSignalBoard,
  LiveTradeTicket,
  MarketTaxonomy,
  ModelPrediction,
  ModelRun,
  OperationReadiness,
  OpsCalendarMeeting,
  PaperTradePosition,
  PaperTradeSession,
  PolymarketEvent,
  PolymarketMarket,
  PricePoint,
  RecordLiveTradeFillRequest,
  RecordLiveTradeFillResponse,
  RefreshDriverAffinityRequest,
  RefreshDriverAffinityResponse,
  RefreshLatestSessionRequest,
  RefreshLatestSessionResponse,
  RefreshedSessionSummary,
  RunBacktestRequest,
  RunPaperTradeRequest,
  RunWeekendCockpitDetails,
  RunWeekendCockpitRequest,
  RunWeekendCockpitResponse,
  SetCalendarOverrideRequest,
  SignalDiagnostic,
  SignalRegistryEntry,
  SignalSnapshot,
  SyncCalendarRequest,
  SyncF1MarketsRequest,
  TradeDecision,
  WeekendCockpitSettlementSummary,
  WeekendCockpitStatus,
  WeekendCockpitStep,
} from "@f1/shared-types";

type QueryValue = boolean | number | string | null | undefined;

export type ListOptions = {
  limit?: number;
};

export type QualityResultOptions = ListOptions & {
  latestPerDataset?: boolean;
};

export type MeetingListOptions = ListOptions & {
  season?: number;
};

export type SessionListOptions = ListOptions & {
  season?: number;
  meetingId?: string;
  sessionCode?: string;
  isPractice?: boolean;
};

export type MarketListOptions = ListOptions & {
  ids?: string[];
  eventId?: string;
  taxonomy?: MarketTaxonomy;
  active?: boolean;
  closed?: boolean;
};

export type MappingListOptions = ListOptions & {
  f1SessionId?: string;
  polymarketMarketId?: string;
  minConfidence?: number;
};

function buildPath(path: string, query?: Record<string, QueryValue>): string {
  if (!query) {
    return path;
  }

  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === null || value === undefined) {
      continue;
    }
    searchParams.set(key, String(value));
  }

  const search = searchParams.toString();
  if (!search) {
    return path;
  }
  return `${path}?${search}`;
}

async function readErrorDetail(response: Response): Promise<string> {
  const raw = await response.text().catch(() => "");
  if (!raw) {
    return response.statusText;
  }

  try {
    const payload = JSON.parse(raw) as {
      detail?: unknown;
      message?: unknown;
    };
    if (typeof payload.detail === "string") {
      return payload.detail;
    }
    if (payload.detail !== undefined) {
      return JSON.stringify(payload.detail);
    }
    if (typeof payload.message === "string") {
      return payload.message;
    }
  } catch {}

  return raw;
}

async function apiGet<T>(
  path: string,
  query?: Record<string, QueryValue>,
): Promise<T> {
  const { NEXT_PUBLIC_API_BASE_URL } = getWebEnv();
  const response = await fetch(
    `${NEXT_PUBLIC_API_BASE_URL}${buildPath(path, query)}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
      cache: "no-store",
    },
  );

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(`API request failed: ${response.status} ${detail}`);
  }

  return (await response.json()) as T;
}

async function apiPost<TReq, TRes>(path: string, body: TReq): Promise<TRes> {
  const { NEXT_PUBLIC_API_BASE_URL } = getWebEnv();
  const response = await fetch(`${NEXT_PUBLIC_API_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(`API request failed: ${response.status} ${detail}`);
  }

  return (await response.json()) as TRes;
}

type FreshnessApi = {
  source: string;
  dataset: string;
  status: string;
  last_fetch_at: string | null;
  records_fetched: number;
};

type IngestionJobRunApi = {
  id: string;
  job_name: string;
  source: string;
  dataset: string;
  status: string;
  execute_mode: string;
  planned_inputs: Record<string, unknown> | null;
  cursor_after: Record<string, unknown> | null;
  records_written: number | null;
  error_message: string | null;
  queued_at: string | null;
  available_at: string | null;
  attempt_count: number;
  max_attempts: number;
  locked_by: string | null;
  locked_at: string | null;
  started_at: string;
  finished_at: string | null;
};

type CursorStateApi = {
  id: string;
  source: string;
  dataset: string;
  cursor_key: string;
  cursor_value: Record<string, unknown> | null;
  updated_at: string;
};

type DataQualityResultApi = {
  id: string;
  dataset: string;
  status: string;
  metrics_json: Record<string, unknown> | null;
  observed_at: string;
};

type F1MeetingApi = {
  id: string;
  meeting_key: number;
  season: number;
  round_number: number | null;
  meeting_name: string;
  meeting_slug: string | null;
  event_format: string | null;
  circuit_short_name: string | null;
  country_name: string | null;
  location: string | null;
  start_date_utc: string | null;
  end_date_utc: string | null;
};

type F1SessionApi = {
  id: string;
  session_key: number;
  meeting_id: string | null;
  session_name: string;
  session_code: string | null;
  session_type: string | null;
  date_start_utc: string | null;
  date_end_utc: string | null;
  is_practice: boolean;
};

type PolymarketEventApi = {
  id: string;
  slug: string;
  title: string;
  start_at_utc: string | null;
  end_at_utc: string | null;
  active: boolean;
  closed: boolean;
};

type PolymarketMarketApi = {
  id: string;
  event_id: string | null;
  question: string;
  slug: string | null;
  taxonomy: PolymarketMarket["taxonomy"];
  taxonomy_confidence: number | null;
  target_session_code: string | null;
  condition_id: string;
  question_id: string | null;
  best_bid: number | null;
  best_ask: number | null;
  last_trade_price: number | null;
  volume: number | null;
  liquidity: number | null;
  active: boolean;
  closed: boolean;
};

type EntityMappingApi = {
  id: string;
  f1_meeting_id: string | null;
  f1_session_id: string | null;
  polymarket_event_id: string | null;
  polymarket_market_id: string | null;
  mapping_type: string;
  confidence: number | null;
  matched_by: string | null;
  override_flag: boolean;
};

function mapFreshness(record: FreshnessApi): FreshnessRecord {
  return {
    source: record.source,
    dataset: record.dataset,
    status: record.status,
    lastFetchAt: record.last_fetch_at,
    recordsFetched: record.records_fetched,
  };
}

function mapIngestionJobRun(record: IngestionJobRunApi): IngestionJobRun {
  return {
    id: record.id,
    jobName: record.job_name,
    source: record.source,
    dataset: record.dataset,
    status: record.status,
    executeMode: record.execute_mode,
    plannedInputs: record.planned_inputs,
    cursorAfter: record.cursor_after,
    recordsWritten: record.records_written,
    errorMessage: record.error_message,
    queuedAt: record.queued_at,
    availableAt: record.available_at,
    attemptCount: record.attempt_count,
    maxAttempts: record.max_attempts,
    lockedBy: record.locked_by,
    lockedAt: record.locked_at,
    startedAt: record.started_at,
    finishedAt: record.finished_at,
  };
}

function mapCursorState(record: CursorStateApi): CursorState {
  return {
    id: record.id,
    source: record.source,
    dataset: record.dataset,
    cursorKey: record.cursor_key,
    cursorValue: record.cursor_value,
    updatedAt: record.updated_at,
  };
}

function mapDataQualityResult(record: DataQualityResultApi): DataQualityResult {
  return {
    id: record.id,
    dataset: record.dataset,
    status: record.status,
    metricsJson: record.metrics_json,
    observedAt: record.observed_at,
  };
}

function mapMeeting(record: F1MeetingApi): F1Meeting {
  return {
    id: record.id,
    meetingKey: record.meeting_key,
    season: record.season,
    roundNumber: record.round_number,
    meetingName: record.meeting_name,
    meetingSlug: record.meeting_slug,
    eventFormat: record.event_format,
    circuitShortName: record.circuit_short_name,
    countryName: record.country_name,
    location: record.location,
    startDateUtc: record.start_date_utc,
    endDateUtc: record.end_date_utc,
  };
}

function mapOpsCalendarMeeting(
  record: OpsCalendarMeetingApi,
): OpsCalendarMeeting {
  return {
    season: record.season,
    meetingKey: record.meeting_key,
    meetingSlug: record.meeting_slug,
    opsSlug: record.ops_slug,
    meetingName: record.meeting_name,
    roundNumber: record.round_number,
    eventFormat: record.event_format,
    startDateUtc: record.start_date_utc,
    endDateUtc: record.end_date_utc,
    countryName: record.country_name,
    location: record.location,
    status: record.status,
    sourceConflict: record.source_conflict,
    sourceLabel: record.source_label,
    sourceUrl: record.source_url,
    note: record.note,
  };
}

function mapSession(record: F1SessionApi): F1Session {
  return {
    id: record.id,
    sessionKey: record.session_key,
    meetingId: record.meeting_id,
    sessionName: record.session_name,
    sessionCode: record.session_code,
    sessionType: record.session_type,
    dateStartUtc: record.date_start_utc,
    dateEndUtc: record.date_end_utc,
    isPractice: record.is_practice,
  };
}

function mapEvent(record: PolymarketEventApi): PolymarketEvent {
  return {
    id: record.id,
    slug: record.slug,
    title: record.title,
    startAt: record.start_at_utc,
    endAt: record.end_at_utc,
    active: record.active,
    closed: record.closed,
  };
}

function mapMarket(record: PolymarketMarketApi): PolymarketMarket {
  return {
    id: record.id,
    eventId: record.event_id,
    question: record.question,
    slug: record.slug,
    taxonomy: record.taxonomy,
    taxonomyConfidence: record.taxonomy_confidence,
    targetSessionCode: record.target_session_code,
    conditionId: record.condition_id,
    questionId: record.question_id,
    bestBid: record.best_bid,
    bestAsk: record.best_ask,
    lastTradePrice: record.last_trade_price,
    volume: record.volume,
    liquidity: record.liquidity,
    active: record.active,
    closed: record.closed,
  };
}

function mapMapping(record: EntityMappingApi): EntityMapping {
  return {
    id: record.id,
    f1MeetingId: record.f1_meeting_id,
    f1SessionId: record.f1_session_id,
    polymarketEventId: record.polymarket_event_id,
    polymarketMarketId: record.polymarket_market_id,
    mappingType: record.mapping_type,
    confidence: record.confidence,
    matchedBy: record.matched_by,
    overrideFlag: record.override_flag,
  };
}

export const sdk = {
  health: () => apiGet<ApiHealth>("/health"),
  freshness: async (options?: ListOptions) =>
    (
      await apiGet<FreshnessApi[]>("/api/v1/freshness", {
        limit: options?.limit,
      })
    ).map(mapFreshness),
  ingestionJobs: async (options?: ListOptions) =>
    (
      await apiGet<IngestionJobRunApi[]>("/api/v1/lineage/jobs", {
        limit: options?.limit,
      })
    ).map(mapIngestionJobRun),
  cursorStates: async (options?: ListOptions) =>
    (
      await apiGet<CursorStateApi[]>("/api/v1/lineage/cursors", {
        limit: options?.limit,
      })
    ).map(mapCursorState),
  qualityResults: async (options?: QualityResultOptions) =>
    (
      await apiGet<DataQualityResultApi[]>("/api/v1/quality/results", {
        limit: options?.limit,
        latest_per_dataset: options?.latestPerDataset,
      })
    ).map(mapDataQualityResult),
  meetings: async (options?: MeetingListOptions) =>
    (
      await apiGet<F1MeetingApi[]>("/api/v1/f1/meetings", {
        limit: options?.limit,
        season: options?.season,
      })
    ).map(mapMeeting),
  sessions: async (options?: SessionListOptions) =>
    (
      await apiGet<F1SessionApi[]>("/api/v1/f1/sessions", {
        limit: options?.limit,
        season: options?.season,
        meeting_id: options?.meetingId,
        session_code: options?.sessionCode,
        is_practice: options?.isPractice,
      })
    ).map(mapSession),
  events: async (options?: ListOptions) =>
    (
      await apiGet<PolymarketEventApi[]>("/api/v1/polymarket/events", {
        limit: options?.limit,
      })
    ).map(mapEvent),
  markets: async (options?: MarketListOptions) =>
    (
      await apiGet<PolymarketMarketApi[]>("/api/v1/polymarket/markets", {
        limit: options?.limit,
        market_ids: options?.ids?.length ? options.ids.join(",") : undefined,
        event_id: options?.eventId,
        taxonomy: options?.taxonomy,
        active: options?.active,
        closed: options?.closed,
      })
    ).map(mapMarket),
  mappings: async (options?: MappingListOptions) =>
    (
      await apiGet<EntityMappingApi[]>("/api/v1/mappings", {
        limit: options?.limit,
        f1_session_id: options?.f1SessionId,
        polymarket_market_id: options?.polymarketMarketId,
        min_confidence: options?.minConfidence,
      })
    ).map(mapMapping),
  meeting: async (meetingId: string): Promise<F1Meeting> => {
    const record = await apiGet<F1MeetingApi>(
      `/api/v1/f1/meetings/${encodeURIComponent(meetingId)}`,
    );
    return mapMeeting(record);
  },
  meetingSessions: async (meetingId: string): Promise<F1Session[]> => {
    const records = await apiGet<F1SessionApi[]>(
      `/api/v1/f1/meetings/${encodeURIComponent(meetingId)}/sessions`,
    );
    return records.map(mapSession);
  },
  drivers: async (): Promise<F1Driver[]> => {
    const records = await apiGet<F1DriverApi[]>("/api/v1/f1/drivers");
    return records.map(mapDriver);
  },
  teams: async (): Promise<F1Team[]> => {
    const records = await apiGet<F1TeamApi[]>("/api/v1/f1/teams");
    return records.map(mapTeam);
  },
  market: async (marketId: string): Promise<PolymarketMarket> => {
    const record = await apiGet<PolymarketMarketApi>(
      `/api/v1/polymarket/markets/${encodeURIComponent(marketId)}`,
    );
    return mapMarket(record);
  },
  marketPrices: async (marketId: string): Promise<PricePoint[]> => {
    const records = await apiGet<PricePointApi[]>(
      `/api/v1/polymarket/markets/${encodeURIComponent(marketId)}/prices`,
    );
    return records.map(mapPricePoint);
  },
  modelRuns: async (): Promise<ModelRun[]> => {
    const records = await apiGet<ModelRunApi[]>("/api/v1/model-runs");
    return records.map(mapModelRun);
  },
  predictions: async (options?: {
    modelRunId?: string;
    marketId?: string;
    limit?: number;
  }): Promise<ModelPrediction[]> => {
    const records = await apiGet<ModelPredictionApi[]>("/api/v1/predictions", {
      model_run_id: options?.modelRunId,
      market_id: options?.marketId,
      limit: options?.limit,
    });
    return records.map(mapModelPrediction);
  },
  signalRegistry: async (): Promise<SignalRegistryEntry[]> => {
    const records = await apiGet<SignalRegistryApi[]>(
      "/api/v1/signals/registry",
    );
    return records.map(mapSignalRegistryEntry);
  },
  signalSnapshots: async (options?: {
    modelRunId?: string;
    marketId?: string;
    signalCode?: string;
    limit?: number;
  }): Promise<SignalSnapshot[]> => {
    const records = await apiGet<SignalSnapshotApi[]>(
      "/api/v1/signals/snapshots",
      {
        model_run_id: options?.modelRunId,
        market_id: options?.marketId,
        signal_code: options?.signalCode,
        limit: options?.limit,
      },
    );
    return records.map(mapSignalSnapshot);
  },
  signalDiagnostics: async (options?: {
    modelRunId?: string;
    marketGroup?: string;
  }): Promise<SignalDiagnostic[]> => {
    const records = await apiGet<SignalDiagnosticApi[]>(
      "/api/v1/signals/diagnostics",
      {
        model_run_id: options?.modelRunId,
        market_group: options?.marketGroup,
      },
    );
    return records.map(mapSignalDiagnostic);
  },
  ensemblePredictions: async (options?: {
    modelRunId?: string;
    marketId?: string;
    limit?: number;
  }): Promise<EnsemblePrediction[]> => {
    const records = await apiGet<EnsemblePredictionApi[]>(
      "/api/v1/ensemble/predictions",
      {
        model_run_id: options?.modelRunId,
        market_id: options?.marketId,
        limit: options?.limit,
      },
    );
    return records.map(mapEnsemblePrediction);
  },
  tradeDecisions: async (options?: {
    modelRunId?: string;
    marketId?: string;
    decisionStatus?: string;
    limit?: number;
  }): Promise<TradeDecision[]> => {
    const records = await apiGet<TradeDecisionApi[]>(
      "/api/v1/trade-decisions",
      {
        model_run_id: options?.modelRunId,
        market_id: options?.marketId,
        decision_status: options?.decisionStatus,
        limit: options?.limit,
      },
    );
    return records.map(mapTradeDecision);
  },
  backtestResults: async (): Promise<BacktestResult[]> => {
    const records = await apiGet<BacktestResultApi[]>(
      "/api/v1/backtest/results",
    );
    return records.map(mapBacktestResult);
  },
  snapshots: async (): Promise<FeatureSnapshot[]> => {
    const records = await apiGet<FeatureSnapshotApi[]>("/api/v1/snapshots");
    return records.map(mapFeatureSnapshot);
  },

  // Action endpoints
  gpRegistry: () => apiGet<GPRegistryItem[]>("/api/v1/actions/gp-registry"),
  opsCalendar: async (options?: {
    season?: number;
    includeCancelled?: boolean;
  }): Promise<OpsCalendarMeeting[]> => {
    const records = await apiGet<OpsCalendarMeetingApi[]>(
      "/api/v1/ops-calendar",
      {
        season: options?.season,
        include_cancelled: options?.includeCancelled,
      },
    );
    return records.map(mapOpsCalendarMeeting);
  },
  ingestDemo: (body?: IngestDemoRequest) =>
    apiPost<IngestDemoRequest, ActionStatusResponse>(
      "/api/v1/actions/ingest-demo",
      body ?? {},
    ),
  syncCalendar: (body?: SyncCalendarRequest) =>
    apiPost<SyncCalendarRequest, ActionStatusResponse>(
      "/api/v1/actions/sync-calendar",
      body ?? {},
    ),
  setCalendarOverride: (body: SetCalendarOverrideRequest) =>
    apiPost<SetCalendarOverrideRequest, ActionStatusResponse>(
      "/api/v1/actions/set-calendar-override",
      body,
    ),
  clearCalendarOverride: (body: ClearCalendarOverrideRequest) =>
    apiPost<ClearCalendarOverrideRequest, ActionStatusResponse>(
      "/api/v1/actions/clear-calendar-override",
      body,
    ),
  runBacktest: (body: RunBacktestRequest) =>
    apiPost<RunBacktestRequest, ActionStatusResponse>(
      "/api/v1/actions/run-backtest",
      body,
    ),
  backfillBacktests: (body?: BackfillBacktestsRequest) =>
    apiPost<BackfillBacktestsRequest, ActionStatusResponse>(
      "/api/v1/actions/backfill-backtests",
      body ?? {},
    ),
  syncF1Markets: (body?: SyncF1MarketsRequest) =>
    apiPost<SyncF1MarketsRequest, ActionStatusResponse>(
      "/api/v1/actions/sync-f1-markets",
      body ?? {},
    ),
  refreshLatestSession: (body: RefreshLatestSessionRequest) =>
    apiPost<RefreshLatestSessionRequest, RefreshLatestSessionResponseApi>(
      "/api/v1/actions/refresh-latest-session",
      body,
    ).then(mapRefreshLatestSessionResponse),
  captureLiveWeekend: (body: CaptureLiveWeekendRequest) =>
    apiPost<CaptureLiveWeekendRequest, CaptureLiveWeekendResponseApi>(
      "/api/v1/actions/capture-live-weekend",
      body,
    ).then(mapCaptureLiveWeekendResponse),
  createLiveTradeTicket: (body: CreateLiveTradeTicketRequest) =>
    apiPost<CreateLiveTradeTicketRequest, CreateLiveTradeTicketResponseApi>(
      "/api/v1/actions/create-live-trade-ticket",
      body,
    ).then(mapCreateLiveTradeTicketResponse),
  recordLiveTradeFill: (body: RecordLiveTradeFillRequest) =>
    apiPost<RecordLiveTradeFillRequest, RecordLiveTradeFillResponseApi>(
      "/api/v1/actions/record-live-trade-fill",
      body,
    ).then(mapRecordLiveTradeFillResponse),
  cancelLiveTradeTicket: (body: CancelLiveTradeTicketRequest) =>
    apiPost<CancelLiveTradeTicketRequest, CancelLiveTradeTicketResponseApi>(
      "/api/v1/actions/cancel-live-trade-ticket",
      body,
    ).then(mapCancelLiveTradeTicketResponse),
  executeManualLivePaperTrade: (body: ExecuteManualLivePaperTradeRequest) =>
    apiPost<
      ExecuteManualLivePaperTradeRequest,
      ExecuteManualLivePaperTradeResponseApi
    >("/api/v1/actions/execute-manual-live-paper-trade", body).then(
      mapExecuteManualLivePaperTradeResponse,
    ),
  weekendCockpitStatus: async (
    gpShortCode?: string,
  ): Promise<WeekendCockpitStatus> => {
    const path = gpShortCode
      ? `/api/v1/weekend-cockpit/status?gp_short_code=${encodeURIComponent(gpShortCode)}`
      : "/api/v1/weekend-cockpit/status";
    const record = await apiGet<WeekendCockpitStatusApi>(path);
    return mapWeekendCockpitStatus(record);
  },
  currentWeekendReadiness: async (options?: {
    gpShortCode?: string;
    season?: number;
    meetingKey?: number;
  }): Promise<CurrentWeekendOperationsReadiness> => {
    const record = await apiGet<CurrentWeekendOperationsReadinessApi>(
      "/api/v1/operations/current-weekend-readiness",
      {
        gp_short_code: options?.gpShortCode,
        season: options?.season,
        meeting_key: options?.meetingKey,
      },
    );
    return mapCurrentWeekendOperationsReadiness(record);
  },
  runWeekendCockpit: (body?: RunWeekendCockpitRequest) =>
    apiPost<RunWeekendCockpitRequest, RunWeekendCockpitResponseApi>(
      "/api/v1/actions/run-weekend-cockpit",
      body ?? {},
    ).then(mapRunWeekendCockpitResponse),
  driverAffinity: async (
    season = 2026,
    meetingKey?: number,
  ): Promise<DriverAffinityReport> => {
    const record = await apiGet<DriverAffinityReportApi>(
      "/api/v1/driver-affinity",
      {
        season,
        meeting_key: meetingKey,
      },
    );
    return mapDriverAffinityReport(record);
  },
  refreshDriverAffinity: (body?: RefreshDriverAffinityRequest) =>
    apiPost<RefreshDriverAffinityRequest, RefreshDriverAffinityResponseApi>(
      "/api/v1/actions/refresh-driver-affinity",
      body ?? {},
    ).then(mapRefreshDriverAffinityResponse),

  liveTradeSignalBoard: async (
    gpShortCode: string,
  ): Promise<LiveTradeSignalBoard> => {
    const record = await apiGet<LiveTradeSignalBoardApi>(
      `/api/v1/live-trading/signal-board?gp_short_code=${encodeURIComponent(gpShortCode)}`,
    );
    return mapLiveTradeSignalBoard(record);
  },
  liveTradeTickets: async (options?: {
    gpSlug?: string;
    status?: string;
    limit?: number;
  }): Promise<LiveTradeTicket[]> => {
    const records = await apiGet<LiveTradeTicketApi[]>(
      "/api/v1/live-trading/tickets",
      {
        gp_slug: options?.gpSlug,
        status: options?.status,
        limit: options?.limit,
      },
    );
    return records.map(mapLiveTradeTicket);
  },
  liveTradeExecutions: async (options?: {
    gpSlug?: string;
    status?: string;
    limit?: number;
  }): Promise<LiveTradeExecution[]> => {
    const records = await apiGet<LiveTradeExecutionApi[]>(
      "/api/v1/live-trading/executions",
      {
        gp_slug: options?.gpSlug,
        status: options?.status,
        limit: options?.limit,
      },
    );
    return records.map(mapLiveTradeExecution);
  },

  // Paper trading
  paperTradeSessions: async (gpSlug?: string): Promise<PaperTradeSession[]> => {
    const path = gpSlug
      ? `/api/v1/paper-trading/sessions?gp_slug=${encodeURIComponent(gpSlug)}`
      : "/api/v1/paper-trading/sessions";
    const records = await apiGet<PaperTradeSessionApi[]>(path);
    return records.map(mapPaperTradeSession);
  },
  paperTradeSession: async (sessionId: string): Promise<PaperTradeSession> => {
    const record = await apiGet<PaperTradeSessionApi>(
      `/api/v1/paper-trading/sessions/${encodeURIComponent(sessionId)}`,
    );
    return mapPaperTradeSession(record);
  },
  paperTradePositions: async (
    sessionId: string,
  ): Promise<PaperTradePosition[]> => {
    const records = await apiGet<PaperTradePositionApi[]>(
      `/api/v1/paper-trading/sessions/${encodeURIComponent(sessionId)}/positions`,
    );
    return records.map(mapPaperTradePosition);
  },
  runPaperTrade: (body: RunPaperTradeRequest) =>
    apiPost<RunPaperTradeRequest, ActionStatusResponse>(
      "/api/v1/actions/run-paper-trade",
      body,
    ),
};

type ModelRunApi = {
  id: string;
  stage: string;
  model_family: string;
  model_name: string;
  dataset_version: string | null;
  feature_snapshot_id: string | null;
  config_json: Record<string, unknown> | null;
  metrics_json: Record<string, unknown> | null;
  artifact_uri: string | null;
  registry_run_id: string | null;
  promotion_status: string;
  promoted_at: string | null;
  created_at: string;
};

type ModelPredictionApi = {
  id: string;
  model_run_id: string;
  market_id: string | null;
  token_id: string | null;
  as_of_ts: string;
  probability_yes: number | null;
  probability_no: number | null;
  raw_score: number | null;
  calibration_version: string | null;
};

type BacktestResultApi = {
  id: string;
  backtest_run_id: string;
  strategy_name: string;
  stage: string;
  start_at: string | null;
  end_at: string | null;
  metrics_json: Record<string, unknown> | null;
  created_at: string;
};

type FeatureSnapshotApi = {
  id: string;
  market_id: string | null;
  session_id: string | null;
  as_of_ts: string;
  snapshot_type: string;
  feature_version: string;
  storage_path: string | null;
  row_count: number | null;
};

type SignalRegistryApi = {
  id: string;
  signal_code: string;
  signal_family: string;
  market_taxonomy: SignalRegistryEntry["marketTaxonomy"];
  market_group: SignalRegistryEntry["marketGroup"];
  description: string | null;
  version: string;
  config_json: Record<string, unknown> | null;
  is_active: boolean;
  created_at: string;
};

type SignalSnapshotApi = {
  id: string;
  model_run_id: string;
  feature_snapshot_id: string | null;
  market_id: string | null;
  token_id: string | null;
  event_id: string | null;
  market_taxonomy: SignalSnapshot["marketTaxonomy"];
  market_group: SignalSnapshot["marketGroup"];
  meeting_key: number | null;
  as_of_ts: string;
  signal_code: string;
  signal_version: string;
  p_yes_raw: number | null;
  p_yes_calibrated: number | null;
  p_market_ref: number | null;
  delta_logit: number | null;
  freshness_sec: number | null;
  coverage_flag: boolean;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
};

type SignalDiagnosticApi = {
  id: string;
  model_run_id: string;
  signal_code: string;
  market_taxonomy: SignalDiagnostic["marketTaxonomy"];
  market_group: SignalDiagnostic["marketGroup"];
  phase_bucket: string | null;
  brier: number | null;
  log_loss: number | null;
  ece: number | null;
  skill_vs_market: number | null;
  coverage_rate: number | null;
  residual_correlation_json: Record<string, unknown> | null;
  stability_json: Record<string, unknown> | null;
  metrics_json: Record<string, unknown> | null;
  created_at: string;
};

type EnsemblePredictionApi = {
  id: string;
  model_run_id: string;
  feature_snapshot_id: string | null;
  market_id: string | null;
  token_id: string | null;
  event_id: string | null;
  market_taxonomy: EnsemblePrediction["marketTaxonomy"];
  market_group: EnsemblePrediction["marketGroup"];
  meeting_key: number | null;
  as_of_ts: string;
  p_market_ref: number | null;
  p_yes_ensemble: number | null;
  z_market: number | null;
  z_ensemble: number | null;
  intercept: number | null;
  disagreement_score: number | null;
  effective_n: number | null;
  uncertainty_score: number | null;
  contributions_json: Record<string, unknown> | null;
  coverage_json: Record<string, unknown> | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
};

type TradeDecisionApi = {
  id: string;
  model_run_id: string;
  ensemble_prediction_id: string | null;
  feature_snapshot_id: string | null;
  market_id: string | null;
  token_id: string | null;
  event_id: string | null;
  market_taxonomy: TradeDecision["marketTaxonomy"];
  market_group: TradeDecision["marketGroup"];
  meeting_key: number | null;
  as_of_ts: string;
  side: string;
  edge: number | null;
  threshold: number | null;
  spread: number | null;
  depth: number | null;
  kelly_fraction_raw: number | null;
  disagreement_penalty: number | null;
  liquidity_factor: number | null;
  size_fraction: number | null;
  decision_status: string;
  decision_reason: string | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
};

function mapModelRun(record: ModelRunApi): ModelRun {
  return {
    id: record.id,
    stage: record.stage,
    modelFamily: record.model_family,
    modelName: record.model_name,
    datasetVersion: record.dataset_version,
    featureSnapshotId: record.feature_snapshot_id,
    configJson: record.config_json,
    metricsJson: record.metrics_json,
    artifactUri: record.artifact_uri,
    registryRunId: record.registry_run_id,
    promotionStatus: record.promotion_status,
    promotedAt: record.promoted_at,
    createdAt: record.created_at,
  };
}

function mapModelPrediction(record: ModelPredictionApi): ModelPrediction {
  return {
    id: record.id,
    modelRunId: record.model_run_id,
    marketId: record.market_id,
    tokenId: record.token_id,
    asOfTs: record.as_of_ts,
    probabilityYes: record.probability_yes,
    probabilityNo: record.probability_no,
    rawScore: record.raw_score,
    calibrationVersion: record.calibration_version,
  };
}

function mapBacktestResult(record: BacktestResultApi): BacktestResult {
  return {
    id: record.id,
    backtestRunId: record.backtest_run_id,
    strategyName: record.strategy_name,
    stage: record.stage,
    startAt: record.start_at,
    endAt: record.end_at,
    metricsJson: record.metrics_json,
    createdAt: record.created_at,
  };
}

function mapFeatureSnapshot(record: FeatureSnapshotApi): FeatureSnapshot {
  return {
    id: record.id,
    marketId: record.market_id,
    sessionId: record.session_id,
    asOfTs: record.as_of_ts,
    snapshotType: record.snapshot_type,
    featureVersion: record.feature_version,
    storagePath: record.storage_path,
    rowCount: record.row_count,
  };
}

function mapSignalRegistryEntry(
  record: SignalRegistryApi,
): SignalRegistryEntry {
  return {
    id: record.id,
    signalCode: record.signal_code,
    signalFamily: record.signal_family,
    marketTaxonomy: record.market_taxonomy,
    marketGroup: record.market_group,
    description: record.description,
    version: record.version,
    configJson: record.config_json,
    isActive: record.is_active,
    createdAt: record.created_at,
  };
}

function mapSignalSnapshot(record: SignalSnapshotApi): SignalSnapshot {
  return {
    id: record.id,
    modelRunId: record.model_run_id,
    featureSnapshotId: record.feature_snapshot_id,
    marketId: record.market_id,
    tokenId: record.token_id,
    eventId: record.event_id,
    marketTaxonomy: record.market_taxonomy,
    marketGroup: record.market_group,
    meetingKey: record.meeting_key,
    asOfTs: record.as_of_ts,
    signalCode: record.signal_code,
    signalVersion: record.signal_version,
    pYesRaw: record.p_yes_raw,
    pYesCalibrated: record.p_yes_calibrated,
    pMarketRef: record.p_market_ref,
    deltaLogit: record.delta_logit,
    freshnessSec: record.freshness_sec,
    coverageFlag: record.coverage_flag,
    metadataJson: record.metadata_json,
    createdAt: record.created_at,
  };
}

function mapSignalDiagnostic(record: SignalDiagnosticApi): SignalDiagnostic {
  return {
    id: record.id,
    modelRunId: record.model_run_id,
    signalCode: record.signal_code,
    marketTaxonomy: record.market_taxonomy,
    marketGroup: record.market_group,
    phaseBucket: record.phase_bucket,
    brier: record.brier,
    logLoss: record.log_loss,
    ece: record.ece,
    skillVsMarket: record.skill_vs_market,
    coverageRate: record.coverage_rate,
    residualCorrelationJson: record.residual_correlation_json,
    stabilityJson: record.stability_json,
    metricsJson: record.metrics_json,
    createdAt: record.created_at,
  };
}

function mapEnsemblePrediction(
  record: EnsemblePredictionApi,
): EnsemblePrediction {
  return {
    id: record.id,
    modelRunId: record.model_run_id,
    featureSnapshotId: record.feature_snapshot_id,
    marketId: record.market_id,
    tokenId: record.token_id,
    eventId: record.event_id,
    marketTaxonomy: record.market_taxonomy,
    marketGroup: record.market_group,
    meetingKey: record.meeting_key,
    asOfTs: record.as_of_ts,
    pMarketRef: record.p_market_ref,
    pYesEnsemble: record.p_yes_ensemble,
    zMarket: record.z_market,
    zEnsemble: record.z_ensemble,
    intercept: record.intercept,
    disagreementScore: record.disagreement_score,
    effectiveN: record.effective_n,
    uncertaintyScore: record.uncertainty_score,
    contributionsJson: record.contributions_json,
    coverageJson: record.coverage_json,
    metadataJson: record.metadata_json,
    createdAt: record.created_at,
  };
}

function mapTradeDecision(record: TradeDecisionApi): TradeDecision {
  return {
    id: record.id,
    modelRunId: record.model_run_id,
    ensemblePredictionId: record.ensemble_prediction_id,
    featureSnapshotId: record.feature_snapshot_id,
    marketId: record.market_id,
    tokenId: record.token_id,
    eventId: record.event_id,
    marketTaxonomy: record.market_taxonomy,
    marketGroup: record.market_group,
    meetingKey: record.meeting_key,
    asOfTs: record.as_of_ts,
    side: record.side,
    edge: record.edge,
    threshold: record.threshold,
    spread: record.spread,
    depth: record.depth,
    kellyFractionRaw: record.kelly_fraction_raw,
    disagreementPenalty: record.disagreement_penalty,
    liquidityFactor: record.liquidity_factor,
    sizeFraction: record.size_fraction,
    decisionStatus: record.decision_status,
    decisionReason: record.decision_reason,
    metadataJson: record.metadata_json,
    createdAt: record.created_at,
  };
}

type F1DriverApi = {
  id: string;
  driver_number: number;
  broadcast_name: string | null;
  full_name: string | null;
  first_name: string | null;
  last_name: string | null;
  name_acronym: string | null;
  team_id: string | null;
  country_code: string | null;
  headshot_url: string | null;
};

type F1TeamApi = {
  id: string;
  team_name: string;
  team_color: string | null;
};

type PricePointApi = {
  id: string;
  market_id: string;
  token_id: string;
  observed_at_utc: string;
  price: number | null;
  midpoint: number | null;
  best_bid: number | null;
  best_ask: number | null;
};

function mapDriver(record: F1DriverApi): F1Driver {
  return {
    id: record.id,
    driverNumber: record.driver_number,
    broadcastName: record.broadcast_name,
    fullName: record.full_name,
    firstName: record.first_name,
    lastName: record.last_name,
    nameAcronym: record.name_acronym,
    teamId: record.team_id,
    countryCode: record.country_code,
    headshotUrl: record.headshot_url,
  };
}

function mapTeam(record: F1TeamApi): F1Team {
  return {
    id: record.id,
    teamName: record.team_name,
    teamColor: record.team_color,
  };
}

function mapPricePoint(record: PricePointApi): PricePoint {
  return {
    id: record.id,
    marketId: record.market_id,
    tokenId: record.token_id,
    observedAtUtc: record.observed_at_utc,
    price: record.price,
    midpoint: record.midpoint,
    bestBid: record.best_bid,
    bestAsk: record.best_ask,
  };
}

type PaperTradeSessionApi = {
  id: string;
  gp_slug: string;
  snapshot_id: string | null;
  model_run_id: string | null;
  status: string;
  config_json: Record<string, unknown> | null;
  summary_json: Record<string, unknown> | null;
  log_path: string | null;
  started_at: string;
  finished_at: string | null;
};

type PaperTradePositionApi = {
  id: string;
  session_id: string;
  market_id: string;
  token_id: string | null;
  side: string;
  quantity: number;
  entry_price: number;
  entry_time: string;
  model_prob: number;
  market_prob: number;
  edge: number;
  status: string;
  exit_price: number | null;
  exit_time: string | null;
  realized_pnl: number | null;
};

type WeekendCockpitStepApi = {
  key: string;
  label: string;
  status: string;
  detail: string;
  session_code: string | null;
  session_key: number | null;
  count: number | null;
  reason_code: string | null;
  actionable_after_utc: string | null;
  resource_label: string | null;
};

type WeekendCockpitStatusApi = {
  now: string;
  auto_selected_gp_short_code: string;
  selected_gp_short_code: string;
  selected_config: GPRegistryItem;
  calendar_status: string;
  meeting_slug: string;
  source_conflict: boolean;
  override_source_url: string | null;
  calendar_meetings: OpsCalendarMeetingApi[];
  cancelled_meetings: OpsCalendarMeetingApi[];
  available_configs: GPRegistryItem[];
  meeting: F1MeetingApi | null;
  focus_session: F1SessionApi | null;
  focus_status: WeekendCockpitStatus["focusStatus"];
  timeline_completed_codes: string[];
  timeline_active_code: string | null;
  source_session: F1SessionApi | null;
  target_session: F1SessionApi | null;
  latest_paper_session: PaperTradeSessionApi | null;
  steps: WeekendCockpitStepApi[];
  blockers: string[];
  ready_to_run: boolean;
  model_ready: boolean;
  required_stage: string | null;
  active_model_run_id: string | null;
  model_blockers: string[];
  session_stage_statuses: Array<{
    gp_short_code: string;
    target_session_code: string;
    required_stage: string | null;
    model_ready: boolean;
    active_model_run_id: string | null;
    model_blockers: string[];
    display_label: string;
  }>;
  live_ticket_summary: {
    ticket_count: number;
    open_ticket_count: number;
    filled_ticket_count: number;
    cancelled_ticket_count: number;
  };
  live_execution_summary: {
    execution_count: number;
    filled_execution_count: number;
  };
  primary_action_title: string;
  primary_action_description: string;
  primary_action_cta: string;
  explanation: string;
};

type IngestionJobRunSummaryApi = {
  id: string;
  job_name: string;
  status: string;
  records_written: number | null;
  started_at: string | null;
  finished_at: string | null;
  error_message: string | null;
};

type OperationReadinessApi = {
  key: string;
  label: string;
  status: string;
  message: string;
  blockers: string[];
  warnings: string[];
  meeting_key: number | null;
  meeting_name: string | null;
  gp_short_code: string | null;
  session_code: string | null;
  session_key: number | null;
  actionable_after_utc: string | null;
  openf1_credentials_configured: boolean;
  last_job_run: IngestionJobRunSummaryApi | null;
  last_report_path: string | null;
  linked_market_count?: number | null;
  token_count?: number | null;
  missing_session_keys?: number[];
  report_is_fresh?: boolean | null;
  latest_ended_session_code?: string | null;
  latest_ended_session_end_utc?: string | null;
};

type CurrentWeekendOperationsReadinessApi = {
  now: string;
  selected_gp_short_code: string;
  selected_config: GPRegistryItem;
  meeting: F1MeetingApi | null;
  latest_ended_session: F1SessionApi | null;
  next_active_session: F1SessionApi | null;
  openf1_credentials_configured: boolean;
  actions: OperationReadinessApi[];
  blockers: string[];
  warnings: string[];
};

type OpsCalendarMeetingApi = {
  season: number;
  meeting_key: number;
  meeting_slug: string;
  ops_slug: string;
  meeting_name: string;
  round_number: number | null;
  event_format: string | null;
  start_date_utc: string | null;
  end_date_utc: string | null;
  country_name: string | null;
  location: string | null;
  status: string;
  source_conflict: boolean;
  source_label: string | null;
  source_url: string | null;
  note: string | null;
};

type RunWeekendCockpitResponseApi = {
  action: string;
  status: string;
  message: string;
  gp_short_code: string;
  snapshot_id: string | null;
  model_run_id: string | null;
  pt_session_id: string | null;
  job_run_id: string | null;
  report_path: string | null;
  preflight_summary: OperationReadinessApi | null;
  warnings: string[];
  executed_steps: WeekendCockpitStepApi[];
  details: RunWeekendCockpitDetailsApi | null;
};

type WeekendCockpitSettlementSummaryApi = {
  settled_session_ids: string[];
  settled_gp_slugs: string[];
  settled_positions: number;
  manual_positions_settled: number;
  unresolved_positions: number;
  unresolved_session_ids: string[];
  winner_driver_id: string | null;
};

type RunWeekendCockpitDetailsApi = {
  snapshot_id: string | null;
  model_run_id: string | null;
  baseline: string | null;
  pt_session_id: string | null;
  log_path: string | null;
  total_signals: number | null;
  trades_executed: number | null;
  open_positions: number | null;
  settled_positions: number | null;
  win_count: number | null;
  loss_count: number | null;
  win_rate: number | null;
  total_pnl: number | null;
  daily_pnl: number | null;
  settlement: WeekendCockpitSettlementSummaryApi | null;
};

type RefreshedSessionSummaryApi = {
  id: string;
  session_key: number;
  session_code: string | null;
  session_name: string;
  date_end_utc: string | null;
};

type ArtifactRefreshSummaryApi = {
  gp_short_code: string;
  status: string;
  snapshot_id: string | null;
  rebuilt_snapshot: boolean;
  bet_count: number | null;
  total_pnl: number | null;
  reason: string | null;
};

type RefreshLatestSessionResponseApi = {
  action: string;
  status: string;
  message: string;
  meeting_id: string;
  meeting_name: string;
  refreshed_session: RefreshedSessionSummaryApi;
  f1_records_written: number;
  markets_discovered: number;
  mappings_written: number;
  markets_hydrated: number;
  artifacts_refreshed: ArtifactRefreshSummaryApi[];
};

type CaptureLiveWeekendResponseApi = {
  action: string;
  status: string;
  message: string;
  job_run_id: string;
  session_key: number;
  capture_seconds: number;
  openf1_messages: number;
  polymarket_messages: number;
  market_count: number;
  polymarket_market_ids: string[];
  records_written: number;
  report_path: string | null;
  preflight_summary: OperationReadinessApi | null;
  warnings: string[];
  summary: {
    openf1_topics: Array<{
      key: string;
      count: number;
    }>;
    polymarket_event_types: Array<{
      key: string;
      count: number;
    }>;
    observed_market_count: number;
    observed_token_count: number;
    market_quotes: Array<{
      market_id: string;
      token_id: string | null;
      outcome: string | null;
      event_type: string;
      observed_at_utc: string;
      price: number | null;
      best_bid: number | null;
      best_ask: number | null;
      midpoint: number | null;
      spread: number | null;
      size: number | null;
      side: string | null;
    }>;
  };
};

type ExecuteManualLivePaperTradeResponseApi = {
  action: string;
  status: string;
  message: string;
  gp_short_code: string;
  market_id: string;
  pt_session_id: string | null;
  signal_action: string;
  quantity: number | null;
  entry_price: number | null;
  stake_cost: number | null;
  market_price: number;
  model_prob: number;
  edge: number;
  side_label: string | null;
  reason: string | null;
};

type CreateLiveTradeTicketResponseApi = {
  action: string;
  status: string;
  message: string;
  ticket_id: string;
  gp_short_code: string;
  market_id: string;
  model_run_id: string | null;
  snapshot_id: string | null;
  promotion_stage: string | null;
  signal_action: string;
  side_label: string;
  recommended_size: number;
  market_price: number;
  model_prob: number;
  edge: number;
  observed_spread: number | null;
  max_spread: number | null;
  observed_at_utc: string;
  expires_at: string | null;
};

type RecordLiveTradeFillResponseApi = {
  action: string;
  status: string;
  message: string;
  ticket_id: string;
  execution_id: string;
  execution_status: string;
  ticket_status: string;
};

type CancelLiveTradeTicketResponseApi = {
  action: string;
  status: string;
  message: string;
  ticket_id: string;
  ticket_status: string;
};

type LiveSignalRowApi = {
  market_id: string;
  token_id: string | null;
  question: string;
  session_code: string;
  promotion_stage: string | null;
  model_run_id: string | null;
  snapshot_id: string | null;
  model_prob: number;
  market_price: number | null;
  edge: number | null;
  spread: number | null;
  signal_action: string;
  side_label: string | null;
  recommended_size: number;
  max_spread: number | null;
  observed_at_utc: string | null;
  event_type: string | null;
};

type LiveTradeSignalBoardApi = {
  gp_short_code: string;
  required_stage: string | null;
  active_model_run_id: string | null;
  model_run_id: string | null;
  snapshot_id: string | null;
  rows: LiveSignalRowApi[];
  blockers: string[];
};

type LiveTradeTicketApi = {
  id: string;
  gp_slug: string;
  session_code: string;
  market_id: string;
  token_id: string | null;
  snapshot_id: string | null;
  model_run_id: string | null;
  promotion_stage: string | null;
  question: string;
  signal_action: string;
  side_label: string;
  model_prob: number;
  market_price: number;
  edge: number;
  recommended_size: number;
  observed_spread: number | null;
  max_spread: number | null;
  observed_at_utc: string;
  source_event_type: string | null;
  status: string;
  rationale_json: Record<string, unknown> | null;
  expires_at: string | null;
  created_at: string;
  updated_at: string;
};

type LiveTradeExecutionApi = {
  id: string;
  ticket_id: string;
  market_id: string;
  side: string;
  submitted_size: number;
  actual_fill_size: number | null;
  actual_fill_price: number | null;
  submitted_at: string;
  filled_at: string | null;
  operator_note: string | null;
  external_reference: string | null;
  realized_pnl: number | null;
  status: string;
  created_at: string;
  updated_at: string;
};

type DriverAffinityEntryApi = {
  canonical_driver_key: string;
  display_driver_id: string | null;
  display_name: string;
  display_broadcast_name: string | null;
  driver_number: number | null;
  team_id: string | null;
  team_name: string | null;
  country_code: string | null;
  headshot_url: string | null;
  rank: number;
  affinity_score: number;
  s1_strength: number;
  s2_strength: number;
  s3_strength: number;
  track_s1_fraction: number;
  track_s2_fraction: number;
  track_s3_fraction: number;
  contributing_session_count: number;
  contributing_session_codes: string[];
  latest_contributing_session_code: string | null;
  latest_contributing_session_end_utc: string | null;
};

type DriverAffinityReportApi = {
  season: number;
  meeting_key: number;
  meeting: F1MeetingApi;
  computed_at_utc: string;
  as_of_utc: string;
  lookback_start_season: number;
  session_code_weights: Record<string, number>;
  season_weights: Record<string, number>;
  track_weights: Record<string, number>;
  default_segment_key?: string | null;
  segments?: DriverAffinitySegmentApi[];
  source_session_codes_included: string[];
  source_max_session_end_utc: string | null;
  latest_ended_relevant_session_code: string | null;
  latest_ended_relevant_session_end_utc: string | null;
  entry_count: number;
  is_fresh: boolean;
  stale_reason: string | null;
  entries: DriverAffinityEntryApi[];
};

type DriverAffinitySegmentApi = {
  key: string;
  title: string;
  description: string;
  source_session_codes_included: string[];
  source_seasons_included: number[];
  entry_count: number;
  entries: DriverAffinityEntryApi[];
};

type RefreshDriverAffinityResponseApi = {
  action: string;
  status: string;
  message: string;
  season: number;
  meeting_key: number;
  computed_at_utc: string | null;
  source_max_session_end_utc: string | null;
  hydrated_session_keys: number[];
  job_run_id: string | null;
  report_path: string | null;
  preflight_summary: OperationReadinessApi | null;
  warnings: string[];
  report: DriverAffinityReportApi | null;
};

function mapPaperTradeSession(record: PaperTradeSessionApi): PaperTradeSession {
  return {
    id: record.id,
    gpSlug: record.gp_slug,
    snapshotId: record.snapshot_id,
    modelRunId: record.model_run_id,
    status: record.status,
    configJson: record.config_json,
    summaryJson: record.summary_json,
    logPath: record.log_path,
    startedAt: record.started_at,
    finishedAt: record.finished_at,
  };
}

function mapPaperTradePosition(
  record: PaperTradePositionApi,
): PaperTradePosition {
  return {
    id: record.id,
    sessionId: record.session_id,
    marketId: record.market_id,
    tokenId: record.token_id,
    side: record.side,
    quantity: record.quantity,
    entryPrice: record.entry_price,
    entryTime: record.entry_time,
    modelProb: record.model_prob,
    marketProb: record.market_prob,
    edge: record.edge,
    status: record.status,
    exitPrice: record.exit_price,
    exitTime: record.exit_time,
    realizedPnl: record.realized_pnl,
  };
}

function mapLiveTradeTicket(record: LiveTradeTicketApi): LiveTradeTicket {
  return {
    id: record.id,
    gpSlug: record.gp_slug,
    sessionCode: record.session_code,
    marketId: record.market_id,
    tokenId: record.token_id,
    snapshotId: record.snapshot_id,
    modelRunId: record.model_run_id,
    promotionStage: record.promotion_stage,
    question: record.question,
    signalAction: record.signal_action,
    sideLabel: record.side_label,
    modelProb: record.model_prob,
    marketPrice: record.market_price,
    edge: record.edge,
    recommendedSize: record.recommended_size,
    observedSpread: record.observed_spread,
    maxSpread: record.max_spread,
    observedAtUtc: record.observed_at_utc,
    sourceEventType: record.source_event_type,
    status: record.status,
    rationaleJson: record.rationale_json,
    expiresAt: record.expires_at,
    createdAt: record.created_at,
    updatedAt: record.updated_at,
  };
}

function mapLiveTradeExecution(
  record: LiveTradeExecutionApi,
): LiveTradeExecution {
  return {
    id: record.id,
    ticketId: record.ticket_id,
    marketId: record.market_id,
    side: record.side,
    submittedSize: record.submitted_size,
    actualFillSize: record.actual_fill_size,
    actualFillPrice: record.actual_fill_price,
    submittedAt: record.submitted_at,
    filledAt: record.filled_at,
    operatorNote: record.operator_note,
    externalReference: record.external_reference,
    realizedPnl: record.realized_pnl,
    status: record.status,
    createdAt: record.created_at,
    updatedAt: record.updated_at,
  };
}

function mapLiveTradeSignalBoard(
  record: LiveTradeSignalBoardApi,
): LiveTradeSignalBoard {
  return {
    gpShortCode: record.gp_short_code,
    requiredStage: record.required_stage,
    activeModelRunId: record.active_model_run_id,
    modelRunId: record.model_run_id,
    snapshotId: record.snapshot_id,
    blockers: record.blockers,
    rows: record.rows.map((row) => ({
      marketId: row.market_id,
      tokenId: row.token_id,
      question: row.question,
      sessionCode: row.session_code,
      promotionStage: row.promotion_stage,
      modelRunId: row.model_run_id,
      snapshotId: row.snapshot_id,
      modelProb: row.model_prob,
      marketPrice: row.market_price,
      edge: row.edge,
      spread: row.spread,
      signalAction: row.signal_action,
      sideLabel: row.side_label,
      recommendedSize: row.recommended_size,
      maxSpread: row.max_spread,
      observedAtUtc: row.observed_at_utc,
      eventType: row.event_type,
    })),
  };
}

function mapWeekendCockpitStep(
  record: WeekendCockpitStepApi,
): WeekendCockpitStep {
  return {
    key: record.key,
    label: record.label,
    status: record.status,
    detail: record.detail,
    sessionCode: record.session_code,
    sessionKey: record.session_key,
    count: record.count,
    reasonCode: record.reason_code,
    actionableAfterUtc: record.actionable_after_utc,
    resourceLabel: record.resource_label,
  };
}

function mapIngestionJobRunSummary(
  record: IngestionJobRunSummaryApi,
): IngestionJobRunSummary {
  return {
    id: record.id,
    jobName: record.job_name,
    status: record.status,
    recordsWritten: record.records_written,
    startedAt: record.started_at,
    finishedAt: record.finished_at,
    errorMessage: record.error_message,
  };
}

function mapOperationReadiness(
  record: OperationReadinessApi,
): OperationReadiness {
  return {
    key: record.key,
    label: record.label,
    status: record.status,
    message: record.message,
    blockers: record.blockers,
    warnings: record.warnings,
    meetingKey: record.meeting_key,
    meetingName: record.meeting_name,
    gpShortCode: record.gp_short_code,
    sessionCode: record.session_code,
    sessionKey: record.session_key,
    actionableAfterUtc: record.actionable_after_utc,
    openf1CredentialsConfigured: record.openf1_credentials_configured,
    lastJobRun: record.last_job_run
      ? mapIngestionJobRunSummary(record.last_job_run)
      : null,
    lastReportPath: record.last_report_path,
    linkedMarketCount: record.linked_market_count ?? null,
    tokenCount: record.token_count ?? null,
    missingSessionKeys: record.missing_session_keys ?? [],
    reportIsFresh: record.report_is_fresh ?? null,
    latestEndedSessionCode: record.latest_ended_session_code ?? null,
    latestEndedSessionEndUtc: record.latest_ended_session_end_utc ?? null,
  };
}

function mapCurrentWeekendOperationsReadiness(
  record: CurrentWeekendOperationsReadinessApi,
): CurrentWeekendOperationsReadiness {
  return {
    now: record.now,
    selectedGpShortCode: record.selected_gp_short_code,
    selectedConfig: record.selected_config,
    meeting: record.meeting ? mapMeeting(record.meeting) : null,
    latestEndedSession: record.latest_ended_session
      ? mapSession(record.latest_ended_session)
      : null,
    nextActiveSession: record.next_active_session
      ? mapSession(record.next_active_session)
      : null,
    openf1CredentialsConfigured: record.openf1_credentials_configured,
    actions: record.actions.map(mapOperationReadiness),
    blockers: record.blockers,
    warnings: record.warnings,
  };
}

function mapWeekendCockpitStatus(
  record: WeekendCockpitStatusApi,
): WeekendCockpitStatus {
  return {
    now: record.now,
    autoSelectedGpShortCode: record.auto_selected_gp_short_code,
    selectedGpShortCode: record.selected_gp_short_code,
    selectedConfig: record.selected_config,
    calendarStatus: record.calendar_status,
    meetingSlug: record.meeting_slug,
    sourceConflict: record.source_conflict,
    overrideSourceUrl: record.override_source_url,
    calendarMeetings: record.calendar_meetings.map(mapOpsCalendarMeeting),
    cancelledMeetings: record.cancelled_meetings.map(mapOpsCalendarMeeting),
    availableConfigs: record.available_configs,
    meeting: record.meeting ? mapMeeting(record.meeting) : null,
    focusSession: record.focus_session
      ? mapSession(record.focus_session)
      : null,
    focusStatus: record.focus_status,
    timelineCompletedCodes: record.timeline_completed_codes,
    timelineActiveCode: record.timeline_active_code,
    sourceSession: record.source_session
      ? mapSession(record.source_session)
      : null,
    targetSession: record.target_session
      ? mapSession(record.target_session)
      : null,
    latestPaperSession: record.latest_paper_session
      ? mapPaperTradeSession(record.latest_paper_session)
      : null,
    steps: record.steps.map(mapWeekendCockpitStep),
    blockers: record.blockers,
    readyToRun: record.ready_to_run,
    modelReady: record.model_ready,
    requiredStage: record.required_stage,
    activeModelRunId: record.active_model_run_id,
    modelBlockers: record.model_blockers,
    sessionStageStatuses: record.session_stage_statuses.map((item) => ({
      gpShortCode: item.gp_short_code,
      targetSessionCode: item.target_session_code,
      requiredStage: item.required_stage,
      modelReady: item.model_ready,
      activeModelRunId: item.active_model_run_id,
      modelBlockers: item.model_blockers,
      displayLabel: item.display_label,
    })),
    liveTicketSummary: {
      ticketCount: record.live_ticket_summary.ticket_count,
      openTicketCount: record.live_ticket_summary.open_ticket_count,
      filledTicketCount: record.live_ticket_summary.filled_ticket_count,
      cancelledTicketCount: record.live_ticket_summary.cancelled_ticket_count,
    },
    liveExecutionSummary: {
      executionCount: record.live_execution_summary.execution_count,
      filledExecutionCount:
        record.live_execution_summary.filled_execution_count,
    },
    primaryActionTitle: record.primary_action_title,
    primaryActionDescription: record.primary_action_description,
    primaryActionCta: record.primary_action_cta,
    explanation: record.explanation,
  };
}

function mapRunWeekendCockpitResponse(
  record: RunWeekendCockpitResponseApi,
): RunWeekendCockpitResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    gpShortCode: record.gp_short_code,
    snapshotId: record.snapshot_id,
    modelRunId: record.model_run_id,
    ptSessionId: record.pt_session_id,
    ...(record.job_run_id !== undefined ? { jobRunId: record.job_run_id } : {}),
    ...(record.report_path !== undefined
      ? { reportPath: record.report_path }
      : {}),
    ...(record.preflight_summary !== undefined
      ? {
          preflightSummary: record.preflight_summary
            ? mapOperationReadiness(record.preflight_summary)
            : null,
        }
      : {}),
    ...(record.warnings !== undefined ? { warnings: record.warnings } : {}),
    executedSteps: record.executed_steps.map(mapWeekendCockpitStep),
    details: record.details
      ? mapRunWeekendCockpitDetails(record.details)
      : null,
  };
}

function mapWeekendCockpitSettlementSummary(
  record: WeekendCockpitSettlementSummaryApi,
): WeekendCockpitSettlementSummary {
  return {
    settledSessionIds: record.settled_session_ids,
    settledGpSlugs: record.settled_gp_slugs,
    settledPositions: record.settled_positions,
    manualPositionsSettled: record.manual_positions_settled,
    unresolvedPositions: record.unresolved_positions,
    unresolvedSessionIds: record.unresolved_session_ids,
    winnerDriverId: record.winner_driver_id,
  };
}

function mapRunWeekendCockpitDetails(
  record: RunWeekendCockpitDetailsApi,
): RunWeekendCockpitDetails {
  return {
    snapshotId: record.snapshot_id,
    modelRunId: record.model_run_id,
    baseline: record.baseline,
    ptSessionId: record.pt_session_id,
    logPath: record.log_path,
    totalSignals: record.total_signals,
    tradesExecuted: record.trades_executed,
    openPositions: record.open_positions,
    settledPositions: record.settled_positions,
    winCount: record.win_count,
    lossCount: record.loss_count,
    winRate: record.win_rate,
    totalPnl: record.total_pnl,
    dailyPnl: record.daily_pnl,
    settlement: record.settlement
      ? mapWeekendCockpitSettlementSummary(record.settlement)
      : null,
  };
}

function mapRefreshedSessionSummary(
  record: RefreshedSessionSummaryApi,
): RefreshedSessionSummary {
  return {
    id: record.id,
    sessionKey: record.session_key,
    sessionCode: record.session_code,
    sessionName: record.session_name,
    dateEndUtc: record.date_end_utc,
  };
}

function mapRefreshLatestSessionResponse(
  record: RefreshLatestSessionResponseApi,
): RefreshLatestSessionResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    meetingId: record.meeting_id,
    meetingName: record.meeting_name,
    refreshedSession: mapRefreshedSessionSummary(record.refreshed_session),
    f1RecordsWritten: record.f1_records_written,
    marketsDiscovered: record.markets_discovered,
    mappingsWritten: record.mappings_written,
    marketsHydrated: record.markets_hydrated,
    artifactsRefreshed: record.artifacts_refreshed.map(
      mapArtifactRefreshSummary,
    ),
  };
}

function mapArtifactRefreshSummary(
  record: ArtifactRefreshSummaryApi,
): ArtifactRefreshSummary {
  return {
    gpShortCode: record.gp_short_code,
    status: record.status,
    snapshotId: record.snapshot_id,
    rebuiltSnapshot: record.rebuilt_snapshot,
    betCount: record.bet_count,
    totalPnl: record.total_pnl,
    reason: record.reason,
  };
}

function mapCaptureLiveWeekendResponse(
  record: CaptureLiveWeekendResponseApi,
): CaptureLiveWeekendResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    jobRunId: record.job_run_id,
    sessionKey: record.session_key,
    captureSeconds: record.capture_seconds,
    openf1Messages: record.openf1_messages,
    polymarketMessages: record.polymarket_messages,
    marketCount: record.market_count,
    polymarketMarketIds: record.polymarket_market_ids,
    recordsWritten: record.records_written,
    ...(record.report_path !== undefined
      ? { reportPath: record.report_path }
      : {}),
    ...(record.preflight_summary !== undefined
      ? {
          preflightSummary: record.preflight_summary
            ? mapOperationReadiness(record.preflight_summary)
            : null,
        }
      : {}),
    ...(record.warnings !== undefined ? { warnings: record.warnings } : {}),
    summary: {
      openf1Topics: record.summary.openf1_topics.map((item) => ({
        key: item.key,
        count: item.count,
      })),
      polymarketEventTypes: record.summary.polymarket_event_types.map(
        (item) => ({
          key: item.key,
          count: item.count,
        }),
      ),
      observedMarketCount: record.summary.observed_market_count,
      observedTokenCount: record.summary.observed_token_count,
      marketQuotes: record.summary.market_quotes.map((item) => ({
        marketId: item.market_id,
        tokenId: item.token_id,
        outcome: item.outcome,
        eventType: item.event_type,
        observedAtUtc: item.observed_at_utc,
        price: item.price,
        bestBid: item.best_bid,
        bestAsk: item.best_ask,
        midpoint: item.midpoint,
        spread: item.spread,
        size: item.size,
        side: item.side,
      })),
    },
  };
}

function mapExecuteManualLivePaperTradeResponse(
  record: ExecuteManualLivePaperTradeResponseApi,
): ExecuteManualLivePaperTradeResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    gpShortCode: record.gp_short_code,
    marketId: record.market_id,
    ptSessionId: record.pt_session_id,
    signalAction: record.signal_action,
    quantity: record.quantity,
    entryPrice: record.entry_price,
    stakeCost: record.stake_cost,
    marketPrice: record.market_price,
    modelProb: record.model_prob,
    edge: record.edge,
    sideLabel: record.side_label,
    reason: record.reason,
  };
}

function mapCreateLiveTradeTicketResponse(
  record: CreateLiveTradeTicketResponseApi,
): CreateLiveTradeTicketResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    ticketId: record.ticket_id,
    gpShortCode: record.gp_short_code,
    marketId: record.market_id,
    modelRunId: record.model_run_id,
    snapshotId: record.snapshot_id,
    promotionStage: record.promotion_stage,
    signalAction: record.signal_action,
    sideLabel: record.side_label,
    recommendedSize: record.recommended_size,
    marketPrice: record.market_price,
    modelProb: record.model_prob,
    edge: record.edge,
    observedSpread: record.observed_spread,
    maxSpread: record.max_spread,
    observedAtUtc: record.observed_at_utc,
    expiresAt: record.expires_at,
  };
}

function mapRecordLiveTradeFillResponse(
  record: RecordLiveTradeFillResponseApi,
): RecordLiveTradeFillResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    ticketId: record.ticket_id,
    executionId: record.execution_id,
    executionStatus: record.execution_status,
    ticketStatus: record.ticket_status,
  };
}

function mapCancelLiveTradeTicketResponse(
  record: CancelLiveTradeTicketResponseApi,
): CancelLiveTradeTicketResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    ticketId: record.ticket_id,
    ticketStatus: record.ticket_status,
  };
}

function mapDriverAffinityEntry(
  record: DriverAffinityEntryApi,
): DriverAffinityEntry {
  return {
    canonicalDriverKey: record.canonical_driver_key,
    displayDriverId: record.display_driver_id,
    displayName: record.display_name,
    displayBroadcastName: record.display_broadcast_name,
    driverNumber: record.driver_number,
    teamId: record.team_id,
    teamName: record.team_name,
    countryCode: record.country_code,
    headshotUrl: record.headshot_url,
    rank: record.rank,
    affinityScore: record.affinity_score,
    s1Strength: record.s1_strength,
    s2Strength: record.s2_strength,
    s3Strength: record.s3_strength,
    trackS1Fraction: record.track_s1_fraction,
    trackS2Fraction: record.track_s2_fraction,
    trackS3Fraction: record.track_s3_fraction,
    contributingSessionCount: record.contributing_session_count,
    contributingSessionCodes: record.contributing_session_codes,
    latestContributingSessionCode: record.latest_contributing_session_code,
    latestContributingSessionEndUtc: record.latest_contributing_session_end_utc,
  };
}

function mapDriverAffinitySegment(
  record: DriverAffinitySegmentApi,
): DriverAffinitySegment {
  return {
    key: record.key,
    title: record.title,
    description: record.description,
    sourceSessionCodesIncluded: record.source_session_codes_included,
    sourceSeasonsIncluded: record.source_seasons_included,
    entryCount: record.entry_count,
    entries: record.entries.map(mapDriverAffinityEntry),
  };
}

function mapDriverAffinityReport(
  record: DriverAffinityReportApi,
): DriverAffinityReport {
  return {
    season: record.season,
    meetingKey: record.meeting_key,
    meeting: mapMeeting(record.meeting),
    computedAtUtc: record.computed_at_utc,
    asOfUtc: record.as_of_utc,
    lookbackStartSeason: record.lookback_start_season,
    sessionCodeWeights: record.session_code_weights,
    seasonWeights: record.season_weights,
    trackWeights: record.track_weights,
    defaultSegmentKey: record.default_segment_key ?? null,
    segments: (record.segments ?? []).map(mapDriverAffinitySegment),
    sourceSessionCodesIncluded: record.source_session_codes_included,
    sourceMaxSessionEndUtc: record.source_max_session_end_utc,
    latestEndedRelevantSessionCode: record.latest_ended_relevant_session_code,
    latestEndedRelevantSessionEndUtc:
      record.latest_ended_relevant_session_end_utc,
    entryCount: record.entry_count,
    isFresh: record.is_fresh,
    staleReason: record.stale_reason,
    entries: record.entries.map(mapDriverAffinityEntry),
  };
}

function mapRefreshDriverAffinityResponse(
  record: RefreshDriverAffinityResponseApi,
): RefreshDriverAffinityResponse {
  return {
    action: record.action,
    status: record.status,
    message: record.message,
    season: record.season,
    meetingKey: record.meeting_key,
    computedAtUtc: record.computed_at_utc,
    sourceMaxSessionEndUtc: record.source_max_session_end_utc,
    hydratedSessionKeys: record.hydrated_session_keys,
    ...(record.job_run_id !== undefined ? { jobRunId: record.job_run_id } : {}),
    ...(record.report_path !== undefined
      ? { reportPath: record.report_path }
      : {}),
    ...(record.preflight_summary !== undefined
      ? {
          preflightSummary: record.preflight_summary
            ? mapOperationReadiness(record.preflight_summary)
            : null,
        }
      : {}),
    ...(record.warnings !== undefined ? { warnings: record.warnings } : {}),
    report: record.report ? mapDriverAffinityReport(record.report) : null,
  };
}
