import { getWebEnv } from "@f1/config";
import type {
  ActionStatusResponse,
  ApiHealth,
  ArtifactRefreshSummary,
  BackfillBacktestsRequest,
  BacktestResult,
  CaptureLiveWeekendRequest,
  CaptureLiveWeekendResponse,
  CursorState,
  DataQualityResult,
  DriverAffinityEntry,
  DriverAffinityReport,
  DriverAffinitySegment,
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
  MarketTaxonomy,
  ModelPrediction,
  ModelRun,
  PaperTradePosition,
  PaperTradeSession,
  PolymarketEvent,
  PolymarketMarket,
  PricePoint,
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
  SyncCalendarRequest,
  SyncF1MarketsRequest,
  WeekendCockpitSettlementSummary,
  WeekendCockpitStatus,
  WeekendCockpitStep,
} from "@f1/shared-types";

type QueryValue = boolean | number | string | null | undefined;

export type ListOptions = {
  limit?: number;
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
  records_written: number | null;
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
    recordsWritten: record.records_written,
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
    circuitShortName: record.circuit_short_name,
    countryName: record.country_name,
    location: record.location,
    startDateUtc: record.start_date_utc,
    endDateUtc: record.end_date_utc,
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
  qualityResults: async (options?: ListOptions) =>
    (
      await apiGet<DataQualityResultApi[]>("/api/v1/quality/results", {
        limit: options?.limit,
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
  predictions: async (modelRunId?: string): Promise<ModelPrediction[]> => {
    const path = modelRunId
      ? `/api/v1/predictions?model_run_id=${encodeURIComponent(modelRunId)}`
      : "/api/v1/predictions";
    const records = await apiGet<ModelPredictionApi[]>(path);
    return records.map(mapModelPrediction);
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
  primary_action_title: string;
  primary_action_description: string;
  primary_action_cta: string;
  explanation: string;
};

type RunWeekendCockpitResponseApi = {
  action: string;
  status: string;
  message: string;
  gp_short_code: string;
  snapshot_id: string | null;
  model_run_id: string | null;
  pt_session_id: string | null;
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

function mapWeekendCockpitStatus(
  record: WeekendCockpitStatusApi,
): WeekendCockpitStatus {
  return {
    now: record.now,
    autoSelectedGpShortCode: record.auto_selected_gp_short_code,
    selectedGpShortCode: record.selected_gp_short_code,
    selectedConfig: record.selected_config,
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
    report: record.report ? mapDriverAffinityReport(record.report) : null,
  };
}
