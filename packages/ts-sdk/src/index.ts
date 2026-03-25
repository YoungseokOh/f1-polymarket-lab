import { getWebEnv } from "@f1/config";
import type {
  ActionStatusResponse,
  ApiHealth,
  BacktestResult,
  EntityMapping,
  F1Driver,
  F1Meeting,
  F1Session,
  F1Team,
  FeatureSnapshot,
  FreshnessRecord,
  GPRegistryItem,
  IngestDemoRequest,
  MarketTaxonomy,
  ModelPrediction,
  ModelRun,
  PaperTradePosition,
  PaperTradeSession,
  PolymarketEvent,
  PolymarketMarket,
  PricePoint,
  RunBacktestRequest,
  RunPaperTradeRequest,
  SyncCalendarRequest,
  SyncF1MarketsRequest,
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
    throw new Error(
      `API request failed: ${response.status} ${response.statusText}`,
    );
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
    const detail = await response.text().catch(() => response.statusText);
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
  syncF1Markets: (body?: SyncF1MarketsRequest) =>
    apiPost<SyncF1MarketsRequest, ActionStatusResponse>(
      "/api/v1/actions/sync-f1-markets",
      body ?? {},
    ),

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
