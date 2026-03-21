import { getWebEnv } from "@f1/config";
import type {
  ApiHealth,
  BacktestResult,
  EntityMapping,
  F1Meeting,
  F1Session,
  FeatureSnapshot,
  FreshnessRecord,
  ModelPrediction,
  ModelRun,
  PolymarketEvent,
  PolymarketMarket,
} from "@f1/shared-types";

async function apiGet<T>(path: string): Promise<T> {
  const { NEXT_PUBLIC_API_BASE_URL } = getWebEnv();
  const response = await fetch(`${NEXT_PUBLIC_API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
    },
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(
      `API request failed: ${response.status} ${response.statusText}`,
    );
  }

  return (await response.json()) as T;
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
  freshness: async () =>
    (await apiGet<FreshnessApi[]>("/api/v1/freshness")).map(mapFreshness),
  meetings: async () =>
    (await apiGet<F1MeetingApi[]>("/api/v1/f1/meetings")).map(mapMeeting),
  sessions: async () =>
    (await apiGet<F1SessionApi[]>("/api/v1/f1/sessions")).map(mapSession),
  events: async () =>
    (await apiGet<PolymarketEventApi[]>("/api/v1/polymarket/events")).map(
      mapEvent,
    ),
  markets: async () =>
    (await apiGet<PolymarketMarketApi[]>("/api/v1/polymarket/markets")).map(
      mapMarket,
    ),
  mappings: async () =>
    (await apiGet<EntityMappingApi[]>("/api/v1/mappings")).map(mapMapping),
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
    const records = await apiGet<BacktestResultApi[]>("/api/v1/backtest/results");
    return records.map(mapBacktestResult);
  },
  snapshots: async (): Promise<FeatureSnapshot[]> => {
    const records = await apiGet<FeatureSnapshotApi[]>("/api/v1/snapshots");
    return records.map(mapFeatureSnapshot);
  },
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
