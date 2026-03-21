export type MarketTaxonomy =
  | "head_to_head_practice"
  | "constructor_fastest_lap_practice"
  | "driver_fastest_lap_practice"
  | "red_flag"
  | "safety_car"
  | "other";

export interface ApiHealth {
  service: string;
  status: string;
  now: string;
}

export interface FreshnessRecord {
  source: string;
  dataset: string;
  status: string;
  lastFetchAt: string | null;
  recordsFetched: number;
}

export interface F1Meeting {
  id: string;
  meetingKey: number;
  season: number;
  roundNumber: number | null;
  meetingName: string;
  circuitShortName: string | null;
  countryName: string | null;
  location: string | null;
  startDateUtc: string | null;
  endDateUtc: string | null;
}

export interface F1Session {
  id: string;
  sessionKey: number;
  meetingId: string | null;
  sessionName: string;
  sessionCode: string | null;
  sessionType: string | null;
  dateStartUtc: string | null;
  dateEndUtc: string | null;
  isPractice: boolean;
}

export interface PolymarketMarket {
  id: string;
  eventId: string | null;
  question: string;
  slug: string | null;
  taxonomy: MarketTaxonomy;
  taxonomyConfidence: number | null;
  targetSessionCode: string | null;
  conditionId: string;
  questionId: string | null;
  bestBid: number | null;
  bestAsk: number | null;
  lastTradePrice: number | null;
  volume: number | null;
  liquidity: number | null;
  active: boolean;
  closed: boolean;
}

export interface PolymarketEvent {
  id: string;
  slug: string;
  title: string;
  startAt: string | null;
  endAt: string | null;
  active: boolean;
  closed: boolean;
}

export interface EntityMapping {
  id: string;
  f1MeetingId: string | null;
  f1SessionId: string | null;
  polymarketEventId: string | null;
  polymarketMarketId: string | null;
  mappingType: string;
  confidence: number | null;
  matchedBy: string | null;
  overrideFlag: boolean;
}

export interface ModelRun {
  id: string;
  stage: string;
  modelFamily: string;
  modelName: string;
  datasetVersion: string | null;
  featureSnapshotId: string | null;
  configJson: Record<string, unknown> | null;
  metricsJson: Record<string, unknown> | null;
  artifactUri: string | null;
  createdAt: string;
}

export interface ModelPrediction {
  id: string;
  modelRunId: string;
  marketId: string | null;
  tokenId: string | null;
  asOfTs: string;
  probabilityYes: number | null;
  probabilityNo: number | null;
  rawScore: number | null;
  calibrationVersion: string | null;
}

export interface BacktestResult {
  id: string;
  backtestRunId: string;
  strategyName: string;
  stage: string;
  startAt: string | null;
  endAt: string | null;
  metricsJson: Record<string, unknown> | null;
  createdAt: string;
}

export interface FeatureSnapshot {
  id: string;
  marketId: string | null;
  sessionId: string | null;
  asOfTs: string;
  snapshotType: string;
  featureVersion: string;
  storagePath: string | null;
  rowCount: number | null;
}
