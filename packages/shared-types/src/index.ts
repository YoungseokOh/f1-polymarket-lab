export type MarketTaxonomy =
  | "head_to_head_session"
  | "head_to_head_practice"
  | "driver_pole_position"
  | "constructor_pole_position"
  | "race_winner"
  | "sprint_winner"
  | "qualifying_winner"
  | "driver_podium"
  | "constructor_scores_first"
  | "constructor_fastest_lap_practice"
  | "driver_fastest_lap_practice"
  | "drivers_champion"
  | "constructors_champion"
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

export interface F1Driver {
  id: string;
  driverNumber: number;
  broadcastName: string | null;
  fullName: string | null;
  firstName: string | null;
  lastName: string | null;
  nameAcronym: string | null;
  teamId: string | null;
  countryCode: string | null;
  headshotUrl: string | null;
}

export interface F1Team {
  id: string;
  teamName: string;
  teamColor: string | null;
}

export interface PricePoint {
  id: string;
  marketId: string;
  tokenId: string;
  observedAtUtc: string;
  price: number | null;
  midpoint: number | null;
  bestBid: number | null;
  bestAsk: number | null;
}

// ---------------------------------------------------------------------------
// Action request / response types
// ---------------------------------------------------------------------------

export interface ActionStatusResponse {
  action: string;
  status: string;
  message: string;
  details?: Record<string, unknown> | null;
}

export interface IngestDemoRequest {
  season?: number;
  weekends?: number;
  market_batches?: number;
}

export interface SyncCalendarRequest {
  season?: number;
}

export interface RunBacktestRequest {
  gp_short_code: string;
  min_edge?: number;
  bet_size?: number;
}

export interface SyncF1MarketsRequest {
  max_pages?: number;
  search_fallback?: boolean;
  start_year?: number;
  end_year?: number | null;
}

export interface GPRegistryItem {
  name: string;
  short_code: string;
  meeting_key: number;
  season: number;
  target_session_code: string;
  variant: string;
}

export interface PaperTradeSession {
  id: string;
  gpSlug: string;
  snapshotId: string | null;
  modelRunId: string | null;
  status: string;
  configJson: Record<string, unknown> | null;
  summaryJson: Record<string, unknown> | null;
  logPath: string | null;
  startedAt: string;
  finishedAt: string | null;
}

export interface PaperTradePosition {
  id: string;
  sessionId: string;
  marketId: string;
  tokenId: string | null;
  side: string;
  quantity: number;
  entryPrice: number;
  entryTime: string;
  modelProb: number;
  marketProb: number;
  edge: number;
  status: string;
  exitPrice: number | null;
  exitTime: string | null;
  realizedPnl: number | null;
}

export interface RunPaperTradeRequest {
  gp_short_code: string;
  snapshot_id?: string | null;
  baseline?: string;
  min_edge?: number;
  bet_size?: number;
}
