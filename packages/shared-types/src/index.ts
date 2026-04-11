export type MarketTaxonomy =
  | "head_to_head_session"
  | "head_to_head_practice"
  | "driver_pole_position"
  | "constructor_fastest_lap_practice"
  | "constructor_fastest_lap_session"
  | "constructor_pole_position"
  | "constructor_scores_first"
  | "constructors_champion"
  | "driver_fastest_lap_practice"
  | "driver_fastest_lap_session"
  | "driver_podium"
  | "drivers_champion"
  | "qualifying_winner"
  | "race_winner"
  | "red_flag"
  | "safety_car"
  | "sprint_winner"
  | "other";

export type MarketGroup =
  | "driver_outright"
  | "constructor_outright"
  | "head_to_head"
  | "incident_binary"
  | "championship"
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

export interface IngestionJobRun {
  id: string;
  jobName: string;
  source: string;
  dataset: string;
  status: string;
  executeMode: string;
  plannedInputs: Record<string, unknown> | null;
  cursorAfter: Record<string, unknown> | null;
  recordsWritten: number | null;
  errorMessage: string | null;
  startedAt: string;
  finishedAt: string | null;
}

export interface CursorState {
  id: string;
  source: string;
  dataset: string;
  cursorKey: string;
  cursorValue: Record<string, unknown> | null;
  updatedAt: string;
}

export interface DataQualityResult {
  id: string;
  dataset: string;
  status: string;
  metricsJson: Record<string, unknown> | null;
  observedAt: string;
}

export interface F1Meeting {
  id: string;
  meetingKey: number;
  season: number;
  roundNumber: number | null;
  meetingName: string;
  meetingSlug: string | null;
  eventFormat: string | null;
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
  registryRunId: string | null;
  promotionStatus: string;
  promotedAt: string | null;
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

export interface SignalRegistryEntry {
  id: string;
  signalCode: string;
  signalFamily: string;
  marketTaxonomy: MarketTaxonomy | null;
  marketGroup: MarketGroup | null;
  description: string | null;
  version: string;
  configJson: Record<string, unknown> | null;
  isActive: boolean;
  createdAt: string;
}

export interface SignalSnapshot {
  id: string;
  modelRunId: string;
  featureSnapshotId: string | null;
  marketId: string | null;
  tokenId: string | null;
  eventId: string | null;
  marketTaxonomy: MarketTaxonomy;
  marketGroup: MarketGroup;
  meetingKey: number | null;
  asOfTs: string;
  signalCode: string;
  signalVersion: string;
  pYesRaw: number | null;
  pYesCalibrated: number | null;
  pMarketRef: number | null;
  deltaLogit: number | null;
  freshnessSec: number | null;
  coverageFlag: boolean;
  metadataJson: Record<string, unknown> | null;
  createdAt: string;
}

export interface SignalDiagnostic {
  id: string;
  modelRunId: string;
  signalCode: string;
  marketTaxonomy: MarketTaxonomy | null;
  marketGroup: MarketGroup | null;
  phaseBucket: string | null;
  brier: number | null;
  logLoss: number | null;
  ece: number | null;
  skillVsMarket: number | null;
  coverageRate: number | null;
  residualCorrelationJson: Record<string, unknown> | null;
  stabilityJson: Record<string, unknown> | null;
  metricsJson: Record<string, unknown> | null;
  createdAt: string;
}

export interface EnsemblePrediction {
  id: string;
  modelRunId: string;
  featureSnapshotId: string | null;
  marketId: string | null;
  tokenId: string | null;
  eventId: string | null;
  marketTaxonomy: MarketTaxonomy;
  marketGroup: MarketGroup;
  meetingKey: number | null;
  asOfTs: string;
  pMarketRef: number | null;
  pYesEnsemble: number | null;
  zMarket: number | null;
  zEnsemble: number | null;
  intercept: number | null;
  disagreementScore: number | null;
  effectiveN: number | null;
  uncertaintyScore: number | null;
  contributionsJson: Record<string, unknown> | null;
  coverageJson: Record<string, unknown> | null;
  metadataJson: Record<string, unknown> | null;
  createdAt: string;
}

export interface TradeDecision {
  id: string;
  modelRunId: string;
  ensemblePredictionId: string | null;
  featureSnapshotId: string | null;
  marketId: string | null;
  tokenId: string | null;
  eventId: string | null;
  marketTaxonomy: MarketTaxonomy;
  marketGroup: MarketGroup;
  meetingKey: number | null;
  asOfTs: string;
  side: string;
  edge: number | null;
  threshold: number | null;
  spread: number | null;
  depth: number | null;
  kellyFractionRaw: number | null;
  disagreementPenalty: number | null;
  liquidityFactor: number | null;
  sizeFraction: number | null;
  decisionStatus: string;
  decisionReason: string | null;
  metadataJson: Record<string, unknown> | null;
  createdAt: string;
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

export interface BackfillBacktestsRequest {
  gp_short_code?: string | null;
  min_edge?: number;
  bet_size?: number;
  rebuild_missing?: boolean;
}

export interface SyncF1MarketsRequest {
  max_pages?: number;
  search_fallback?: boolean;
  start_year?: number;
  end_year?: number | null;
}

export interface SetCalendarOverrideRequest {
  season?: number;
  meeting_slug: string;
  status: string;
  ops_slug?: string | null;
  effective_round_number?: number | null;
  effective_start_date_utc?: string | null;
  effective_end_date_utc?: string | null;
  effective_meeting_name?: string | null;
  effective_country_name?: string | null;
  effective_location?: string | null;
  source_label?: string | null;
  source_url?: string | null;
  note?: string | null;
}

export interface ClearCalendarOverrideRequest {
  season?: number;
  meeting_slug: string;
}

export interface RefreshLatestSessionRequest {
  meeting_id: string;
  search_fallback?: boolean;
  discover_max_pages?: number;
  hydrate_market_history?: boolean;
  sync_calendar?: boolean;
  hydrate_f1_session_data?: boolean;
  include_extended_f1_data?: boolean;
  include_heavy_f1_data?: boolean;
  refresh_artifacts?: boolean;
}

export interface RefreshedSessionSummary {
  id: string;
  sessionKey: number;
  sessionCode: string | null;
  sessionName: string;
  dateEndUtc: string | null;
}

export interface ArtifactRefreshSummary {
  gpShortCode: string;
  status: string;
  snapshotId: string | null;
  rebuiltSnapshot: boolean;
  betCount: number | null;
  totalPnl: number | null;
  reason: string | null;
}

export interface RefreshLatestSessionResponse {
  action: string;
  status: string;
  message: string;
  meetingId: string;
  meetingName: string;
  refreshedSession: RefreshedSessionSummary;
  f1RecordsWritten: number;
  marketsDiscovered: number;
  mappingsWritten: number;
  marketsHydrated: number;
  artifactsRefreshed: ArtifactRefreshSummary[];
}

export interface CaptureLiveWeekendRequest {
  session_key: number;
  market_ids?: string[] | null;
  capture_seconds?: number;
  start_buffer_min?: number;
  stop_buffer_min?: number;
  message_limit?: number | null;
}

export interface CaptureLiveWeekendCount {
  key: string;
  count: number;
}

export interface CaptureLiveWeekendMarketQuote {
  marketId: string;
  tokenId: string | null;
  outcome: string | null;
  eventType: string;
  observedAtUtc: string;
  price: number | null;
  bestBid: number | null;
  bestAsk: number | null;
  midpoint: number | null;
  spread: number | null;
  size: number | null;
  side: string | null;
}

export interface CaptureLiveWeekendSummary {
  openf1Topics: CaptureLiveWeekendCount[];
  polymarketEventTypes: CaptureLiveWeekendCount[];
  observedMarketCount: number;
  observedTokenCount: number;
  marketQuotes: CaptureLiveWeekendMarketQuote[];
}

export interface IngestionJobRunSummary {
  id: string;
  jobName: string;
  status: string;
  recordsWritten: number | null;
  startedAt: string | null;
  finishedAt: string | null;
  errorMessage: string | null;
}

export interface OperationReadiness {
  key: string;
  label: string;
  status: string;
  message: string;
  blockers: string[];
  warnings: string[];
  meetingKey: number | null;
  meetingName: string | null;
  gpShortCode: string | null;
  sessionCode: string | null;
  sessionKey: number | null;
  actionableAfterUtc: string | null;
  openf1CredentialsConfigured: boolean;
  lastJobRun: IngestionJobRunSummary | null;
  lastReportPath: string | null;
  linkedMarketCount?: number | null;
  tokenCount?: number | null;
  missingSessionKeys?: number[];
  reportIsFresh?: boolean | null;
  latestEndedSessionCode?: string | null;
  latestEndedSessionEndUtc?: string | null;
}

export interface CurrentWeekendOperationsReadiness {
  now: string;
  selectedGpShortCode: string;
  selectedConfig: GPRegistryItem;
  meeting: F1Meeting | null;
  latestEndedSession: F1Session | null;
  nextActiveSession: F1Session | null;
  openf1CredentialsConfigured: boolean;
  actions: OperationReadiness[];
  blockers: string[];
  warnings: string[];
}

export interface CaptureLiveWeekendResponse {
  action: string;
  status: string;
  message: string;
  jobRunId: string;
  sessionKey: number;
  captureSeconds: number;
  openf1Messages: number;
  polymarketMessages: number;
  marketCount: number;
  polymarketMarketIds: string[];
  recordsWritten: number;
  reportPath?: string | null;
  preflightSummary?: OperationReadiness | null;
  warnings?: string[];
  summary: CaptureLiveWeekendSummary;
}

export interface ExecuteManualLivePaperTradeRequest {
  gp_short_code: string;
  market_id: string;
  token_id?: string | null;
  model_run_id?: string | null;
  snapshot_id?: string | null;
  model_prob: number;
  market_price: number;
  observed_at_utc?: string | null;
  observed_spread?: number | null;
  source_event_type?: string | null;
  min_edge?: number;
  max_spread?: number | null;
  bet_size?: number;
}

export interface ExecuteManualLivePaperTradeResponse {
  action: string;
  status: string;
  message: string;
  gpShortCode: string;
  marketId: string;
  ptSessionId: string | null;
  signalAction: string;
  quantity: number | null;
  entryPrice: number | null;
  stakeCost: number | null;
  marketPrice: number;
  modelProb: number;
  edge: number;
  sideLabel: string | null;
  reason: string | null;
}

export interface GPRegistryItem {
  name: string;
  short_code: string;
  meeting_key: number;
  season: number;
  meeting_slug?: string | null;
  target_session_code: string;
  variant: string;
  source_session_code: string | null;
  market_taxonomy: MarketTaxonomy;
  stage_rank: number;
  stage_label: string;
  display_label: string;
  display_description: string;
  required_model_stage?: string | null;
  live_bet_size?: number | null;
  live_min_edge?: number | null;
  live_max_daily_loss?: number | null;
  live_max_spread?: number | null;
  calendar_status?: string;
  source_conflict?: boolean;
  override_source_url?: string | null;
}

export interface OpsCalendarMeeting {
  season: number;
  meetingKey: number;
  meetingSlug: string;
  opsSlug: string;
  meetingName: string;
  roundNumber: number | null;
  eventFormat: string | null;
  startDateUtc: string | null;
  endDateUtc: string | null;
  countryName: string | null;
  location: string | null;
  status: string;
  sourceConflict: boolean;
  sourceLabel: string | null;
  sourceUrl: string | null;
  note: string | null;
}

export interface WeekendCockpitSessionStageStatus {
  gpShortCode: string;
  targetSessionCode: string;
  requiredStage: string | null;
  modelReady: boolean;
  activeModelRunId: string | null;
  modelBlockers: string[];
  displayLabel: string;
}

export interface LiveTradeTicketSummary {
  ticketCount: number;
  openTicketCount: number;
  filledTicketCount: number;
  cancelledTicketCount: number;
}

export interface LiveTradeExecutionSummary {
  executionCount: number;
  filledExecutionCount: number;
}

export interface WeekendCockpitStep {
  key: string;
  label: string;
  status: string;
  detail: string;
  sessionCode: string | null;
  sessionKey: number | null;
  count: number | null;
  reasonCode: string | null;
  actionableAfterUtc: string | null;
  resourceLabel: string | null;
}

export interface WeekendCockpitStatus {
  now: string;
  autoSelectedGpShortCode: string;
  selectedGpShortCode: string;
  selectedConfig: GPRegistryItem;
  calendarStatus: string;
  meetingSlug: string;
  sourceConflict: boolean;
  overrideSourceUrl: string | null;
  calendarMeetings: OpsCalendarMeeting[];
  cancelledMeetings: OpsCalendarMeeting[];
  availableConfigs: GPRegistryItem[];
  meeting: F1Meeting | null;
  focusSession: F1Session | null;
  focusStatus: "upcoming" | "live" | "ended";
  timelineCompletedCodes: string[];
  timelineActiveCode: string | null;
  sourceSession: F1Session | null;
  targetSession: F1Session | null;
  latestPaperSession: PaperTradeSession | null;
  steps: WeekendCockpitStep[];
  blockers: string[];
  readyToRun: boolean;
  modelReady: boolean;
  requiredStage: string | null;
  activeModelRunId: string | null;
  modelBlockers: string[];
  sessionStageStatuses: WeekendCockpitSessionStageStatus[];
  liveTicketSummary: LiveTradeTicketSummary;
  liveExecutionSummary: LiveTradeExecutionSummary;
  primaryActionTitle: string;
  primaryActionDescription: string;
  primaryActionCta: string;
  explanation: string;
}

export interface DriverAffinityEntry {
  canonicalDriverKey: string;
  displayDriverId: string | null;
  displayName: string;
  displayBroadcastName: string | null;
  driverNumber: number | null;
  teamId: string | null;
  teamName: string | null;
  countryCode: string | null;
  headshotUrl: string | null;
  rank: number;
  affinityScore: number;
  s1Strength: number;
  s2Strength: number;
  s3Strength: number;
  trackS1Fraction: number;
  trackS2Fraction: number;
  trackS3Fraction: number;
  contributingSessionCount: number;
  contributingSessionCodes: string[];
  latestContributingSessionCode: string | null;
  latestContributingSessionEndUtc: string | null;
}

export interface DriverAffinitySegment {
  key: string;
  title: string;
  description: string;
  sourceSessionCodesIncluded: string[];
  sourceSeasonsIncluded: number[];
  entryCount: number;
  entries: DriverAffinityEntry[];
}

export interface DriverAffinityReport {
  season: number;
  meetingKey: number;
  meeting: F1Meeting;
  computedAtUtc: string;
  asOfUtc: string;
  lookbackStartSeason: number;
  sessionCodeWeights: Record<string, number>;
  seasonWeights: Record<string, number>;
  trackWeights: Record<string, number>;
  defaultSegmentKey: string | null;
  segments: DriverAffinitySegment[];
  sourceSessionCodesIncluded: string[];
  sourceMaxSessionEndUtc: string | null;
  latestEndedRelevantSessionCode: string | null;
  latestEndedRelevantSessionEndUtc: string | null;
  entryCount: number;
  isFresh: boolean;
  staleReason: string | null;
  entries: DriverAffinityEntry[];
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

export interface LiveSignalRow {
  marketId: string;
  tokenId: string | null;
  question: string;
  sessionCode: string;
  promotionStage: string | null;
  modelRunId: string | null;
  snapshotId: string | null;
  modelProb: number;
  marketPrice: number | null;
  edge: number | null;
  spread: number | null;
  signalAction: string;
  sideLabel: string | null;
  recommendedSize: number;
  maxSpread: number | null;
  observedAtUtc: string | null;
  eventType: string | null;
}

export interface LiveTradeSignalBoard {
  gpShortCode: string;
  requiredStage: string | null;
  activeModelRunId: string | null;
  modelRunId: string | null;
  snapshotId: string | null;
  rows: LiveSignalRow[];
  blockers: string[];
}

export interface CreateLiveTradeTicketRequest {
  gp_short_code: string;
  market_id: string;
  observed_market_price?: number | null;
  observed_spread?: number | null;
  observed_at_utc?: string | null;
  source_event_type?: string | null;
  bet_size?: number | null;
  min_edge?: number | null;
  max_spread?: number | null;
}

export interface CreateLiveTradeTicketResponse {
  action: string;
  status: string;
  message: string;
  ticketId: string;
  gpShortCode: string;
  marketId: string;
  modelRunId: string | null;
  snapshotId: string | null;
  promotionStage: string | null;
  signalAction: string;
  sideLabel: string;
  recommendedSize: number;
  marketPrice: number;
  modelProb: number;
  edge: number;
  observedSpread: number | null;
  maxSpread: number | null;
  observedAtUtc: string;
  expiresAt: string | null;
}

export interface RecordLiveTradeFillRequest {
  ticket_id: string;
  submitted_size: number;
  actual_fill_size?: number | null;
  actual_fill_price?: number | null;
  submitted_at?: string | null;
  filled_at?: string | null;
  operator_note?: string | null;
  external_reference?: string | null;
  status?: string;
  realized_pnl?: number | null;
}

export interface RecordLiveTradeFillResponse {
  action: string;
  status: string;
  message: string;
  ticketId: string;
  executionId: string;
  executionStatus: string;
  ticketStatus: string;
}

export interface CancelLiveTradeTicketRequest {
  ticket_id: string;
  operator_note?: string | null;
}

export interface CancelLiveTradeTicketResponse {
  action: string;
  status: string;
  message: string;
  ticketId: string;
  ticketStatus: string;
}

export interface LiveTradeTicket {
  id: string;
  gpSlug: string;
  sessionCode: string;
  marketId: string;
  tokenId: string | null;
  snapshotId: string | null;
  modelRunId: string | null;
  promotionStage: string | null;
  question: string;
  signalAction: string;
  sideLabel: string;
  modelProb: number;
  marketPrice: number;
  edge: number;
  recommendedSize: number;
  observedSpread: number | null;
  maxSpread: number | null;
  observedAtUtc: string;
  sourceEventType: string | null;
  status: string;
  rationaleJson: Record<string, unknown> | null;
  expiresAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface LiveTradeExecution {
  id: string;
  ticketId: string;
  marketId: string;
  side: string;
  submittedSize: number;
  actualFillSize: number | null;
  actualFillPrice: number | null;
  submittedAt: string;
  filledAt: string | null;
  operatorNote: string | null;
  externalReference: string | null;
  realizedPnl: number | null;
  status: string;
  createdAt: string;
  updatedAt: string;
}

export interface RunPaperTradeRequest {
  gp_short_code: string;
  snapshot_id?: string | null;
  baseline?: string;
  min_edge?: number;
  bet_size?: number;
}

export interface RunWeekendCockpitRequest {
  gp_short_code?: string | null;
  baseline?: string;
  min_edge?: number;
  bet_size?: number;
  search_fallback?: boolean;
  discover_max_pages?: number;
}

export interface WeekendCockpitSettlementSummary {
  settledSessionIds: string[];
  settledGpSlugs: string[];
  settledPositions: number;
  manualPositionsSettled: number;
  unresolvedPositions: number;
  unresolvedSessionIds: string[];
  winnerDriverId: string | null;
}

export interface RunWeekendCockpitDetails {
  snapshotId: string | null;
  modelRunId: string | null;
  baseline: string | null;
  ptSessionId: string | null;
  logPath: string | null;
  totalSignals: number | null;
  tradesExecuted: number | null;
  openPositions: number | null;
  settledPositions: number | null;
  winCount: number | null;
  lossCount: number | null;
  winRate: number | null;
  totalPnl: number | null;
  dailyPnl: number | null;
  settlement: WeekendCockpitSettlementSummary | null;
}

export interface RunWeekendCockpitResponse {
  action: string;
  status: string;
  message: string;
  gpShortCode: string;
  snapshotId: string | null;
  modelRunId: string | null;
  ptSessionId: string | null;
  jobRunId?: string | null;
  reportPath?: string | null;
  preflightSummary?: OperationReadiness | null;
  warnings?: string[];
  executedSteps: WeekendCockpitStep[];
  details: RunWeekendCockpitDetails | null;
}

export interface RefreshDriverAffinityRequest {
  season?: number;
  meeting_key?: number | null;
  force?: boolean;
}

export interface RefreshDriverAffinityResponse {
  action: string;
  status: string;
  message: string;
  season: number;
  meetingKey: number;
  computedAtUtc: string | null;
  sourceMaxSessionEndUtc: string | null;
  hydratedSessionKeys: number[];
  jobRunId?: string | null;
  reportPath?: string | null;
  preflightSummary?: OperationReadiness | null;
  warnings?: string[];
  report: DriverAffinityReport | null;
}
