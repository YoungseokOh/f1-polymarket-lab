import { getWebEnv } from "@f1/config";
import type {
  ApiHealth,
  EntityMapping,
  F1Meeting,
  F1Session,
  FreshnessRecord,
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
};
