import { afterEach, describe, expect, it, vi } from "vitest";

import { sdk } from "./index";

describe("sdk", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("serializes session filters into the request URL", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });
    vi.stubGlobal("fetch", fetchMock);

    await sdk.sessions({
      limit: 25,
      season: 2026,
      meetingId: "meeting-2026",
      sessionCode: "Q",
      isPractice: false,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/f1/sessions?limit=25&season=2026&meeting_id=meeting-2026&session_code=Q&is_practice=false",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
  });

  it("serializes market filters and maps response fields", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => [
        {
          id: "market-1",
          event_id: "event-1",
          question: "Who gets pole?",
          slug: "pole",
          taxonomy: "driver_pole_position",
          taxonomy_confidence: 0.9,
          target_session_code: "Q",
          condition_id: "condition-1",
          question_id: "question-1",
          best_bid: 0.48,
          best_ask: 0.5,
          last_trade_price: 0.49,
          volume: 12,
          liquidity: 18,
          active: true,
          closed: false,
        },
      ],
    });
    vi.stubGlobal("fetch", fetchMock);

    const markets = await sdk.markets({
      taxonomy: "driver_pole_position",
      active: true,
      closed: false,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/polymarket/markets?taxonomy=driver_pole_position&active=true&closed=false",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(markets).toEqual([
      expect.objectContaining({
        id: "market-1",
        eventId: "event-1",
        taxonomy: "driver_pole_position",
        targetSessionCode: "Q",
      }),
    ]);
  });
});
