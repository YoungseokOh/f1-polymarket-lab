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

  it("maps weekend cockpit status payloads", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        now: "2026-03-27T00:00:00Z",
        auto_selected_gp_short_code: "japan_pre",
        selected_gp_short_code: "japan_pre",
        selected_config: {
          name: "Japanese Grand Prix",
          short_code: "japan_pre",
          meeting_key: 1281,
          season: 2026,
          target_session_code: "Q",
          variant: "pre_weekend",
          source_session_code: null,
          market_taxonomy: "driver_pole_position",
          stage_rank: 0,
          stage_label: "Pre-Weekend -> Q",
          display_label: "예선 시장 사전 준비",
          display_description:
            "연습주행 전 정보만으로 예선 관련 시장을 먼저 살펴보는 단계입니다.",
        },
        available_configs: [
          {
            name: "Japanese Grand Prix",
            short_code: "japan_pre",
            meeting_key: 1281,
            season: 2026,
            target_session_code: "Q",
            variant: "pre_weekend",
            source_session_code: null,
            market_taxonomy: "driver_pole_position",
            stage_rank: 0,
            stage_label: "Pre-Weekend -> Q",
            display_label: "예선 시장 사전 준비",
            display_description:
              "연습주행 전 정보만으로 예선 관련 시장을 먼저 살펴보는 단계입니다.",
          },
        ],
        meeting: {
          id: "meeting:1281",
          meeting_key: 1281,
          season: 2026,
          round_number: 3,
          meeting_name: "Japanese Grand Prix",
          circuit_short_name: "Suzuka",
          country_name: "Japan",
          location: "Suzuka",
          start_date_utc: "2026-03-27T02:30:00Z",
          end_date_utc: "2026-03-29T07:00:00Z",
        },
        focus_session: {
          id: "session:11249",
          session_key: 11249,
          meeting_id: "meeting:1281",
          session_name: "Qualifying",
          session_code: "Q",
          session_type: "Qualifying",
          date_start_utc: "2026-03-28T06:00:00Z",
          date_end_utc: "2026-03-28T07:00:00Z",
          is_practice: false,
        },
        focus_status: "upcoming",
        timeline_completed_codes: [],
        timeline_active_code: "Q",
        source_session: null,
        target_session: {
          id: "session:11249",
          session_key: 11249,
          meeting_id: "meeting:1281",
          session_name: "Qualifying",
          session_code: "Q",
          session_type: "Qualifying",
          date_start_utc: "2026-03-28T06:00:00Z",
          date_end_utc: "2026-03-28T07:00:00Z",
          is_practice: false,
        },
        latest_paper_session: null,
        steps: [
          {
            key: "sync_calendar",
            label: "주말 일정 확인",
            status: "completed",
            detail: "이번 그랑프리 일정과 필요한 세션 정보를 불러왔습니다.",
            session_code: null,
            session_key: null,
            count: null,
            reason_code: "already_loaded",
            actionable_after_utc: null,
            resource_label: "주말 일정",
          },
        ],
        blockers: [],
        ready_to_run: true,
        primary_action_title: "예선 시장 사전 준비 시작",
        primary_action_description:
          "버튼을 누르면 예선 관련 시장 준비와 페이퍼 트레이딩 준비를 순서대로 진행합니다.",
        primary_action_cta: "예선 시장 준비 실행",
        explanation:
          "이 단계는 연습주행 전 정보를 바탕으로 예선 관련 시장을 미리 살펴보는 단계입니다.",
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const status = await sdk.weekendCockpitStatus("japan_pre");

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/weekend-cockpit/status?gp_short_code=japan_pre",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(status).toEqual(
      expect.objectContaining({
        autoSelectedGpShortCode: "japan_pre",
        selectedGpShortCode: "japan_pre",
        readyToRun: true,
        focusStatus: "upcoming",
        timelineActiveCode: "Q",
        primaryActionTitle: "예선 시장 사전 준비 시작",
        primaryActionCta: "예선 시장 준비 실행",
        targetSession: expect.objectContaining({
          sessionCode: "Q",
        }),
        steps: [
          expect.objectContaining({
            key: "sync_calendar",
            status: "completed",
            resourceLabel: "주말 일정",
          }),
        ],
      }),
    );
  });
});
