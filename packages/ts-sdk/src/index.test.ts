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
          display_label: "Prepare Qualifying markets before practice",
          display_description:
            "Review Qualifying markets before practice sessions begin.",
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
            display_label: "Prepare Qualifying markets before practice",
            display_description:
              "Review Qualifying markets before practice sessions begin.",
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
            label: "Load weekend schedule",
            status: "completed",
            detail:
              "Loaded the Grand Prix schedule and the sessions required for this stage.",
            session_code: null,
            session_key: null,
            count: null,
            reason_code: "already_loaded",
            actionable_after_utc: null,
            resource_label: "Weekend schedule",
          },
        ],
        blockers: [],
        ready_to_run: true,
        primary_action_title: "Prepare Qualifying markets",
        primary_action_description:
          "This will discover Qualifying markets first, then continue into paper trading.",
        primary_action_cta: "Find Qualifying markets",
        explanation:
          "This stage reviews pre-practice information to prepare Qualifying markets.",
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
        primaryActionTitle: "Prepare Qualifying markets",
        primaryActionCta: "Find Qualifying markets",
        targetSession: expect.objectContaining({
          sessionCode: "Q",
        }),
        steps: [
          expect.objectContaining({
            key: "sync_calendar",
            status: "completed",
            resourceLabel: "Weekend schedule",
          }),
        ],
      }),
    );
  });

  it("serializes driver affinity queries and maps the report payload", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        season: 2026,
        meeting_key: 1281,
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
        computed_at_utc: "2026-03-27T08:45:00Z",
        as_of_utc: "2026-03-27T08:45:00Z",
        lookback_start_season: 2024,
        session_code_weights: { Q: 1.0, FP3: 0.8, FP2: 0.6, FP1: 0.4 },
        season_weights: { 2026: 1.0, 2025: 0.65, 2024: 0.4 },
        track_weights: {
          s1_fraction: 0.35,
          s2_fraction: 0.44,
          s3_fraction: 0.21,
        },
        source_session_codes_included: ["FP1", "FP2"],
        source_max_session_end_utc: "2026-03-27T07:00:00Z",
        latest_ended_relevant_session_code: "FP2",
        latest_ended_relevant_session_end_utc: "2026-03-27T07:00:00Z",
        entry_count: 1,
        is_fresh: true,
        stale_reason: null,
        entries: [
          {
            canonical_driver_key: "lando norris",
            display_driver_id: "driver:1",
            display_name: "Lando NORRIS",
            display_broadcast_name: "L NORRIS",
            driver_number: 1,
            team_id: "team:mclaren",
            team_name: "McLaren",
            country_code: "GBR",
            headshot_url: null,
            rank: 1,
            affinity_score: 1.23,
            s1_strength: 1.0,
            s2_strength: 1.2,
            s3_strength: 1.1,
            track_s1_fraction: 0.35,
            track_s2_fraction: 0.44,
            track_s3_fraction: 0.21,
            contributing_session_count: 6,
            contributing_session_codes: ["Q", "FP2"],
            latest_contributing_session_code: "FP2",
            latest_contributing_session_end_utc: "2026-03-27T07:00:00Z",
          },
        ],
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const report = await sdk.driverAffinity(2026, 1281);

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/driver-affinity?season=2026&meeting_key=1281",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(report).toEqual(
      expect.objectContaining({
        meetingKey: 1281,
        isFresh: true,
        sourceSessionCodesIncluded: ["FP1", "FP2"],
        entries: [
          expect.objectContaining({
            canonicalDriverKey: "lando norris",
            displayName: "Lando NORRIS",
            teamName: "McLaren",
          }),
        ],
      }),
    );
  });

  it("posts driver affinity refresh requests and maps the response", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        action: "refresh-driver-affinity",
        status: "blocked",
        message:
          "Driver affinity needs newer ended session data, but OpenF1 credentials are missing.",
        season: 2026,
        meeting_key: 1281,
        computed_at_utc: null,
        source_max_session_end_utc: "2026-03-27T07:00:00Z",
        hydrated_session_keys: [],
        report: null,
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await sdk.refreshDriverAffinity({
      season: 2026,
      meeting_key: 1281,
      force: true,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/actions/refresh-driver-affinity",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          season: 2026,
          meeting_key: 1281,
          force: true,
        }),
      }),
    );
    expect(response).toEqual({
      action: "refresh-driver-affinity",
      status: "blocked",
      message:
        "Driver affinity needs newer ended session data, but OpenF1 credentials are missing.",
      season: 2026,
      meetingKey: 1281,
      computedAtUtc: null,
      sourceMaxSessionEndUtc: "2026-03-27T07:00:00Z",
      hydratedSessionKeys: [],
      report: null,
    });
  });
});
