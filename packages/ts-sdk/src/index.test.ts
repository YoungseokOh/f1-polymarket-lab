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

  it("serializes batched market ids into the request URL", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    });
    vi.stubGlobal("fetch", fetchMock);

    await sdk.markets({
      ids: ["market-good", "market-unknown"],
      limit: 10,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/polymarket/markets?limit=10&market_ids=market-good%2Cmarket-unknown",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
  });

  it("maps lineage observability resources", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          {
            id: "job-1",
            job_name: "sync-calendar",
            source: "openf1",
            dataset: "sessions",
            status: "completed",
            execute_mode: "execute",
            planned_inputs: { season: 2026 },
            cursor_after: { synced_at: "2026-03-28T01:01:00Z" },
            records_written: 22,
            error_message: null,
            started_at: "2026-03-28T01:00:00Z",
            finished_at: "2026-03-28T01:01:00Z",
          },
        ],
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          {
            id: "cursor-1",
            source: "polymarket",
            dataset: "markets",
            cursor_key: "next_cursor",
            cursor_value: { page: 3 },
            updated_at: "2026-03-28T01:02:00Z",
          },
        ],
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          {
            id: "dq-1",
            dataset: "polymarket_ws_message_manifest",
            status: "fail",
            metrics_json: { row_count: 0 },
            observed_at: "2026-03-28T01:03:00Z",
          },
        ],
      });
    vi.stubGlobal("fetch", fetchMock);

    const [jobs, cursors, qualityResults] = await Promise.all([
      sdk.ingestionJobs({ limit: 5 }),
      sdk.cursorStates({ limit: 10 }),
      sdk.qualityResults({ limit: 10 }),
    ]);

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8000/api/v1/lineage/jobs?limit=5",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8000/api/v1/lineage/cursors?limit=10",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "http://127.0.0.1:8000/api/v1/quality/results?limit=10",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(jobs[0]).toEqual(
      expect.objectContaining({
        jobName: "sync-calendar",
        executeMode: "execute",
        plannedInputs: { season: 2026 },
        cursorAfter: { synced_at: "2026-03-28T01:01:00Z" },
        recordsWritten: 22,
        errorMessage: null,
      }),
    );
    expect(cursors[0]).toEqual(
      expect.objectContaining({
        cursorKey: "next_cursor",
        cursorValue: { page: 3 },
      }),
    );
    expect(qualityResults[0]).toEqual(
      expect.objectContaining({
        dataset: "polymarket_ws_message_manifest",
        status: "fail",
        metricsJson: { row_count: 0 },
      }),
    );
  });

  it("extracts detail strings from failed action responses", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      statusText: "Conflict",
      text: async () =>
        JSON.stringify({
          detail: "No Polymarket mappings found for SQ session",
        }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(sdk.runBacktest({ gp_short_code: "china" })).rejects.toThrow(
      "API request failed: 409 No Polymarket mappings found for SQ session",
    );
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
          meeting_slug: "japanese-grand-prix",
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
        calendar_status: "scheduled",
        meeting_slug: "japanese-grand-prix",
        source_conflict: false,
        override_source_url: null,
        calendar_meetings: [
          {
            season: 2026,
            meeting_key: 1281,
            meeting_slug: "japanese-grand-prix",
            ops_slug: "japan",
            meeting_name: "Japanese Grand Prix",
            round_number: 3,
            event_format: "conventional",
            start_date_utc: "2026-03-27T02:30:00Z",
            end_date_utc: "2026-03-29T07:00:00Z",
            country_name: "Japan",
            location: "Suzuka",
            status: "scheduled",
            source_conflict: false,
            source_label: null,
            source_url: null,
            note: null,
          },
        ],
        cancelled_meetings: [
          {
            season: 2026,
            meeting_key: 1282,
            meeting_slug: "bahrain-grand-prix",
            ops_slug: "bahrain",
            meeting_name: "Bahrain Grand Prix",
            round_number: 2,
            event_format: "conventional",
            start_date_utc: "2026-04-10T10:30:00Z",
            end_date_utc: "2026-04-12T16:00:00Z",
            country_name: "Bahrain",
            location: "Sakhir",
            status: "cancelled",
            source_conflict: true,
            source_label: "Formula 1 official",
            source_url:
              "https://www.formula1.com/en/latest/article/bahrain-and-saudi-arabian-grands-prix-will-not-take-place-in-april.1hnqllVG85RSt8pbFc5Ivx/",
            note: null,
          },
        ],
        available_configs: [
          {
            name: "Japanese Grand Prix",
            short_code: "japan_pre",
            meeting_key: 1281,
            season: 2026,
            meeting_slug: "japanese-grand-prix",
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
          meeting_slug: "japanese-grand-prix",
          event_format: "conventional",
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
        model_ready: true,
        required_stage: null,
        active_model_run_id: null,
        model_blockers: [],
        session_stage_statuses: [],
        live_ticket_summary: {
          ticket_count: 0,
          open_ticket_count: 0,
          filled_ticket_count: 0,
          cancelled_ticket_count: 0,
        },
        live_execution_summary: {
          execution_count: 0,
          filled_execution_count: 0,
        },
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
        calendarStatus: "scheduled",
        meetingSlug: "japanese-grand-prix",
        sourceConflict: false,
        overrideSourceUrl: null,
        readyToRun: true,
        modelReady: true,
        requiredStage: null,
        activeModelRunId: null,
        modelBlockers: [],
        sessionStageStatuses: [],
        liveTicketSummary: {
          ticketCount: 0,
          openTicketCount: 0,
          filledTicketCount: 0,
          cancelledTicketCount: 0,
        },
        liveExecutionSummary: {
          executionCount: 0,
          filledExecutionCount: 0,
        },
        calendarMeetings: [
          expect.objectContaining({
            meetingSlug: "japanese-grand-prix",
            opsSlug: "japan",
          }),
        ],
        cancelledMeetings: [
          expect.objectContaining({
            meetingSlug: "bahrain-grand-prix",
            status: "cancelled",
          }),
        ],
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

  it("loads current weekend readiness and maps action summaries", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        now: "2026-03-27T05:13:00Z",
        selected_gp_short_code: "japan_fp1_fp2",
        selected_config: {
          name: "Japanese Grand Prix",
          short_code: "japan_fp1_fp2",
          meeting_key: 1281,
          season: 2026,
          target_session_code: "FP2",
          variant: "fp1_to_fp2",
          source_session_code: "FP1",
          market_taxonomy: "driver_fastest_lap_practice",
          stage_rank: 1,
          stage_label: "FP1 -> FP2",
          display_label: "Use FP1 results to prepare FP2",
          display_description:
            "Use FP1 results to find FP2 markets and prepare paper trading.",
        },
        meeting: null,
        latest_ended_session: null,
        next_active_session: null,
        openf1_credentials_configured: true,
        actions: [
          {
            key: "weekend_cockpit",
            label: "Weekend cockpit",
            status: "ready",
            message: "Ready to run.",
            blockers: [],
            warnings: [],
            meeting_key: 1281,
            meeting_name: "Japanese Grand Prix",
            gp_short_code: "japan_fp1_fp2",
            session_code: "FP2",
            session_key: 11247,
            actionable_after_utc: null,
            openf1_credentials_configured: true,
            last_job_run: {
              id: "job-1",
              job_name: "run-weekend-cockpit",
              status: "completed",
              records_written: 2,
              started_at: "2026-03-27T05:00:00Z",
              finished_at: "2026-03-27T05:01:00Z",
              error_message: null,
            },
            last_report_path: "/tmp/run-weekend-cockpit.json",
          },
        ],
        blockers: [],
        warnings: [],
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const readiness = await sdk.currentWeekendReadiness({
      gpShortCode: "japan_fp1_fp2",
      season: 2026,
      meetingKey: 1281,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/operations/current-weekend-readiness?gp_short_code=japan_fp1_fp2&season=2026&meeting_key=1281",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
    expect(readiness.actions[0]).toEqual(
      expect.objectContaining({
        key: "weekend_cockpit",
        status: "ready",
        gpShortCode: "japan_fp1_fp2",
        lastJobRun: expect.objectContaining({
          jobName: "run-weekend-cockpit",
        }),
      }),
    );
  });

  it("posts latest session refresh requests and maps the response", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        action: "refresh-latest-session",
        status: "ok",
        message: "Updated latest ended session Q for Japanese Grand Prix.",
        meeting_id: "meeting:1281",
        meeting_name: "Japanese Grand Prix",
        refreshed_session: {
          id: "session:11249",
          session_key: 11249,
          session_code: "Q",
          session_name: "Qualifying",
          date_end_utc: "2026-03-28T07:00:00Z",
        },
        f1_records_written: 12,
        markets_discovered: 4,
        mappings_written: 2,
        markets_hydrated: 3,
        artifacts_refreshed: [
          {
            gp_short_code: "japan_fp3",
            status: "processed",
            snapshot_id: "snapshot:fp3",
            rebuilt_snapshot: true,
            bet_count: 1,
            total_pnl: 9.6,
            reason: null,
          },
        ],
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await sdk.refreshLatestSession({
      meeting_id: "meeting:1281",
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/actions/refresh-latest-session",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ meeting_id: "meeting:1281" }),
      }),
    );
    expect(result).toEqual(
      expect.objectContaining({
        meetingId: "meeting:1281",
        meetingName: "Japanese Grand Prix",
        f1RecordsWritten: 12,
        marketsDiscovered: 4,
        mappingsWritten: 2,
        marketsHydrated: 3,
        artifactsRefreshed: [
          expect.objectContaining({
            gpShortCode: "japan_fp3",
            rebuiltSnapshot: true,
            totalPnl: 9.6,
          }),
        ],
        refreshedSession: expect.objectContaining({
          sessionCode: "Q",
          sessionKey: 11249,
        }),
      }),
    );
  });

  it("posts live capture requests and maps the response", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        action: "capture-live-weekend",
        status: "ok",
        message:
          "Captured 20s of live data for Qualifying across 12 market(s).",
        job_run_id: "job-live-1",
        session_key: 11249,
        capture_seconds: 20,
        openf1_messages: 14,
        polymarket_messages: 9,
        market_count: 12,
        polymarket_market_ids: ["market-1", "market-2"],
        records_written: 31,
        summary: {
          openf1_topics: [{ key: "v1/laps", count: 14 }],
          polymarket_event_types: [{ key: "book", count: 9 }],
          observed_market_count: 1,
          observed_token_count: 1,
          market_quotes: [
            {
              market_id: "market-1",
              token_id: "token-1",
              outcome: "Yes",
              event_type: "best_bid_ask",
              observed_at_utc: "2026-03-28T06:20:00Z",
              price: 0.41,
              best_bid: 0.4,
              best_ask: 0.42,
              midpoint: 0.41,
              spread: 0.02,
              size: 12,
              side: "buy",
            },
          ],
        },
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await sdk.captureLiveWeekend({
      session_key: 11249,
      capture_seconds: 20,
      message_limit: 250,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/actions/capture-live-weekend",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          session_key: 11249,
          capture_seconds: 20,
          message_limit: 250,
        }),
      }),
    );
    expect(result).toEqual({
      action: "capture-live-weekend",
      status: "ok",
      message: "Captured 20s of live data for Qualifying across 12 market(s).",
      jobRunId: "job-live-1",
      sessionKey: 11249,
      captureSeconds: 20,
      openf1Messages: 14,
      polymarketMessages: 9,
      marketCount: 12,
      polymarketMarketIds: ["market-1", "market-2"],
      recordsWritten: 31,
      summary: {
        openf1Topics: [{ key: "v1/laps", count: 14 }],
        polymarketEventTypes: [{ key: "book", count: 9 }],
        observedMarketCount: 1,
        observedTokenCount: 1,
        marketQuotes: [
          {
            marketId: "market-1",
            tokenId: "token-1",
            outcome: "Yes",
            eventType: "best_bid_ask",
            observedAtUtc: "2026-03-28T06:20:00Z",
            price: 0.41,
            bestBid: 0.4,
            bestAsk: 0.42,
            midpoint: 0.41,
            spread: 0.02,
            size: 12,
            side: "buy",
          },
        ],
      },
    });
  });

  it("posts manual live paper-trade requests and maps the response", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        action: "execute-manual-live-paper-trade",
        status: "ok",
        message: "Opened manual YES paper trade.",
        gp_short_code: "japan_fp1_fp2",
        market_id: "market-1",
        pt_session_id: "pt-live-1",
        signal_action: "buy_yes",
        quantity: 10,
        entry_price: 0.41,
        stake_cost: 4.1,
        market_price: 0.41,
        model_prob: 0.62,
        edge: 0.21,
        side_label: "YES",
        reason: "signal_accepted",
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await sdk.executeManualLivePaperTrade({
      gp_short_code: "japan_fp1_fp2",
      market_id: "market-1",
      token_id: "token-1",
      model_run_id: "model-run-live",
      snapshot_id: "snapshot-live",
      model_prob: 0.62,
      market_price: 0.41,
      observed_at_utc: "2026-03-27T06:20:00Z",
      observed_spread: 0.02,
      source_event_type: "best_bid_ask",
      min_edge: 0.07,
      max_spread: 0.03,
      bet_size: 12,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/actions/execute-manual-live-paper-trade",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          gp_short_code: "japan_fp1_fp2",
          market_id: "market-1",
          token_id: "token-1",
          model_run_id: "model-run-live",
          snapshot_id: "snapshot-live",
          model_prob: 0.62,
          market_price: 0.41,
          observed_at_utc: "2026-03-27T06:20:00Z",
          observed_spread: 0.02,
          source_event_type: "best_bid_ask",
          min_edge: 0.07,
          max_spread: 0.03,
          bet_size: 12,
        }),
      }),
    );
    expect(result).toEqual({
      action: "execute-manual-live-paper-trade",
      status: "ok",
      message: "Opened manual YES paper trade.",
      gpShortCode: "japan_fp1_fp2",
      marketId: "market-1",
      ptSessionId: "pt-live-1",
      signalAction: "buy_yes",
      quantity: 10,
      entryPrice: 0.41,
      stakeCost: 4.1,
      marketPrice: 0.41,
      modelProb: 0.62,
      edge: 0.21,
      sideLabel: "YES",
      reason: "signal_accepted",
    });
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
