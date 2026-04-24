# Core Tables

Current schema baseline includes the required canonical table families:

- relational guards:
  - core F1 and Polymarket tables now enforce foreign keys on the main meeting/session/event/market
    chains plus mapping tables
  - destructive cleanup should rely on database constraints first; code-level delete fan-out is a
    compatibility layer, not the primary integrity boundary

- ingestion:
  - `source_fetch_log`
  - `ingestion_job_definitions`, `ingestion_job_runs`
  - `ingestion_job_runs` includes queued-job controls: `queued_at`, `available_at`,
    `attempt_count`, `max_attempts`, `locked_by`, and `locked_at`
  - `source_cursor_state`
  - `bronze_object_manifest`, `schema_registry`
  - `data_quality_checks`, `data_quality_results`
  - data quality metadata uses stable symbolic IDs, so `data_quality_checks.id`, `data_quality_results.id`, and `data_quality_results.check_id` allow longer identifiers than UUID-only tables
- F1:
  - `f1_meetings`, `f1_sessions`, `f1_drivers`, `f1_teams`
  - `f1_meetings` now stores `meeting_slug` and `event_format` for ops-stage planning
  - `f1_calendar_overrides` is the authority layer for schedule corrections such as cancelled or postponed GPs
  - `f1_session_results`, `f1_laps`, `f1_stints`
  - `f1_weather`, `f1_race_control`, `f1_positions`, `f1_intervals`, `f1_pit`
  - `f1_telemetry_index`, `f1_team_radio_metadata`, `f1_starting_grid`
  - `circuit_metadata`
  - timing semantics:
    - `f1_session_results` stores `result_time_*`, `gap_to_leader_*`, and explicit `dnf/dns/dsq`
    - `f1_intervals` stores `*_display`, `*_seconds`, `*_laps_behind`, and `*_status`
  - historical source authority:
    - `1950-2022`: `f1db` bootstrap with `jolpica` repair and validation
    - `2023+`: `openf1` canonical race-weekend session and telemetry coverage (`FP1/FP2/FP3/Q/SQ/S/R`)
- Polymarket:
  - `polymarket_events`, `polymarket_markets`, `polymarket_tokens`
  - `polymarket_market_rules`, `polymarket_market_status_history`
  - `polymarket_price_history`, `polymarket_open_interest_history`
  - `polymarket_orderbook_snapshots`, `polymarket_orderbook_levels`
  - `polymarket_trades`, `polymarket_resolution`, `polymarket_ws_message_manifest`
- cross-source:
  - `market_taxonomy_versions`, `market_taxonomy_labels`
  - `mapping_candidates`, `manual_mapping_overrides`
  - `entity_mapping_f1_to_polymarket`
- Gold:
  - `dataset_version_manifest`
  - `feature_registry`, `feature_snapshots`, `snapshot_run_manifest`
  - `model_runs`, `model_run_promotions`, `model_predictions`
  - `backtest_orders`, `backtest_positions`, `backtest_results`
  - `live_trade_tickets`, `live_trade_executions`
