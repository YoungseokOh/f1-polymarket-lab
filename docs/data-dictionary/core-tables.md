# Core Tables

Current schema baseline includes the required canonical table families:

- ingestion:
  - `source_fetch_log`
  - `ingestion_job_definitions`, `ingestion_job_runs`
  - `source_cursor_state`
  - `bronze_object_manifest`, `schema_registry`
  - `data_quality_checks`, `data_quality_results`
- F1:
  - `f1_meetings`, `f1_sessions`, `f1_drivers`, `f1_teams`
  - `f1_session_results`, `f1_laps`, `f1_stints`
  - `f1_weather`, `f1_race_control`, `f1_positions`, `f1_intervals`, `f1_pit`
  - `f1_telemetry_index`, `f1_team_radio_metadata`, `f1_starting_grid`
  - `circuit_metadata`
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
  - `model_runs`, `model_predictions`
  - `backtest_orders`, `backtest_positions`, `backtest_results`
