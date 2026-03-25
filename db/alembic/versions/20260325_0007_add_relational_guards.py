"""add relational guards for core entities"""

from __future__ import annotations

from alembic import op

revision = "20260325_0007"
down_revision = "20260320_0006"
branch_labels = None
depends_on = None


def _null_missing_fk(table: str, column: str, ref_table: str, ref_column: str = "id") -> None:
    op.execute(
        f"""
        UPDATE {table}
        SET {column} = NULL
        WHERE {column} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM {ref_table}
            WHERE {ref_table}.{ref_column} = {table}.{column}
          )
        """
    )


def _delete_missing_fk(table: str, column: str, ref_table: str, ref_column: str = "id") -> None:
    op.execute(
        f"""
        DELETE FROM {table}
        WHERE {column} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
            FROM {ref_table}
            WHERE {ref_table}.{ref_column} = {table}.{column}
          )
        """
    )


def _drop_fk(name: str, table: str) -> None:
    op.drop_constraint(name, table, type_="foreignkey")


def upgrade() -> None:
    _null_missing_fk("source_fetch_log", "job_run_id", "ingestion_job_runs")
    _null_missing_fk("ingestion_job_runs", "job_definition_id", "ingestion_job_definitions")
    _null_missing_fk("bronze_object_manifest", "job_run_id", "ingestion_job_runs")
    _null_missing_fk("data_quality_results", "check_id", "data_quality_checks")
    _null_missing_fk("data_quality_results", "job_run_id", "ingestion_job_runs")

    _null_missing_fk("f1_sessions", "meeting_id", "f1_meetings")
    _delete_missing_fk("f1_session_results", "session_id", "f1_sessions")
    _delete_missing_fk("f1_laps", "session_id", "f1_sessions")
    _delete_missing_fk("f1_stints", "session_id", "f1_sessions")
    _null_missing_fk("f1_weather", "meeting_id", "f1_meetings")
    _null_missing_fk("f1_weather", "session_id", "f1_sessions")
    _null_missing_fk("f1_race_control", "meeting_id", "f1_meetings")
    _null_missing_fk("f1_race_control", "session_id", "f1_sessions")
    _delete_missing_fk("f1_positions", "session_id", "f1_sessions")
    _delete_missing_fk("f1_intervals", "session_id", "f1_sessions")
    _delete_missing_fk("f1_pit", "session_id", "f1_sessions")
    _delete_missing_fk("f1_telemetry_index", "session_id", "f1_sessions")
    _delete_missing_fk("f1_team_radio_metadata", "session_id", "f1_sessions")
    _delete_missing_fk("f1_starting_grid", "session_id", "f1_sessions")

    _null_missing_fk("polymarket_markets", "event_id", "polymarket_events")
    _delete_missing_fk("polymarket_market_status_history", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_tokens", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_market_rules", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_price_history", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_open_interest_history", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_orderbook_snapshots", "market_id", "polymarket_markets")
    _delete_missing_fk(
        "polymarket_orderbook_levels",
        "snapshot_id",
        "polymarket_orderbook_snapshots",
    )
    _delete_missing_fk("polymarket_orderbook_levels", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_trades", "market_id", "polymarket_markets")
    _delete_missing_fk("polymarket_resolution", "market_id", "polymarket_markets")
    _null_missing_fk("polymarket_ws_message_manifest", "market_id", "polymarket_markets")
    _delete_missing_fk("market_taxonomy_labels", "market_id", "polymarket_markets")
    _null_missing_fk("market_taxonomy_labels", "taxonomy_version_id", "market_taxonomy_versions")

    _null_missing_fk("mapping_candidates", "f1_meeting_id", "f1_meetings")
    _null_missing_fk("mapping_candidates", "f1_session_id", "f1_sessions")
    _null_missing_fk("mapping_candidates", "polymarket_event_id", "polymarket_events")
    _null_missing_fk("mapping_candidates", "polymarket_market_id", "polymarket_markets")
    _delete_missing_fk("manual_mapping_overrides", "polymarket_market_id", "polymarket_markets")
    _null_missing_fk("manual_mapping_overrides", "f1_session_id", "f1_sessions")
    _null_missing_fk("manual_mapping_overrides", "f1_meeting_id", "f1_meetings")
    _null_missing_fk("entity_mapping_f1_to_polymarket", "f1_meeting_id", "f1_meetings")
    _null_missing_fk("entity_mapping_f1_to_polymarket", "f1_session_id", "f1_sessions")
    _null_missing_fk("entity_mapping_f1_to_polymarket", "polymarket_event_id", "polymarket_events")
    _null_missing_fk(
        "entity_mapping_f1_to_polymarket",
        "polymarket_market_id",
        "polymarket_markets",
    )

    op.create_foreign_key(
        "fk_fetch_log_job_run",
        "source_fetch_log",
        "ingestion_job_runs",
        ["job_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_job_run_definition",
        "ingestion_job_runs",
        "ingestion_job_definitions",
        ["job_definition_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_bronze_manifest_job_run",
        "bronze_object_manifest",
        "ingestion_job_runs",
        ["job_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_dq_result_check",
        "data_quality_results",
        "data_quality_checks",
        ["check_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_dq_result_job_run",
        "data_quality_results",
        "ingestion_job_runs",
        ["job_run_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_foreign_key(
        "fk_f1_session_meeting",
        "f1_sessions",
        "f1_meetings",
        ["meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_result_session",
        "f1_session_results",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_lap_session",
        "f1_laps",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_stint_session",
        "f1_stints",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_weather_meeting",
        "f1_weather",
        "f1_meetings",
        ["meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_weather_session",
        "f1_weather",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_race_control_meeting",
        "f1_race_control",
        "f1_meetings",
        ["meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_race_control_session",
        "f1_race_control",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_position_session",
        "f1_positions",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_interval_session",
        "f1_intervals",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_pit_session",
        "f1_pit",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_telemetry_session",
        "f1_telemetry_index",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_team_radio_session",
        "f1_team_radio_metadata",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_f1_starting_grid_session",
        "f1_starting_grid",
        "f1_sessions",
        ["session_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        "fk_market_event",
        "polymarket_markets",
        "polymarket_events",
        ["event_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_market_status_market",
        "polymarket_market_status_history",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_token_market",
        "polymarket_tokens",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_market_rule_market",
        "polymarket_market_rules",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_price_history_market",
        "polymarket_price_history",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_open_interest_market",
        "polymarket_open_interest_history",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_orderbook_snapshot_market",
        "polymarket_orderbook_snapshots",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_orderbook_level_snapshot",
        "polymarket_orderbook_levels",
        "polymarket_orderbook_snapshots",
        ["snapshot_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_orderbook_level_market",
        "polymarket_orderbook_levels",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_trade_market",
        "polymarket_trades",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_resolution_market",
        "polymarket_resolution",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_ws_manifest_market",
        "polymarket_ws_message_manifest",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_taxonomy_label_market",
        "market_taxonomy_labels",
        "polymarket_markets",
        ["market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_taxonomy_label_version",
        "market_taxonomy_labels",
        "market_taxonomy_versions",
        ["taxonomy_version_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_foreign_key(
        "fk_mapping_candidate_meeting",
        "mapping_candidates",
        "f1_meetings",
        ["f1_meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_mapping_candidate_session",
        "mapping_candidates",
        "f1_sessions",
        ["f1_session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_mapping_candidate_event",
        "mapping_candidates",
        "polymarket_events",
        ["polymarket_event_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_mapping_candidate_market",
        "mapping_candidates",
        "polymarket_markets",
        ["polymarket_market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_manual_override_market",
        "manual_mapping_overrides",
        "polymarket_markets",
        ["polymarket_market_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_manual_override_session",
        "manual_mapping_overrides",
        "f1_sessions",
        ["f1_session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_manual_override_meeting",
        "manual_mapping_overrides",
        "f1_meetings",
        ["f1_meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_entity_mapping_meeting",
        "entity_mapping_f1_to_polymarket",
        "f1_meetings",
        ["f1_meeting_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_entity_mapping_session",
        "entity_mapping_f1_to_polymarket",
        "f1_sessions",
        ["f1_session_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_entity_mapping_event",
        "entity_mapping_f1_to_polymarket",
        "polymarket_events",
        ["polymarket_event_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_entity_mapping_market",
        "entity_mapping_f1_to_polymarket",
        "polymarket_markets",
        ["polymarket_market_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    _drop_fk("fk_entity_mapping_market", "entity_mapping_f1_to_polymarket")
    _drop_fk("fk_entity_mapping_event", "entity_mapping_f1_to_polymarket")
    _drop_fk("fk_entity_mapping_session", "entity_mapping_f1_to_polymarket")
    _drop_fk("fk_entity_mapping_meeting", "entity_mapping_f1_to_polymarket")
    _drop_fk("fk_manual_override_meeting", "manual_mapping_overrides")
    _drop_fk("fk_manual_override_session", "manual_mapping_overrides")
    _drop_fk("fk_manual_override_market", "manual_mapping_overrides")
    _drop_fk("fk_mapping_candidate_market", "mapping_candidates")
    _drop_fk("fk_mapping_candidate_event", "mapping_candidates")
    _drop_fk("fk_mapping_candidate_session", "mapping_candidates")
    _drop_fk("fk_mapping_candidate_meeting", "mapping_candidates")
    _drop_fk("fk_taxonomy_label_version", "market_taxonomy_labels")
    _drop_fk("fk_taxonomy_label_market", "market_taxonomy_labels")
    _drop_fk("fk_ws_manifest_market", "polymarket_ws_message_manifest")
    _drop_fk("fk_resolution_market", "polymarket_resolution")
    _drop_fk("fk_trade_market", "polymarket_trades")
    _drop_fk("fk_orderbook_level_market", "polymarket_orderbook_levels")
    _drop_fk("fk_orderbook_level_snapshot", "polymarket_orderbook_levels")
    _drop_fk("fk_orderbook_snapshot_market", "polymarket_orderbook_snapshots")
    _drop_fk("fk_open_interest_market", "polymarket_open_interest_history")
    _drop_fk("fk_price_history_market", "polymarket_price_history")
    _drop_fk("fk_market_rule_market", "polymarket_market_rules")
    _drop_fk("fk_token_market", "polymarket_tokens")
    _drop_fk("fk_market_status_market", "polymarket_market_status_history")
    _drop_fk("fk_market_event", "polymarket_markets")
    _drop_fk("fk_f1_starting_grid_session", "f1_starting_grid")
    _drop_fk("fk_f1_team_radio_session", "f1_team_radio_metadata")
    _drop_fk("fk_f1_telemetry_session", "f1_telemetry_index")
    _drop_fk("fk_f1_pit_session", "f1_pit")
    _drop_fk("fk_f1_interval_session", "f1_intervals")
    _drop_fk("fk_f1_position_session", "f1_positions")
    _drop_fk("fk_f1_race_control_session", "f1_race_control")
    _drop_fk("fk_f1_race_control_meeting", "f1_race_control")
    _drop_fk("fk_f1_weather_session", "f1_weather")
    _drop_fk("fk_f1_weather_meeting", "f1_weather")
    _drop_fk("fk_f1_stint_session", "f1_stints")
    _drop_fk("fk_f1_lap_session", "f1_laps")
    _drop_fk("fk_f1_result_session", "f1_session_results")
    _drop_fk("fk_f1_session_meeting", "f1_sessions")
    _drop_fk("fk_dq_result_job_run", "data_quality_results")
    _drop_fk("fk_dq_result_check", "data_quality_results")
    _drop_fk("fk_bronze_manifest_job_run", "bronze_object_manifest")
    _drop_fk("fk_job_run_definition", "ingestion_job_runs")
    _drop_fk("fk_fetch_log_job_run", "source_fetch_log")
