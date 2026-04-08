"""add signal ensemble storage tables"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260408_0013"
down_revision = "20260408_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "signal_registry",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("signal_code", sa.String(length=128), nullable=False),
        sa.Column("signal_family", sa.String(length=128), nullable=False),
        sa.Column("market_taxonomy", sa.String(length=64), nullable=True),
        sa.Column("market_group", sa.String(length=64), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("version", sa.String(length=32), nullable=False, server_default="v1"),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "signal_code",
            "version",
            "market_taxonomy",
            "market_group",
            name="uq_signal_registry_code_version_scope",
        ),
    )
    op.create_index("ix_signal_registry_signal_code", "signal_registry", ["signal_code"])
    op.create_index("ix_signal_registry_signal_family", "signal_registry", ["signal_family"])
    op.create_index("ix_signal_registry_market_taxonomy", "signal_registry", ["market_taxonomy"])
    op.create_index("ix_signal_registry_market_group", "signal_registry", ["market_group"])
    op.create_index("ix_signal_registry_is_active", "signal_registry", ["is_active"])

    op.create_table(
        "signal_snapshots",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=False),
        sa.Column("feature_snapshot_id", sa.String(length=36), nullable=True),
        sa.Column("market_id", sa.String(length=64), nullable=True),
        sa.Column("token_id", sa.String(length=128), nullable=True),
        sa.Column("event_id", sa.String(length=128), nullable=True),
        sa.Column("market_taxonomy", sa.String(length=64), nullable=True),
        sa.Column("market_group", sa.String(length=64), nullable=True),
        sa.Column("meeting_key", sa.Integer(), nullable=True),
        sa.Column("as_of_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_code", sa.String(length=128), nullable=False),
        sa.Column("signal_version", sa.String(length=32), nullable=False, server_default="v1"),
        sa.Column("p_yes_raw", sa.Float(), nullable=True),
        sa.Column("p_yes_calibrated", sa.Float(), nullable=True),
        sa.Column("p_market_ref", sa.Float(), nullable=True),
        sa.Column("delta_logit", sa.Float(), nullable=True),
        sa.Column("freshness_sec", sa.Float(), nullable=True),
        sa.Column("coverage_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "model_run_id",
            "market_id",
            "token_id",
            "as_of_ts",
            "signal_code",
            name="uq_signal_snapshots_run_market_asof_signal",
        ),
    )
    op.create_index("ix_signal_snapshots_model_run_id", "signal_snapshots", ["model_run_id"])
    op.create_index("ix_signal_snapshots_feature_snapshot_id", "signal_snapshots", ["feature_snapshot_id"])
    op.create_index("ix_signal_snapshots_market_id", "signal_snapshots", ["market_id"])
    op.create_index("ix_signal_snapshots_token_id", "signal_snapshots", ["token_id"])
    op.create_index("ix_signal_snapshots_event_id", "signal_snapshots", ["event_id"])
    op.create_index("ix_signal_snapshots_market_taxonomy", "signal_snapshots", ["market_taxonomy"])
    op.create_index("ix_signal_snapshots_market_group", "signal_snapshots", ["market_group"])
    op.create_index("ix_signal_snapshots_meeting_key", "signal_snapshots", ["meeting_key"])
    op.create_index("ix_signal_snapshots_as_of_ts", "signal_snapshots", ["as_of_ts"])
    op.create_index("ix_signal_snapshots_signal_code", "signal_snapshots", ["signal_code"])
    op.create_index("ix_signal_snapshots_coverage_flag", "signal_snapshots", ["coverage_flag"])

    op.create_table(
        "signal_diagnostics",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=False),
        sa.Column("signal_code", sa.String(length=128), nullable=False),
        sa.Column("market_taxonomy", sa.String(length=64), nullable=True),
        sa.Column("market_group", sa.String(length=64), nullable=True),
        sa.Column("phase_bucket", sa.String(length=64), nullable=True),
        sa.Column("brier", sa.Float(), nullable=True),
        sa.Column("log_loss", sa.Float(), nullable=True),
        sa.Column("ece", sa.Float(), nullable=True),
        sa.Column("skill_vs_market", sa.Float(), nullable=True),
        sa.Column("coverage_rate", sa.Float(), nullable=True),
        sa.Column("residual_correlation_json", sa.JSON(), nullable=True),
        sa.Column("stability_json", sa.JSON(), nullable=True),
        sa.Column("metrics_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "model_run_id",
            "signal_code",
            "market_group",
            "market_taxonomy",
            "phase_bucket",
            name="uq_signal_diagnostics_run_signal_scope",
        ),
    )
    op.create_index("ix_signal_diagnostics_model_run_id", "signal_diagnostics", ["model_run_id"])
    op.create_index("ix_signal_diagnostics_signal_code", "signal_diagnostics", ["signal_code"])
    op.create_index("ix_signal_diagnostics_market_taxonomy", "signal_diagnostics", ["market_taxonomy"])
    op.create_index("ix_signal_diagnostics_market_group", "signal_diagnostics", ["market_group"])
    op.create_index("ix_signal_diagnostics_phase_bucket", "signal_diagnostics", ["phase_bucket"])

    op.create_table(
        "ensemble_predictions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=False),
        sa.Column("feature_snapshot_id", sa.String(length=36), nullable=True),
        sa.Column("market_id", sa.String(length=64), nullable=True),
        sa.Column("token_id", sa.String(length=128), nullable=True),
        sa.Column("event_id", sa.String(length=128), nullable=True),
        sa.Column("market_taxonomy", sa.String(length=64), nullable=True),
        sa.Column("market_group", sa.String(length=64), nullable=True),
        sa.Column("meeting_key", sa.Integer(), nullable=True),
        sa.Column("as_of_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("p_market_ref", sa.Float(), nullable=True),
        sa.Column("p_yes_ensemble", sa.Float(), nullable=True),
        sa.Column("z_market", sa.Float(), nullable=True),
        sa.Column("z_ensemble", sa.Float(), nullable=True),
        sa.Column("intercept", sa.Float(), nullable=True),
        sa.Column("disagreement_score", sa.Float(), nullable=True),
        sa.Column("effective_n", sa.Float(), nullable=True),
        sa.Column("uncertainty_score", sa.Float(), nullable=True),
        sa.Column("contributions_json", sa.JSON(), nullable=True),
        sa.Column("coverage_json", sa.JSON(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "model_run_id",
            "market_id",
            "token_id",
            "as_of_ts",
            name="uq_ensemble_predictions_run_market_asof",
        ),
    )
    op.create_index("ix_ensemble_predictions_model_run_id", "ensemble_predictions", ["model_run_id"])
    op.create_index(
        "ix_ensemble_predictions_feature_snapshot_id",
        "ensemble_predictions",
        ["feature_snapshot_id"],
    )
    op.create_index("ix_ensemble_predictions_market_id", "ensemble_predictions", ["market_id"])
    op.create_index("ix_ensemble_predictions_token_id", "ensemble_predictions", ["token_id"])
    op.create_index("ix_ensemble_predictions_event_id", "ensemble_predictions", ["event_id"])
    op.create_index(
        "ix_ensemble_predictions_market_taxonomy",
        "ensemble_predictions",
        ["market_taxonomy"],
    )
    op.create_index("ix_ensemble_predictions_market_group", "ensemble_predictions", ["market_group"])
    op.create_index("ix_ensemble_predictions_meeting_key", "ensemble_predictions", ["meeting_key"])
    op.create_index("ix_ensemble_predictions_as_of_ts", "ensemble_predictions", ["as_of_ts"])

    op.create_table(
        "trade_decisions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("model_run_id", sa.String(length=36), nullable=False),
        sa.Column("ensemble_prediction_id", sa.String(length=36), nullable=True),
        sa.Column("feature_snapshot_id", sa.String(length=36), nullable=True),
        sa.Column("market_id", sa.String(length=64), nullable=True),
        sa.Column("token_id", sa.String(length=128), nullable=True),
        sa.Column("event_id", sa.String(length=128), nullable=True),
        sa.Column("market_taxonomy", sa.String(length=64), nullable=True),
        sa.Column("market_group", sa.String(length=64), nullable=True),
        sa.Column("meeting_key", sa.Integer(), nullable=True),
        sa.Column("as_of_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False, server_default="skip"),
        sa.Column("edge", sa.Float(), nullable=True),
        sa.Column("threshold", sa.Float(), nullable=True),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("depth", sa.Float(), nullable=True),
        sa.Column("kelly_fraction_raw", sa.Float(), nullable=True),
        sa.Column("disagreement_penalty", sa.Float(), nullable=True),
        sa.Column("liquidity_factor", sa.Float(), nullable=True),
        sa.Column("size_fraction", sa.Float(), nullable=True),
        sa.Column("decision_status", sa.String(length=32), nullable=False, server_default="skip"),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "model_run_id",
            "market_id",
            "token_id",
            "as_of_ts",
            name="uq_trade_decisions_run_market_asof",
        ),
    )
    op.create_index("ix_trade_decisions_model_run_id", "trade_decisions", ["model_run_id"])
    op.create_index("ix_trade_decisions_ensemble_prediction_id", "trade_decisions", ["ensemble_prediction_id"])
    op.create_index("ix_trade_decisions_feature_snapshot_id", "trade_decisions", ["feature_snapshot_id"])
    op.create_index("ix_trade_decisions_market_id", "trade_decisions", ["market_id"])
    op.create_index("ix_trade_decisions_token_id", "trade_decisions", ["token_id"])
    op.create_index("ix_trade_decisions_event_id", "trade_decisions", ["event_id"])
    op.create_index("ix_trade_decisions_market_taxonomy", "trade_decisions", ["market_taxonomy"])
    op.create_index("ix_trade_decisions_market_group", "trade_decisions", ["market_group"])
    op.create_index("ix_trade_decisions_meeting_key", "trade_decisions", ["meeting_key"])
    op.create_index("ix_trade_decisions_as_of_ts", "trade_decisions", ["as_of_ts"])
    op.create_index("ix_trade_decisions_decision_status", "trade_decisions", ["decision_status"])


def downgrade() -> None:
    op.drop_index("ix_trade_decisions_decision_status", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_as_of_ts", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_meeting_key", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_market_group", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_market_taxonomy", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_event_id", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_token_id", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_market_id", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_feature_snapshot_id", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_ensemble_prediction_id", table_name="trade_decisions")
    op.drop_index("ix_trade_decisions_model_run_id", table_name="trade_decisions")
    op.drop_table("trade_decisions")

    op.drop_index("ix_ensemble_predictions_as_of_ts", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_meeting_key", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_market_group", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_market_taxonomy", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_event_id", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_token_id", table_name="ensemble_predictions")
    op.drop_index("ix_ensemble_predictions_market_id", table_name="ensemble_predictions")
    op.drop_index(
        "ix_ensemble_predictions_feature_snapshot_id",
        table_name="ensemble_predictions",
    )
    op.drop_index("ix_ensemble_predictions_model_run_id", table_name="ensemble_predictions")
    op.drop_table("ensemble_predictions")

    op.drop_index("ix_signal_diagnostics_phase_bucket", table_name="signal_diagnostics")
    op.drop_index("ix_signal_diagnostics_market_group", table_name="signal_diagnostics")
    op.drop_index("ix_signal_diagnostics_market_taxonomy", table_name="signal_diagnostics")
    op.drop_index("ix_signal_diagnostics_signal_code", table_name="signal_diagnostics")
    op.drop_index("ix_signal_diagnostics_model_run_id", table_name="signal_diagnostics")
    op.drop_table("signal_diagnostics")

    op.drop_index("ix_signal_snapshots_coverage_flag", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_signal_code", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_as_of_ts", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_meeting_key", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_market_group", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_market_taxonomy", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_event_id", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_token_id", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_market_id", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_feature_snapshot_id", table_name="signal_snapshots")
    op.drop_index("ix_signal_snapshots_model_run_id", table_name="signal_snapshots")
    op.drop_table("signal_snapshots")

    op.drop_index("ix_signal_registry_is_active", table_name="signal_registry")
    op.drop_index("ix_signal_registry_market_group", table_name="signal_registry")
    op.drop_index("ix_signal_registry_market_taxonomy", table_name="signal_registry")
    op.drop_index("ix_signal_registry_signal_family", table_name="signal_registry")
    op.drop_index("ix_signal_registry_signal_code", table_name="signal_registry")
    op.drop_table("signal_registry")
