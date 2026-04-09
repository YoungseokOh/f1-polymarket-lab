"""add signal ensemble storage tables"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260408_0013"
down_revision = "20260408_0012"
branch_labels = None
depends_on = None


def _has_index(bind: sa.Connection, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(bind)
    return index_name in {index["name"] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    existing_tables = set(sa.inspect(bind).get_table_names())

    if "signal_registry" not in existing_tables:
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
    for index_name, columns in (
        ("ix_signal_registry_signal_code", ["signal_code"]),
        ("ix_signal_registry_signal_family", ["signal_family"]),
        ("ix_signal_registry_market_taxonomy", ["market_taxonomy"]),
        ("ix_signal_registry_market_group", ["market_group"]),
        ("ix_signal_registry_is_active", ["is_active"]),
    ):
        if _has_index(bind, "signal_registry", index_name) is False:
            op.create_index(index_name, "signal_registry", columns)

    if "signal_snapshots" not in existing_tables:
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
    for index_name, columns in (
        ("ix_signal_snapshots_model_run_id", ["model_run_id"]),
        ("ix_signal_snapshots_feature_snapshot_id", ["feature_snapshot_id"]),
        ("ix_signal_snapshots_market_id", ["market_id"]),
        ("ix_signal_snapshots_token_id", ["token_id"]),
        ("ix_signal_snapshots_event_id", ["event_id"]),
        ("ix_signal_snapshots_market_taxonomy", ["market_taxonomy"]),
        ("ix_signal_snapshots_market_group", ["market_group"]),
        ("ix_signal_snapshots_meeting_key", ["meeting_key"]),
        ("ix_signal_snapshots_as_of_ts", ["as_of_ts"]),
        ("ix_signal_snapshots_signal_code", ["signal_code"]),
        ("ix_signal_snapshots_coverage_flag", ["coverage_flag"]),
    ):
        if _has_index(bind, "signal_snapshots", index_name) is False:
            op.create_index(index_name, "signal_snapshots", columns)

    if "signal_diagnostics" not in existing_tables:
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
    for index_name, columns in (
        ("ix_signal_diagnostics_model_run_id", ["model_run_id"]),
        ("ix_signal_diagnostics_signal_code", ["signal_code"]),
        ("ix_signal_diagnostics_market_taxonomy", ["market_taxonomy"]),
        ("ix_signal_diagnostics_market_group", ["market_group"]),
        ("ix_signal_diagnostics_phase_bucket", ["phase_bucket"]),
    ):
        if _has_index(bind, "signal_diagnostics", index_name) is False:
            op.create_index(index_name, "signal_diagnostics", columns)

    if "ensemble_predictions" not in existing_tables:
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
    for index_name, columns in (
        ("ix_ensemble_predictions_model_run_id", ["model_run_id"]),
        ("ix_ensemble_predictions_feature_snapshot_id", ["feature_snapshot_id"]),
        ("ix_ensemble_predictions_market_id", ["market_id"]),
        ("ix_ensemble_predictions_token_id", ["token_id"]),
        ("ix_ensemble_predictions_event_id", ["event_id"]),
        ("ix_ensemble_predictions_market_taxonomy", ["market_taxonomy"]),
        ("ix_ensemble_predictions_market_group", ["market_group"]),
        ("ix_ensemble_predictions_meeting_key", ["meeting_key"]),
        ("ix_ensemble_predictions_as_of_ts", ["as_of_ts"]),
    ):
        if _has_index(bind, "ensemble_predictions", index_name) is False:
            op.create_index(index_name, "ensemble_predictions", columns)

    if "trade_decisions" not in existing_tables:
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
            sa.Column(
                "decision_status",
                sa.String(length=32),
                nullable=False,
                server_default="skip",
            ),
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
    for index_name, columns in (
        ("ix_trade_decisions_model_run_id", ["model_run_id"]),
        ("ix_trade_decisions_ensemble_prediction_id", ["ensemble_prediction_id"]),
        ("ix_trade_decisions_feature_snapshot_id", ["feature_snapshot_id"]),
        ("ix_trade_decisions_market_id", ["market_id"]),
        ("ix_trade_decisions_token_id", ["token_id"]),
        ("ix_trade_decisions_event_id", ["event_id"]),
        ("ix_trade_decisions_market_taxonomy", ["market_taxonomy"]),
        ("ix_trade_decisions_market_group", ["market_group"]),
        ("ix_trade_decisions_meeting_key", ["meeting_key"]),
        ("ix_trade_decisions_as_of_ts", ["as_of_ts"]),
        ("ix_trade_decisions_decision_status", ["decision_status"]),
    ):
        if _has_index(bind, "trade_decisions", index_name) is False:
            op.create_index(index_name, "trade_decisions", columns)


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
