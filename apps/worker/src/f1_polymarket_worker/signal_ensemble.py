from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
from f1_polymarket_lab.common import ensure_dir, stable_uuid, utc_now
from f1_polymarket_lab.models import (
    SIGNAL_ENSEMBLE_STAGE,
    SignalEnsembleConfig,
    default_signal_registry_entries,
    score_signal_ensemble_frame,
    train_signal_ensemble_split,
)
from f1_polymarket_lab.storage.models import (
    BacktestOrder,
    BacktestPosition,
    BacktestResult,
    EnsemblePrediction,
    FeatureSnapshot,
    ModelPrediction,
    ModelRun,
    SignalDiagnostic,
    SignalRegistryEntry,
    SignalSnapshot,
    TradeDecision,
)
from f1_polymarket_lab.storage.repository import upsert_records
from sqlalchemy import select

from f1_polymarket_worker.backtest import FEE_RATE, _compute_backtest_metrics
from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)
from f1_polymarket_worker.pipeline import PipelineContext

SIGNAL_ENSEMBLE_MODEL_FAMILY = "signal_ensemble"
SIGNAL_ENSEMBLE_STRATEGY_NAME = "ensemble_capped_kelly"


def _decision_lookup_key(
    *,
    market_id: Any,
    token_id: Any,
    as_of_ts: Any,
) -> tuple[str, str, str]:
    if hasattr(as_of_ts, "isoformat"):
        as_of_value = str(as_of_ts.isoformat())
    else:
        as_of_value = str(as_of_ts or "")
    return (str(market_id or ""), str(token_id or ""), as_of_value)


def register_default_signals(ctx: PipelineContext) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="register-default-signals",
        source="model",
        dataset="signal_registry",
        description="Register default signal ensemble definitions.",
        schedule_hint="manual",
    )
    run = start_job_run(ctx.db, definition=definition, execute=ctx.execute, planned_inputs={})
    rows = default_signal_registry_entries()
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "signal_count": len(rows)}

    upsert_records(
        ctx.db,
        SignalRegistryEntry,
        rows,
        conflict_columns=["id"],
    )
    active_ids = {str(row["id"]) for row in rows}
    signal_codes = {str(row["signal_code"]) for row in rows}
    existing_rows = ctx.db.scalars(
        select(SignalRegistryEntry).where(SignalRegistryEntry.signal_code.in_(signal_codes))
    ).all()
    for record in existing_rows:
        record.is_active = str(record.id) in active_ids
        record.updated_at = utc_now()
    finish_job_run(ctx.db, run, status="completed", records_written=len(rows))
    return {"job_run_id": run.id, "status": "completed", "signal_count": len(rows)}


def _load_snapshot_frame(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
) -> tuple[FeatureSnapshot, pl.DataFrame]:
    snapshot = ctx.db.get(FeatureSnapshot, snapshot_id)
    if snapshot is None or snapshot.storage_path is None:
        raise ValueError(f"snapshot_id={snapshot_id} not found or missing storage_path")
    frame = pl.read_parquet(snapshot.storage_path)
    if frame.height == 0:
        raise ValueError(f"snapshot_id={snapshot_id} contains no rows")
    return snapshot, frame


def train_signal_ensemble_from_snapshot_ids(
    ctx: PipelineContext,
    *,
    snapshot_ids: list[str],
    stage: str = SIGNAL_ENSEMBLE_STAGE,
    config: SignalEnsembleConfig | None = None,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="train-signal-ensemble",
        source="model",
        dataset="signal_ensemble",
        description="Train the generic signal ensemble on stored feature snapshots.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"snapshot_ids": snapshot_ids, "stage": stage},
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned", "snapshot_ids": snapshot_ids}

    register_default_signals(ctx)

    snapshots_and_frames = [
        _load_snapshot_frame(ctx, snapshot_id=snapshot_id)
        for snapshot_id in snapshot_ids
    ]
    ordered = sorted(
        snapshots_and_frames,
        key=lambda item: (
            item[0].source_cutoffs.get("meeting_key") if item[0].source_cutoffs else 0,
            item[0].as_of_ts,
        ),
    )
    meeting_keys = [
        item[0].source_cutoffs.get("meeting_key")
        for item in ordered
        if item[0].source_cutoffs and item[0].source_cutoffs.get("meeting_key") is not None
    ]
    if len(set(meeting_keys)) < 2:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message="signal ensemble training requires at least two meeting groups",
        )
        raise ValueError("signal ensemble training requires at least two meeting groups")

    train_parts = []
    for snapshot, frame in ordered[:-1]:
        train_parts.append(frame.with_columns(pl.lit(snapshot.id).alias("feature_snapshot_id")))
    test_snapshot, test_frame = ordered[-1]
    test_frame = test_frame.with_columns(pl.lit(test_snapshot.id).alias("feature_snapshot_id"))
    train_df = pl.concat(train_parts, how="diagonal_relaxed")

    model_run_id = stable_uuid(
        "signal-ensemble-run",
        stage,
        ",".join(snapshot_ids),
    )
    artifact_dir = ensure_dir(
        Path(ctx.settings.data_root) / "artifacts" / "model_runs" / model_run_id
    )
    result = train_signal_ensemble_split(
        train_df,
        test_frame,
        model_run_id=model_run_id,
        stage=stage,
        config=config,
        artifact_dir=artifact_dir,
        feature_snapshot_id=test_snapshot.id,
    )

    model_run_row = {
        "id": model_run_id,
        "stage": stage,
        "model_family": SIGNAL_ENSEMBLE_MODEL_FAMILY,
        "model_name": SIGNAL_ENSEMBLE_MODEL_FAMILY,
        "dataset_version": test_snapshot.feature_version,
        "feature_snapshot_id": test_snapshot.id,
        "train_start": ordered[0][0].as_of_ts,
        "train_end": ordered[-2][0].as_of_ts if len(ordered) > 1 else ordered[0][0].as_of_ts,
        "test_start": test_snapshot.as_of_ts,
        "test_end": test_snapshot.as_of_ts,
        "config_json": result.config,
        "metrics_json": result.metrics,
        "artifact_uri": str(artifact_dir),
        "created_at": utc_now(),
    }
    upsert_records(ctx.db, ModelRun, [model_run_row], conflict_columns=["id"])
    upsert_records(ctx.db, SignalSnapshot, result.signal_snapshots, conflict_columns=["id"])
    upsert_records(ctx.db, SignalDiagnostic, result.diagnostics, conflict_columns=["id"])
    upsert_records(ctx.db, EnsemblePrediction, result.ensemble_predictions, conflict_columns=["id"])
    upsert_records(ctx.db, TradeDecision, result.trade_decisions, conflict_columns=["id"])
    upsert_records(ctx.db, ModelPrediction, result.predictions, conflict_columns=["id"])

    summary_path = artifact_dir / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "snapshot_ids": snapshot_ids,
                "test_snapshot_id": test_snapshot.id,
                "metrics": result.metrics,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    finish_job_run(ctx.db, run, status="completed", records_written=len(result.predictions))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "model_run_id": model_run_id,
        "feature_snapshot_id": test_snapshot.id,
        "prediction_count": len(result.predictions),
        "trade_decision_count": len(result.trade_decisions),
        "artifact_uri": str(artifact_dir),
        "metrics": result.metrics,
    }


def score_signal_ensemble_snapshot(
    ctx: PipelineContext,
    *,
    snapshot_id: str,
    model_run_id: str,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="score-signal-ensemble-snapshot",
        source="model",
        dataset="signal_ensemble_predictions",
        description="Score a stored feature snapshot with a trained signal ensemble run.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={"snapshot_id": snapshot_id, "model_run_id": model_run_id},
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {
            "job_run_id": run.id,
            "status": "planned",
            "snapshot_id": snapshot_id,
            "model_run_id": model_run_id,
        }

    model_run = ctx.db.get(ModelRun, model_run_id)
    if model_run is None or not model_run.artifact_uri:
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message=f"model_run_id={model_run_id} missing artifact_uri",
        )
        raise ValueError(f"model_run_id={model_run_id} missing artifact_uri")

    snapshot, frame = _load_snapshot_frame(ctx, snapshot_id=snapshot_id)
    scored_run_id = stable_uuid("signal-ensemble-score", model_run_id, snapshot_id)
    scored_artifact_dir = ensure_dir(
        Path(ctx.settings.data_root) / "artifacts" / "model_runs" / scored_run_id
    )
    payload = score_signal_ensemble_frame(
        frame,
        artifact_dir=Path(model_run.artifact_uri),
        model_run_id=scored_run_id,
        feature_snapshot_id=snapshot.id,
    )
    scored_model_run = {
        "id": scored_run_id,
        "stage": model_run.stage,
        "model_family": "signal_ensemble_scored",
        "model_name": f"{model_run.model_name}_scored",
        "dataset_version": snapshot.feature_version,
        "feature_snapshot_id": snapshot.id,
        "config_json": {
            "parent_model_run_id": model_run.id,
            "source_feature_snapshot_id": snapshot.id,
        },
        "metrics_json": {
            "row_count": len(payload["predictions"]),
        },
        "artifact_uri": str(scored_artifact_dir),
        "created_at": utc_now(),
    }
    (scored_artifact_dir / "scored_predictions.json").write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    upsert_records(ctx.db, ModelRun, [scored_model_run], conflict_columns=["id"])
    upsert_records(ctx.db, SignalSnapshot, payload["signal_snapshots"], conflict_columns=["id"])
    upsert_records(
        ctx.db,
        EnsemblePrediction,
        payload["ensemble_predictions"],
        conflict_columns=["id"],
    )
    upsert_records(ctx.db, TradeDecision, payload["trade_decisions"], conflict_columns=["id"])
    upsert_records(ctx.db, ModelPrediction, payload["predictions"], conflict_columns=["id"])
    finish_job_run(ctx.db, run, status="completed", records_written=len(payload["predictions"]))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "model_run_id": scored_run_id,
        "source_model_run_id": model_run.id,
        "feature_snapshot_id": snapshot.id,
        "prediction_count": len(payload["predictions"]),
    }


def settle_signal_ensemble_backtest(
    ctx: PipelineContext,
    *,
    model_run_id: str,
    snapshot_id: str,
    strategy_name: str = SIGNAL_ENSEMBLE_STRATEGY_NAME,
) -> dict[str, Any]:
    definition = ensure_job_definition(
        ctx.db,
        job_name="settle-signal-ensemble-backtest",
        source="model",
        dataset="backtest",
        description="Settle ensemble trade decisions against binary outcomes.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "model_run_id": model_run_id,
            "snapshot_id": snapshot_id,
            "strategy_name": strategy_name,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {"job_run_id": run.id, "status": "planned"}

    snapshot, frame = _load_snapshot_frame(ctx, snapshot_id=snapshot_id)
    decision_rows = ctx.db.scalars(
        select(TradeDecision).where(
            TradeDecision.model_run_id == model_run_id,
            TradeDecision.feature_snapshot_id == snapshot_id,
        )
    ).all()
    prediction_rows = ctx.db.scalars(
        select(EnsemblePrediction).where(
            EnsemblePrediction.model_run_id == model_run_id,
            EnsemblePrediction.feature_snapshot_id == snapshot_id,
        )
    ).all()
    decision_by_key = {
        _decision_lookup_key(
            market_id=row.market_id,
            token_id=row.token_id,
            as_of_ts=row.as_of_ts,
        ): row
        for row in decision_rows
    }
    prediction_by_key = {
        _decision_lookup_key(
            market_id=row.market_id,
            token_id=row.token_id,
            as_of_ts=row.as_of_ts,
        ): row
        for row in prediction_rows
    }
    rows = frame.to_dicts()
    backtest_run_id = stable_uuid("signal-ensemble-backtest", model_run_id, snapshot_id)

    order_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    settled_rows: list[dict[str, Any]] = []
    for row in rows:
        row_key = _decision_lookup_key(
            market_id=row.get("market_id"),
            token_id=row.get("token_id"),
            as_of_ts=row.get("as_of_ts") or row.get("entry_observed_at_utc"),
        )
        decision = decision_by_key.get(row_key)
        if decision is None or decision.decision_status != "trade":
            continue
        prediction = prediction_by_key.get(row_key)
        label = row.get("label_yes")
        if label is None:
            continue
        metadata = decision.metadata_json or {}
        if decision.side == "YES":
            entry_price = float(metadata.get("yes_entry_price") or 0.0)
            outcome = int(label)
        else:
            entry_price = float(metadata.get("no_entry_price") or 0.0)
            outcome = 1 - int(label)
        if entry_price <= 0:
            continue
        stake = 10.0 * float(decision.size_fraction or 0.0)
        if stake <= 0:
            continue
        quantity = stake / entry_price
        fees = quantity * entry_price * FEE_RATE
        ensemble_probability = (
            float(prediction.p_yes_ensemble)
            if prediction is not None and prediction.p_yes_ensemble is not None
            else float(row.get("entry_yes_price") or 0.5)
        )
        model_probability = (
            ensemble_probability if decision.side == "YES" else 1.0 - ensemble_probability
        )
        pnl = (
            ((1.0 - entry_price) * quantity - fees)
            if outcome == 1
            else (-entry_price * quantity - fees)
        )
        order_rows.append(
            {
                "id": stable_uuid(
                    "bt-order",
                    backtest_run_id,
                    decision.market_id,
                    decision.token_id,
                    decision.as_of_ts.isoformat(),
                    decision.side,
                ),
                "backtest_run_id": backtest_run_id,
                "market_id": decision.market_id,
                "token_id": decision.token_id,
                "side": decision.side,
                "quantity": quantity,
                "limit_price": entry_price,
                "executed_price": entry_price,
                "executed_at": decision.as_of_ts,
                "fees": fees,
                "slippage": 0.0,
                "raw_json": {
                    "decision_edge": decision.edge,
                    "size_fraction": decision.size_fraction,
                    "market_group": decision.market_group,
                },
            }
        )
        position_rows.append(
            {
                "id": stable_uuid(
                    "bt-position",
                    backtest_run_id,
                    decision.market_id,
                    decision.token_id,
                    decision.as_of_ts.isoformat(),
                    decision.side,
                ),
                "backtest_run_id": backtest_run_id,
                "market_id": decision.market_id,
                "token_id": decision.token_id,
                "quantity": quantity,
                "avg_entry_price": entry_price,
                "opened_at": decision.as_of_ts,
                "closed_at": decision.as_of_ts,
                "realized_pnl": pnl,
                "unrealized_pnl": 0.0,
                "status": "settled",
            }
        )
        settled_rows.append(
            {
                "market_id": decision.market_id,
                "model_probability": model_probability,
                "entry_price": entry_price,
                "edge": float(decision.edge or 0.0),
                "outcome": outcome,
                "quantity": quantity,
                "pnl": pnl,
                "market_group": decision.market_group,
                "taxonomy": decision.market_taxonomy,
                "spread": decision.spread,
                "liquidity_factor": decision.liquidity_factor,
            }
        )

    metrics = _compute_backtest_metrics(settled_rows, bet_size=10.0)
    if settled_rows:
        metrics["market_group_breakdown"] = _breakdown_numeric(settled_rows, key="market_group")
        metrics["taxonomy_breakdown"] = _breakdown_numeric(settled_rows, key="taxonomy")
        metrics["spread_regime_breakdown"] = _spread_breakdown(settled_rows)
        metrics["average_executable_edge_captured"] = round(
            sum(float(row["edge"]) for row in settled_rows) / len(settled_rows), 6
        )
    result_row = {
        "id": stable_uuid("bt-result", backtest_run_id),
        "backtest_run_id": backtest_run_id,
        "strategy_name": strategy_name,
        "stage": SIGNAL_ENSEMBLE_STAGE,
        "start_at": snapshot.as_of_ts,
        "end_at": snapshot.as_of_ts,
        "metrics_json": metrics,
        "created_at": utc_now(),
    }
    if order_rows:
        upsert_records(ctx.db, BacktestOrder, order_rows, conflict_columns=["id"])
    if position_rows:
        upsert_records(ctx.db, BacktestPosition, position_rows, conflict_columns=["id"])
    upsert_records(ctx.db, BacktestResult, [result_row], conflict_columns=["id"])
    finish_job_run(ctx.db, run, status="completed", records_written=len(order_rows))
    return {
        "job_run_id": run.id,
        "status": "completed",
        "backtest_run_id": backtest_run_id,
        "bet_count": len(settled_rows),
        "metrics": metrics,
    }


def _breakdown_numeric(rows: list[dict[str, Any]], *, key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or "unknown"), []).append(row)
    payload: dict[str, dict[str, Any]] = {}
    for group_key, items in grouped.items():
        count = len(items)
        total_pnl = float(sum(float(item["pnl"]) for item in items))
        payload[group_key] = {
            "bet_count": count,
            "total_pnl": round(total_pnl, 4),
            "avg_edge": round(sum(float(item["edge"]) for item in items) / count, 6),
        }
    return payload


def _spread_breakdown(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "tight": [],
        "medium": [],
        "wide": [],
        "unknown": [],
    }
    for row in rows:
        spread = row.get("spread")
        if spread is None:
            buckets["unknown"].append(row)
        elif float(spread) <= 0.02:
            buckets["tight"].append(row)
        elif float(spread) <= 0.05:
            buckets["medium"].append(row)
        else:
            buckets["wide"].append(row)
    return {
        bucket: {
            "bet_count": len(items),
            "total_pnl": round(sum(float(item["pnl"]) for item in items), 4),
        }
        for bucket, items in buckets.items()
        if items
    }
