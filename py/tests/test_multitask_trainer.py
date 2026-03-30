from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
import torch
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_lab.models.multitask_model import MultitaskModelConfig, MultitaskTabularModel
from f1_polymarket_lab.models.multitask_trainer import (
    MultitaskTrainerConfig,
    train_multitask_split,
)
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import ModelPrediction, ModelRun
from f1_polymarket_worker.cli import app
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from typer.testing import CliRunner


def test_multitask_model_returns_all_heads() -> None:
    model = MultitaskTabularModel(MultitaskModelConfig(input_dim=12))
    x = torch.randn(5, 12)

    outputs = model(x)

    assert set(outputs) == {"constructor_pole", "h2h", "pole", "winner"}
    for tensor in outputs.values():
        assert tensor.shape == (5,)


def test_multitask_model_supports_residual_depth() -> None:
    model = MultitaskTabularModel(MultitaskModelConfig(input_dim=8, hidden_dim=16, depth=3))
    x = torch.randn(2, 8)

    outputs = model(x)

    assert outputs["winner"].dtype == torch.float32


def build_multitask_df(meeting_key: int) -> pl.DataFrame:
    rows = []
    for family in ("pole", "constructor_pole", "winner", "h2h"):
        for idx in range(6):
            rows.append(
                {
                    "meeting_key": meeting_key,
                    "event_id": f"{meeting_key}-{family}",
                    "market_id": f"{meeting_key}-{family}-{idx}",
                    "token_id": f"token-{meeting_key}-{family}-{idx}",
                    "target_market_family": family,
                    "as_of_checkpoint": "Q",
                    "entry_yes_price": 0.15 + idx * 0.02,
                    "entry_spread": 0.04,
                    "entry_midpoint": 0.15 + idx * 0.02,
                    "trade_count_pre_entry": 20 + idx,
                    "last_trade_age_seconds": 60.0,
                    "has_fp1": 1,
                    "has_fp2": 1,
                    "has_fp3": 1,
                    "has_q": 1,
                    "checkpoint_ordinal": 4,
                    "market_family_is_pole": int(family == "pole"),
                    "market_family_is_constructor_pole": int(family == "constructor_pole"),
                    "market_family_is_winner": int(family == "winner"),
                    "market_family_is_h2h": int(family == "h2h"),
                    "label_yes": int(idx == 0),
                }
            )
    return pl.DataFrame(rows)


def build_multitask_df_for_checkpoint(meeting_key: int, checkpoint: str) -> pl.DataFrame:
    checkpoint_ordinal = {"FP1": 1, "FP2": 2, "FP3": 3, "Q": 4}[checkpoint]
    df = build_multitask_df(meeting_key)
    return df.with_columns(
        pl.lit(checkpoint).alias("as_of_checkpoint"),
        pl.lit(int(checkpoint_ordinal >= 1)).alias("has_fp1"),
        pl.lit(int(checkpoint_ordinal >= 2)).alias("has_fp2"),
        pl.lit(int(checkpoint_ordinal >= 3)).alias("has_fp3"),
        pl.lit(int(checkpoint_ordinal >= 4)).alias("has_q"),
        pl.lit(checkpoint_ordinal).alias("checkpoint_ordinal"),
    )


def test_train_multitask_split_returns_predictions_for_each_head() -> None:
    train_df = pl.concat([build_multitask_df(100), build_multitask_df(200)])
    test_df = build_multitask_df(300)

    result = train_multitask_split(
        train_df,
        test_df,
        model_run_id="mt-run-300",
        stage="multitask_qr",
        config=MultitaskTrainerConfig(max_epochs=2, batch_size=8),
    )

    assert result.model_run_id == "mt-run-300"
    assert len(result.predictions) == len(test_df)
    assert "brier_score" in result.metrics
    assert "log_loss" in result.metrics
    assert "realized_pnl_total" in result.metrics
    assert "roi_pct" in result.metrics
    assert "family_pnl_share_max" in result.metrics
    assert "family_metrics" in result.metrics


def test_train_multitask_walk_forward_cli_plan_only(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "snapshots": [
                    {
                        "meeting_key": 1280,
                        "season": 2026,
                        "checkpoint": "FP1",
                        "snapshot_id": "s1",
                        "path": "a.parquet",
                    },
                    {
                        "meeting_key": 1281,
                        "season": 2026,
                        "checkpoint": "Q",
                        "snapshot_id": "s2",
                        "path": "b.parquet",
                    },
                    {
                        "meeting_key": 1282,
                        "season": 2026,
                        "checkpoint": "Q",
                        "snapshot_id": "s3",
                        "path": "c.parquet",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["train-multitask-walk-forward", "--manifest", str(manifest), "--plan-only"],
    )

    assert result.exit_code == 0
    assert "[plan]" in result.stdout


def test_train_multitask_walk_forward_cli_execute_persists_all_checkpoints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)

    train_path = tmp_path / "1280_q.parquet"
    fp1_test_path = tmp_path / "1281_fp1.parquet"
    empty_test_path = tmp_path / "1281_fp2.parquet"
    q_test_path = tmp_path / "1281_q.parquet"
    build_multitask_df_for_checkpoint(1280, "Q").write_parquet(train_path)
    build_multitask_df_for_checkpoint(1281, "FP1").write_parquet(fp1_test_path)
    pl.DataFrame().write_parquet(empty_test_path)
    build_multitask_df_for_checkpoint(1281, "Q").write_parquet(q_test_path)

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "snapshots": [
                    {
                        "meeting_key": 1280,
                        "season": 2026,
                        "checkpoint": "Q",
                        "snapshot_id": "s0",
                        "path": str(train_path),
                    },
                    {
                        "meeting_key": 1281,
                        "season": 2026,
                        "checkpoint": "FP1",
                        "snapshot_id": "s1",
                        "path": str(fp1_test_path),
                    },
                    {
                        "meeting_key": 1281,
                        "season": 2026,
                        "checkpoint": "FP2",
                        "snapshot_id": "s2",
                        "path": str(empty_test_path),
                    },
                    {
                        "meeting_key": 1281,
                        "season": 2026,
                        "checkpoint": "Q",
                        "snapshot_id": "s3",
                        "path": str(q_test_path),
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    @contextmanager
    def fake_db_session(_: str) -> Session:
        yield session

    monkeypatch.setattr("f1_polymarket_worker.cli.get_settings", lambda: Settings())
    monkeypatch.setattr("f1_polymarket_worker.cli.db_session", fake_db_session)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "train-multitask-walk-forward",
            "--manifest",
            str(manifest),
            "--min-train-gps",
            "1",
            "--execute",
        ],
    )

    runs = session.scalars(select(ModelRun)).all()
    predictions = session.scalars(select(ModelPrediction)).all()

    assert result.exit_code == 0
    assert len(runs) == 1
    assert len(predictions) == len(build_multitask_df(1281)) * 2
    assert {row.explanation_json["as_of_checkpoint"] for row in predictions} == {"FP1", "Q"}
