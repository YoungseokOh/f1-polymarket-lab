from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from f1_polymarket_lab.storage.db import Base
from f1_polymarket_lab.storage.models import (
    FeatureSnapshot,
    ModelPrediction,
    ModelRun,
    ModelRunPromotion,
)
from f1_polymarket_worker.model_registry import (
    MULTITASK_PROMOTION_STAGE,
    SQ_POLE_LIVE_PROMOTION_STAGE,
    eligible_promotion_candidates,
    evaluate_live_baseline_promotion_gate,
    promote_best_model_run,
    promote_live_baseline_model_run,
    promote_model_run,
    score_promoted_multitask_snapshot,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def test_promote_model_run_marks_previous_active_run_superseded(tmp_path: Path) -> None:
    session = build_session()
    try:
        artifact_a = tmp_path / "run-a"
        artifact_b = tmp_path / "run-b"
        artifact_a.mkdir(parents=True)
        artifact_b.mkdir(parents=True)
        session.add_all(
            [
                ModelRun(
                    id="run-a",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    dataset_version="multitask_v1",
                    metrics_json={
                        "total_pnl": 8.0,
                        "roi_pct": 11.0,
                        "bet_count": 24,
                        "ece": 0.05,
                        "family_pnl_share_max": 0.52,
                    },
                    artifact_uri=str(artifact_a),
                ),
                ModelRun(
                    id="run-b",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    dataset_version="multitask_v1",
                    metrics_json={
                        "total_pnl": 12.0,
                        "roi_pct": 15.0,
                        "bet_count": 28,
                        "ece": 0.04,
                        "family_pnl_share_max": 0.49,
                    },
                    artifact_uri=str(artifact_b),
                ),
                ModelRunPromotion(
                    id="promotion-run-a",
                    model_run_id="run-a",
                    stage=MULTITASK_PROMOTION_STAGE,
                    status="active",
                    gate_metrics_json={"total_pnl": 8.0},
                    promoted_at=datetime(2026, 3, 28, 7, 0, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

        promotion = promote_model_run(
            session,
            model_run_id="run-b",
            stage=MULTITASK_PROMOTION_STAGE,
        )
        session.commit()

        previous = session.scalar(
            select(ModelRunPromotion).where(ModelRunPromotion.model_run_id == "run-a")
        )

        assert promotion.model_run_id == "run-b"
        assert previous is not None
        assert previous.status == "superseded"
    finally:
        session.close()


def test_promote_model_run_rejects_failed_gate(tmp_path: Path) -> None:
    session = build_session()
    try:
        artifact_dir = tmp_path / "run-fail"
        artifact_dir.mkdir(parents=True)
        session.add(
            ModelRun(
                id="run-fail",
                stage=MULTITASK_PROMOTION_STAGE,
                model_family="torch_multitask",
                model_name="shared_encoder_multitask_v2",
                dataset_version="multitask_v1",
                metrics_json={
                    "total_pnl": -1.0,
                    "roi_pct": 2.0,
                    "bet_count": 24,
                    "ece": 0.04,
                    "family_pnl_share_max": 0.49,
                },
                artifact_uri=str(artifact_dir),
            )
        )
        session.commit()

        with pytest.raises(ValueError, match="Promotion gate failed"):
            promote_model_run(
                session,
                model_run_id="run-fail",
                stage=MULTITASK_PROMOTION_STAGE,
            )
    finally:
        session.close()


def test_promote_best_model_run_selects_highest_eligible_candidate(tmp_path: Path) -> None:
    session = build_session()
    try:
        artifact_a = tmp_path / "run-a"
        artifact_b = tmp_path / "run-b"
        artifact_c = tmp_path / "run-c"
        artifact_a.mkdir(parents=True)
        artifact_b.mkdir(parents=True)
        artifact_c.mkdir(parents=True)
        base_metrics = {
            "roi_pct": 8.0,
            "bet_count": 24,
            "ece": 0.04,
            "family_pnl_share_max": 0.49,
        }
        session.add_all(
            [
                ModelRun(
                    id="run-a",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    metrics_json={**base_metrics, "total_pnl": 7.0},
                    artifact_uri=str(artifact_a),
                ),
                ModelRun(
                    id="run-b",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    metrics_json={**base_metrics, "total_pnl": 12.0},
                    artifact_uri=str(artifact_b),
                ),
                ModelRun(
                    id="run-c",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    metrics_json={**base_metrics, "total_pnl": 30.0, "bet_count": 4},
                    artifact_uri=str(artifact_c),
                ),
            ]
        )
        session.commit()

        candidates = eligible_promotion_candidates(session, stage=MULTITASK_PROMOTION_STAGE)
        promotion = promote_best_model_run(session, stage=MULTITASK_PROMOTION_STAGE)
        session.commit()

        assert [candidate[0].id for candidate in candidates] == ["run-b", "run-a"]
        assert promotion.model_run_id == "run-b"
        assert promotion.status == "active"
    finally:
        session.close()


def test_promote_live_baseline_model_run_uses_operational_gate(tmp_path: Path) -> None:
    session = build_session()
    try:
        snapshot_path = tmp_path / "miami-sq.parquet"
        rows = [
            {
                "row_id": f"row-{index}",
                "market_id": f"market-{index}",
                "token_id": f"token-{index}",
                "entry_yes_price": 0.35,
                "label_yes": None,
            }
            for index in range(12)
        ]
        pl.DataFrame(rows).write_parquet(snapshot_path)
        session.add_all(
            [
                FeatureSnapshot(
                    id="snapshot-sq",
                    market_id=None,
                    session_id=None,
                    as_of_ts=datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc),
                    snapshot_type="miami_fp1_to_sq_pole_live_snapshot",
                    feature_version="miami_fp1_to_sq_pole_live_snapshot_v1",
                    storage_path=str(snapshot_path),
                    row_count=12,
                ),
                ModelRun(
                    id="run-sq-hybrid",
                    stage=SQ_POLE_LIVE_PROMOTION_STAGE,
                    model_family="baseline",
                    model_name="hybrid",
                    dataset_version="miami_fp1_to_sq_pole_live_snapshot_v1",
                    feature_snapshot_id="snapshot-sq",
                    metrics_json={
                        "row_count": 12,
                        "bet_count": 3,
                        "average_edge": 0.11,
                    },
                    artifact_uri=str(snapshot_path),
                ),
                ModelRunPromotion(
                    id="promotion-old-sq",
                    model_run_id="run-old-sq",
                    stage=SQ_POLE_LIVE_PROMOTION_STAGE,
                    status="active",
                    gate_metrics_json={"live_baseline_gate": 1.0},
                    promoted_at=datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc),
                ),
            ]
        )
        session.add_all(
            [
                ModelPrediction(
                    id=f"prediction-{index}",
                    model_run_id="run-sq-hybrid",
                    market_id=f"market-{index}",
                    token_id=f"token-{index}",
                    as_of_ts=datetime(2026, 5, 1, 19, 5, tzinfo=timezone.utc),
                    probability_yes=0.45,
                    probability_no=0.55,
                    raw_score=0.3,
                    calibration_version="none",
                )
                for index in range(12)
            ]
        )
        session.commit()

        decision = evaluate_live_baseline_promotion_gate(
            session,
            model_run_id="run-sq-hybrid",
            stage=SQ_POLE_LIVE_PROMOTION_STAGE,
        )
        promotion = promote_live_baseline_model_run(
            session,
            model_run_id="run-sq-hybrid",
            stage=SQ_POLE_LIVE_PROMOTION_STAGE,
        )
        session.commit()

        previous = session.get(ModelRunPromotion, "promotion-old-sq")

        assert decision.eligible is True
        assert decision.actuals["row_count"] == 12.0
        assert decision.actuals["labeled_row_count"] == 0.0
        assert promotion.status == "active"
        assert promotion.gate_metrics_json is not None
        assert promotion.gate_metrics_json["promotion_gate"] == "live_baseline_v1"
        assert promotion.gate_metrics_json["model_name"] == "hybrid"
        assert previous is not None
        assert previous.status == "superseded"
    finally:
        session.close()


def test_live_baseline_promotion_gate_rejects_incomplete_predictions(
    tmp_path: Path,
) -> None:
    session = build_session()
    try:
        snapshot_path = tmp_path / "miami-sq.parquet"
        pl.DataFrame(
            [
                {
                    "row_id": f"row-{index}",
                    "market_id": f"market-{index}",
                    "token_id": f"token-{index}",
                    "entry_yes_price": 0.35,
                    "label_yes": None,
                }
                for index in range(12)
            ]
        ).write_parquet(snapshot_path)
        session.add_all(
            [
                FeatureSnapshot(
                    id="snapshot-sq",
                    market_id=None,
                    session_id=None,
                    as_of_ts=datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc),
                    snapshot_type="miami_fp1_to_sq_pole_live_snapshot",
                    feature_version="miami_fp1_to_sq_pole_live_snapshot_v1",
                    storage_path=str(snapshot_path),
                    row_count=12,
                ),
                ModelRun(
                    id="run-sq-hybrid",
                    stage=SQ_POLE_LIVE_PROMOTION_STAGE,
                    model_family="baseline",
                    model_name="hybrid",
                    dataset_version="miami_fp1_to_sq_pole_live_snapshot_v1",
                    feature_snapshot_id="snapshot-sq",
                    artifact_uri=str(snapshot_path),
                ),
            ]
        )
        session.add_all(
            [
                ModelPrediction(
                    id=f"prediction-{index}",
                    model_run_id="run-sq-hybrid",
                    market_id=f"market-{index}",
                    token_id=f"token-{index}",
                    as_of_ts=datetime(2026, 5, 1, 19, 5, tzinfo=timezone.utc),
                    probability_yes=0.45,
                    probability_no=0.55,
                    raw_score=0.3,
                    calibration_version="none",
                )
                for index in range(9)
            ]
        )
        session.commit()

        decision = evaluate_live_baseline_promotion_gate(
            session,
            model_run_id="run-sq-hybrid",
            stage=SQ_POLE_LIVE_PROMOTION_STAGE,
        )

        assert decision.eligible is False
        assert any("prediction_count=9" in rule for rule in decision.failed_rules)
        assert any(
            "prediction_count=9 must equal snapshot row_count=12" in rule
            for rule in decision.failed_rules
        )
    finally:
        session.close()


def test_score_promoted_multitask_snapshot_persists_scored_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = build_session()
    try:
        artifact_dir = tmp_path / "champion-run"
        artifact_dir.mkdir(parents=True)
        snapshot_path = tmp_path / "snapshot.parquet"
        pl.DataFrame(
            [
                {
                    "meeting_key": 1281,
                    "market_id": "market-1",
                    "token_id": "token-1",
                    "target_market_family": "pole",
                    "as_of_checkpoint": "Q",
                    "entry_yes_price": 0.44,
                    "label_yes": 1,
                }
            ]
        ).write_parquet(snapshot_path)

        session.add_all(
            [
                FeatureSnapshot(
                    id="snapshot-1",
                    market_id=None,
                    session_id=None,
                    as_of_ts=datetime(2026, 3, 28, 8, 0, tzinfo=timezone.utc),
                    snapshot_type="multitask_qr_q",
                    feature_version="multitask_v1",
                    storage_path=str(snapshot_path),
                    row_count=1,
                ),
                ModelRun(
                    id="champion-run",
                    stage=MULTITASK_PROMOTION_STAGE,
                    model_family="torch_multitask",
                    model_name="shared_encoder_multitask_v2",
                    dataset_version="multitask_v1",
                    metrics_json={
                        "total_pnl": 12.0,
                        "roi_pct": 15.0,
                        "bet_count": 28,
                        "ece": 0.04,
                        "family_pnl_share_max": 0.49,
                    },
                    artifact_uri=str(artifact_dir),
                    registry_run_id="mlflow-champion-run",
                ),
                ModelRunPromotion(
                    id="promotion-champion",
                    model_run_id="champion-run",
                    stage=MULTITASK_PROMOTION_STAGE,
                    status="active",
                    gate_metrics_json={"total_pnl": 12.0},
                    promoted_at=datetime(2026, 3, 28, 7, 50, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

        monkeypatch.setattr(
            "f1_polymarket_lab.models.score_multitask_frame",
            lambda frame, **kwargs: [
                {
                    "model_run_id": kwargs["model_run_id"],
                    "market_id": "market-1",
                    "token_id": "token-1",
                    "as_of_ts": datetime(2026, 3, 28, 8, 5, tzinfo=timezone.utc),
                    "probability_yes": 0.63,
                    "probability_no": 0.37,
                    "raw_score": 0.61,
                    "calibration_version": "isotonic_v1",
                    "explanation_json": {
                        "target_market_family": "pole",
                        "as_of_checkpoint": "Q",
                        "stage": MULTITASK_PROMOTION_STAGE,
                        "feature_snapshot_id": "snapshot-1",
                    },
                }
            ],
        )

        snapshot = session.get(FeatureSnapshot, "snapshot-1")
        assert snapshot is not None

        result = score_promoted_multitask_snapshot(
            session,
            data_root=tmp_path,
            snapshot=snapshot,
            stage=MULTITASK_PROMOTION_STAGE,
        )
        session.commit()

        scored_run = session.get(ModelRun, result["model_run_id"])
        predictions = session.scalars(
            select(ModelPrediction).where(ModelPrediction.model_run_id == result["model_run_id"])
        ).all()

        assert result["source_model_run_id"] == "champion-run"
        assert Path(result["artifact_path"]).exists()
        assert scored_run is not None
        assert scored_run.registry_run_id == "mlflow-champion-run"
        assert scored_run.config_json["parent_model_run_id"] == "champion-run"
        assert len(predictions) == 1
        assert predictions[0].probability_yes == pytest.approx(0.63)
    finally:
        session.close()
