from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from f1_polymarket_lab.common import ensure_dir, payload_checksum, utc_now


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    version: str
    schema: dict[str, Any]


@dataclass(frozen=True, slots=True)
class LakeObject:
    storage_tier: str
    source: str
    dataset: str
    path: Path
    checksum: str
    record_count: int
    schema_version: str
    partition_values: dict[str, str]


DEFAULT_SILVER_SCHEMAS: dict[str, DatasetSchema] = {
    "f1_sessions": DatasetSchema(
        version="v1",
        schema={
            "id": pl.String,
            "source": pl.String,
            "session_key": pl.Int64,
            "meeting_id": pl.String,
            "session_name": pl.String,
            "session_type": pl.String,
            "session_code": pl.String,
            "date_start_utc": pl.Datetime(time_zone="UTC"),
            "date_end_utc": pl.Datetime(time_zone="UTC"),
            "status": pl.String,
            "session_order": pl.Int64,
            "is_practice": pl.Boolean,
            "raw_payload": pl.String,
        },
    ),
    "polymarket_markets": DatasetSchema(
        version="v1",
        schema={
            "id": pl.String,
            "event_id": pl.String,
            "question": pl.String,
            "slug": pl.String,
            "condition_id": pl.String,
            "question_id": pl.String,
            "taxonomy": pl.String,
            "taxonomy_confidence": pl.Float64,
            "target_session_code": pl.String,
            "driver_a": pl.String,
            "driver_b": pl.String,
            "team_name": pl.String,
            "rules_text": pl.String,
            "description": pl.String,
            "start_at_utc": pl.Datetime(time_zone="UTC"),
            "end_at_utc": pl.Datetime(time_zone="UTC"),
            "accepting_orders": pl.Boolean,
            "active": pl.Boolean,
            "closed": pl.Boolean,
            "archived": pl.Boolean,
            "enable_order_book": pl.Boolean,
            "best_bid": pl.Float64,
            "best_ask": pl.Float64,
            "spread": pl.Float64,
            "last_trade_price": pl.Float64,
            "volume": pl.Float64,
            "liquidity": pl.Float64,
            "clob_token_ids": pl.String,
            "raw_payload": pl.String,
        },
    ),
}


class LakeWriter:
    def __init__(self, root: Path, *, silver_schemas: dict[str, DatasetSchema] | None = None):
        self.root = ensure_dir(root)
        self.bronze_root = ensure_dir(self.root / "lake" / "bronze")
        self.silver_root = ensure_dir(self.root / "lake" / "silver")
        self.gold_root = ensure_dir(self.root / "lake" / "gold")
        self.manifest_root = ensure_dir(self.root / "lake" / "_manifests")
        ensure_dir(self.root / "warehouse")
        self.silver_schemas = silver_schemas or DEFAULT_SILVER_SCHEMAS

    def write_bronze(self, source: str, dataset: str, payload: Any) -> Path:
        object_ref = self.write_bronze_object(source, dataset, payload)
        return object_ref.path

    def write_bronze_object(
        self,
        source: str,
        dataset: str,
        payload: Any,
        *,
        partition: dict[str, str] | str | None = None,
        schema_version: str = "v1",
    ) -> LakeObject:
        timestamp = utc_now().strftime("%Y%m%dT%H%M%S")
        checksum = payload_checksum(payload)
        partition_values = self._normalize_partition(partition)
        target_dir = self._build_partition_dir(
            self.bronze_root / source / dataset,
            partition_values,
            include_date_partition=True,
        )
        target_path = target_dir / f"{timestamp}_{checksum[:12]}.json"
        serialized = json.dumps(payload, default=str)
        target_path.write_text(serialized, encoding="utf-8")
        object_ref = LakeObject(
            storage_tier="bronze",
            source=source,
            dataset=dataset,
            path=target_path,
            checksum=checksum,
            record_count=len(payload) if isinstance(payload, list) else 1,
            schema_version=schema_version,
            partition_values=partition_values,
        )
        self._write_manifest(object_ref)
        return object_ref

    def write_silver(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        partition: str | None = None,
    ) -> Path | None:
        object_ref = self.write_silver_object(dataset, records, partition=partition)
        return None if object_ref is None else object_ref.path

    def write_silver_object(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        *,
        partition: dict[str, str] | str | None = None,
        schema_version: str | None = None,
    ) -> LakeObject | None:
        if not records:
            return None

        spec = self.silver_schemas.get(dataset)
        version = schema_version or (spec.version if spec is not None else "v1")
        partition_values = self._normalize_partition(partition)
        target_dir = self._build_partition_dir(
            self.silver_root / dataset,
            partition_values,
            include_date_partition=False,
        )
        target_path = target_dir / f"{utc_now().strftime('%Y%m%dT%H%M%S')}.parquet"

        normalized_records = [self._normalize_record(record) for record in records]
        if spec is not None:
            frame = pl.DataFrame(
                normalized_records,
                schema=spec.schema,
                strict=False,
                infer_schema_length=None,
            )
        else:
            frame = pl.DataFrame(normalized_records, infer_schema_length=None)
        frame.write_parquet(target_path)

        object_ref = LakeObject(
            storage_tier="silver",
            source="normalized",
            dataset=dataset,
            path=target_path,
            checksum=payload_checksum(normalized_records),
            record_count=len(normalized_records),
            schema_version=version,
            partition_values=partition_values,
        )
        self._write_manifest(object_ref)
        return object_ref

    def _normalize_partition(self, partition: dict[str, str] | str | None) -> dict[str, str]:
        if partition is None:
            return {}
        if isinstance(partition, str):
            return {"partition": partition}
        return {str(key): str(value) for key, value in partition.items()}

    def _build_partition_dir(
        self,
        root: Path,
        partition_values: dict[str, str],
        *,
        include_date_partition: bool,
    ) -> Path:
        target_dir: Path = ensure_dir(root)
        for key, value in partition_values.items():
            target_dir = ensure_dir(target_dir / f"{key}={value}")
        if include_date_partition:
            target_dir = ensure_dir(target_dir / f"dt={utc_now().date().isoformat()}")
        return Path(target_dir)

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):
                normalized[key] = json.dumps(value, default=str, sort_keys=True)
            else:
                normalized[key] = value
        return normalized

    def _write_manifest(self, object_ref: LakeObject) -> None:
        manifest_dir = ensure_dir(
            self.manifest_root / object_ref.storage_tier / object_ref.source / object_ref.dataset
        )
        manifest_path = manifest_dir / f"{object_ref.path.stem}.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "storage_tier": object_ref.storage_tier,
                    "source": object_ref.source,
                    "dataset": object_ref.dataset,
                    "path": str(object_ref.path),
                    "checksum": object_ref.checksum,
                    "record_count": object_ref.record_count,
                    "schema_version": object_ref.schema_version,
                    "partition_values": object_ref.partition_values,
                    "created_at": utc_now().isoformat(),
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
