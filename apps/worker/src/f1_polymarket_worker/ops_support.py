from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.storage.models import IngestionJobRun
from sqlalchemy import select
from sqlalchemy.orm import Session


def _ensure_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def operation_report_dir(
    *,
    root: Path,
    season: int,
    meeting_key: int,
    action: str,
) -> Path:
    return root / "reports" / "operations" / str(season) / str(meeting_key) / action


def write_operation_report(
    *,
    root: Path,
    season: int,
    meeting_key: int,
    action: str,
    payload: dict[str, Any],
    job_run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str:
    timestamp = _ensure_utc(observed_at or utc_now()).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"__{job_run_id}" if job_run_id else ""
    report_dir = operation_report_dir(
        root=root,
        season=season,
        meeting_key=meeting_key,
        action=action,
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"{timestamp}{suffix}.json"
    md_path = report_dir / f"{timestamp}{suffix}.md"
    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    md_path.write_text(render_operation_report_markdown(payload), encoding="utf-8")
    return str(json_path)


def render_operation_report_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Operation Report",
        "",
        f"- Action: `{payload.get('action', 'unknown')}`",
        f"- Status: `{payload.get('status', 'unknown')}`",
        f"- Message: {payload.get('message', 'No message')}",
    ]
    if payload.get("meeting_name"):
        lines.append(f"- Meeting: {payload['meeting_name']}")
    if payload.get("gp_short_code"):
        lines.append(f"- GP config: `{payload['gp_short_code']}`")
    if payload.get("job_run_id"):
        lines.append(f"- Job run: `{payload['job_run_id']}`")

    blockers = payload.get("blockers") or []
    warnings = payload.get("warnings") or []
    if blockers:
        lines.extend(["", "## Blockers"])
        lines.extend(f"- {item}" for item in blockers)
    if warnings:
        lines.extend(["", "## Warnings"])
        lines.extend(f"- {item}" for item in warnings)

    preflight = payload.get("preflight_summary")
    if isinstance(preflight, dict):
        lines.extend(
            [
                "",
                "## Preflight",
                f"- Status: `{preflight.get('status', 'unknown')}`",
                f"- Message: {preflight.get('message', 'No message')}",
            ]
        )

    executed_steps = payload.get("executed_steps") or []
    if executed_steps:
        lines.extend(["", "## Steps"])
        for step in executed_steps:
            if not isinstance(step, dict):
                continue
            lines.append(
                f"- `{step.get('key', 'step')}` `{step.get('status', 'unknown')}`: "
                f"{step.get('detail', 'No detail')}"
            )

    details = payload.get("details")
    if isinstance(details, dict) and details:
        lines.extend(
            [
                "",
                "## Details",
                "```json",
                json.dumps(details, indent=2, sort_keys=True, default=str),
                "```",
            ]
        )

    return "\n".join(lines) + "\n"


def latest_job_run_for_name(db: Session, *, job_name: str) -> IngestionJobRun | None:
    return db.scalar(
        select(IngestionJobRun)
        .where(IngestionJobRun.job_name == job_name)
        .order_by(IngestionJobRun.started_at.desc(), IngestionJobRun.id.desc())
    )


def job_run_summary(run: IngestionJobRun | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "job_name": run.job_name,
        "status": run.status,
        "records_written": run.records_written,
        "started_at": None if run.started_at is None else _ensure_utc(run.started_at).isoformat(),
        "finished_at": None
        if run.finished_at is None
        else _ensure_utc(run.finished_at).isoformat(),
        "error_message": run.error_message,
    }


def latest_operation_report_path(
    *,
    root: Path,
    season: int,
    meeting_key: int,
    action: str,
    job_run_id: str | None = None,
) -> str | None:
    report_dir = operation_report_dir(
        root=root,
        season=season,
        meeting_key=meeting_key,
        action=action,
    )
    if not report_dir.exists():
        return None
    if job_run_id:
        matches = sorted(report_dir.glob(f"*__{job_run_id}.json"))
        if matches:
            return str(matches[-1])
    reports = sorted(report_dir.glob("*.json"))
    if not reports:
        return None
    return str(reports[-1])
