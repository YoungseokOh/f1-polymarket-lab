from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.features.driver_profile import (
    DEFAULT_AFFINITY_SEASON_WEIGHTS,
    DEFAULT_AFFINITY_SESSION_WEIGHTS,
    build_driver_identity_map,
    canonical_driver_identity,
    compute_driver_sector_profiles,
    compute_driver_track_affinity,
    compute_track_sector_weights,
)
from f1_polymarket_lab.storage.models import F1Driver, F1Meeting, F1Session, F1Team
from sqlalchemy import func, select, text

from f1_polymarket_worker.lineage import (
    ensure_job_definition,
    finish_job_run,
    start_job_run,
)
from f1_polymarket_worker.pipeline import PipelineContext, hydrate_f1_session

AFFINITY_RELEVANT_SESSION_CODES: tuple[str, ...] = ("FP1", "FP2", "FP3", "Q")


def _ensure_utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _meeting_sort_key(meeting: F1Meeting, *, now: datetime) -> tuple[int, float]:
    start = _ensure_utc(meeting.start_date_utc) or now
    end = _ensure_utc(meeting.end_date_utc) or start
    if start <= now <= end:
        return (0, abs((now - start).total_seconds()))
    if now < start:
        return (1, abs((start - now).total_seconds()))
    return (2, abs((now - end).total_seconds()))


def _resolve_meeting(
    ctx: PipelineContext,
    *,
    season: int,
    meeting_key: int | None,
    now: datetime,
) -> F1Meeting:
    if meeting_key is not None:
        meeting = ctx.db.scalar(
            select(F1Meeting).where(
                F1Meeting.meeting_key == meeting_key,
                F1Meeting.season == season,
            )
        )
        if meeting is None:
            raise ValueError(f"meeting_key={meeting_key} season={season} not found")
        return meeting

    meetings = list(ctx.db.scalars(select(F1Meeting).where(F1Meeting.season == season)).all())
    if not meetings:
        raise ValueError(f"No meetings found for season={season}")
    return min(meetings, key=lambda item: (_meeting_sort_key(item, now=now), item.meeting_key))


def _relevant_sessions(ctx: PipelineContext, *, meeting_id: str) -> list[F1Session]:
    return list(
        ctx.db.scalars(
            select(F1Session)
            .where(
                F1Session.meeting_id == meeting_id,
                F1Session.session_code.in_(AFFINITY_RELEVANT_SESSION_CODES),
            )
            .order_by(F1Session.date_end_utc.asc(), F1Session.session_key.asc())
        ).all()
    )


def _ended_sessions(sessions: list[F1Session], *, now: datetime) -> list[F1Session]:
    ended: list[F1Session] = []
    for session in sessions:
        end_utc = _ensure_utc(session.date_end_utc)
        if end_utc is not None and end_utc <= now:
            ended.append(session)
    return ended


def _latest_ended_session(sessions: list[F1Session], *, now: datetime) -> F1Session | None:
    ended = _ended_sessions(sessions, now=now)
    if not ended:
        return None
    return max(
        ended,
        key=lambda item: (_ensure_utc(item.date_end_utc) or now, item.session_key),
    )


def _session_lap_count(ctx: PipelineContext, *, session_id: str) -> int:
    from f1_polymarket_lab.storage.models import F1Lap

    return int(
        ctx.db.scalar(
            select(func.count()).select_from(F1Lap).where(F1Lap.session_id == session_id)
        )
        or 0
    )


def _missing_affinity_sessions(
    ctx: PipelineContext,
    *,
    sessions: list[F1Session],
    now: datetime,
) -> list[F1Session]:
    missing: list[F1Session] = []
    for session in _ended_sessions(sessions, now=now):
        if _session_lap_count(ctx, session_id=session.id) <= 0:
            missing.append(session)
    return missing


def _report_path(*, root: Path, season: int, meeting_key: int) -> Path:
    return root / "reports" / "driver_affinity" / str(season) / str(meeting_key) / "latest.json"


def _load_report(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _display_rows_by_identity(
    ctx: PipelineContext,
    *,
    season: int,
    as_of_utc: datetime,
) -> dict[str, dict[str, Any]]:
    rows = ctx.db.execute(
        select(
            F1Driver.id,
            F1Driver.driver_number,
            F1Driver.full_name,
            F1Driver.broadcast_name,
            F1Driver.team_id,
            F1Driver.country_code,
            F1Driver.headshot_url,
            F1Team.team_name,
        ).join(F1Team, F1Team.id == F1Driver.team_id, isouter=True)
    ).all()
    base_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = canonical_driver_identity(
            full_name=row.full_name,
            broadcast_name=row.broadcast_name,
            driver_id=row.id,
        )
        base_rows.setdefault(
            identity,
            {
                "display_driver_id": row.id,
                "display_name": row.full_name or row.broadcast_name or row.id,
                "display_broadcast_name": row.broadcast_name,
                "driver_number": row.driver_number,
                "team_id": row.team_id,
                "team_name": row.team_name,
                "country_code": row.country_code,
                "headshot_url": row.headshot_url,
                "season": None,
                "date_end_utc": None,
            },
        )

    activity_rows = ctx.db.execute(
        text(
            """
            SELECT d.id AS driver_id,
                   d.driver_number,
                   d.full_name,
                   d.broadcast_name,
                   d.team_id,
                   d.country_code,
                   d.headshot_url,
                   t.team_name,
                   m.season,
                   s.date_end_utc
            FROM f1_drivers d
            JOIN f1_session_results sr ON sr.driver_id = d.id
            JOIN f1_sessions s ON s.id = sr.session_id
            JOIN f1_meetings m ON m.id = s.meeting_id
            LEFT JOIN f1_teams t ON t.id = d.team_id
            WHERE s.date_end_utc IS NOT NULL
              AND s.date_end_utc <= :as_of_utc
            """
        ),
        {"as_of_utc": as_of_utc},
    ).fetchall()

    def _score(row: dict[str, Any]) -> tuple[int, int, float, int]:
        ended = _ensure_utc(row.get("date_end_utc"))
        row_season = int(row.get("season") or -1)
        return (
            1 if row_season == season else 0,
            1 if ended is not None else 0,
            ended.timestamp() if ended is not None else float("-inf"),
            int(row.get("driver_number") or -1),
        )

    selected = dict(base_rows)
    for row in activity_rows:
        row_dict = dict(row._mapping)
        identity = canonical_driver_identity(
            full_name=row_dict.get("full_name"),
            broadcast_name=row_dict.get("broadcast_name"),
            driver_id=row_dict.get("driver_id"),
        )
        candidate = {
            "display_driver_id": row_dict["driver_id"],
            "display_name": row_dict.get("full_name")
            or row_dict.get("broadcast_name")
            or row_dict["driver_id"],
            "display_broadcast_name": row_dict.get("broadcast_name"),
            "driver_number": row_dict.get("driver_number"),
            "team_id": row_dict.get("team_id"),
            "team_name": row_dict.get("team_name"),
            "country_code": row_dict.get("country_code"),
            "headshot_url": row_dict.get("headshot_url"),
            "season": row_dict.get("season"),
            "date_end_utc": row_dict.get("date_end_utc"),
        }
        existing = selected.get(identity)
        if existing is None or _score(candidate) > _score(existing):
            selected[identity] = candidate
    return selected


def build_driver_affinity_report(
    ctx: PipelineContext,
    *,
    season: int,
    meeting_key: int | None = None,
    as_of_utc: datetime | None = None,
) -> dict[str, Any]:
    now = _ensure_utc(as_of_utc) or _ensure_utc(utc_now()) or datetime.now(timezone.utc)
    meeting = _resolve_meeting(ctx, season=season, meeting_key=meeting_key, now=now)
    sessions = _relevant_sessions(ctx, meeting_id=meeting.id)
    latest_ended = _latest_ended_session(sessions, now=now)
    source_sessions = [
        session
        for session in _ended_sessions(sessions, now=now)
        if _session_lap_count(ctx, session_id=session.id) > 0
    ]

    circuit_short_name = meeting.circuit_short_name or ""
    profiles = compute_driver_sector_profiles(
        ctx.db,
        circuit_short_name=circuit_short_name,
        as_of_utc=now,
    )
    track_weights = compute_track_sector_weights(
        ctx.db,
        circuit_short_name=circuit_short_name,
        as_of_utc=now,
    )
    display_rows = _display_rows_by_identity(ctx, season=season, as_of_utc=now)
    identity_map = build_driver_identity_map(ctx.db)

    entries: list[dict[str, Any]] = []
    for driver_identity, profile in profiles.items():
        display_row = display_rows.get(driver_identity)
        affinity_score = compute_driver_track_affinity(
            driver_profile=profile,
            track_weights=track_weights,
        )
        entries.append(
            {
                "canonical_driver_key": driver_identity,
                "display_driver_id": (
                    display_row["display_driver_id"]
                    if display_row is not None
                    else next(
                        (
                            driver_id
                            for driver_id, identity in identity_map.items()
                            if identity == driver_identity
                        ),
                        None,
                    )
                ),
                "display_name": (
                    display_row["display_name"]
                    if display_row is not None
                    else driver_identity.title()
                ),
                "display_broadcast_name": (
                    None
                    if display_row is None
                    else display_row["display_broadcast_name"]
                ),
                "driver_number": None if display_row is None else display_row["driver_number"],
                "team_id": None if display_row is None else display_row["team_id"],
                "team_name": None if display_row is None else display_row["team_name"],
                "country_code": None if display_row is None else display_row["country_code"],
                "headshot_url": None if display_row is None else display_row["headshot_url"],
                "affinity_score": affinity_score,
                "s1_strength": profile.get("s1_strength", 0.0),
                "s2_strength": profile.get("s2_strength", 0.0),
                "s3_strength": profile.get("s3_strength", 0.0),
                "track_s1_fraction": track_weights["s1_fraction"],
                "track_s2_fraction": track_weights["s2_fraction"],
                "track_s3_fraction": track_weights["s3_fraction"],
                "contributing_session_count": profile.get("n_sessions", 0),
                "contributing_session_codes": profile.get("session_codes", []),
                "latest_contributing_session_code": profile.get("latest_session_code"),
                "latest_contributing_session_end_utc": (
                    None
                    if profile.get("latest_session_end_utc") is None
                    else _ensure_utc(profile["latest_session_end_utc"]).isoformat()
                ),
            }
        )

    entries.sort(
        key=lambda item: (-float(item["affinity_score"]), str(item["display_name"])),
    )
    for index, entry in enumerate(entries, start=1):
        entry["rank"] = index

    report = {
        "season": season,
        "meeting_key": meeting.meeting_key,
        "meeting": {
            "id": meeting.id,
            "meeting_key": meeting.meeting_key,
            "season": meeting.season,
            "meeting_name": meeting.meeting_name,
            "circuit_short_name": meeting.circuit_short_name,
            "country_name": meeting.country_name,
            "location": meeting.location,
            "start_date_utc": None
            if meeting.start_date_utc is None
            else _ensure_utc(meeting.start_date_utc).isoformat(),
            "end_date_utc": None
            if meeting.end_date_utc is None
            else _ensure_utc(meeting.end_date_utc).isoformat(),
        },
        "computed_at_utc": now.isoformat(),
        "as_of_utc": now.isoformat(),
        "lookback_start_season": 2024,
        "session_code_weights": DEFAULT_AFFINITY_SESSION_WEIGHTS,
        "season_weights": DEFAULT_AFFINITY_SEASON_WEIGHTS,
        "track_weights": track_weights,
        "source_session_codes_included": [session.session_code for session in source_sessions],
        "source_max_session_end_utc": (
            None
            if not source_sessions
            else max(
                _ensure_utc(session.date_end_utc) or now for session in source_sessions
            ).isoformat()
        ),
        "latest_ended_relevant_session_code": None
        if latest_ended is None
        else latest_ended.session_code,
        "latest_ended_relevant_session_end_utc": None
        if latest_ended is None or latest_ended.date_end_utc is None
        else _ensure_utc(latest_ended.date_end_utc).isoformat(),
        "entry_count": len(entries),
        "entries": entries,
    }
    return augment_driver_affinity_report(
        ctx,
        report=report,
        season=season,
        meeting_key=meeting.meeting_key,
        now=now,
    )


def augment_driver_affinity_report(
    ctx: PipelineContext,
    *,
    report: dict[str, Any],
    season: int,
    meeting_key: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    reference_now = _ensure_utc(now) or _ensure_utc(utc_now()) or datetime.now(timezone.utc)
    meeting = _resolve_meeting(ctx, season=season, meeting_key=meeting_key, now=reference_now)
    sessions = _relevant_sessions(ctx, meeting_id=meeting.id)
    latest_ended = _latest_ended_session(sessions, now=reference_now)
    latest_ended_end_utc = (
        None
        if latest_ended is None or latest_ended.date_end_utc is None
        else _ensure_utc(latest_ended.date_end_utc).isoformat()
    )
    source_max_session_end_utc = report.get("source_max_session_end_utc")
    is_fresh = latest_ended_end_utc == source_max_session_end_utc
    stale_reason = None
    if not is_fresh:
        if latest_ended is None:
            stale_reason = "No ended relevant session is available yet."
        elif source_max_session_end_utc is None:
            stale_reason = (
                f"Missing hydrated data through {latest_ended.session_code} "
                f"({latest_ended_end_utc})."
            )
        else:
            stale_reason = (
                f"Latest report includes data through {source_max_session_end_utc}, "
                f"but {latest_ended.session_code} ended at {latest_ended_end_utc}."
            )
    return {
        **report,
        "is_fresh": is_fresh,
        "stale_reason": stale_reason,
        "latest_ended_relevant_session_code": (
            None if latest_ended is None else latest_ended.session_code
        ),
        "latest_ended_relevant_session_end_utc": latest_ended_end_utc,
    }


def get_driver_affinity_report(
    ctx: PipelineContext,
    *,
    season: int = 2026,
    meeting_key: int | None = None,
) -> dict[str, Any]:
    now = _ensure_utc(utc_now()) or datetime.now(timezone.utc)
    meeting = _resolve_meeting(ctx, season=season, meeting_key=meeting_key, now=now)
    path = _report_path(
        root=ctx.settings.data_root,
        season=season,
        meeting_key=meeting.meeting_key,
    )
    report = _load_report(path)
    if report is None:
        raise FileNotFoundError(
            f"No driver affinity report found for season={season} meeting_key={meeting.meeting_key}"
        )
    return augment_driver_affinity_report(
        ctx,
        report=report,
        season=season,
        meeting_key=meeting.meeting_key,
        now=now,
    )


def refresh_driver_affinity(
    ctx: PipelineContext,
    *,
    season: int = 2026,
    meeting_key: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    now = _ensure_utc(utc_now()) or datetime.now(timezone.utc)
    meeting = _resolve_meeting(ctx, season=season, meeting_key=meeting_key, now=now)
    sessions = _relevant_sessions(ctx, meeting_id=meeting.id)
    latest_ended = _latest_ended_session(sessions, now=now)
    latest_ended_end_utc = (
        None
        if latest_ended is None or latest_ended.date_end_utc is None
        else _ensure_utc(latest_ended.date_end_utc).isoformat()
    )
    path = _report_path(root=ctx.settings.data_root, season=season, meeting_key=meeting.meeting_key)
    existing_report = _load_report(path)

    definition = ensure_job_definition(
        ctx.db,
        job_name="refresh-driver-affinity",
        source="derived",
        dataset="driver_affinity_report",
        description="Refresh the current meeting driver affinity report.",
        schedule_hint="manual",
    )
    run = start_job_run(
        ctx.db,
        definition=definition,
        execute=ctx.execute,
        planned_inputs={
            "season": season,
            "meeting_key": meeting.meeting_key,
            "force": force,
        },
    )
    if not ctx.execute:
        finish_job_run(ctx.db, run, status="planned", records_written=0)
        return {
            "action": "refresh-driver-affinity",
            "status": "planned",
            "message": "Driver affinity refresh planned.",
            "season": season,
            "meeting_key": meeting.meeting_key,
            "computed_at_utc": None,
            "source_max_session_end_utc": latest_ended_end_utc,
            "hydrated_session_keys": [],
            "report": None,
        }

    if (
        not force
        and existing_report is not None
        and existing_report.get("source_max_session_end_utc") == latest_ended_end_utc
    ):
        augmented = augment_driver_affinity_report(
            ctx,
            report=existing_report,
            season=season,
            meeting_key=meeting.meeting_key,
            now=now,
        )
        finish_job_run(
            ctx.db,
            run,
            status="completed",
            records_written=int(augmented.get("entry_count", 0)),
        )
        return {
            "action": "refresh-driver-affinity",
            "status": "skipped",
            "message": (
                "Driver affinity is already fresh for "
                f"{meeting.meeting_name} ({meeting.meeting_key})."
            ),
            "season": season,
            "meeting_key": meeting.meeting_key,
            "computed_at_utc": augmented.get("computed_at_utc"),
            "source_max_session_end_utc": augmented.get("source_max_session_end_utc"),
            "hydrated_session_keys": [],
            "report": augmented,
        }

    missing_sessions = _missing_affinity_sessions(ctx, sessions=sessions, now=now)
    if missing_sessions and (
        not ctx.settings.openf1_username or not ctx.settings.openf1_password
    ):
        finish_job_run(
            ctx.db,
            run,
            status="failed",
            records_written=0,
            error_message=(
                "OPENF1_USERNAME and OPENF1_PASSWORD are required to hydrate ended sessions "
                "before refreshing driver affinity."
            ),
        )
        augmented = (
            None
            if existing_report is None
            else augment_driver_affinity_report(
                ctx,
                report=existing_report,
                season=season,
                meeting_key=meeting.meeting_key,
                now=now,
            )
        )
        return {
            "action": "refresh-driver-affinity",
            "status": "blocked",
            "message": (
                "Driver affinity needs newer ended session data, "
                "but OpenF1 credentials are missing."
            ),
            "season": season,
            "meeting_key": meeting.meeting_key,
            "computed_at_utc": None if augmented is None else augmented.get("computed_at_utc"),
            "source_max_session_end_utc": latest_ended_end_utc,
            "hydrated_session_keys": [],
            "report": augmented,
        }

    hydrated_session_keys: list[int] = []
    for session in missing_sessions:
        hydrate_f1_session(
            ctx,
            session_key=session.session_key,
            include_extended=False,
            include_heavy=False,
        )
        hydrated_session_keys.append(session.session_key)

    report = build_driver_affinity_report(
        ctx,
        season=season,
        meeting_key=meeting.meeting_key,
        as_of_utc=now,
    )
    _write_report(path, report)
    finish_job_run(
        ctx.db,
        run,
        status="completed",
        records_written=int(report.get("entry_count", 0)),
    )
    return {
        "action": "refresh-driver-affinity",
        "status": "refreshed",
        "message": (
            f"Driver affinity refreshed for {meeting.meeting_name} ({meeting.meeting_key})."
        ),
        "season": season,
        "meeting_key": meeting.meeting_key,
        "computed_at_utc": report.get("computed_at_utc"),
        "source_max_session_end_utc": report.get("source_max_session_end_utc"),
        "hydrated_session_keys": hydrated_session_keys,
        "report": report,
    }
