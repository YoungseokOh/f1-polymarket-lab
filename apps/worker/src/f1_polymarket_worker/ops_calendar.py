from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from f1_polymarket_lab.common import slugify, utc_now
from f1_polymarket_lab.storage.models import F1CalendarOverride, F1Meeting, F1Session
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from f1_polymarket_worker.gp_registry import GP_REGISTRY, GPConfig

CALENDAR_STATUS_SCHEDULED = "scheduled"
CALENDAR_STATUS_CANCELLED = "cancelled"
CALENDAR_STATUS_POSTPONED = "postponed"
VALID_CALENDAR_STATUSES = frozenset(
    {
        CALENDAR_STATUS_SCHEDULED,
        CALENDAR_STATUS_CANCELLED,
        CALENDAR_STATUS_POSTPONED,
    }
)

SPRINT_EVENT_FORMATS = frozenset(
    {
        "sprint",
        "sprint_shootout",
        "sprint_qualifying",
    }
)
SPRINT_SESSION_CODES = frozenset({"FP1", "SQ", "S", "Q", "R"})
CONVENTIONAL_SESSION_CODES = frozenset({"FP1", "FP2", "FP3", "Q", "R"})

COUNTRY_FALLBACK_MEETING_NAMES = {
    "saudi arabia": "Saudi Arabian Grand Prix",
}

OPS_STAGE_TEMPLATE_ORDER = {
    "fp1_sq": 1,
    "sq_sprint": 2,
    "fp1_q": 3,
    "q_r": 4,
}

OPS_STAGE_TEMPLATES: dict[str, dict[str, Any]] = {
    "fp1_sq": {
        "target_session_code": "SQ",
        "source_session_code": "FP1",
        "variant": "fp1_to_sq",
        "market_taxonomy": "driver_pole_position",
        "required_model_stage": "sq_pole_live_v1",
        "baseline_stage_factory": lambda ops_slug: "sq_pole_live_v1",
        "snapshot_type_factory": lambda ops_slug: f"{ops_slug}_fp1_to_sq_pole_live_snapshot",
        "snapshot_dataset_factory": lambda ops_slug: f"{ops_slug}_fp1_to_sq_pole_live_snapshot",
        "report_slug_factory": lambda season, meeting_slug: f"{season}-{meeting_slug}-sq-pole-live",
        "title_suffix": "SQ Pole Live",
        "notes": (
            "Dynamic ops stage for FP1 -> Sprint Qualifying pole markets.",
            "Used for operator tickets and manual execution support.",
        ),
        "baseline_names": ("market_implied", "fp1_pace", "hybrid"),
    },
    "sq_sprint": {
        "target_session_code": "S",
        "source_session_code": "SQ",
        "variant": "sq_to_sprint",
        "market_taxonomy": "sprint_winner",
        "required_model_stage": "sprint_winner_live_v1",
        "baseline_stage_factory": lambda ops_slug: "sprint_winner_live_v1",
        "snapshot_type_factory": lambda ops_slug: f"{ops_slug}_sq_to_sprint_winner_live_snapshot",
        "snapshot_dataset_factory": lambda ops_slug: (
            f"{ops_slug}_sq_to_sprint_winner_live_snapshot"
        ),
        "report_slug_factory": lambda season, meeting_slug: (
            f"{season}-{meeting_slug}-sprint-winner-live"
        ),
        "title_suffix": "Sprint Winner Live",
        "notes": (
            "Dynamic ops stage for Sprint Qualifying -> Sprint winner markets.",
            "Used for operator tickets and manual execution support.",
        ),
        "baseline_names": ("market_implied", "sq_pace", "hybrid"),
    },
    "fp1_q": {
        "target_session_code": "Q",
        "source_session_code": "FP1",
        "variant": "fp1_to_q",
        "market_taxonomy": "driver_pole_position",
        "required_model_stage": "multitask_qr",
        "baseline_stage_factory": lambda ops_slug: f"{ops_slug}_q_pole_live",
        "snapshot_type_factory": lambda ops_slug: f"{ops_slug}_fp1_to_q_pole_live_snapshot",
        "snapshot_dataset_factory": lambda ops_slug: f"{ops_slug}_fp1_to_q_pole_live_snapshot",
        "report_slug_factory": lambda season, meeting_slug: f"{season}-{meeting_slug}-q-pole-live",
        "title_suffix": "Q Pole Live",
        "notes": (
            "Dynamic ops stage for FP1 -> Qualifying pole markets.",
            "Live scoring uses the promoted multitask_qr champion.",
        ),
        "baseline_names": ("market_implied", "fp1_pace", "hybrid"),
    },
    "q_r": {
        "target_session_code": "R",
        "source_session_code": "Q",
        "variant": "q_to_race",
        "market_taxonomy": "race_winner",
        "required_model_stage": "multitask_qr",
        "baseline_stage_factory": lambda ops_slug: f"{ops_slug}_race_winner_live",
        "snapshot_type_factory": lambda ops_slug: f"{ops_slug}_q_to_race_winner_live_snapshot",
        "snapshot_dataset_factory": lambda ops_slug: f"{ops_slug}_q_to_race_winner_live_snapshot",
        "report_slug_factory": lambda season, meeting_slug: (
            f"{season}-{meeting_slug}-race-winner-live"
        ),
        "title_suffix": "Race Winner Live",
        "notes": (
            "Dynamic ops stage for Qualifying -> Race winner markets.",
            "Live scoring uses the promoted multitask_qr champion.",
        ),
        "baseline_names": ("market_implied", "pre_race_pace", "hybrid"),
    },
}


@dataclass(frozen=True, slots=True)
class EffectiveOpsMeeting:
    id: str
    meeting_key: int
    season: int
    round_number: int | None
    meeting_name: str
    meeting_slug: str
    ops_slug: str
    event_format: str | None
    country_name: str | None
    location: str | None
    start_date_utc: datetime | None
    end_date_utc: datetime | None
    status: str
    source_conflict: bool
    source_label: str | None
    source_url: str | None
    note: str | None
    override_active: bool


def normalize_event_format(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text.replace(" ", "_").replace("-", "_")


def infer_event_format_from_session_codes(session_codes: set[str]) -> str | None:
    if SPRINT_SESSION_CODES.issubset(session_codes):
        return "sprint"
    if CONVENTIONAL_SESSION_CODES.issubset(session_codes):
        return "conventional"
    return None


def infer_event_format_from_sessions(sessions: list[dict[str, Any]]) -> str | None:
    session_codes = {
        str(code)
        for code in (
            _session_code_from_name(str(item.get("session_name") or "")) for item in sessions
        )
        if code is not None
    }
    return infer_event_format_from_session_codes(session_codes)


def derive_meeting_slug(
    *,
    season: int,
    meeting_key: int,
    schedule_row: dict[str, Any] | None,
    first_session: dict[str, Any],
) -> str:
    schedule_row = schedule_row or {}
    for candidate in (
        schedule_row.get("EventName"),
        first_session.get("meeting_name"),
        _legacy_meeting_name(season=season, meeting_key=meeting_key),
    ):
        if candidate:
            return str(slugify(str(candidate)))

    country_name = str(first_session.get("country_name") or "").strip()
    if country_name:
        fallback_name = COUNTRY_FALLBACK_MEETING_NAMES.get(
            country_name.lower(),
            f"{country_name} Grand Prix",
        )
        return str(slugify(fallback_name))

    location = str(first_session.get("location") or "").strip()
    if location:
        return str(slugify(f"{location} Grand Prix"))
    return str(slugify(f"{season}-meeting-{meeting_key}"))


def set_calendar_override(
    session: Session,
    *,
    season: int,
    meeting_slug: str,
    status: str,
    ops_slug: str | None = None,
    effective_round_number: int | None = None,
    effective_start_date_utc: datetime | None = None,
    effective_end_date_utc: datetime | None = None,
    effective_meeting_name: str | None = None,
    effective_country_name: str | None = None,
    effective_location: str | None = None,
    source_label: str | None = None,
    source_url: str | None = None,
    note: str | None = None,
) -> F1CalendarOverride:
    normalized_status = str(status).strip().lower()
    if normalized_status not in VALID_CALENDAR_STATUSES:
        raise ValueError(
            "status must be one of: "
            + ", ".join(sorted(VALID_CALENDAR_STATUSES))
        )

    normalized_meeting_slug = slugify(meeting_slug)
    normalized_ops_slug = None if ops_slug is None else slugify(ops_slug)
    override = session.scalar(
        select(F1CalendarOverride).where(
            F1CalendarOverride.season == season,
            F1CalendarOverride.meeting_slug == normalized_meeting_slug,
        )
    )
    if override is None:
        override = F1CalendarOverride(
            season=season,
            meeting_slug=normalized_meeting_slug,
        )
        session.add(override)

    override.ops_slug = normalized_ops_slug
    override.status = normalized_status
    override.effective_round_number = effective_round_number
    override.effective_start_date_utc = effective_start_date_utc
    override.effective_end_date_utc = effective_end_date_utc
    override.effective_meeting_name = effective_meeting_name
    override.effective_country_name = effective_country_name
    override.effective_location = effective_location
    override.source_label = source_label
    override.source_url = source_url
    override.note = note
    override.is_active = True
    override.updated_at = utc_now()
    session.flush()
    return override


def clear_calendar_override(
    session: Session,
    *,
    season: int,
    meeting_slug: str,
) -> F1CalendarOverride:
    normalized_meeting_slug = slugify(meeting_slug)
    override = session.scalar(
        select(F1CalendarOverride).where(
            F1CalendarOverride.season == season,
            F1CalendarOverride.meeting_slug == normalized_meeting_slug,
        )
    )
    if override is None:
        raise KeyError(
            f"season={season} meeting_slug={normalized_meeting_slug} override not found"
        )
    override.is_active = False
    override.updated_at = utc_now()
    session.flush()
    return override


def resolve_ops_season(session: Session, *, now: datetime | None = None) -> int:
    target_season = (now or datetime.now(tz=timezone.utc)).year
    meeting_count = session.scalar(
        select(func.count())
        .select_from(F1Meeting)
        .where(F1Meeting.season == target_season)
    ) or 0
    if meeting_count > 0:
        return int(target_season)
    latest_loaded_season = session.scalar(select(func.max(F1Meeting.season)))
    if latest_loaded_season is None:
        raise ValueError("No F1 meetings are loaded. Run sync-f1-calendar first.")
    return int(latest_loaded_season)


def resolve_effective_ops_calendar(
    session: Session,
    *,
    season: int,
    include_cancelled: bool = False,
) -> list[EffectiveOpsMeeting]:
    meetings = session.scalars(
        select(F1Meeting).where(F1Meeting.season == season)
    ).all()
    overrides = session.scalars(
        select(F1CalendarOverride).where(
            F1CalendarOverride.season == season,
            F1CalendarOverride.is_active.is_(True),
        )
    ).all()
    override_by_slug = {row.meeting_slug: row for row in overrides}

    results: list[EffectiveOpsMeeting] = []
    for meeting in meetings:
        meeting_slug = meeting.meeting_slug or slugify(meeting.meeting_name)
        override = override_by_slug.get(meeting_slug)
        base_ops_slug = _legacy_ops_slug_for_meeting(
            season=meeting.season,
            meeting_key=meeting.meeting_key,
            fallback_slug=meeting_slug,
        )
        ops_slug = (
            override.ops_slug
            if override is not None and override.ops_slug
            else base_ops_slug
        )
        status = (
            override.status
            if override is not None and override.is_active
            else CALENDAR_STATUS_SCHEDULED
        )
        round_number = (
            override.effective_round_number
            if override is not None and override.effective_round_number is not None
            else meeting.round_number
        )
        start_date_utc = (
            override.effective_start_date_utc
            if override is not None and override.effective_start_date_utc is not None
            else meeting.start_date_utc
        )
        end_date_utc = (
            override.effective_end_date_utc
            if override is not None and override.effective_end_date_utc is not None
            else meeting.end_date_utc
        )
        meeting_name = (
            override.effective_meeting_name
            if override is not None and override.effective_meeting_name
            else meeting.meeting_name
        )
        country_name = (
            override.effective_country_name
            if override is not None and override.effective_country_name
            else meeting.country_name
        )
        location = (
            override.effective_location
            if override is not None and override.effective_location
            else meeting.location
        )
        source_conflict = False
        if override is not None and override.is_active:
            source_conflict = any(
                (
                    status != CALENDAR_STATUS_SCHEDULED,
                    round_number != meeting.round_number,
                    start_date_utc != meeting.start_date_utc,
                    end_date_utc != meeting.end_date_utc,
                    meeting_name != meeting.meeting_name,
                    country_name != meeting.country_name,
                    location != meeting.location,
                    ops_slug != base_ops_slug,
                )
            )
        payload = EffectiveOpsMeeting(
            id=meeting.id,
            meeting_key=meeting.meeting_key,
            season=meeting.season,
            round_number=round_number,
            meeting_name=meeting_name,
            meeting_slug=meeting_slug,
            ops_slug=ops_slug,
            event_format=meeting.event_format,
            country_name=country_name,
            location=location,
            start_date_utc=start_date_utc,
            end_date_utc=end_date_utc,
            status=status,
            source_conflict=source_conflict,
            source_label=None if override is None else override.source_label,
            source_url=None if override is None else override.source_url,
            note=None if override is None else override.note,
            override_active=override is not None and override.is_active,
        )
        if payload.status == CALENDAR_STATUS_CANCELLED and not include_cancelled:
            continue
        results.append(payload)

    results.sort(
        key=lambda meeting: (
            meeting.round_number is None,
            meeting.round_number if meeting.round_number is not None else 10_000,
            meeting.start_date_utc or datetime.max.replace(tzinfo=timezone.utc),
            meeting.meeting_key,
        )
    )
    return results


def list_ops_stage_configs(
    session: Session,
    *,
    season: int,
    include_cancelled: bool = False,
) -> list[tuple[EffectiveOpsMeeting, GPConfig]]:
    meetings = resolve_effective_ops_calendar(
        session,
        season=season,
        include_cancelled=include_cancelled,
    )
    meeting_ids = [meeting.id for meeting in meetings]
    session_rows = session.execute(
        select(F1Session.meeting_id, F1Session.session_code)
        .where(F1Session.meeting_id.in_(meeting_ids))
    ).all()
    session_codes_by_meeting: dict[str, set[str]] = {}
    for meeting_id, session_code in session_rows:
        if meeting_id is None or session_code is None:
            continue
        session_codes_by_meeting.setdefault(str(meeting_id), set()).add(str(session_code))

    configs: list[tuple[EffectiveOpsMeeting, GPConfig]] = []
    for meeting in meetings:
        session_codes = session_codes_by_meeting.get(meeting.id, set())
        for suffix in _stage_suffixes_for_meeting(
            event_format=meeting.event_format,
            session_codes=session_codes,
        ):
            configs.append((meeting, _build_ops_stage_config(meeting=meeting, suffix=suffix)))
    return configs


def list_ops_stage_configs_for_meeting(
    session: Session,
    *,
    season: int,
    meeting_key: int,
    include_cancelled: bool = False,
) -> list[tuple[EffectiveOpsMeeting, GPConfig]]:
    return [
        item
        for item in list_ops_stage_configs(
            session,
            season=season,
            include_cancelled=include_cancelled,
        )
        if item[0].meeting_key == meeting_key
    ]


def get_ops_stage_config(
    session: Session,
    *,
    short_code: str,
    now: datetime | None = None,
) -> tuple[EffectiveOpsMeeting, GPConfig]:
    requested = slugify(short_code).replace("-", "_")
    seasons: list[int] = []
    try:
        seasons.append(resolve_ops_season(session, now=now))
    except ValueError:
        pass
    seasons.extend(
        season
        for season in session.scalars(
            select(F1Meeting.season).distinct().order_by(F1Meeting.season.desc())
        ).all()
        if season not in seasons
    )
    for season in seasons:
        for meeting, config in list_ops_stage_configs(
            session,
            season=season,
            include_cancelled=True,
        ):
            if config.short_code != requested:
                continue
            if meeting.status == CALENDAR_STATUS_CANCELLED:
                raise ValueError(
                    f"GP stage '{short_code}' is cancelled by calendar override."
                )
            return meeting, config
    legacy_config = _legacy_config_for_short_code(requested)
    if legacy_config is not None:
        return _legacy_meeting_for_config(session, legacy_config), legacy_config
    raise KeyError(f"Unknown GP short_code: {short_code!r}")


def _build_ops_stage_config(*, meeting: EffectiveOpsMeeting, suffix: str) -> GPConfig:
    template = OPS_STAGE_TEMPLATES[suffix]
    snapshot_type = template["snapshot_type_factory"](meeting.ops_slug)
    return GPConfig(
        name=meeting.meeting_name,
        short_code=f"{meeting.ops_slug}_{suffix}",
        meeting_key=meeting.meeting_key,
        season=meeting.season,
        target_session_code=template["target_session_code"],
        snapshot_type=snapshot_type,
        snapshot_dataset=template["snapshot_dataset_factory"](meeting.ops_slug),
        baseline_stage=template["baseline_stage_factory"](meeting.ops_slug),
        baseline_names=template["baseline_names"],
        report_slug=template["report_slug_factory"](meeting.season, meeting.meeting_slug),
        title_suffix=template["title_suffix"],
        notes=template["notes"],
        variant=template["variant"],
        source_session_code=template["source_session_code"],
        market_taxonomy=template["market_taxonomy"],
        stage_rank=OPS_STAGE_TEMPLATE_ORDER[suffix],
        required_model_stage=template["required_model_stage"],
    )


def _legacy_meeting_name(*, season: int, meeting_key: int) -> str | None:
    for config in GP_REGISTRY:
        if config.season == season and config.meeting_key == meeting_key:
            return config.name
    return None


def _legacy_config_for_short_code(short_code: str) -> GPConfig | None:
    for config in GP_REGISTRY:
        if config.short_code == short_code:
            return config
    return None


def _legacy_meeting_for_config(session: Session, config: GPConfig) -> EffectiveOpsMeeting:
    for meeting in resolve_effective_ops_calendar(
        session,
        season=config.season,
        include_cancelled=True,
    ):
        if meeting.meeting_key == config.meeting_key:
            if meeting.status == CALENDAR_STATUS_CANCELLED:
                raise ValueError(
                    f"GP stage '{config.short_code}' is cancelled by calendar override."
                )
            return meeting

    meeting_slug = slugify(config.name)
    ops_slug = config.short_code.split("_", 1)[0]
    return EffectiveOpsMeeting(
        id=f"meeting:legacy:{config.season}:{config.meeting_key}",
        meeting_key=config.meeting_key,
        season=config.season,
        round_number=None,
        meeting_name=config.name,
        meeting_slug=meeting_slug,
        ops_slug=ops_slug,
        event_format=None,
        country_name=None,
        location=None,
        start_date_utc=None,
        end_date_utc=None,
        status=CALENDAR_STATUS_SCHEDULED,
        source_conflict=False,
        source_label="legacy GP registry",
        source_url=None,
        note="Synthesized from legacy GP registry because calendar rows are not loaded.",
        override_active=False,
    )


def _legacy_ops_slug_for_meeting(
    *,
    season: int,
    meeting_key: int,
    fallback_slug: str,
) -> str:
    short_codes = [
        config.short_code
        for config in GP_REGISTRY
        if (
            config.season == season
            and config.meeting_key == meeting_key
            and slugify(config.name) == fallback_slug
        )
    ]
    if not short_codes:
        return fallback_slug
    prefixes = {short_code.split("_", 1)[0] for short_code in short_codes}
    if len(prefixes) == 1:
        return next(iter(prefixes))
    shortest = min(short_codes, key=len)
    return shortest.split("_", 1)[0]


def _session_code_from_name(name: str) -> str | None:
    return {
        "Practice 1": "FP1",
        "Practice 2": "FP2",
        "Practice 3": "FP3",
        "Qualifying": "Q",
        "Sprint Qualifying": "SQ",
        "Sprint Shootout": "SQ",
        "Sprint": "S",
        "Race": "R",
    }.get(name)


def _stage_suffixes_for_meeting(
    *,
    event_format: str | None,
    session_codes: set[str],
) -> tuple[str, ...]:
    normalized_format = normalize_event_format(event_format)
    inferred_format = normalized_format or infer_event_format_from_session_codes(session_codes)
    if inferred_format in SPRINT_EVENT_FORMATS or SPRINT_SESSION_CODES.issubset(session_codes):
        return ("fp1_sq", "sq_sprint", "fp1_q", "q_r")
    return ("fp1_q", "q_r")
