from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest
import typer
from f1_polymarket_lab.common.settings import Settings
from f1_polymarket_worker import cli
from f1_polymarket_worker.pipeline import PipelineContext


@contextmanager
def fake_db_session(_: str) -> Iterator[object]:
    yield object()


def test_sync_polymarket_catalog_command_routes_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "get_settings", lambda: Settings())
    monkeypatch.setattr(cli, "db_session", fake_db_session)

    def fake_sync_polymarket_catalog(
        context: PipelineContext,
        *,
        max_pages: int,
        batch_size: int,
        active: bool | None,
        closed: bool | None,
        archived: bool | None,
    ) -> dict[str, str]:
        captured.update(
            {
                "execute": context.execute,
                "max_pages": max_pages,
                "batch_size": batch_size,
                "active": active,
                "closed": closed,
                "archived": archived,
            }
        )
        return {"status": "completed"}

    monkeypatch.setattr(cli, "sync_polymarket_catalog", fake_sync_polymarket_catalog)

    cli.sync_polymarket_catalog_command(
        max_pages=3,
        batch_size=25,
        execute=True,
        active=True,
        closed=False,
        archived=False,
    )

    assert captured == {
        "execute": True,
        "max_pages": 3,
        "batch_size": 25,
        "active": True,
        "closed": False,
        "archived": False,
    }


def test_worker_command_fails_fast() -> None:
    with pytest.raises(typer.Exit) as exc_info:
        cli.worker()

    assert exc_info.value.exit_code == 2
