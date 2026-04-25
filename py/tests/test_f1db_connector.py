from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Literal

import pytest
from f1_polymarket_lab.connectors.f1db import F1DBConnector


class _FakeStreamResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self) -> Iterator[bytes]:
        yield self._payload


class _FakeClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def stream(self, method: str, url: str, **kwargs: object) -> _FakeStreamResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return _FakeStreamResponse(self.payload)


def _build_archive_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("nested/f1db.db", b"sqlite-fixture")
    return buffer.getvalue()


def test_ensure_sqlite_path_follows_redirects_for_release_assets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = F1DBConnector(tmp_path)
    connector.client = _FakeClient(_build_archive_bytes())
    monkeypatch.setattr(
        connector,
        "fetch_latest_release",
        lambda: {
            "assets": [
                {
                    "name": connector.asset_name,
                    "browser_download_url": "https://example.com/f1db-sqlite.zip",
                }
            ]
        },
    )

    database_path = connector.ensure_sqlite_path()

    assert database_path == tmp_path / "f1db.db"
    assert database_path.read_bytes() == b"sqlite-fixture"
    assert connector.client.calls == [
        {
            "method": "GET",
            "url": "https://example.com/f1db-sqlite.zip",
            "follow_redirects": True,
        }
    ]
