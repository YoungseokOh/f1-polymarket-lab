from __future__ import annotations

import sqlite3
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
from f1_polymarket_lab.common import ensure_dir
from tenacity import retry, stop_after_attempt, wait_exponential


class F1DBConnector:
    github_release_url = "https://api.github.com/repos/f1db/f1db/releases/latest"
    asset_name = "f1db-sqlite.zip"
    db_filename = "f1db.db"

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = ensure_dir(cache_dir)
        self.client = httpx.Client(
            timeout=60.0,
            headers={"User-Agent": "f1-polymarket-lab/0.1.0"},
        )

    @retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5), reraise=True)
    def fetch_latest_release(self) -> dict[str, Any]:
        response = self.client.get(self.github_release_url)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("F1DB latest release payload was not an object")
        return payload

    def ensure_sqlite_path(self, *, refresh: bool = False) -> Path:
        database_path = self.cache_dir / self.db_filename
        if database_path.exists() and not refresh:
            return Path(database_path)

        release = self.fetch_latest_release()
        assets = release.get("assets") or []
        asset = next(
            (
                item
                for item in assets
                if isinstance(item, dict) and item.get("name") == self.asset_name
            ),
            None,
        )
        if asset is None:
            raise ValueError(f"F1DB release did not include asset={self.asset_name}")

        download_url = asset.get("browser_download_url")
        if not isinstance(download_url, str) or not download_url:
            raise ValueError("F1DB asset did not include a browser_download_url")

        archive_path = self.cache_dir / self.asset_name
        with self.client.stream("GET", download_url) as response:
            response.raise_for_status()
            with archive_path.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)

        with zipfile.ZipFile(archive_path) as archive:
            member = next(
                (name for name in archive.namelist() if name.endswith(self.db_filename)),
                None,
            )
            if member is None:
                raise ValueError(f"F1DB archive did not contain {self.db_filename}")
            archive.extract(member, self.cache_dir)
            extracted_path = self.cache_dir / member
            if extracted_path != database_path:
                extracted_path.replace(database_path)
        return Path(database_path)

    @contextmanager
    def sqlite_connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(str(self.ensure_sqlite_path()))
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.sqlite_connection() as connection:
            cursor = connection.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def fetch_drivers(self) -> list[dict[str, Any]]:
        return self._query(
            """
            select
                d.*,
                c.alpha2_code as nationality_alpha2_code
            from driver d
            left join country c on c.id = d.nationality_country_id
            order by d.id
            """
        )

    def fetch_constructors(self) -> list[dict[str, Any]]:
        return self._query(
            """
            select
                constructor.*,
                country.alpha2_code as country_alpha2_code
            from constructor
            left join country on country.id = constructor.country_id
            order by constructor.id
            """
        )

    def fetch_races(self, season: int) -> list[dict[str, Any]]:
        return self._query(
            """
            select
                race.*,
                grand_prix.name as grand_prix_name,
                grand_prix.full_name as grand_prix_full_name,
                grand_prix.short_name as grand_prix_short_name,
                circuit.name as circuit_name,
                circuit.full_name as circuit_full_name,
                circuit.place_name as circuit_place_name,
                country.name as country_name,
                country.alpha2_code as country_alpha2_code
            from race
            join grand_prix on grand_prix.id = race.grand_prix_id
            join circuit on circuit.id = race.circuit_id
            left join country on country.id = circuit.country_id
            where race.year = ?
            order by race.round
            """,
            (season,),
        )

    def fetch_race_data(self, season: int) -> list[dict[str, Any]]:
        return self._query(
            """
            select
                race_data.*,
                race.year as season,
                race.round as round_number
            from race_data
            join race on race.id = race_data.race_id
            where race.year = ?
            order by race.round, race_data.type, race_data.position_display_order
            """,
            (season,),
        )
