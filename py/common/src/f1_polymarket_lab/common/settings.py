from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_user: str = "postgres"
    postgres_password: str = ""
    postgres_db: str = "f1_polymarket_lab"
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    redis_url: str = "redis://127.0.0.1:6379/0"
    mlflow_tracking_uri: str = "http://127.0.0.1:5001"
    data_root: Path = Path("data")
    duckdb_path: Path = Path("data/warehouse/lab.duckdb")
    openf1_username: str | None = None
    openf1_password: str | None = None
    next_public_api_base_url: str = "http://127.0.0.1:8000"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url(self) -> str:
        auth = self.postgres_user
        if self.postgres_password:
            auth = f"{auth}:{self.postgres_password}"
        return (
            f"postgresql+psycopg://{auth}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def bronze_root(self) -> Path:
        return self.data_root / "lake" / "bronze"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def silver_root(self) -> Path:
        return self.data_root / "lake" / "silver"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gold_root(self) -> Path:
        return self.data_root / "lake" / "gold"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
