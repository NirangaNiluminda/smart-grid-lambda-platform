"""serving-api configuration (ready for Part 2 to use)."""

from __future__ import annotations

from energy_common.config import BaseServiceSettings
from pydantic import Field


class Settings(BaseServiceSettings):
    service_name: str = "serving-api"
    metrics_port: int = 8000  # served on the same port as the API

    pg_dsn: str = "postgresql://serving_ro:serving_ro@postgres:5432/energy_serving"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # /healthz fails if the newest rt_zone_metrics row is older than this (sim minutes).
    freshness_budget_minutes: int = Field(default=5, gt=0)


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
