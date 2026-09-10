"""alert-engine configuration (ready for Part 2 to use)."""

from __future__ import annotations

from energy_common.config import BaseServiceSettings
from pydantic import Field


class Settings(BaseServiceSettings):
    service_name: str = "alert-engine"
    metrics_port: int = 9114

    pg_dsn: str = "postgresql://energy:energy@postgres:5432/energy_serving"
    prometheus_url: str = "http://prometheus:9090"

    evaluation_interval_seconds: float = Field(default=15.0, gt=0)

    alert_no_data_minutes: int = 3
    alert_low_renewable_pct: float = 15.0
    alert_max_reject_rate: float = 0.05
    alert_webhook_url: str | None = None


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
