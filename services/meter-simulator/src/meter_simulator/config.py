"""meter-simulator configuration. See ``.env.example`` for the full list."""

from __future__ import annotations

from pathlib import Path

from energy_common.config import BaseServiceSettings
from pydantic import Field


class Settings(BaseServiceSettings):
    service_name: str = "meter-simulator"
    metrics_port: int = 9101

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_readings_topic: str = "meter.readings"

    sim_household_count: int = Field(default=200, gt=0)
    sim_emit_interval_seconds: float = Field(default=2.0, gt=0)
    sim_anomaly_rate: float = Field(default=0.02, ge=0.0, le=1.0)
    sim_seed: int = 42

    # Seed population. If the file is missing, a synthetic population is built.
    seed_households_csv: Path = Path("data/seed/households.csv")


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
