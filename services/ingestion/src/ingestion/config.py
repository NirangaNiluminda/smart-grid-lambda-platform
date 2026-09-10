"""ingestion configuration."""

from __future__ import annotations

from pathlib import Path

from energy_common.config import BaseServiceSettings
from pydantic import Field


class Settings(BaseServiceSettings):
    service_name: str = "ingestion"
    metrics_port: int = 9102

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_readings_topic: str = "meter.readings"
    kafka_readings_dlq_topic: str = "meter.readings.dlq"
    kafka_consumer_group_ingestion: str = "ingestion-raw-lake"

    lake_root: Path = Path("/data/lake")

    flush_max_rows: int = Field(default=500, gt=0)
    flush_max_age_seconds: float = Field(default=30.0, gt=0)


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
