"""stream-processor configuration (ready for Part 2 to use)."""

from __future__ import annotations

from pathlib import Path

from energy_common.config import BaseServiceSettings
from pydantic import Field


class Settings(BaseServiceSettings):
    service_name: str = "stream-processor"
    metrics_port: int = 9111

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_readings_topic: str = "meter.readings"
    kafka_readings_dlq_topic: str = "meter.readings.dlq"
    kafka_consumer_group_stream: str = "stream-processor-speed-layer"

    pg_dsn: str = "postgresql://energy:energy@postgres:5432/energy_serving"

    checkpoint_dir: Path = Path("/data/checkpoints/stream-processor")
    watermark_minutes: int = Field(default=2, gt=0)
    tumbling_window_seconds: int = 60
    sliding_window_seconds: int = 300
    sliding_slide_seconds: int = 60


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
