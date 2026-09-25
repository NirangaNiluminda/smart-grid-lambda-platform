"""stream-processor configuration.

Spark runs in **local mode** inside this one container (`spark://` master not
required) — appropriate at this project's data volume; see
`docs/ARCHITECTURE.md`'s production-scale notes for how this would change
(a real cluster / `spark-submit --master yarn|k8s://...`) at production scale.
"""

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

    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = Field(default=4, gt=0)
    checkpoint_root: Path = Path("/data/checkpoints/stream-processor")
    trigger_interval_seconds: float = Field(default=5.0, gt=0)

    watermark_minutes: int = Field(default=2, gt=0)
    tumbling_window_seconds: int = 60
    sliding_window_seconds: int = 300
    sliding_slide_seconds: int = 60


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
