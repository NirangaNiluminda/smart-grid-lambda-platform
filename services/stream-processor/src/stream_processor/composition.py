"""Composition root for stream-processor."""

from __future__ import annotations

from energy_common.storage.postgres import pg_engine

from stream_processor.adapters.repository import PostgresZoneMetricsRepository
from stream_processor.adapters.spark_session import build_spark_session
from stream_processor.application.pipeline import StreamPipeline
from stream_processor.config import Settings


def build_pipeline(settings: Settings) -> StreamPipeline:
    spark = build_spark_session(settings)
    repository = PostgresZoneMetricsRepository(pg_engine(settings.pg_dsn))
    return StreamPipeline(spark, settings, repository)
