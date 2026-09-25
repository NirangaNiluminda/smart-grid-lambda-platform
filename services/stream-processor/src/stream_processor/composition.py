"""Composition root for stream-processor."""

from __future__ import annotations

from energy_common.messaging import build_consumer, build_producer
from energy_common.messaging.kafka_adapters import KafkaDeadLetterSink, KafkaReadingConsumer
from energy_common.observability import serve_metrics
from energy_common.storage import pg_engine

from stream_processor.adapters.repository import PostgresZoneMetricsRepository
from stream_processor.application.pipeline import StreamPipeline
from stream_processor.config import Settings
from stream_processor.domain.aggregates import StreamAggregator


def build_pipeline(settings: Settings) -> StreamPipeline:
    serve_metrics(settings.metrics_port)

    engine = pg_engine(settings.pg_dsn)
    repo = PostgresZoneMetricsRepository(engine)

    raw_consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_stream,
        topics=[settings.kafka_readings_topic],
    )
    consumer = KafkaReadingConsumer(raw_consumer)

    dlq_producer = build_producer(
        settings.kafka_bootstrap_servers,
        client_id=f"{settings.service_name}-dlq",
    )
    dlq = KafkaDeadLetterSink(dlq_producer, settings.kafka_readings_dlq_topic)

    aggregator = StreamAggregator(
        watermark_minutes=settings.watermark_minutes,
        tumbling_window_seconds=settings.tumbling_window_seconds,
        sliding_window_seconds=settings.sliding_window_seconds,
        sliding_slide_seconds=settings.sliding_slide_seconds,
    )

    return StreamPipeline(
        consumer=consumer,
        repository=repo,
        dlq=dlq,
        aggregator=aggregator,
        service_name=settings.service_name,
    )
