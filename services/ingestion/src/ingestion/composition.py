"""Composition root for ingestion."""

from __future__ import annotations

from energy_common import SimClock
from energy_common.messaging import build_consumer, build_producer
from energy_common.messaging.kafka_adapters import KafkaDeadLetterSink, KafkaReadingConsumer
from energy_common.storage import ParquetObjectStore

from ingestion.application.ingest_service import IngestService
from ingestion.config import Settings
from ingestion.domain.partition_buffer import PartitionBuffer


def build_service(settings: Settings) -> IngestService:
    clock = SimClock(
        sim_day_seconds=settings.sim_day_seconds,
        sim_start_date=settings.sim_start_date,
        sim_epoch_wallclock=settings.sim_epoch_wallclock,
    )
    raw_consumer = build_consumer(
        settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_ingestion,
        topics=[settings.kafka_readings_topic],
    )
    dlq_producer = build_producer(
        settings.kafka_bootstrap_servers, client_id=f"{settings.service_name}-dlq"
    )
    return IngestService(
        consumer=KafkaReadingConsumer(raw_consumer),
        lake=ParquetObjectStore(settings.lake_root),
        dlq=KafkaDeadLetterSink(dlq_producer, settings.kafka_readings_dlq_topic),
        clock=clock,
        buffer=PartitionBuffer(
            max_rows=settings.flush_max_rows,
            max_age_seconds=settings.flush_max_age_seconds,
        ),
    )
