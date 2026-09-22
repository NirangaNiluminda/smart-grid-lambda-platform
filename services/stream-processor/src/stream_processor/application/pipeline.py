"""Streaming pipeline application use-case."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from energy_common import get_logger
from energy_common.domain.models import MeterReading
from energy_common.domain.ports import DeadLetterSink, RawMessage, ReadingConsumer
from energy_common.messaging.codec import ReadingCodec
from energy_common.observability import counter, histogram

from stream_processor.domain.aggregates import StreamAggregator

if TYPE_CHECKING:
    from stream_processor.adapters.repository import PostgresZoneMetricsRepository

logger = get_logger("stream-processor")

RECORDS_CONSUMED = counter(
    "records_consumed_total",
    "Total meter reading records consumed",
    labelnames=("service", "kafka_partition"),
)
RECORDS_REJECTED = counter(
    "records_rejected_total",
    "Total meter reading records rejected",
    labelnames=("service", "reason"),
)
MICROBATCH_DURATION = histogram(
    "microbatch_duration_seconds",
    "Duration of streaming microbatch processing in seconds",
)


class StreamPipeline:
    """Orchestrates consuming meter readings, validation, window aggregation, and upserts."""

    def __init__(
        self,
        consumer: ReadingConsumer,
        repository: PostgresZoneMetricsRepository,
        dlq: DeadLetterSink,
        aggregator: StreamAggregator,
        *,
        service_name: str = "stream-processor",
    ) -> None:
        self.consumer = consumer
        self.repository = repository
        self.dlq = dlq
        self.aggregator = aggregator
        self.service_name = service_name
        self._running = False

    def process_raw_batch(self, messages: list[RawMessage]) -> int:
        start_time = time.monotonic()
        valid_readings: list[MeterReading] = []
        raw_map: dict[str, tuple[bytes | None, int, int]] = {}

        for msg in messages:
            try:
                reading = ReadingCodec.decode(msg.value)
                valid_readings.append(reading)
                raw_map[reading.event_id] = (msg.value, msg.partition, msg.offset)
                RECORDS_CONSUMED.labels(
                    service=self.service_name,
                    kafka_partition=str(msg.partition),
                ).inc()
            except Exception as exc:
                RECORDS_REJECTED.labels(
                    service=self.service_name,
                    reason="parse_error",
                ).inc()
                self.dlq.send(
                    raw=msg.value,
                    reason="parse_error",
                    error=str(exc),
                    source_partition=msg.partition,
                    source_offset=msg.offset,
                )

        if not valid_readings:
            return 0

        res = self.aggregator.process_batch(valid_readings)

        # Route business-rule rejections to DLQ
        for reading, outcome in res.rejected:
            raw, partition, offset = raw_map.get(reading.event_id, (None, -1, -1))
            reason_str = str(outcome.reason.value if outcome.reason else "validation_failure")
            RECORDS_REJECTED.labels(
                service=self.service_name,
                reason=reason_str,
            ).inc()
            self.dlq.send(
                raw=raw,
                reason=reason_str,
                error=f"validation rejected: {reason_str}",
                source_partition=partition,
                source_offset=offset,
            )

        # Upsert windowed metrics to Postgres
        if res.metrics:
            self.repository.upsert_metrics(res.metrics)
            logger.info(
                "microbatch.upserted",
                metrics_count=len(res.metrics),
                rejected_count=len(res.rejected),
                dropped_late=res.dropped_late_count,
            )

        duration = time.monotonic() - start_time
        MICROBATCH_DURATION.observe(duration)
        return len(res.metrics)

    def run(self) -> None:
        """Main streaming loop."""
        self._running = True
        logger.info("stream_processor.started")
        try:
            while self._running:
                raw_batch = list(self.consumer.poll(timeout_s=1.0))
                if raw_batch:
                    self.process_raw_batch(raw_batch)
                    self.consumer.commit()
                else:
                    time.sleep(0.1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("stream_processor.stopping")
        finally:
            self.consumer.close()
            logger.info("stream_processor.stopped")

    def stop(self) -> None:
        self._running = False
