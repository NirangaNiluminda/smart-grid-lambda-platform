"""Ingestion use-case: Kafka ``meter.readings`` -> Parquet master dataset.

Flow per polled message:
    decode ── ok ──▶ derive business_date ─▶ buffer ─▶ (flush when full/old)
           └─ ContractViolation ─▶ dead-letter sink

Partition lifecycle: a ``business_date`` partition stays *open* while the
simulated clock is still on or before that day. When the clock advances past it,
the service flushes the remainder and writes the ``_SUCCESS`` marker. Readings
that arrive for an already-closed partition are still written (a new part file)
and counted as ``late_arrival``.
"""

from __future__ import annotations

import datetime as dt

from energy_common.domain.errors import ContractViolation
from energy_common.domain.models import MeterReading
from energy_common.domain.ports import (
    Clock,
    DeadLetterSink,
    ObjectStore,
    RawMessage,
    ReadingConsumer,
)
from energy_common.messaging import ReadingCodec
from energy_common.observability import bind_context, counter, gauge, get_logger
from energy_common.simclock import business_date_for

from ingestion.domain.partition_buffer import PartitionBuffer

_log = get_logger("ingestion")

_consumed = counter("readings_consumed_total", "Readings consumed", ("kafka_partition",))
_rejected = counter("readings_rejected_total", "Readings routed to DLQ", ("reason",))
_late = counter(
    "late_arrival_total", "Readings for an already-closed partition", ("business_date",)
)
_partition_rows = gauge(
    "lake_partition_rows", "Rows written to a lake partition", ("business_date",)
)

DATASET = "readings_raw"


class IngestService:
    def __init__(
        self,
        *,
        consumer: ReadingConsumer,
        lake: ObjectStore,
        dlq: DeadLetterSink,
        clock: Clock,
        buffer: PartitionBuffer | None = None,
    ) -> None:
        self._consumer = consumer
        self._lake = lake
        self._dlq = dlq
        self._clock = clock
        self._buffer = buffer or PartitionBuffer()
        self._seen_business_date: dt.date | None = None

    def run(self, *, max_polls: int | None = None) -> None:
        polls = 0
        try:
            while max_polls is None or polls < max_polls:
                handled = self._poll_once()
                self._roll_partitions()
                if handled:
                    self._consumer.commit()
                polls += 1
        finally:
            self._flush_all()
            self._consumer.close()

    # -- internals ---------------------------------------------------
    def _poll_once(self) -> int:
        handled = 0
        for msg in self._consumer.poll(timeout_s=1.0):
            self._handle(msg)
            handled += 1
        # opportunistic size/age-based flush
        now = self._clock.now_wall()
        for business_date in list(self._buffer.pending_partitions()):
            if self._buffer.should_flush(business_date, now_wall=now):
                self._flush(business_date)
        return handled

    def _handle(self, msg: RawMessage) -> None:
        try:
            reading = ReadingCodec.decode(msg.value)
        except ContractViolation as exc:
            self._to_dlq(msg, reason="contract_violation", error=str(exc))
            return
        self._buffer_reading(reading, msg)

    def _buffer_reading(self, reading: MeterReading, msg: RawMessage) -> None:
        business_date = business_date_for(reading.event_timestamp)
        _consumed.labels(kafka_partition=str(msg.partition)).inc()

        if self._buffer.is_known_and_closed(business_date):
            _late.labels(business_date=business_date.isoformat()).inc()
            _log.warning(
                "late.arrival",
                business_date=business_date.isoformat(),
                meter_id=reading.meter_id,
            )

        self._buffer.add(
            business_date,
            self._row(reading, msg),
            now_wall=self._clock.now_wall(),
        )

    def _roll_partitions(self) -> None:
        """Close every partition strictly before the current simulated day."""
        sim_today = self._clock.now_sim().date()
        if sim_today == self._seen_business_date:
            return
        self._seen_business_date = sim_today
        for business_date in sorted(self._buffer.pending_partitions()):
            if business_date < sim_today:
                self._close_partition(business_date)
        # a partition may have no pending rows but still need its marker
        for business_date in self._closable_without_rows(sim_today):
            self._close_partition(business_date)

    def _closable_without_rows(self, sim_today: dt.date) -> list[dt.date]:
        return [
            d
            for d in self._buffer.all_partitions()
            if d < sim_today and not self._buffer.is_known_and_closed(d)
        ]

    def _close_partition(self, business_date: dt.date) -> None:
        self._flush(business_date)
        self._buffer.mark_closed(business_date)
        self._lake.mark_complete(DATASET, _partition_path(business_date))
        _log.info(
            "partition.success_marker",
            business_date=business_date.isoformat(),
            rows=self._buffer.flushed_count(business_date),
        )

    def _flush(self, business_date: dt.date) -> None:
        rows = self._buffer.drain(business_date)
        if not rows:
            return
        written = self._lake.append_rows(DATASET, _partition_path(business_date), rows)
        _partition_rows.labels(business_date=business_date.isoformat()).set(
            self._buffer.flushed_count(business_date)
        )
        _log.info("partition.flushed", business_date=business_date.isoformat(), rows=written)

    def _flush_all(self) -> None:
        for business_date in sorted(self._buffer.pending_partitions()):
            self._flush(business_date)

    def _to_dlq(self, msg: RawMessage, *, reason: str, error: str) -> None:
        _rejected.labels(reason=reason).inc()
        self._dlq.send(
            raw=msg.value,
            reason=reason,
            error=error,
            source_partition=msg.partition,
            source_offset=msg.offset,
        )
        _log.warning("dlq.routed", reason=reason, error=error, kafka_offset=msg.offset)

    @staticmethod
    def _row(reading: MeterReading, msg: RawMessage) -> dict[str, object]:
        return {
            "schema_version": reading.schema_version,
            "event_id": reading.event_id,
            "meter_id": reading.meter_id,
            "household_id": reading.household_id,
            "grid_zone": reading.grid_zone,
            "power_consumption_kwh": reading.power_consumption_kwh,
            "solar_generation_kwh": reading.solar_generation_kwh,
            "reading_interval_seconds": reading.reading_interval_seconds,
            "event_timestamp": reading.event_timestamp,
            "ingest_timestamp": dt.datetime.now(dt.UTC),
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "business_date": business_date_for(reading.event_timestamp),
        }


def _partition_path(business_date: dt.date) -> str:
    return f"business_date={business_date.isoformat()}"


def bind_run_context() -> None:
    bind_context(component="ingestion")
