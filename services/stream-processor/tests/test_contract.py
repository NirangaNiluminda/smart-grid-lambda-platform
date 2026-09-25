"""Executable spec for stream-processor (Part 2 §B1)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import NamedTuple

from energy_common.domain.models import MeterReading, ZoneMetric
from energy_common.domain.ports import DeadLetterSink, ReadingConsumer
from energy_common.domain.transforms import renewable_pct
from stream_processor.application.pipeline import StreamPipeline
from stream_processor.domain.aggregates import StreamAggregator


class FakeRawMessage(NamedTuple):
    key: bytes | None
    value: bytes | None
    partition: int
    offset: int


class FakeDeadLetterSink(DeadLetterSink):
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def send(
        self,
        *,
        raw: bytes | None,
        reason: str,
        error: str,
        source_partition: int,
        source_offset: int,
    ) -> None:
        self.sent.append(
            {
                "raw": raw,
                "reason": reason,
                "error": error,
                "partition": source_partition,
                "offset": source_offset,
            }
        )


@dataclass
class InMemoryZoneMetricsRepository:
    def __init__(self) -> None:
        self.storage: dict[tuple[str, str, dt.datetime], ZoneMetric] = {}

    def upsert_metrics(self, metrics: list[ZoneMetric]) -> int:
        for m in metrics:
            self.storage[(m.grid_zone, m.window_type, m.window_start)] = m
        return len(metrics)


class FakeConsumer(ReadingConsumer):
    def __init__(self, messages: list[FakeRawMessage]) -> None:
        self._messages = messages

    def poll(self, timeout_s: float = 1.0):
        yield from self._messages

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


def _sample_reading(
    *,
    event_id: str = "evt-1",
    meter_id: str = "MTR-001",
    grid_zone: str = "zone-north",
    consumption: float = 1.5,
    solar: float = 0.5,
    ts: dt.datetime | None = None,
) -> MeterReading:
    if ts is None:
        ts = dt.datetime(2026, 1, 1, 12, 0, 15, tzinfo=dt.UTC)
    return MeterReading(
        event_id=event_id,
        meter_id=meter_id,
        household_id="HH-001",
        grid_zone=grid_zone,
        power_consumption_kwh=consumption,
        solar_generation_kwh=solar,
        reading_interval_seconds=2,
        event_timestamp=ts,
    )


def test_valid_reading_contributes_to_its_zone_window() -> None:
    """A valid MeterReading updates total_consumption_kwh for its grid_zone in the
    tumbling 1-minute window covering its event_timestamp."""
    aggregator = StreamAggregator()
    reading = _sample_reading(
        consumption=1.5,
        solar=0.5,
        ts=dt.datetime(2026, 1, 1, 12, 0, 15, tzinfo=dt.UTC),
    )
    res = aggregator.process_batch([reading])

    tumbling_metric = next(
        m for m in res.metrics if m.window_type == "tumbling_1m" and m.grid_zone == "zone-north"
    )
    assert tumbling_metric.window_start == dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    assert tumbling_metric.window_end == dt.datetime(2026, 1, 1, 12, 1, 0, tzinfo=dt.UTC)
    assert tumbling_metric.total_consumption_kwh == 1.5
    assert tumbling_metric.total_solar_kwh == 0.5


def test_negative_reading_is_routed_to_dlq_not_aggregated() -> None:
    """validate_reading() rejects it -> meter.readings.dlq, not counted."""
    bad_reading = _sample_reading(consumption=-0.5, solar=0.2)
    fake_msg = FakeRawMessage(
        key=b"zone-north",
        value=bad_reading.model_dump_json().encode("utf-8"),
        partition=0,
        offset=10,
    )

    repo = InMemoryZoneMetricsRepository()
    dlq = FakeDeadLetterSink()
    aggregator = StreamAggregator()
    pipeline = StreamPipeline(
        consumer=FakeConsumer([fake_msg]),
        repository=repo,  # type: ignore[arg-type]
        dlq=dlq,
        aggregator=aggregator,
    )

    pipeline.process_raw_batch([fake_msg])

    assert len(dlq.sent) == 1
    assert dlq.sent[0]["reason"] == "negative_consumption"
    assert len(repo.storage) == 0


def test_renewable_pct_matches_shared_transform() -> None:
    """renewable_pct in rt_zone_metrics equals
    energy_common.domain.transforms.renewable_pct(consumption, solar)."""
    aggregator = StreamAggregator()
    reading = _sample_reading(consumption=10.0, solar=3.0)
    res = aggregator.process_batch([reading])

    expected_pct = renewable_pct(10.0, 3.0)
    for m in res.metrics:
        assert m.renewable_pct == expected_pct


def test_upsert_is_idempotent_on_window_key() -> None:
    """Re-processing the same micro-batch does not double-count
    (grid_zone, window_type, window_start)."""
    repo = InMemoryZoneMetricsRepository()
    reading = _sample_reading(consumption=2.0, solar=1.0)
    aggregator = StreamAggregator()

    res1 = aggregator.process_batch([reading])
    repo.upsert_metrics(res1.metrics)
    count1 = len(repo.storage)

    # Re-process identical reading batch
    res2 = aggregator.process_batch([reading])
    repo.upsert_metrics(res2.metrics)
    count2 = len(repo.storage)

    assert count1 == count2
    key = ("zone-north", "tumbling_1m", dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC))
    assert repo.storage[key].total_consumption_kwh == 2.0


def test_active_meter_count_is_distinct_meters_in_window() -> None:
    """Distinct meters in the window are counted towards active_meter_count."""
    aggregator = StreamAggregator()
    ts = dt.datetime(2026, 1, 1, 12, 0, 10, tzinfo=dt.UTC)
    readings = [
        _sample_reading(event_id="e1", meter_id="MTR-001", ts=ts),
        _sample_reading(event_id="e2", meter_id="MTR-001", ts=ts),
        _sample_reading(event_id="e3", meter_id="MTR-002", ts=ts),
    ]
    res = aggregator.process_batch(readings)
    metric = next(m for m in res.metrics if m.window_type == "tumbling_1m")
    assert metric.active_meter_count == 2


def test_late_event_past_watermark_is_dropped_from_live_view() -> None:
    """Speed layer is allowed to be approximate; the batch layer accounts for it."""
    aggregator = StreamAggregator(watermark_minutes=2)
    new_event = _sample_reading(
        event_id="new",
        ts=dt.datetime(2026, 1, 1, 12, 10, 0, tzinfo=dt.UTC),
    )
    late_event = _sample_reading(
        event_id="late",
        ts=dt.datetime(2026, 1, 1, 12, 5, 0, tzinfo=dt.UTC),
    )

    # First event establishes watermark at 12:10:00 - 2m = 12:08:00
    aggregator.process_batch([new_event])

    # Late event is at 12:05:00 (< 12:08:00) -> dropped
    res = aggregator.process_batch([late_event])
    assert res.dropped_late_count == 1
    assert len(res.metrics) == 0
