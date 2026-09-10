from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass, field

from energy_common.domain.models import MeterReading
from energy_common.messaging import ReadingCodec
from ingestion.application.ingest_service import IngestService
from ingestion.domain.partition_buffer import PartitionBuffer


@dataclass
class FakeMsg:
    value: bytes | None
    partition: int = 0
    offset: int = 0
    key: bytes | None = None


class FakeConsumer:
    """Delivers pre-canned message batches, one batch per poll() call."""

    def __init__(self, batches: list[list[FakeMsg]]) -> None:
        self._batches = list(batches)
        self.commits = 0
        self.closed = False

    def poll(self, timeout_s: float = 1.0) -> Iterator[FakeMsg]:
        if self._batches:
            yield from self._batches.pop(0)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeLake:
    appended: dict[str, list[dict]] = field(default_factory=dict)
    completed: list[str] = field(default_factory=list)

    def append_rows(self, dataset: str, partition: str, rows: list[dict]) -> int:
        self.appended.setdefault(partition, []).extend(rows)
        return len(rows)

    def mark_complete(self, dataset: str, partition: str) -> None:
        self.completed.append(partition)

    def partition_is_complete(self, dataset: str, partition: str) -> bool:
        return partition in self.completed


@dataclass
class FakeDlq:
    parked: list[dict] = field(default_factory=list)

    def send(self, *, raw, reason, error, source_partition, source_offset) -> None:  # type: ignore[no-untyped-def]
        self.parked.append({"raw": raw, "reason": reason, "error": error})


class FixedClock:
    def __init__(self, wall: dt.datetime, sim: dt.datetime) -> None:
        self.wall = wall
        self.sim = sim

    def now_wall(self) -> dt.datetime:
        return self.wall

    def now_sim(self) -> dt.datetime:
        return self.sim


def _reading(day: int = 1, meter: str = "MTR-1") -> bytes:
    return ReadingCodec.encode(
        MeterReading(
            event_id=f"e-{meter}-{day}",
            meter_id=meter,
            household_id="HH-1",
            grid_zone="zone-north",
            power_consumption_kwh=0.04,
            solar_generation_kwh=0.0,
            event_timestamp=dt.datetime(2026, 1, day, 10, 0, tzinfo=dt.UTC),
        )
    )


def test_valid_readings_land_in_the_right_partition() -> None:
    consumer = FakeConsumer([[FakeMsg(_reading(1)), FakeMsg(_reading(1, "MTR-2"))]])
    lake = FakeLake()
    svc = IngestService(
        consumer=consumer,
        lake=lake,
        dlq=FakeDlq(),
        clock=FixedClock(
            dt.datetime(2026, 6, 1, tzinfo=dt.UTC), dt.datetime(2026, 1, 1, 10, 5, tzinfo=dt.UTC)
        ),
        buffer=PartitionBuffer(max_rows=1),  # force immediate flush
    )
    svc.run(max_polls=1)
    assert "business_date=2026-01-01" in lake.appended
    assert len(lake.appended["business_date=2026-01-01"]) == 2
    assert consumer.commits == 1
    assert consumer.closed


def test_malformed_message_is_dead_lettered() -> None:
    dlq = FakeDlq()
    consumer = FakeConsumer([[FakeMsg(b"{broken"), FakeMsg(_reading(1))]])
    lake = FakeLake()
    svc = IngestService(
        consumer=consumer,
        lake=lake,
        dlq=dlq,
        clock=FixedClock(
            dt.datetime(2026, 6, 1, tzinfo=dt.UTC), dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        ),
        buffer=PartitionBuffer(max_rows=1),
    )
    svc.run(max_polls=1)
    assert len(dlq.parked) == 1
    assert dlq.parked[0]["reason"] == "contract_violation"
    assert dlq.parked[0]["raw"] == b"{broken"
    assert len(lake.appended["business_date=2026-01-01"]) == 1


def test_partition_closed_and_marked_when_sim_clock_advances() -> None:
    consumer = FakeConsumer([[FakeMsg(_reading(1))], []])
    lake = FakeLake()
    # first poll: sim time on Jan 1; second poll: sim time Jan 2 -> Jan 1 closes
    clock = FixedClock(
        dt.datetime(2026, 6, 1, tzinfo=dt.UTC), dt.datetime(2026, 1, 1, 23, 0, tzinfo=dt.UTC)
    )
    svc = IngestService(
        consumer=consumer, lake=lake, dlq=FakeDlq(), clock=clock, buffer=PartitionBuffer()
    )
    svc.run(max_polls=1)
    assert lake.completed == []  # still open
    clock.sim = dt.datetime(2026, 1, 2, 0, 30, tzinfo=dt.UTC)
    svc.run(max_polls=1)
    assert lake.completed == ["business_date=2026-01-01"]
    assert lake.appended["business_date=2026-01-01"]  # remainder flushed


def test_late_arrival_still_written_after_close() -> None:
    lake = FakeLake()
    clock = FixedClock(
        dt.datetime(2026, 6, 1, tzinfo=dt.UTC), dt.datetime(2026, 1, 2, 1, 0, tzinfo=dt.UTC)
    )
    consumer = FakeConsumer([[FakeMsg(_reading(1))], [FakeMsg(_reading(1, "MTR-late"))]])
    svc = IngestService(
        consumer=consumer, lake=lake, dlq=FakeDlq(), clock=clock, buffer=PartitionBuffer(max_rows=1)
    )
    svc.run(max_polls=1)  # Jan 1 gets closed immediately (sim already Jan 2)
    assert lake.completed == ["business_date=2026-01-01"]
    svc.run(max_polls=1)  # late Jan 1 reading
    rows = lake.appended["business_date=2026-01-01"]
    assert any(r["meter_id"] == "MTR-late" for r in rows)


def test_no_messages_still_commits_nothing_and_closes() -> None:
    consumer = FakeConsumer([[]])
    svc = IngestService(
        consumer=consumer,
        lake=FakeLake(),
        dlq=FakeDlq(),
        clock=FixedClock(
            dt.datetime(2026, 6, 1, tzinfo=dt.UTC), dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        ),
    )
    svc.run(max_polls=2)
    assert consumer.commits == 0
    assert consumer.closed
