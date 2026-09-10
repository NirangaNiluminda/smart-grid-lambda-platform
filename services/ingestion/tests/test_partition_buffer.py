from __future__ import annotations

import datetime as dt

from ingestion.domain.partition_buffer import PartitionBuffer

D1 = dt.date(2026, 1, 1)
D2 = dt.date(2026, 1, 2)
T0 = dt.datetime(2026, 6, 1, 12, 0, tzinfo=dt.UTC)


def test_groups_rows_by_business_date() -> None:
    buf = PartitionBuffer(max_rows=10)
    buf.add(D1, {"a": 1}, now_wall=T0)
    buf.add(D2, {"a": 2}, now_wall=T0)
    assert sorted(buf.pending_partitions()) == [D1, D2]


def test_flush_on_row_count() -> None:
    buf = PartitionBuffer(max_rows=3, max_age_seconds=999)
    for i in range(2):
        buf.add(D1, {"i": i}, now_wall=T0)
    assert not buf.should_flush(D1, now_wall=T0)
    buf.add(D1, {"i": 2}, now_wall=T0)
    assert buf.should_flush(D1, now_wall=T0)


def test_flush_on_age() -> None:
    buf = PartitionBuffer(max_rows=999, max_age_seconds=30)
    buf.add(D1, {"x": 1}, now_wall=T0)
    assert not buf.should_flush(D1, now_wall=T0 + dt.timedelta(seconds=10))
    assert buf.should_flush(D1, now_wall=T0 + dt.timedelta(seconds=31))


def test_drain_clears_and_counts() -> None:
    buf = PartitionBuffer()
    buf.add(D1, {"x": 1}, now_wall=T0)
    buf.add(D1, {"x": 2}, now_wall=T0)
    assert buf.drain(D1) == [{"x": 1}, {"x": 2}]
    assert buf.drain(D1) == []
    assert buf.flushed_count(D1) == 2


def test_age_resets_after_drain() -> None:
    buf = PartitionBuffer(max_rows=999, max_age_seconds=30)
    buf.add(D1, {"x": 1}, now_wall=T0)
    buf.drain(D1)
    buf.add(D1, {"x": 2}, now_wall=T0 + dt.timedelta(seconds=60))
    assert not buf.should_flush(D1, now_wall=T0 + dt.timedelta(seconds=70))


def test_closed_tracking() -> None:
    buf = PartitionBuffer()
    assert not buf.is_known_and_closed(D1)
    buf.mark_closed(D1)
    assert buf.is_known_and_closed(D1)
    assert D1 in buf.all_partitions()
