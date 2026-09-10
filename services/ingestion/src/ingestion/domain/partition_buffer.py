"""In-memory staging for the Parquet master dataset.

Rows are grouped by ``business_date`` (the partition key). The buffer decides
*when* to flush a partition (size or age) but never does I/O — the application
layer owns the ``ObjectStore``. Keeping this logic pure makes the partition
lifecycle (open → flush → close) straightforward to unit-test.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

Row = dict[str, object]


@dataclass
class _Partition:
    rows: list[Row] = field(default_factory=list)
    first_added_wall: dt.datetime | None = None
    total_flushed: int = 0
    closed: bool = False


class PartitionBuffer:
    def __init__(self, *, max_rows: int = 500, max_age_seconds: float = 30.0) -> None:
        self._max_rows = max_rows
        self._max_age = max_age_seconds
        self._partitions: dict[dt.date, _Partition] = {}

    # -- writes -------------------------------------------------------
    def add(self, business_date: dt.date, row: Row, *, now_wall: dt.datetime) -> None:
        part = self._partitions.setdefault(business_date, _Partition())
        if part.first_added_wall is None:
            part.first_added_wall = now_wall
        part.rows.append(row)

    def mark_closed(self, business_date: dt.date) -> None:
        self._partitions.setdefault(business_date, _Partition()).closed = True

    # -- flush decisions -------------------------------------------
    def should_flush(self, business_date: dt.date, *, now_wall: dt.datetime) -> bool:
        part = self._partitions.get(business_date)
        if not part or not part.rows:
            return False
        if len(part.rows) >= self._max_rows:
            return True
        age = (now_wall - (part.first_added_wall or now_wall)).total_seconds()
        return age >= self._max_age

    def pending_partitions(self) -> list[dt.date]:
        return [d for d, p in self._partitions.items() if p.rows]

    def all_partitions(self) -> list[dt.date]:
        return list(self._partitions)

    def is_known_and_closed(self, business_date: dt.date) -> bool:
        part = self._partitions.get(business_date)
        return bool(part and part.closed)

    # -- drain -------------------------------------------------------
    def drain(self, business_date: dt.date) -> list[Row]:
        """Return and clear the buffered rows for a partition."""
        part = self._partitions.get(business_date)
        if not part or not part.rows:
            return []
        rows, part.rows = part.rows, []
        part.first_added_wall = None
        part.total_flushed += len(rows)
        return rows

    def flushed_count(self, business_date: dt.date) -> int:
        part = self._partitions.get(business_date)
        return part.total_flushed if part else 0
