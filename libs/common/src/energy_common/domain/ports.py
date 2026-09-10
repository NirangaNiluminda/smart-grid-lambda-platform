"""Ports — the interfaces the application layer depends on.

These are `typing.Protocol` classes (structural typing): an adapter satisfies a
port simply by having the right methods, so infrastructure code never needs to
import this module. The application layer and tests, however, type against these.

Rule of thumb: if a class here grows a method that only one caller needs, split
the port. Keep them small.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from energy_common.domain.models import Alert, MeterReading


@runtime_checkable
class Clock(Protocol):
    """Abstraction over 'what time is it', so business logic is testable.

    Implemented by :class:`energy_common.simclock.SimClock` in production and by
    trivial fakes in tests.
    """

    def now_wall(self) -> dt.datetime:
        """Real wall-clock time (UTC)."""

    def now_sim(self) -> dt.datetime:
        """Current *simulated* time (UTC)."""


@runtime_checkable
class ReadingPublisher(Protocol):
    """Outbound port: hand a :class:`MeterReading` to the messaging layer."""

    def publish(self, reading: MeterReading) -> None: ...

    def flush(self, timeout_s: float = 10.0) -> None:
        """Block until buffered records are acknowledged."""


@runtime_checkable
class RawMessage(Protocol):
    """A message as delivered by the bus, before any parsing."""

    @property
    def key(self) -> bytes | None: ...
    @property
    def value(self) -> bytes | None: ...
    @property
    def partition(self) -> int: ...
    @property
    def offset(self) -> int: ...


@runtime_checkable
class ReadingConsumer(Protocol):
    """Inbound port: pull raw messages from the bus.

    Yields raw messages (not parsed models) so the consumer owns the parse/DLQ
    decision. ``commit`` advances the group offset after a batch is durably handled.
    """

    def poll(self, timeout_s: float = 1.0) -> Iterator[RawMessage]: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class DeadLetterSink(Protocol):
    """Outbound port: park a message that could not be processed."""

    def send(
        self,
        *,
        raw: bytes | None,
        reason: str,
        error: str,
        source_partition: int,
        source_offset: int,
    ) -> None: ...


@runtime_checkable
class ObjectStore(Protocol):
    """Outbound port: append rows to a partitioned, immutable dataset (the lake)."""

    def append_rows(self, dataset: str, partition: str, rows: list[dict[str, object]]) -> int:
        """Append ``rows`` to ``dataset`` under ``partition``; return count written."""

    def mark_complete(self, dataset: str, partition: str) -> None:
        """Write the ``_SUCCESS`` marker for a partition (idempotent)."""

    def partition_is_complete(self, dataset: str, partition: str) -> bool: ...


@runtime_checkable
class LandingZone(Protocol):
    """Outbound port: drop a daily reference file where the batch layer can sense it."""

    def write_csv(self, name: str, header: list[str], rows: list[list[object]]) -> None: ...

    def write_success_marker(self, business_date: dt.date) -> None: ...


@runtime_checkable
class Repository(Protocol):
    """Marker port for a persistence gateway. Concrete repositories add their own
    query methods; the application layer depends on those narrower interfaces."""

    def healthcheck(self) -> bool: ...


@runtime_checkable
class AlertSink(Protocol):
    """Observer endpoint for :class:`Alert` fan-out (DB / log / webhook)."""

    def deliver(self, alert: Alert) -> None: ...
