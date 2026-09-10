"""Kafka adapters — concrete implementations of the messaging ports.

These are the only classes in the platform that touch ``confluent_kafka`` types
directly. Application code depends on the ports in
:mod:`energy_common.domain.ports`; a composition root swaps these in.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, Message, Producer
from energy_common.domain.errors import PublishError
from energy_common.domain.models import MeterReading
from energy_common.messaging.codec import ReadingCodec


@dataclass(frozen=True, slots=True)
class _RawMessage:
    """Plain carrier satisfying ``ports.RawMessage`` (no Kafka types leak out)."""

    key: bytes | None
    value: bytes | None
    partition: int
    offset: int


class KafkaReadingPublisher:
    """Implements :class:`energy_common.domain.ports.ReadingPublisher`."""

    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self._pending_errors: list[str] = []

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._pending_errors.append(str(err))

    def publish(self, reading: MeterReading) -> None:
        self._producer.poll(0)
        if self._pending_errors:
            errs, self._pending_errors = self._pending_errors, []
            raise PublishError(f"delivery failures: {errs}")
        self._producer.produce(
            self._topic,
            key=ReadingCodec.partition_key(reading),
            value=ReadingCodec.encode(reading),
            on_delivery=self._on_delivery,
        )

    def flush(self, timeout_s: float = 10.0) -> None:
        remaining = self._producer.flush(timeout_s)
        if remaining:
            raise PublishError(f"{remaining} messages still un-acked after {timeout_s}s")
        if self._pending_errors:
            errs, self._pending_errors = self._pending_errors, []
            raise PublishError(f"delivery failures: {errs}")


class KafkaReadingConsumer:
    """Implements :class:`energy_common.domain.ports.ReadingConsumer`."""

    def __init__(self, consumer: Consumer, *, max_batch: int = 500) -> None:
        self._consumer = consumer
        self._max_batch = max_batch

    def poll(self, timeout_s: float = 1.0) -> Iterator[_RawMessage]:
        for _ in range(self._max_batch):
            msg = self._consumer.poll(timeout_s)
            if msg is None:
                return
            err = msg.error()
            if err is not None:
                # Partition EOF is not an error we care about; re-raise anything else.
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"kafka consume error: {err}")
            raw_key = msg.key()
            raw_value = msg.value()
            partition = msg.partition()
            offset = msg.offset()
            yield _RawMessage(
                key=raw_key if isinstance(raw_key, bytes) else None,
                value=raw_value if isinstance(raw_value, bytes) else None,
                partition=partition if partition is not None else -1,
                offset=offset if offset is not None else -1,
            )

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


class KafkaDeadLetterSink:
    """Implements :class:`energy_common.domain.ports.DeadLetterSink`.

    Writes a self-describing envelope so a human (or a replay tool) can see why a
    message was parked and recover the original bytes.
    """

    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def send(
        self,
        *,
        raw: bytes | None,
        reason: str,
        error: str,
        source_partition: int,
        source_offset: int,
    ) -> None:
        from datetime import UTC, datetime

        envelope = {
            "reason": reason,
            "error": error,
            "raw": base64.b64encode(raw).decode("ascii") if raw else None,
            "received_at": datetime.now(UTC).isoformat(),
            "source_partition": source_partition,
            "source_offset": source_offset,
        }
        self._producer.produce(self._topic, value=json.dumps(envelope).encode("utf-8"))
        self._producer.poll(0)
