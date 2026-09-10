"""Factory functions that build configured ``confluent_kafka`` clients.

One construction path for every service so producer/consumer tuning is consistent
and testable. ``confluent_kafka`` is an optional dependency (``energy-common[kafka]``);
importing this module without it raises a clear error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from confluent_kafka import Consumer, Producer
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "confluent-kafka is not installed. Add the 'kafka' extra: "
        "pip install 'energy-common[kafka]'"
    ) from exc

if TYPE_CHECKING:
    pass


def build_producer(bootstrap_servers: str, *, client_id: str, **overrides: Any) -> Producer:
    """A durable producer: waits for all in-sync replicas, retries, idempotent."""
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "linger.ms": 20,
        "compression.type": "zstd",
    }
    conf.update(overrides)
    return Producer(conf)


def build_consumer(
    bootstrap_servers: str,
    *,
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
    **overrides: Any,
) -> Consumer:
    """A manual-commit consumer (the app commits after a batch is durably handled)."""
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
    }
    conf.update(overrides)
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    return consumer
