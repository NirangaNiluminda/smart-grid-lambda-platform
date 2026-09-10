"""Messaging layer. ``ReadingCodec`` is dependency-free; the factories and
adapters need the ``kafka`` extra (``pip install 'energy-common[kafka]'``)."""

from typing import Any

from energy_common.messaging.codec import ReadingCodec

__all__ = ["ReadingCodec", "build_consumer", "build_producer"]


def __getattr__(name: str) -> Any:
    # Lazy import so confluent-kafka stays an optional extra.
    if name in {"build_producer", "build_consumer"}:
        from energy_common.messaging import kafka_factory

        return getattr(kafka_factory, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
