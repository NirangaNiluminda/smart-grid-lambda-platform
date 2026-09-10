"""Domain-level exception hierarchy.

Adapters translate infrastructure errors into these so the application layer never
has to catch a ``confluent_kafka`` or ``sqlalchemy`` exception directly.
"""

from __future__ import annotations


class EnergyPlatformError(Exception):
    """Base class for every error raised by platform code."""


class ConfigError(EnergyPlatformError):
    """Configuration is missing or invalid. Raised at startup, never recovered from."""


class ContractViolation(EnergyPlatformError):
    """A payload does not satisfy a published data contract.

    Carries the raw bytes so a caller can route the offending message to a DLQ.
    """

    def __init__(self, message: str, *, raw: bytes | None = None) -> None:
        super().__init__(message)
        self.raw = raw


class PublishError(EnergyPlatformError):
    """An outbound adapter failed to hand a record to its destination."""


class StorageError(EnergyPlatformError):
    """A storage adapter (Postgres, Parquet lake) failed."""
