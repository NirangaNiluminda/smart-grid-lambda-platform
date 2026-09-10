"""Wire codec — the single boundary between Kafka bytes and domain models.

DTO + Mapper pattern: nothing outside this module knows the on-the-wire JSON
shape. ``encode`` is used by the producer; ``decode`` by consumers, which convert
a failure into a DLQ decision via :class:`ContractViolation`.
"""

from __future__ import annotations

import json

from energy_common.domain.errors import ContractViolation
from energy_common.domain.models import MeterReading
from pydantic import ValidationError


class ReadingCodec:
    """Encode/decode :class:`MeterReading` <-> UTF-8 JSON bytes."""

    @staticmethod
    def encode(reading: MeterReading) -> bytes:
        return reading.model_dump_json().encode("utf-8")

    @staticmethod
    def decode(raw: bytes | None) -> MeterReading:
        if raw is None:
            raise ContractViolation("empty message value", raw=raw)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ContractViolation(f"not valid JSON: {exc}", raw=raw) from exc
        if not isinstance(payload, dict):
            raise ContractViolation("JSON payload is not an object", raw=raw)
        version = str(payload.get("schema_version", ""))
        if (
            version
            and version.split(".")[0]
            != MeterReading.model_fields["schema_version"].default.split(".")[0]
        ):
            raise ContractViolation(f"unsupported schema_version {version!r}", raw=raw)
        try:
            return MeterReading.model_validate(payload)
        except ValidationError as exc:
            raise ContractViolation(f"schema validation failed: {exc}", raw=raw) from exc

    @staticmethod
    def partition_key(reading: MeterReading) -> bytes:
        """Kafka message key. Partitioning by zone gives per-zone ordering."""
        return reading.grid_zone.encode("utf-8")
