"""ReadingCodec — the Kafka <-> domain boundary. Round-trip and DLQ decisions."""

from __future__ import annotations

import datetime as dt

import pytest
from energy_common.domain.errors import ContractViolation
from energy_common.domain.models import MeterReading
from energy_common.messaging import ReadingCodec


def _reading() -> MeterReading:
    return MeterReading(
        event_id="evt-1",
        meter_id="MTR-1",
        household_id="HH-1",
        grid_zone="zone-south",
        power_consumption_kwh=0.05,
        solar_generation_kwh=0.02,
        event_timestamp=dt.datetime(2026, 1, 1, 6, 30, tzinfo=dt.UTC),
    )


def test_round_trip() -> None:
    original = _reading()
    assert ReadingCodec.decode(ReadingCodec.encode(original)) == original


def test_partition_key_is_zone() -> None:
    assert ReadingCodec.partition_key(_reading()) == b"zone-south"


def test_decode_none_raises_contract_violation() -> None:
    with pytest.raises(ContractViolation, match="empty message"):
        ReadingCodec.decode(None)


def test_decode_bad_json_carries_raw() -> None:
    with pytest.raises(ContractViolation) as exc:
        ReadingCodec.decode(b"{not json")
    assert exc.value.raw == b"{not json"


def test_decode_missing_field_is_contract_violation() -> None:
    with pytest.raises(ContractViolation, match="schema validation failed"):
        ReadingCodec.decode(b'{"event_id": "e", "grid_zone": "zone-north"}')


def test_decode_rejects_incompatible_major_version() -> None:
    with pytest.raises(ContractViolation, match="unsupported schema_version"):
        ReadingCodec.decode(b'{"schema_version": "2.0", "grid_zone": "zone-north"}')


def test_decode_non_object_payload() -> None:
    with pytest.raises(ContractViolation, match="not an object"):
        ReadingCodec.decode(b"[1, 2, 3]")
