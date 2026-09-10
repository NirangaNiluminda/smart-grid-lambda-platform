"""Domain models: immutability, validation, and wire-JSON stability."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from energy_common.domain.models import (
    READING_SCHEMA_VERSION,
    BillingTier,
    HouseholdBill,
    MeterReading,
    TariffRecord,
)
from pydantic import ValidationError


def _reading(**overrides: object) -> MeterReading:
    base = {
        "event_id": "evt-1",
        "meter_id": "MTR-1",
        "household_id": "HH-1",
        "grid_zone": "zone-north",
        "power_consumption_kwh": 0.04,
        "solar_generation_kwh": 0.01,
        "event_timestamp": dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.UTC),
    }
    base.update(overrides)
    return MeterReading(**base)  # type: ignore[arg-type]


def test_reading_is_frozen() -> None:
    r = _reading()
    with pytest.raises(ValidationError):
        r.power_consumption_kwh = 1.0  # type: ignore[misc]


def test_reading_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _reading(event_timestamp=dt.datetime(2026, 1, 1, 12, 0))


def test_reading_rejects_unknown_zone() -> None:
    with pytest.raises(ValidationError, match="unknown grid_zone"):
        _reading(grid_zone="zone-atlantis")


def test_reading_normalises_timestamp_to_utc() -> None:
    tz = dt.timezone(dt.timedelta(hours=5, minutes=30))
    r = _reading(event_timestamp=dt.datetime(2026, 1, 1, 17, 30, tzinfo=tz))
    assert r.event_timestamp == dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.UTC)


def test_reading_json_shape_is_stable() -> None:
    """Guards the contract in docs/DATA_CONTRACTS.md §1."""
    payload = _reading().model_dump(mode="json")
    assert set(payload) == {
        "schema_version",
        "event_id",
        "meter_id",
        "household_id",
        "grid_zone",
        "power_consumption_kwh",
        "solar_generation_kwh",
        "reading_interval_seconds",
        "event_timestamp",
    }
    assert payload["schema_version"] == READING_SCHEMA_VERSION


def test_reading_allows_anomalous_values_at_type_level() -> None:
    # Negative / spike values are structurally valid; business validation is separate.
    assert _reading(power_consumption_kwh=-3.0).power_consumption_kwh == -3.0


def test_tariff_record_requires_positive_rate() -> None:
    with pytest.raises(ValidationError):
        TariffRecord(
            household_id="HH-1",
            tariff_rate=Decimal("0"),
            billing_tier=BillingTier.FLAT,
            subsidy_flag=False,
            effective_date=dt.date(2026, 1, 1),
        )


def test_household_bill_percent_bounds() -> None:
    with pytest.raises(ValidationError):
        HouseholdBill(
            business_date=dt.date(2026, 1, 1),
            household_id="HH-1",
            grid_zone="zone-north",
            total_consumption_kwh=-1.0,  # NonNegFloat -> rejected
            total_solar_kwh=0.0,
            self_consumed_kwh=0.0,
            grid_import_kwh=0.0,
            solar_export_kwh=0.0,
            tariff_rate=Decimal("0.5"),
            billing_tier=BillingTier.FLAT,
            subsidy_flag=False,
            energy_charge=Decimal("0"),
            solar_credit=Decimal("0"),
            net_amount=Decimal("0"),
            computed_at=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        )
