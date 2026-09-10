"""Pure transforms shared by the speed and batch layers."""

from __future__ import annotations

import datetime as dt

import pytest
from energy_common.domain.models import MeterReading
from energy_common.domain.transforms import (
    RejectReason,
    energy_balance,
    renewable_pct,
    validate_reading,
)


def _reading(consumption: float, solar: float = 0.0, interval: int = 2) -> MeterReading:
    return MeterReading(
        event_id="e",
        meter_id="MTR-1",
        household_id="HH-1",
        grid_zone="zone-west",
        power_consumption_kwh=consumption,
        solar_generation_kwh=solar,
        reading_interval_seconds=interval,
        event_timestamp=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    )


class TestValidateReading:
    def test_accepts_normal(self) -> None:
        assert validate_reading(_reading(0.05, 0.01)).ok

    def test_rejects_negative_consumption(self) -> None:
        out = validate_reading(_reading(-0.1))
        assert not out.ok and out.reason is RejectReason.NEGATIVE_CONSUMPTION

    def test_rejects_negative_solar(self) -> None:
        out = validate_reading(_reading(0.1, -0.2))
        assert out.reason is RejectReason.NEGATIVE_SOLAR

    def test_rejects_spike(self) -> None:
        out = validate_reading(_reading(999.0))
        assert out.reason is RejectReason.IMPLAUSIBLE_SPIKE

    def test_spike_cap_scales_with_interval(self) -> None:
        # 4 kWh in a 2s interval is a spike; the same in a 4s interval is under the cap.
        assert not validate_reading(_reading(6.0, interval=2)).ok
        assert validate_reading(_reading(6.0, interval=4)).ok


class TestRenewablePct:
    def test_basic(self) -> None:
        assert renewable_pct(100.0, 25.0) == pytest.approx(25.0)

    def test_zero_consumption(self) -> None:
        assert renewable_pct(0.0, 5.0) == 0.0

    def test_clamped_to_100(self) -> None:
        assert renewable_pct(10.0, 40.0) == 100.0


class TestEnergyBalance:
    def test_solar_covers_part_of_load(self) -> None:
        b = energy_balance(10.0, 4.0)
        assert (b.self_consumed_kwh, b.grid_import_kwh, b.solar_export_kwh) == (4.0, 6.0, 0.0)

    def test_solar_surplus_is_exported(self) -> None:
        b = energy_balance(3.0, 8.0)
        assert (b.self_consumed_kwh, b.grid_import_kwh, b.solar_export_kwh) == (3.0, 0.0, 5.0)

    def test_no_solar(self) -> None:
        b = energy_balance(7.0, 0.0)
        assert b.grid_import_kwh == 7.0 and b.solar_export_kwh == 0.0

    def test_negatives_are_floored(self) -> None:
        b = energy_balance(-5.0, -2.0)
        assert b == energy_balance(0.0, 0.0)
