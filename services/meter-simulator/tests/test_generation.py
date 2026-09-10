from __future__ import annotations

import datetime as dt

from meter_simulator.domain.generation import (
    expected_consumption_kwh,
    expected_solar_kwh,
    seasonal_irradiance_index,
)


def _at(hour: int, minute: int = 0) -> dt.datetime:
    return dt.datetime(2026, 6, 21, hour, minute, tzinfo=dt.UTC)


def test_consumption_is_non_negative_all_day() -> None:
    for h in range(24):
        assert expected_consumption_kwh(0.5, _at(h), 2) >= 0.0


def test_consumption_has_evening_peak_above_small_hours() -> None:
    night = expected_consumption_kwh(0.5, _at(3), 2)
    evening = expected_consumption_kwh(0.5, _at(19), 2)
    assert evening > 3 * night


def test_consumption_scales_with_interval() -> None:
    two = expected_consumption_kwh(0.5, _at(19), 2)
    ten = expected_consumption_kwh(0.5, _at(19), 10)
    assert ten == two * 5


def test_no_solar_without_capacity() -> None:
    assert expected_solar_kwh(0.0, _at(12), 2) == 0.0


def test_solar_zero_at_night_peaks_at_noon() -> None:
    assert expected_solar_kwh(4.0, _at(2), 2) == 0.0
    assert expected_solar_kwh(4.0, _at(23), 2) == 0.0
    noon = expected_solar_kwh(4.0, _at(12), 2)
    morning = expected_solar_kwh(4.0, _at(8), 2)
    assert noon > morning > 0.0


def test_irradiance_index_in_range() -> None:
    for month in range(1, 13):
        idx = seasonal_irradiance_index(dt.datetime(2026, month, 15, tzinfo=dt.UTC))
        assert 0.3 <= idx <= 1.0
