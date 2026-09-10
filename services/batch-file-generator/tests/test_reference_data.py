from __future__ import annotations

import datetime as dt

from batch_file_generator.domain.reference_data import (
    HouseholdRef,
    build_tariff_rows,
    build_weather_rows,
)
from energy_common.domain.models import GRID_ZONES, BillingTier

HOUSEHOLDS = [
    HouseholdRef(f"HH-{i:03d}", GRID_ZONES[i % 6], occupants=(i % 5) + 1) for i in range(60)
]
DATE = dt.date(2026, 3, 3)


class TestTariff:
    def test_one_row_per_household(self) -> None:
        rows = build_tariff_rows(HOUSEHOLDS, DATE, seed=42)
        assert {r.household_id for r in rows} == {h.household_id for h in HOUSEHOLDS}

    def test_deterministic_for_same_date_and_seed(self) -> None:
        a = build_tariff_rows(HOUSEHOLDS, DATE, seed=42)
        b = build_tariff_rows(HOUSEHOLDS, DATE, seed=42)
        assert a == b

    def test_differs_across_dates(self) -> None:
        a = build_tariff_rows(HOUSEHOLDS, DATE, seed=42)
        b = build_tariff_rows(HOUSEHOLDS, dt.date(2026, 3, 4), seed=42)
        assert a != b

    def test_rates_positive_and_effective_date_matches(self) -> None:
        for r in build_tariff_rows(HOUSEHOLDS, DATE, seed=1):
            assert r.tariff_rate > 0
            assert r.effective_date == DATE

    def test_subsidised_tier_always_has_subsidy_flag(self) -> None:
        for r in build_tariff_rows(HOUSEHOLDS, DATE, seed=7):
            if r.billing_tier is BillingTier.SUBSIDISED:
                assert r.subsidy_flag is True


class TestWeather:
    def test_one_row_per_zone(self) -> None:
        rows = build_weather_rows(DATE, seed=42)
        assert {r.grid_zone for r in rows} == set(GRID_ZONES)

    def test_forecast_date_matches_and_bounds_hold(self) -> None:
        for r in build_weather_rows(DATE, seed=3):
            assert r.forecast_date == DATE
            assert 0 <= r.cloud_cover_pct <= 100
            assert 0.0 <= r.solar_irradiance_index <= 1.0

    def test_deterministic(self) -> None:
        assert build_weather_rows(DATE, seed=9) == build_weather_rows(DATE, seed=9)
