"""Data quality assertions for batch-processor."""

from __future__ import annotations

from energy_common.domain.errors import EnergyPlatformError
from energy_common.domain.models import HouseholdBill, WeatherForecast, ZoneSolarSummary


class DataQualityError(EnergyPlatformError):
    """Raised when batch output violates an invariant."""


def assert_all_households_billed(
    bills: list[HouseholdBill],
    expected_household_ids: set[str],
) -> None:
    billed_ids = {b.household_id for b in bills}
    missing = expected_household_ids - billed_ids
    duplicates = len(bills) - len(billed_ids)

    if missing:
        sample = sorted(missing)[:5]
        raise DataQualityError(
            f"Data quality failure: {len(missing)} active households were not billed: {sample}"
        )
    if duplicates > 0:
        raise DataQualityError(
            f"Data quality failure: {duplicates} duplicate household billings found"
        )


def assert_solar_reconciles(
    bills: list[HouseholdBill],
    summaries: list[ZoneSolarSummary],
) -> None:
    zone_bills_solar: dict[str, float] = {}
    for b in bills:
        zone_bills_solar[b.grid_zone] = zone_bills_solar.get(b.grid_zone, 0.0) + b.total_solar_kwh

    for s in summaries:
        hh_solar = zone_bills_solar.get(s.grid_zone, 0.0)
        diff = abs(hh_solar - s.total_solar_generation_kwh)
        if diff > 0.01:
            raise DataQualityError(
                f"Data quality failure: zone {s.grid_zone} summary solar "
                f"({s.total_solar_generation_kwh}) does not reconcile to bills ({hh_solar:.4f})"
            )


def assert_weather_sanity(
    summaries: list[ZoneSolarSummary],
    weather_forecasts: list[WeatherForecast],
) -> None:
    forecast_map = {w.grid_zone: w for w in weather_forecasts}
    for s in summaries:
        forecast = forecast_map.get(s.grid_zone)
        if (
            forecast is not None
            and forecast.solar_irradiance_index == 0.0
            and s.total_solar_generation_kwh > 0.5
        ):
            raise DataQualityError(
                f"Data quality failure: zone {s.grid_zone} produced solar "
                f"({s.total_solar_generation_kwh}) despite zero solar irradiance"
            )


def assert_charges_valid(bills: list[HouseholdBill]) -> None:
    for b in bills:
        if b.energy_charge < 0:
            raise DataQualityError(
                f"Data quality failure: household {b.household_id} has negative charge "
                f"{b.energy_charge}"
            )
        if b.total_consumption_kwh < 0 or b.total_solar_kwh < 0:
            raise DataQualityError(
                f"Data quality failure: household {b.household_id} has negative power numbers"
            )
