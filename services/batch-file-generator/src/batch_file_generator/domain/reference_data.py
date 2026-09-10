"""Pure builders for the daily reference feed.

Given the shared household population (same seed CSV the simulator uses) and a
business date, produce the rows that will be written to
``tariff_reference_<date>.csv`` and ``weather_forecast_<date>.csv``.

Deterministic given ``(business_date, seed)`` so a re-run for a past day is
reproducible — the property the Lambda batch layer relies on.
"""

from __future__ import annotations

import datetime as dt
import math
import random
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from energy_common.domain.models import GRID_ZONES, BillingTier, TariffRecord, WeatherForecast

# Base import tariff, currency units (LKR) per kWh, before tier adjustment.
_BASE_TARIFF = Decimal("32.00")
_TIER_WEIGHTS = {BillingTier.FLAT: 0.50, BillingTier.TIERED: 0.35, BillingTier.SUBSIDISED: 0.15}
_TIER_MULTIPLIER = {
    BillingTier.FLAT: Decimal("1.00"),
    BillingTier.TIERED: Decimal("1.15"),
    BillingTier.SUBSIDISED: Decimal("0.70"),
}


@dataclass(frozen=True, slots=True)
class HouseholdRef:
    """The subset of a household profile this service needs."""

    household_id: str
    grid_zone: str
    occupants: int


def _q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def build_tariff_rows(
    households: list[HouseholdRef], business_date: dt.date, *, seed: int
) -> list[TariffRecord]:
    rng = random.Random(f"{seed}:{business_date.isoformat()}")
    tiers = list(_TIER_WEIGHTS)
    weights = [_TIER_WEIGHTS[t] for t in tiers]
    rows: list[TariffRecord] = []
    for hh in households:
        tier = rng.choices(tiers, weights=weights, k=1)[0]
        # Small daily wholesale-price wobble shared by all households.
        wobble = Decimal(str(round(1.0 + 0.04 * math.sin(business_date.toordinal() / 5.0), 4)))
        rate = _q2(_BASE_TARIFF * _TIER_MULTIPLIER[tier] * wobble)
        subsidy = tier is BillingTier.SUBSIDISED or (hh.occupants <= 2 and rng.random() < 0.2)
        rows.append(
            TariffRecord(
                household_id=hh.household_id,
                tariff_rate=rate,
                billing_tier=tier,
                subsidy_flag=subsidy,
                effective_date=business_date,
            )
        )
    return rows


def build_weather_rows(business_date: dt.date, *, seed: int) -> list[WeatherForecast]:
    rng = random.Random(f"{seed}:weather:{business_date.isoformat()}")
    day_angle = 2.0 * math.pi * (business_date.timetuple().tm_yday / 365.0)
    seasonal_clear = 0.65 + 0.30 * math.sin(day_angle - math.pi / 2.0)
    rows: list[WeatherForecast] = []
    for zone in GRID_ZONES:
        cloud = max(0, min(100, int(rng.gauss((1.0 - seasonal_clear) * 100, 12))))
        irradiance = round(max(0.05, min(1.0, seasonal_clear * (1.0 - cloud / 140.0))), 2)
        rows.append(
            WeatherForecast(
                grid_zone=zone,
                forecast_date=business_date,
                cloud_cover_pct=cloud,
                temp_c=round(rng.gauss(28.0, 3.0), 1),
                solar_irradiance_index=irradiance,
            )
        )
    return rows


TARIFF_HEADER = ["household_id", "tariff_rate", "billing_tier", "subsidy_flag", "effective_date"]
WEATHER_HEADER = [
    "grid_zone",
    "forecast_date",
    "cloud_cover_pct",
    "temp_c",
    "solar_irradiance_index",
]


def tariff_to_row(rec: TariffRecord) -> list[object]:
    return [
        rec.household_id,
        str(rec.tariff_rate),
        rec.billing_tier.value,
        str(rec.subsidy_flag).lower(),
        rec.effective_date.isoformat(),
    ]


def weather_to_row(rec: WeatherForecast) -> list[object]:
    return [
        rec.grid_zone,
        rec.forecast_date.isoformat(),
        rec.cloud_cover_pct,
        rec.temp_c,
        rec.solar_irradiance_index,
    ]
