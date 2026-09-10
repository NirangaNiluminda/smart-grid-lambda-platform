"""Pure generation model: given a household profile and a simulated instant,
what does the meter read for this interval?

No randomness lives here beyond what the caller passes in — the loop owns the RNG
so runs are reproducible from a seed. All functions are pure and unit-tested.
"""

from __future__ import annotations

import datetime as dt
import math

# Domestic demand over the day, as a multiplier on the household's average hourly
# load. Built from an overnight baseline plus a morning and an evening peak. The
# mean over 24h is ~1.0 so daily totals track ``base_load_kwh_day``.
_BASELINE = 0.45
_PEAKS = ((7.5, 1.5, 2.0), (19.0, 2.2, 2.6))  # (hour, width, height)


def _daily_load_multiplier(hour_of_day: float) -> float:
    value = _BASELINE
    for centre, width, height in _PEAKS:
        value += height * math.exp(-(((hour_of_day - centre) / width) ** 2))
    return value


def _solar_multiplier(hour_of_day: float) -> float:
    """Bell curve of irradiance, zero outside ~06:00-18:00, peak at solar noon."""
    if hour_of_day <= 6.0 or hour_of_day >= 18.0:
        return 0.0
    return math.sin(math.pi * (hour_of_day - 6.0) / 12.0) ** 2


def seasonal_irradiance_index(sim_time: dt.datetime) -> float:
    """Slow day-to-day clear-sky factor in [0.35, 0.95] driven by day-of-year.

    A deterministic stand-in for weather; the batch layer uses the *real* daily
    ``weather_forecast`` file, this just gives the stream believable variation.
    """
    day_angle = 2.0 * math.pi * (sim_time.timetuple().tm_yday / 365.0)
    return 0.65 + 0.30 * math.sin(day_angle - math.pi / 2.0)


def expected_consumption_kwh(
    hourly_base_kwh: float, sim_time: dt.datetime, interval_seconds: int
) -> float:
    hour = sim_time.hour + sim_time.minute / 60.0 + sim_time.second / 3600.0
    per_hour = hourly_base_kwh * _daily_load_multiplier(hour)
    return max(0.0, per_hour * interval_seconds / 3600.0)


def expected_solar_kwh(
    solar_capacity_kwp: float,
    sim_time: dt.datetime,
    interval_seconds: int,
    *,
    irradiance_index: float | None = None,
) -> float:
    if solar_capacity_kwp <= 0.0:
        return 0.0
    hour = sim_time.hour + sim_time.minute / 60.0 + sim_time.second / 3600.0
    idx = seasonal_irradiance_index(sim_time) if irradiance_index is None else irradiance_index
    per_hour = solar_capacity_kwp * _solar_multiplier(hour) * idx
    return max(0.0, per_hour * interval_seconds / 3600.0)
