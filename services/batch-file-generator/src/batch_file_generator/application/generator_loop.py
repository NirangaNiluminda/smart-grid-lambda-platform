"""Daily-feed use-case.

Waits for the simulated clock to cross midnight, then drops the tariff + weather
files for the day that just **closed**. Depends only on the ``Clock`` and
``LandingZone`` ports, so tests drive it with a fake clock and an in-memory
landing zone.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable

from energy_common.domain.ports import Clock, LandingZone
from energy_common.observability import bind_context, counter, get_logger

from batch_file_generator.domain.reference_data import (
    TARIFF_HEADER,
    WEATHER_HEADER,
    HouseholdRef,
    build_tariff_rows,
    build_weather_rows,
    tariff_to_row,
    weather_to_row,
)

_log = get_logger("batch-file-generator")
_files_written = counter("feed_files_written_total", "Reference files written", ("kind",))


class GeneratorLoop:
    def __init__(
        self,
        *,
        clock: Clock,
        landing_zone: LandingZone,
        households: list[HouseholdRef],
        seed: int = 42,
        sleep: Callable[[float], None] | None = None,
        poll_seconds: float = 1.0,
    ) -> None:
        if not households:
            raise ValueError("GeneratorLoop needs the household population")
        self._clock = clock
        self._lz = landing_zone
        self._households = households
        self._seed = seed
        self._sleep = sleep or _real_sleep
        self._poll = poll_seconds
        self._last_seen_date: dt.date | None = None

    def run(self, *, max_days: int | None = None) -> int:
        """Loop until ``max_days`` feeds have been produced (``None`` = forever)."""
        produced = 0
        self._last_seen_date = self._clock.now_sim().date()
        while max_days is None or produced < max_days:
            sim_today = self._clock.now_sim().date()
            if sim_today > self._last_seen_date:
                for closed_day in _dates_between(self._last_seen_date, sim_today):
                    self.generate_for(closed_day)
                    produced += 1
                    if max_days is not None and produced >= max_days:
                        break
                self._last_seen_date = sim_today
            if max_days is None or produced < max_days:
                self._sleep(self._poll)
        return produced

    def generate_for(self, business_date: dt.date) -> None:
        """Write the tariff + weather feed for one closed business day."""
        bind_context(business_date=business_date.isoformat())
        tariffs = build_tariff_rows(self._households, business_date, seed=self._seed)
        weather = build_weather_rows(business_date, seed=self._seed)

        self._lz.write_csv(
            f"tariff_reference_{business_date.isoformat()}.csv",
            TARIFF_HEADER,
            [tariff_to_row(r) for r in tariffs],
        )
        _files_written.labels(kind="tariff").inc()
        _log.info("feed.tariff_written", rows=len(tariffs))

        self._lz.write_csv(
            f"weather_forecast_{business_date.isoformat()}.csv",
            WEATHER_HEADER,
            [weather_to_row(r) for r in weather],
        )
        _files_written.labels(kind="weather").inc()
        _log.info("feed.weather_written", rows=len(weather))

        self._lz.write_success_marker(business_date)
        _log.info("feed.success_marker", business_date=business_date.isoformat())


def _dates_between(start_exclusive: dt.date, end_exclusive: dt.date) -> list[dt.date]:
    """All whole days in ``(start_exclusive, end_exclusive)`` — the days that have
    fully closed since we last looked."""
    out: list[dt.date] = []
    cursor = start_exclusive
    while cursor < end_exclusive:
        out.append(cursor)
        cursor += dt.timedelta(days=1)
    return out


def _real_sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)
