from __future__ import annotations

import datetime as dt

from batch_file_generator.application.generator_loop import GeneratorLoop
from batch_file_generator.domain.reference_data import HouseholdRef


class StepClock:
    def __init__(self, start: dt.datetime, step: dt.timedelta) -> None:
        self._t = start
        self._step = step

    def now_wall(self) -> dt.datetime:
        return dt.datetime(2026, 6, 1, tzinfo=dt.UTC)

    def now_sim(self) -> dt.datetime:
        now, self._t = self._t, self._t + self._step
        return now


class InMemoryLandingZone:
    def __init__(self) -> None:
        self.csvs: dict[str, tuple[list[str], list[list[object]]]] = {}
        self.markers: list[dt.date] = []
        self.write_order: list[str] = []

    def write_csv(self, name: str, header: list[str], rows: list[list[object]]) -> None:
        self.csvs[name] = (header, rows)
        self.write_order.append(name)

    def write_success_marker(self, business_date: dt.date) -> None:
        self.markers.append(business_date)
        self.write_order.append(f"_SUCCESS_{business_date.isoformat()}")


HH = [HouseholdRef(f"HH-{i:03d}", "zone-north", 3) for i in range(10)]


def _loop(clock: StepClock, lz: InMemoryLandingZone) -> GeneratorLoop:
    return GeneratorLoop(clock=clock, landing_zone=lz, households=HH, seed=1, sleep=lambda _s: None)


def test_generates_feed_for_the_day_that_closed() -> None:
    lz = InMemoryLandingZone()
    clock = StepClock(dt.datetime(2026, 1, 1, 23, 0, tzinfo=dt.UTC), dt.timedelta(hours=2))
    _loop(clock, lz).run(max_days=1)
    assert lz.markers == [dt.date(2026, 1, 1)]
    assert "tariff_reference_2026-01-01.csv" in lz.csvs
    assert "weather_forecast_2026-01-01.csv" in lz.csvs


def test_success_marker_written_last() -> None:
    lz = InMemoryLandingZone()
    clock = StepClock(dt.datetime(2026, 1, 1, 23, 0, tzinfo=dt.UTC), dt.timedelta(hours=2))
    _loop(clock, lz).run(max_days=1)
    assert lz.write_order[-1] == "_SUCCESS_2026-01-01"
    assert lz.write_order.index("tariff_reference_2026-01-01.csv") < len(lz.write_order) - 1


def test_catches_up_multiple_closed_days() -> None:
    lz = InMemoryLandingZone()
    # jump three days per poll
    clock = StepClock(dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.UTC), dt.timedelta(days=3))
    _loop(clock, lz).run(max_days=3)
    assert lz.markers == [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)]


def test_generate_for_is_idempotent_in_content() -> None:
    lz1, lz2 = InMemoryLandingZone(), InMemoryLandingZone()
    GeneratorLoop(
        clock=StepClock(dt.datetime(2026, 1, 1, tzinfo=dt.UTC), dt.timedelta()),
        landing_zone=lz1,
        households=HH,
        seed=1,
    ).generate_for(dt.date(2026, 5, 5))
    GeneratorLoop(
        clock=StepClock(dt.datetime(2026, 1, 1, tzinfo=dt.UTC), dt.timedelta()),
        landing_zone=lz2,
        households=HH,
        seed=1,
    ).generate_for(dt.date(2026, 5, 5))
    assert lz1.csvs == lz2.csvs
