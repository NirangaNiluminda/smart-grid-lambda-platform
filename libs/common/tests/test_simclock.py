"""Simulated clock — conversions and the midnight boundary the batch layer hinges on."""

from __future__ import annotations

import datetime as dt

import pytest
from energy_common.simclock import SimClock, business_date_for

EPOCH = dt.datetime(2026, 6, 1, 0, 0, tzinfo=dt.UTC)  # wall-clock anchor


def make_clock(sim_day_seconds: float = 600.0) -> SimClock:
    return SimClock(
        sim_day_seconds=sim_day_seconds,
        sim_start_date=dt.date(2026, 1, 1),
        sim_epoch_wallclock=EPOCH,
    )


def test_epoch_maps_to_sim_start_midnight() -> None:
    clock = make_clock()
    assert clock.wall_to_sim(EPOCH) == dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.UTC)


def test_one_sim_day_per_sim_day_seconds() -> None:
    clock = make_clock(sim_day_seconds=600)
    later = EPOCH + dt.timedelta(seconds=600)
    assert clock.wall_to_sim(later) == dt.datetime(2026, 1, 2, 0, 0, tzinfo=dt.UTC)


def test_half_a_sim_day() -> None:
    clock = make_clock(sim_day_seconds=600)
    mid = EPOCH + dt.timedelta(seconds=300)
    assert clock.wall_to_sim(mid) == dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.UTC)


def test_wall_and_sim_are_inverse() -> None:
    clock = make_clock()
    sim = dt.datetime(2026, 3, 14, 9, 26, 53, tzinfo=dt.UTC)
    round_tripped = clock.wall_to_sim(clock.sim_to_wall(sim))
    # Float speed-up multiply/divide loses a few microseconds; 1ms is plenty tight.
    assert abs((round_tripped - sim).total_seconds()) < 1e-3


def test_seconds_until_next_sim_midnight() -> None:
    clock = make_clock(sim_day_seconds=600)
    # 300 sim-day-seconds in = simulated noon on day 1 -> half a sim day (300s) left.
    at = EPOCH + dt.timedelta(seconds=300)
    assert clock.seconds_until_next_sim_midnight(at=at) == pytest.approx(300.0, abs=1e-6)


@pytest.mark.parametrize(
    ("ts", "expected"),
    [
        (dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.UTC), dt.date(2026, 1, 1)),
        (dt.datetime(2026, 1, 1, 23, 59, 59, tzinfo=dt.UTC), dt.date(2026, 1, 1)),
        (dt.datetime(2026, 1, 2, 0, 0, 0, tzinfo=dt.UTC), dt.date(2026, 1, 2)),
    ],
)
def test_business_date_boundary(ts: dt.datetime, expected: dt.date) -> None:
    assert business_date_for(ts) == expected


def test_business_date_requires_tz() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        business_date_for(dt.datetime(2026, 1, 1, 0, 0))


def test_rejects_non_positive_sim_day() -> None:
    with pytest.raises(ValueError, match="positive"):
        SimClock(sim_day_seconds=0, sim_start_date=dt.date(2026, 1, 1))
