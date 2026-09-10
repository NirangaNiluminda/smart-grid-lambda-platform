from __future__ import annotations

import datetime as dt

from energy_common.domain.models import MeterReading
from meter_simulator.application.simulation_loop import LoopConfig, SimulationLoop
from meter_simulator.domain.anomalies import AnomalyInjector
from meter_simulator.domain.profiles import synthetic_population


class FakeClock:
    """Clock port: advances simulated time by a fixed step per call."""

    def __init__(self, start: dt.datetime, step: dt.timedelta) -> None:
        self._t = start
        self._step = step

    def now_wall(self) -> dt.datetime:
        return dt.datetime(2026, 6, 1, tzinfo=dt.UTC)

    def now_sim(self) -> dt.datetime:
        now, self._t = self._t, self._t + self._step
        return now


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[MeterReading] = []
        self.flushes = 0

    def publish(self, reading: MeterReading) -> None:
        self.published.append(reading)

    def flush(self, timeout_s: float = 10.0) -> None:
        self.flushes += 1


def _loop(**kw: object) -> tuple[SimulationLoop, RecordingPublisher]:
    pub = RecordingPublisher()
    profiles = synthetic_population(10, seed=1)
    loop = SimulationLoop(
        clock=FakeClock(dt.datetime(2026, 1, 1, 11, 0, tzinfo=dt.UTC), dt.timedelta(minutes=30)),
        publisher=pub,
        profiles=profiles,
        injector=AnomalyInjector(0.0),
        config=LoopConfig(emit_interval_seconds=2, rng_seed=7),
        sleep=lambda _s: None,
        **kw,  # type: ignore[arg-type]
    )
    return loop, pub


def test_publishes_one_reading_per_household_per_tick_with_no_anomalies() -> None:
    loop, pub = _loop()
    published = loop.run(max_ticks=3)
    assert published == 30
    assert len(pub.published) == 30
    assert {r.grid_zone for r in pub.published} <= {
        p.grid_zone for p in synthetic_population(10, seed=1)
    }


def test_all_readings_valid_meter_readings_with_utc_timestamps() -> None:
    loop, pub = _loop()
    loop.run(max_ticks=2)
    for r in pub.published:
        assert r.event_timestamp.tzinfo is not None
        assert r.power_consumption_kwh >= 0.0


def test_flush_called_on_completion() -> None:
    loop, pub = _loop()
    loop.run(max_ticks=1)
    assert pub.flushes >= 1


def test_dropouts_reduce_volume_when_anomalies_on() -> None:
    pub = RecordingPublisher()
    loop = SimulationLoop(
        clock=FakeClock(dt.datetime(2026, 1, 1, 18, 0, tzinfo=dt.UTC), dt.timedelta(minutes=1)),
        publisher=pub,
        profiles=synthetic_population(50, seed=3),
        injector=AnomalyInjector(1.0),  # force an anomaly every reading
        config=LoopConfig(emit_interval_seconds=2, rng_seed=3),
        sleep=lambda _s: None,
    )
    published = loop.run(max_ticks=4)
    # With every reading anomalous, a meaningful share are dropouts -> fewer than 200.
    assert published < 200


def test_day_rollover_is_handled() -> None:
    pub = RecordingPublisher()
    loop = SimulationLoop(
        clock=FakeClock(dt.datetime(2026, 1, 1, 23, 30, tzinfo=dt.UTC), dt.timedelta(hours=1)),
        publisher=pub,
        profiles=synthetic_population(3, seed=1),
        injector=AnomalyInjector(0.0),
        config=LoopConfig(emit_interval_seconds=2),
        sleep=lambda _s: None,
    )
    # ticks at 23:30 (Jan 1), 00:30 (Jan 2), 01:30 (Jan 2) -> crosses midnight once
    loop.run(max_ticks=3)
    dates = {r.event_timestamp.date() for r in pub.published}
    assert dates == {dt.date(2026, 1, 1), dt.date(2026, 1, 2)}
