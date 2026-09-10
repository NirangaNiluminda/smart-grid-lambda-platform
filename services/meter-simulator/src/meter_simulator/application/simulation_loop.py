"""The simulation use-case.

Clock-driven emit loop. Depends only on ports (``Clock``, ``ReadingPublisher``)
and pure domain code, so the whole thing is exercised in unit tests with a fake
publisher and a frozen clock — no Kafka, no sleeping.
"""

from __future__ import annotations

import datetime as dt
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from energy_common.domain.models import MeterReading
from energy_common.domain.ports import Clock, ReadingPublisher
from energy_common.observability import bind_context, counter, get_logger

from meter_simulator.domain.anomalies import AnomalyInjector, AnomalyKind
from meter_simulator.domain.generation import expected_consumption_kwh, expected_solar_kwh
from meter_simulator.domain.profiles import HouseholdProfile, hourly_base_kwh

_log = get_logger("meter-simulator")

_readings_total = counter(
    "readings_produced_total", "Meter readings published", ("grid_zone", "anomaly")
)
_publish_errors = counter("simulator_publish_errors_total", "Publish failures")


@dataclass(slots=True)
class LoopConfig:
    emit_interval_seconds: float = 2.0
    noise_pct: float = 0.15  # +/- multiplicative noise on the modelled value
    rng_seed: int = 42
    flush_every_ticks: int = 5


class SimulationLoop:
    def __init__(
        self,
        *,
        clock: Clock,
        publisher: ReadingPublisher,
        profiles: list[HouseholdProfile],
        injector: AnomalyInjector,
        config: LoopConfig | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if not profiles:
            raise ValueError("SimulationLoop needs at least one household profile")
        self._clock = clock
        self._publisher = publisher
        self._profiles = profiles
        self._injector = injector
        self._cfg = config or LoopConfig()
        self._sleep = sleep or _real_sleep
        self._rng = random.Random(self._cfg.rng_seed)
        self._last_value: dict[str, tuple[float, float]] = {}  # meter_id -> (cons, solar)
        self._current_business_date: dt.date | None = None

    # -- public ---------------------------------------------------------
    def run(self, *, max_ticks: int | None = None) -> int:
        """Run the loop. Returns the number of readings published.

        ``max_ticks`` bounds the loop for tests / demos; ``None`` runs forever.
        """
        published = 0
        tick = 0
        while max_ticks is None or tick < max_ticks:
            sim_now = self._clock.now_sim()
            self._maybe_log_rollover(sim_now)
            published += self._emit_tick(sim_now)
            tick += 1
            if tick % self._cfg.flush_every_ticks == 0:
                self._safe_flush()
            if max_ticks is None or tick < max_ticks:
                self._sleep(self._cfg.emit_interval_seconds)
        self._safe_flush()
        return published

    # -- internals ----------------------------------------------------
    def _maybe_log_rollover(self, sim_now: dt.datetime) -> None:
        business_date = sim_now.date()
        if business_date != self._current_business_date:
            if self._current_business_date is not None:
                _log.info(
                    "sim.day_rollover",
                    previous_business_date=self._current_business_date.isoformat(),
                    business_date=business_date.isoformat(),
                )
            self._current_business_date = business_date
            bind_context(business_date=business_date.isoformat())

    def _emit_tick(self, sim_now: dt.datetime) -> int:
        interval = max(1, round(self._cfg.emit_interval_seconds))
        count = 0
        for profile in self._profiles:
            reading = self._model_reading(profile, sim_now, interval)
            outcome = self._injector.maybe_apply(reading, self._rng)

            if outcome.kind is AnomalyKind.DROPOUT:
                _log.debug("reading.anomaly_injected", meter_id=profile.meter_id, anomaly="dropout")
                continue

            emit = outcome.reading
            assert emit is not None
            if outcome.kind is AnomalyKind.STUCK:
                emit = self._apply_stuck(emit)

            if outcome.kind is not None:
                _log.debug(
                    "reading.anomaly_injected", meter_id=profile.meter_id, anomaly=outcome.kind
                )

            self._publish(emit, anomaly=outcome.kind)
            self._last_value[profile.meter_id] = (
                emit.power_consumption_kwh,
                emit.solar_generation_kwh,
            )
            count += 1
        return count

    def _model_reading(
        self, profile: HouseholdProfile, sim_now: dt.datetime, interval: int
    ) -> MeterReading:
        cons = expected_consumption_kwh(hourly_base_kwh(profile), sim_now, interval)
        solar = expected_solar_kwh(profile.solar_capacity_kwp, sim_now, interval)
        cons *= 1.0 + self._rng.uniform(-self._cfg.noise_pct, self._cfg.noise_pct)
        if solar > 0:
            solar *= 1.0 + self._rng.uniform(-self._cfg.noise_pct, self._cfg.noise_pct)
        return MeterReading(
            event_id=str(uuid.uuid4()),
            meter_id=profile.meter_id,
            household_id=profile.household_id,
            grid_zone=profile.grid_zone,
            power_consumption_kwh=round(max(0.0, cons), 5),
            solar_generation_kwh=round(max(0.0, solar), 5),
            reading_interval_seconds=interval,
            event_timestamp=sim_now,
        )

    def _apply_stuck(self, reading: MeterReading) -> MeterReading:
        prev = self._last_value.get(reading.meter_id)
        if prev is None:
            return reading
        return reading.model_copy(
            update={"power_consumption_kwh": prev[0], "solar_generation_kwh": prev[1]}
        )

    def _publish(self, reading: MeterReading, *, anomaly: AnomalyKind | None) -> None:
        try:
            self._publisher.publish(reading)
        except Exception:
            _publish_errors.inc()
            _log.warning("publish.error", meter_id=reading.meter_id, exc_info=True)
            return
        _readings_total.labels(
            grid_zone=reading.grid_zone, anomaly=(anomaly.value if anomaly else "none")
        ).inc()
        _log.debug("reading.published", meter_id=reading.meter_id, grid_zone=reading.grid_zone)

    def _safe_flush(self) -> None:
        try:
            self._publisher.flush()
        except Exception:
            _publish_errors.inc()
            _log.warning("flush.error", exc_info=True)


def _real_sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)
