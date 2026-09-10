"""Composition root — the only module that knows both the ports and the concrete
adapters. Everything else depends on abstractions.
"""

from __future__ import annotations

from energy_common import SimClock, get_logger
from energy_common.messaging import build_producer
from energy_common.messaging.kafka_adapters import KafkaReadingPublisher

from meter_simulator.application.simulation_loop import LoopConfig, SimulationLoop
from meter_simulator.config import Settings
from meter_simulator.domain.anomalies import AnomalyInjector
from meter_simulator.domain.profiles import HouseholdProfile, load_profiles, synthetic_population

_log = get_logger("meter-simulator")


def _resolve_profiles(settings: Settings) -> list[HouseholdProfile]:
    if settings.seed_households_csv.exists():
        profiles = load_profiles(settings.seed_households_csv)
        _log.info("profiles.loaded", source=str(settings.seed_households_csv), count=len(profiles))
        return profiles
    profiles = synthetic_population(settings.sim_household_count, seed=settings.sim_seed)
    _log.warning(
        "profiles.synthetic",
        reason=f"{settings.seed_households_csv} not found",
        count=len(profiles),
    )
    return profiles


def build_loop(settings: Settings) -> SimulationLoop:
    clock = SimClock(
        sim_day_seconds=settings.sim_day_seconds,
        sim_start_date=settings.sim_start_date,
        sim_epoch_wallclock=settings.sim_epoch_wallclock,
    )
    producer = build_producer(settings.kafka_bootstrap_servers, client_id=settings.service_name)
    publisher = KafkaReadingPublisher(producer, settings.kafka_readings_topic)
    return SimulationLoop(
        clock=clock,
        publisher=publisher,
        profiles=_resolve_profiles(settings),
        injector=AnomalyInjector(settings.sim_anomaly_rate),
        config=LoopConfig(
            emit_interval_seconds=settings.sim_emit_interval_seconds,
            rng_seed=settings.sim_seed,
        ),
    )
