"""Composition root for batch-file-generator."""

from __future__ import annotations

from energy_common import SimClock, get_logger
from energy_common.seeds import load_households, synthetic_households

from batch_file_generator.adapters.landing_zone import CsvLandingZoneWriter
from batch_file_generator.application.generator_loop import GeneratorLoop
from batch_file_generator.config import Settings
from batch_file_generator.domain.reference_data import HouseholdRef

_log = get_logger("batch-file-generator")


def _load_households(settings: Settings) -> list[HouseholdRef]:
    if settings.seed_households_csv.exists():
        households = load_households(settings.seed_households_csv)
        _log.info(
            "households.loaded", source=str(settings.seed_households_csv), count=len(households)
        )
    else:
        households = synthetic_households(settings.sim_household_count, seed=settings.sim_seed)
        _log.warning("households.synthetic", count=len(households))
    return [HouseholdRef(h.household_id, h.grid_zone, h.occupants) for h in households]


def build_loop(settings: Settings) -> GeneratorLoop:
    clock = SimClock(
        sim_day_seconds=settings.sim_day_seconds,
        sim_start_date=settings.sim_start_date,
        sim_epoch_wallclock=settings.sim_epoch_wallclock,
    )
    return GeneratorLoop(
        clock=clock,
        landing_zone=CsvLandingZoneWriter(settings.landing_root),
        households=_load_households(settings),
        seed=settings.sim_seed,
    )
