"""Household profiles that drive the generation model.

The population is defined once in ``energy_common.seeds`` (shared with
batch-file-generator so the two services cannot drift). This module just
re-exports it under the name the simulator uses and adds one derived quantity.
"""

from __future__ import annotations

from pathlib import Path

from energy_common.seeds import SeedHousehold, load_households, synthetic_households

# The simulator works directly with the shared seed model.
HouseholdProfile = SeedHousehold


def hourly_base_kwh(profile: HouseholdProfile) -> float:
    """Average load per hour, before the diurnal shape is applied."""
    return profile.base_load_kwh_day / 24.0


def load_profiles(seed_csv: str | Path) -> list[HouseholdProfile]:
    return load_households(seed_csv)


def synthetic_population(count: int, *, seed: int = 42) -> list[HouseholdProfile]:
    return synthetic_households(count, seed=seed)
