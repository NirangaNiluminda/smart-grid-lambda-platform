"""Canonical seed population.

The household catalogue is a *contract* shared by ``meter-simulator`` (models
their load) and ``batch-file-generator`` (bills them). To guarantee the two never
drift, the population is defined **once** here. ``data/seed/households.csv`` is
generated from :func:`synthetic_households` (see ``scripts/generate_seed.py``);
services load the CSV in production and fall back to this function in dev/tests.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from energy_common.domain.models import GRID_ZONES

DWELLING_TYPES = ("apartment", "terraced", "semi_detached", "detached")

CSV_HEADER = [
    "household_id",
    "meter_id",
    "grid_zone",
    "dwelling_type",
    "occupants",
    "has_solar",
    "base_load_kwh_day",
    "solar_capacity_kwp",
]


@dataclass(frozen=True, slots=True)
class SeedHousehold:
    household_id: str
    meter_id: str
    grid_zone: str
    dwelling_type: str
    occupants: int
    has_solar: bool
    base_load_kwh_day: float
    solar_capacity_kwp: float

    def to_csv_row(self) -> list[object]:
        return [
            self.household_id,
            self.meter_id,
            self.grid_zone,
            self.dwelling_type,
            self.occupants,
            str(self.has_solar).lower(),
            self.base_load_kwh_day,
            self.solar_capacity_kwp,
        ]

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> SeedHousehold:
        has_solar = row["has_solar"].strip().lower() in {"1", "true", "yes"}
        return cls(
            household_id=row["household_id"],
            meter_id=row.get("meter_id") or row["household_id"].replace("HH-", "MTR-"),
            grid_zone=row["grid_zone"],
            dwelling_type=row["dwelling_type"],
            occupants=int(row["occupants"]),
            has_solar=has_solar,
            base_load_kwh_day=float(row["base_load_kwh_day"]),
            solar_capacity_kwp=float(row.get("solar_capacity_kwp") or 0.0),
        )


def synthetic_households(count: int, *, seed: int = 42) -> list[SeedHousehold]:
    """Deterministic population spread evenly across the six grid zones."""
    if count <= 0:
        raise ValueError("count must be positive")
    rng = random.Random(seed)
    out: list[SeedHousehold] = []
    for i in range(count):
        occupants = rng.randint(1, 6)
        has_solar = rng.random() < 0.35
        out.append(
            SeedHousehold(
                household_id=f"HH-{i:05d}",
                meter_id=f"MTR-{i:05d}",
                grid_zone=GRID_ZONES[i % len(GRID_ZONES)],
                dwelling_type=rng.choice(DWELLING_TYPES),
                occupants=occupants,
                has_solar=has_solar,
                base_load_kwh_day=round(4.0 + 2.5 * occupants + rng.uniform(-1.5, 1.5), 2),
                solar_capacity_kwp=round(rng.uniform(2.5, 6.0), 2) if has_solar else 0.0,
            )
        )
    return out


def load_households(csv_path: str | Path) -> list[SeedHousehold]:
    path = Path(csv_path)
    with path.open(newline="") as fh:
        rows = [SeedHousehold.from_csv_row(r) for r in csv.DictReader(fh)]
    if not rows:
        raise ValueError(f"no households in {path}")
    return rows


def write_households_csv(households: list[SeedHousehold], csv_path: str | Path) -> None:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)
        writer.writerows(h.to_csv_row() for h in households)
