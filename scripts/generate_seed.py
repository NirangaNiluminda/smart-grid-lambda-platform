#!/usr/bin/env python3
"""Regenerate the committed seed data from ``energy_common.seeds``.

Run this if you change the population size or model. The CSVs it writes are the
*contract* shared by meter-simulator and batch-file-generator, so they are
committed to the repo.

    python scripts/generate_seed.py --households 200 --seed 42
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from energy_common.domain.models import GRID_ZONES
from energy_common.seeds import synthetic_households, write_households_csv

REPO_ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = REPO_ROOT / "data" / "seed"

ZONE_META = {
    "zone-north": ("Northern Industrial", "North", 1800),
    "zone-south": ("Southern Suburbs", "South", 1500),
    "zone-east": ("Eastern Residential", "East", 1400),
    "zone-west": ("Western Mixed", "West", 1600),
    "zone-central": ("Central Business District", "Central", 2200),
    "zone-coastal": ("Coastal Villages", "Coast", 900),
}


def write_zones(path: Path) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["grid_zone", "name", "region", "meter_capacity"])
        for zone in GRID_ZONES:
            name, region, capacity = ZONE_META[zone]
            writer.writerow([zone, name, region, capacity])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--households", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    SEED_DIR.mkdir(parents=True, exist_ok=True)
    households = synthetic_households(args.households, seed=args.seed)
    write_households_csv(households, SEED_DIR / "households.csv")
    write_zones(SEED_DIR / "zones.csv")

    with_solar = sum(h.has_solar for h in households)
    print(
        f"wrote {len(households)} households ({with_solar} with solar) "
        f"and {len(GRID_ZONES)} zones to {SEED_DIR}"
    )


if __name__ == "__main__":
    main()
