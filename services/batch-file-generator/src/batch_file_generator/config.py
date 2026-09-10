"""batch-file-generator configuration."""

from __future__ import annotations

from pathlib import Path

from energy_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "batch-file-generator"
    metrics_port: int = 9103

    landing_root: Path = Path("/data/landing")
    seed_households_csv: Path = Path("data/seed/households.csv")
    sim_seed: int = 42
    sim_household_count: int = 200


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
