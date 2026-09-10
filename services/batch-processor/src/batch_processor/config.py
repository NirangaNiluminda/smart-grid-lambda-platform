"""batch-processor configuration (ready for Part 2 to use)."""

from __future__ import annotations

from pathlib import Path

from energy_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "batch-processor"
    metrics_port: int = 9112

    pg_dsn: str = "postgresql://energy:energy@postgres:5432/energy_serving"
    lake_root: Path = Path("/data/lake")
    landing_root: Path = Path("/data/landing")
    report_out_dir: Path = Path("reports/out")

    # Feed-in credit paid per kWh of solar exported to the grid.
    solar_export_rate: float = 18.0
    currency: str = "LKR"
    # Tiered-tariff block boundaries in kWh/day and their multipliers.
    tier_block_kwh: tuple[float, ...] = (5.0, 15.0)
    tier_multipliers: tuple[float, ...] = (0.8, 1.0, 1.4)


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
