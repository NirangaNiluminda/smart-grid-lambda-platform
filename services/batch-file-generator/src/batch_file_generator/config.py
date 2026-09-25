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

    # Off by default: only meaningful once Part 2's Airflow is deployed
    # (docker-compose.part2.yml sets this to true). See adapters/airflow_trigger.py.
    airflow_trigger_enabled: bool = False
    airflow_base_url: str = "http://airflow:8080"
    airflow_dag_id: str = "daily_energy_reconciliation"
    airflow_username: str = "admin"
    airflow_password: str = "admin"


def load_settings() -> Settings:
    return Settings.load()  # type: ignore[return-value]
