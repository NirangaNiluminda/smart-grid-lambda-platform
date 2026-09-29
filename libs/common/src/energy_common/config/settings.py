"""Configuration base class.

Every service defines its own ``Settings(BaseServiceSettings)`` with the extra
fields it needs. Construction reads the process environment (and a local ``.env``
if present), validates, and raises :class:`ConfigError` on the first problem so a
misconfigured service dies at startup instead of midway through processing.
"""

from __future__ import annotations

import datetime as dt
from functools import lru_cache
from typing import Literal

from energy_common.domain.errors import ConfigError
from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Fields shared by every service. Subclass, don't edit."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    service_name: str = "unnamed-service"

    # --- simulated clock (see energy_common.simclock) ---
    sim_day_seconds: float = Field(default=600.0, gt=0)
    sim_epoch_wallclock: dt.datetime | None = None
    sim_start_date: dt.date = dt.date(2026, 1, 1)

    @field_validator("sim_epoch_wallclock", mode="before")
    @classmethod
    def _blank_env_value_means_unset(cls, v: object) -> object:
        # `.env.example` documents SIM_EPOCH_WALLCLOCK= (blank) as "leave unset
        # to anchor on process start" -- but a blank line in a .env file sets
        # the env var to "", not "absent", and pydantic then fails trying to
        # parse "" as a datetime. Every service reading the exact .env the
        # docs tell you to create (`cp .env.example .env`) would crash at
        # startup on this before it did anything else.
        return None if v == "" else v

    # --- observability ---
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"
    metrics_port: int = Field(default=9101, ge=1, le=65535)

    @classmethod
    def load(cls) -> BaseServiceSettings:
        """Instantiate from the environment, converting validation errors."""
        try:
            return cls()
        except ValidationError as exc:  # pragma: no cover - exercised via subclasses
            raise ConfigError(f"invalid configuration for {cls.__name__}:\n{exc}") from exc


@lru_cache(maxsize=1)
def get_settings() -> BaseServiceSettings:
    """Process-wide base settings. Services usually call their own subclass's
    ``load()`` instead; this exists for library code that only needs the shared
    fields (e.g. the logging setup)."""
    return BaseServiceSettings.load()
