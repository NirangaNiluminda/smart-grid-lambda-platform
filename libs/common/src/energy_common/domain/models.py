"""Domain models — the canonical, code-level data contracts.

Every model is **frozen** (immutable, hashable) and validated. These types are the
single definition of the platform's data shapes; the tables in
``docs/DATA_CONTRACTS.md`` mirror them and a round-trip test keeps the wire JSON
stable.

Nothing in this module imports infrastructure. It is safe to import from any layer.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Semantic version of the streaming payload contract. Consumers branch on the major.
READING_SCHEMA_VERSION = "1.0"

# The six seeded grid zones. Kept here so producers and the seed data cannot drift.
GRID_ZONES: tuple[str, ...] = (
    "zone-north",
    "zone-south",
    "zone-east",
    "zone-west",
    "zone-central",
    "zone-coastal",
)


class _Frozen(BaseModel):
    """Base for all domain models: immutable, strict, no unexpected fields."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


NonNegFloat = Annotated[float, Field(ge=0.0)]
Percent = Annotated[float, Field(ge=0.0, le=100.0)]


class BillingTier(StrEnum):
    FLAT = "flat"
    TIERED = "tiered"
    SUBSIDISED = "subsidised"


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class BatchRunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Streaming source
# ---------------------------------------------------------------------------
class MeterReading(_Frozen):
    """One smart-meter reading. Kafka value on ``meter.readings``.

    ``event_timestamp`` is *simulated* time (UTC). Anomalous readings (negative
    values, spikes) are still valid ``MeterReading`` instances at the type level —
    the *validation* the speed layer applies is a separate, business rule (see
    :mod:`energy_common.domain.transforms`). Only structurally broken payloads
    (missing field, wrong type) fail here and go to the DLQ.
    """

    schema_version: str = READING_SCHEMA_VERSION
    event_id: str
    meter_id: str
    household_id: str
    grid_zone: str
    power_consumption_kwh: float
    solar_generation_kwh: float
    reading_interval_seconds: Annotated[int, Field(gt=0)] = 2
    event_timestamp: dt.datetime

    @field_validator("event_timestamp")
    @classmethod
    def _tz_aware_utc(cls, v: dt.datetime) -> dt.datetime:
        if v.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware (UTC)")
        return v.astimezone(dt.UTC)

    @field_validator("grid_zone")
    @classmethod
    def _known_zone(cls, v: str) -> str:
        if v not in GRID_ZONES:
            raise ValueError(f"unknown grid_zone {v!r}; expected one of {GRID_ZONES}")
        return v


# ---------------------------------------------------------------------------
# Daily batch source
# ---------------------------------------------------------------------------
class TariffRecord(_Frozen):
    """One row of ``tariff_reference_<date>.csv``."""

    household_id: str
    tariff_rate: Decimal = Field(gt=0)
    billing_tier: BillingTier
    subsidy_flag: bool
    effective_date: dt.date


class WeatherForecast(_Frozen):
    """One row of ``weather_forecast_<date>.csv`` (one per grid zone)."""

    grid_zone: str
    forecast_date: dt.date
    cloud_cover_pct: Annotated[int, Field(ge=0, le=100)]
    temp_c: float
    solar_irradiance_index: Annotated[float, Field(ge=0.0, le=1.0)]


# ---------------------------------------------------------------------------
# Speed-layer output
# ---------------------------------------------------------------------------
class ZoneMetric(_Frozen):
    """A windowed per-zone aggregate. Row of ``rt_zone_metrics``."""

    grid_zone: str
    window_type: str  # "tumbling_1m" | "sliding_5m"
    window_start: dt.datetime
    window_end: dt.datetime
    total_consumption_kwh: NonNegFloat
    total_solar_kwh: NonNegFloat
    renewable_pct: Percent
    active_meter_count: Annotated[int, Field(ge=0)]
    computed_at: dt.datetime


# ---------------------------------------------------------------------------
# Batch-layer output
# ---------------------------------------------------------------------------
class HouseholdBill(_Frozen):
    """A household's bill for one business day. Row of ``daily_household_billing``."""

    business_date: dt.date
    household_id: str
    grid_zone: str
    total_consumption_kwh: NonNegFloat
    total_solar_kwh: NonNegFloat
    self_consumed_kwh: NonNegFloat
    grid_import_kwh: NonNegFloat
    solar_export_kwh: NonNegFloat
    tariff_rate: Decimal
    billing_tier: BillingTier
    subsidy_flag: bool
    energy_charge: Decimal
    solar_credit: Decimal
    net_amount: Decimal
    currency: str = "LKR"
    computed_at: dt.datetime


class ZoneSolarSummary(_Frozen):
    """Per-zone daily solar contribution. Row of ``daily_zone_solar_summary``."""

    business_date: dt.date
    grid_zone: str
    total_consumption_kwh: NonNegFloat
    total_solar_generation_kwh: NonNegFloat
    renewable_pct: Percent
    household_count: Annotated[int, Field(ge=0)]
    computed_at: dt.datetime


# ---------------------------------------------------------------------------
# Operations / observability
# ---------------------------------------------------------------------------
class Alert(_Frozen):
    """A fired alert. Row of ``alerts``."""

    alert_id: str
    rule_name: str
    severity: AlertSeverity
    message: str
    grid_zone: str | None = None
    business_date: dt.date | None = None
    context: dict[str, str] = Field(default_factory=dict)
    fired_at: dt.datetime
    resolved_at: dt.datetime | None = None

    def dedupe_key(self) -> tuple[str, str | None, dt.date | None]:
        """Identity used to avoid re-firing an unresolved alert."""
        return (self.rule_name, self.grid_zone, self.business_date)


class BatchRun(_Frozen):
    """An orchestrated batch-layer run. Row of ``batch_runs``."""

    run_id: str
    dag_id: str
    business_date: dt.date
    status: BatchRunStatus
    rows_written: int = 0
    started_at: dt.datetime
    finished_at: dt.datetime | None = None
    notes: str | None = None


__all__ = [
    "GRID_ZONES",
    "READING_SCHEMA_VERSION",
    "Alert",
    "AlertSeverity",
    "BatchRun",
    "BatchRunStatus",
    "BillingTier",
    "HouseholdBill",
    "MeterReading",
    "TariffRecord",
    "WeatherForecast",
    "ZoneMetric",
    "ZoneSolarSummary",
]
