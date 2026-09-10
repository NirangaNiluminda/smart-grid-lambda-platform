"""Pure per-reading transforms shared by the speed layer and the batch layer.

**Why this module exists:** Lambda's classic weakness is two code paths that drift.
Both the Spark Structured Streaming job (speed) and the Spark batch job (batch)
import these functions, so the validation and energy-balance maths are defined
exactly once. Everything here is a pure function of its arguments — no I/O, no
Spark, no clock — so it is trivially unit-tested and can be called from a Spark
UDF or a plain loop alike.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from energy_common.domain.models import MeterReading

# A household consuming/generating more than this in one short interval is
# physically implausible for a domestic smart meter -> treat as a spike.
MAX_PLAUSIBLE_KWH_PER_INTERVAL = 5.0


class RejectReason(StrEnum):
    NEGATIVE_CONSUMPTION = "negative_consumption"
    NEGATIVE_SOLAR = "negative_solar"
    IMPLAUSIBLE_SPIKE = "implausible_spike"
    STALE_TIMESTAMP = "stale_timestamp"


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """Result of applying business validation to a structurally valid reading."""

    ok: bool
    reason: RejectReason | None = None

    @classmethod
    def accepted(cls) -> ValidationOutcome:
        return cls(ok=True)

    @classmethod
    def rejected(cls, reason: RejectReason) -> ValidationOutcome:
        return cls(ok=False, reason=reason)


def validate_reading(reading: MeterReading) -> ValidationOutcome:
    """Business-rule validation. Structurally-broken payloads never reach here;
    this catches values that parse but cannot be trusted in an aggregate.

    Rejected readings are routed to the DLQ by the caller. The batch layer's
    data-quality gate uses the same function so both layers agree on what "bad"
    means.
    """
    if reading.power_consumption_kwh < 0:
        return ValidationOutcome.rejected(RejectReason.NEGATIVE_CONSUMPTION)
    if reading.solar_generation_kwh < 0:
        return ValidationOutcome.rejected(RejectReason.NEGATIVE_SOLAR)
    per_interval_cap = MAX_PLAUSIBLE_KWH_PER_INTERVAL * (reading.reading_interval_seconds / 2)
    if reading.power_consumption_kwh > per_interval_cap:
        return ValidationOutcome.rejected(RejectReason.IMPLAUSIBLE_SPIKE)
    if reading.solar_generation_kwh > per_interval_cap:
        return ValidationOutcome.rejected(RejectReason.IMPLAUSIBLE_SPIKE)
    return ValidationOutcome.accepted()


def renewable_pct(total_consumption_kwh: float, total_solar_kwh: float) -> float:
    """Solar as a percentage of consumption, clamped to [0, 100].

    Values above 100 (a zone exporting more than it consumes in the window) are
    clamped so the metric stays a "share of demand met by solar".
    """
    if total_consumption_kwh <= 0:
        return 0.0
    pct = 100.0 * total_solar_kwh / total_consumption_kwh
    return max(0.0, min(100.0, pct))


@dataclass(frozen=True, slots=True)
class EnergyBalance:
    """How a household's gross consumption and solar split for billing."""

    self_consumed_kwh: float
    grid_import_kwh: float
    solar_export_kwh: float


def energy_balance(total_consumption_kwh: float, total_solar_kwh: float) -> EnergyBalance:
    """Net-metering split used by the billing calculation.

    - solar first offsets on-site consumption (``self_consumed``)
    - the shortfall is drawn from the grid (``grid_import``, what the tariff charges)
    - any surplus solar is exported (``solar_export``, what earns the feed-in credit)
    """
    consumption = max(0.0, total_consumption_kwh)
    solar = max(0.0, total_solar_kwh)
    self_consumed = min(consumption, solar)
    grid_import = consumption - self_consumed
    solar_export = solar - self_consumed
    return EnergyBalance(
        self_consumed_kwh=self_consumed,
        grid_import_kwh=grid_import,
        solar_export_kwh=solar_export,
    )
