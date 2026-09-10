"""Anomaly injection — Strategy pattern.

Each anomaly is a small strategy with a relative weight. The injector picks one
(or none) per reading according to ``anomaly_rate``. The consumers downstream are
*expected* to cope with every anomaly here — see ``docs/DATA_CONTRACTS.md §1``.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from energy_common.domain.models import MeterReading


class AnomalyKind(StrEnum):
    DROPOUT = "dropout"  # reading suppressed entirely (handled in the loop)
    NEGATIVE = "negative"
    SPIKE = "spike"
    STUCK = "stuck"  # value pinned; realised in the loop via last-value cache
    ZERO = "zero"


class AnomalyStrategy(Protocol):
    @property
    def kind(self) -> AnomalyKind: ...

    @property
    def weight(self) -> float: ...

    def apply(self, reading: MeterReading, rng: random.Random) -> MeterReading: ...


@dataclass(frozen=True, slots=True)
class NegativeConsumption:
    kind: AnomalyKind = AnomalyKind.NEGATIVE
    weight: float = 1.0

    def apply(self, reading: MeterReading, rng: random.Random) -> MeterReading:
        return replace_reading(
            reading, power_consumption_kwh=-abs(reading.power_consumption_kwh) - 0.05
        )


@dataclass(frozen=True, slots=True)
class ConsumptionSpike:
    kind: AnomalyKind = AnomalyKind.SPIKE
    weight: float = 1.5

    def apply(self, reading: MeterReading, rng: random.Random) -> MeterReading:
        factor = rng.uniform(10.0, 45.0)
        return replace_reading(
            reading, power_consumption_kwh=reading.power_consumption_kwh * factor + 3.0
        )


@dataclass(frozen=True, slots=True)
class ZeroReading:
    kind: AnomalyKind = AnomalyKind.ZERO
    weight: float = 1.0

    def apply(self, reading: MeterReading, rng: random.Random) -> MeterReading:
        return replace_reading(reading, power_consumption_kwh=0.0, solar_generation_kwh=0.0)


def replace_reading(reading: MeterReading, **changes: float) -> MeterReading:
    """`dataclasses.replace`-style helper for the frozen pydantic model."""
    return reading.model_copy(update=changes)


# Dropout and stuck-meter need loop-level state, so they are represented as
# "markers": the injector returns the reading unchanged plus the kind, and the
# loop decides what to do (suppress / repeat last value).
_MARKER_KINDS = frozenset({AnomalyKind.DROPOUT, AnomalyKind.STUCK})


@dataclass(frozen=True, slots=True)
class _Marker:
    kind: AnomalyKind
    weight: float

    def apply(self, reading: MeterReading, rng: random.Random) -> MeterReading:
        return reading


@dataclass(frozen=True, slots=True)
class AnomalyOutcome:
    reading: MeterReading | None  # None => suppress (dropout)
    kind: AnomalyKind | None
    is_marker: bool = False


class AnomalyInjector:
    """Applies at most one anomaly per reading at rate ``anomaly_rate``."""

    def __init__(
        self, anomaly_rate: float, strategies: list[AnomalyStrategy] | None = None
    ) -> None:
        if not 0.0 <= anomaly_rate <= 1.0:
            raise ValueError("anomaly_rate must be in [0, 1]")
        self._rate = anomaly_rate
        self._strategies: list[AnomalyStrategy] = strategies or [
            NegativeConsumption(),
            ConsumptionSpike(),
            ZeroReading(),
            _Marker(kind=AnomalyKind.DROPOUT, weight=1.5),
            _Marker(kind=AnomalyKind.STUCK, weight=1.0),
        ]
        self._weights = [s.weight for s in self._strategies]

    def maybe_apply(self, reading: MeterReading, rng: random.Random) -> AnomalyOutcome:
        if rng.random() >= self._rate:
            return AnomalyOutcome(reading=reading, kind=None)
        strategy = rng.choices(self._strategies, weights=self._weights, k=1)[0]
        if strategy.kind == AnomalyKind.DROPOUT:
            return AnomalyOutcome(reading=None, kind=AnomalyKind.DROPOUT, is_marker=True)
        if strategy.kind in _MARKER_KINDS:
            return AnomalyOutcome(reading=reading, kind=strategy.kind, is_marker=True)
        return AnomalyOutcome(reading=strategy.apply(reading, rng), kind=strategy.kind)
