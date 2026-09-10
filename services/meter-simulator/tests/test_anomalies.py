from __future__ import annotations

import datetime as dt
import random

from energy_common.domain.models import MeterReading
from meter_simulator.domain.anomalies import AnomalyInjector, AnomalyKind


def _reading() -> MeterReading:
    return MeterReading(
        event_id="e",
        meter_id="MTR-1",
        household_id="HH-1",
        grid_zone="zone-north",
        power_consumption_kwh=0.05,
        solar_generation_kwh=0.01,
        event_timestamp=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    )


def test_rate_zero_never_injects() -> None:
    inj = AnomalyInjector(0.0)
    rng = random.Random(1)
    for _ in range(500):
        out = inj.maybe_apply(_reading(), rng)
        assert out.kind is None and out.reading == _reading()


def test_rate_one_always_injects() -> None:
    inj = AnomalyInjector(1.0)
    rng = random.Random(1)
    kinds = {inj.maybe_apply(_reading(), rng).kind for _ in range(200)}
    assert None not in kinds
    assert kinds <= set(AnomalyKind)


def test_dropout_suppresses_reading() -> None:
    from meter_simulator.domain.anomalies import _Marker

    inj = AnomalyInjector(1.0, strategies=[_Marker(kind=AnomalyKind.DROPOUT, weight=1.0)])
    out = inj.maybe_apply(_reading(), random.Random(0))
    assert out.reading is None and out.kind is AnomalyKind.DROPOUT and out.is_marker


def test_negative_strategy_makes_consumption_negative() -> None:
    from meter_simulator.domain.anomalies import NegativeConsumption

    inj = AnomalyInjector(1.0, strategies=[NegativeConsumption()])
    out = inj.maybe_apply(_reading(), random.Random(0))
    assert out.reading is not None and out.reading.power_consumption_kwh < 0


def test_spike_strategy_inflates_consumption() -> None:
    from meter_simulator.domain.anomalies import ConsumptionSpike

    inj = AnomalyInjector(1.0, strategies=[ConsumptionSpike()])
    out = inj.maybe_apply(_reading(), random.Random(0))
    assert out.reading is not None and out.reading.power_consumption_kwh > 3.0


def test_invalid_rate_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        AnomalyInjector(1.5)
