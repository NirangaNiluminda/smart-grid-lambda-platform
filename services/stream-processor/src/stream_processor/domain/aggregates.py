"""Pure windowing and aggregation logic for stream-processor.

No Spark or I/O imports here. Fully unit-testable with standard Python types.
Shared transforms (validate_reading, renewable_pct) are imported from energy_common.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from typing import NamedTuple

from energy_common.domain.models import MeterReading, ZoneMetric
from energy_common.domain.transforms import (
    ValidationOutcome,
    renewable_pct,
    validate_reading,
)


class WindowKey(NamedTuple):
    grid_zone: str
    window_type: str  # "tumbling_1m" | "sliding_5m"
    window_start: dt.datetime
    window_end: dt.datetime


@dataclass
class WindowAccumulator:
    total_consumption_kwh: float = 0.0
    total_solar_kwh: float = 0.0
    meter_ids: set[str] = field(default_factory=set)

    def add(self, reading: MeterReading) -> None:
        self.total_consumption_kwh += reading.power_consumption_kwh
        self.total_solar_kwh += reading.solar_generation_kwh
        self.meter_ids.add(reading.meter_id)

    def to_zone_metric(self, key: WindowKey, computed_at: dt.datetime) -> ZoneMetric:
        pct = renewable_pct(self.total_consumption_kwh, self.total_solar_kwh)
        return ZoneMetric(
            grid_zone=key.grid_zone,
            window_type=key.window_type,
            window_start=key.window_start,
            window_end=key.window_end,
            total_consumption_kwh=round(self.total_consumption_kwh, 4),
            total_solar_kwh=round(self.total_solar_kwh, 4),
            renewable_pct=round(pct, 2),
            active_meter_count=len(self.meter_ids),
            computed_at=computed_at,
        )


def floor_timestamp_to_seconds(ts: dt.datetime, seconds: int) -> dt.datetime:
    """Floor a timezone-aware timestamp to the nearest interval in seconds."""
    epoch = int(ts.timestamp())
    floored_epoch = (epoch // seconds) * seconds
    return dt.datetime.fromtimestamp(floored_epoch, tz=ts.tzinfo or dt.UTC)


def compute_tumbling_window(
    ts: dt.datetime, window_seconds: int = 60
) -> tuple[dt.datetime, dt.datetime]:
    start = floor_timestamp_to_seconds(ts, window_seconds)
    end = start + dt.timedelta(seconds=window_seconds)
    return start, end


def compute_sliding_windows(
    ts: dt.datetime, window_seconds: int = 300, slide_seconds: int = 60
) -> list[tuple[dt.datetime, dt.datetime]]:
    """Compute all overlapping sliding windows of length window_seconds that cover ts.

    For example, with 300s window and 60s slide, an event belongs to 5 windows.
    """
    last_start = floor_timestamp_to_seconds(ts, slide_seconds)
    windows: list[tuple[dt.datetime, dt.datetime]] = []
    num_windows = window_seconds // slide_seconds

    for i in range(num_windows):
        start = last_start - dt.timedelta(seconds=i * slide_seconds)
        end = start + dt.timedelta(seconds=window_seconds)
        if start <= ts < end:
            windows.append((start, end))

    return windows


@dataclass
class ProcessingResult:
    metrics: list[ZoneMetric]
    rejected: list[tuple[MeterReading, ValidationOutcome]]
    dropped_late_count: int


class StreamAggregator:
    """Stateful or micro-batch aggregator enforcing watermark, validation and windowing."""

    def __init__(
        self,
        *,
        watermark_minutes: int = 2,
        tumbling_window_seconds: int = 60,
        sliding_window_seconds: int = 300,
        sliding_slide_seconds: int = 60,
    ) -> None:
        self.watermark_delta = dt.timedelta(minutes=watermark_minutes)
        self.tumbling_window_seconds = tumbling_window_seconds
        self.sliding_window_seconds = sliding_window_seconds
        self.sliding_slide_seconds = sliding_slide_seconds
        self.max_seen_timestamp: dt.datetime | None = None

    def process_batch(
        self, readings: list[MeterReading], computed_at: dt.datetime | None = None
    ) -> ProcessingResult:
        if computed_at is None:
            computed_at = dt.datetime.now(dt.UTC)

        # 1. Update watermark based on max observed timestamp
        for r in readings:
            if self.max_seen_timestamp is None or r.event_timestamp > self.max_seen_timestamp:
                self.max_seen_timestamp = r.event_timestamp

        watermark_cutoff = (
            (self.max_seen_timestamp - self.watermark_delta)
            if self.max_seen_timestamp is not None
            else None
        )

        accumulators: dict[WindowKey, WindowAccumulator] = defaultdict(WindowAccumulator)
        rejected: list[tuple[MeterReading, ValidationOutcome]] = []
        dropped_late = 0

        for r in readings:
            # Check watermark (late data dropped)
            if watermark_cutoff is not None and r.event_timestamp < watermark_cutoff:
                dropped_late += 1
                continue

            # Business validation
            outcome = validate_reading(r)
            if not outcome.ok:
                rejected.append((r, outcome))
                continue

            # Tumbling window
            t_start, t_end = compute_tumbling_window(
                r.event_timestamp, self.tumbling_window_seconds
            )
            t_key = WindowKey(
                grid_zone=r.grid_zone,
                window_type="tumbling_1m",
                window_start=t_start,
                window_end=t_end,
            )
            accumulators[t_key].add(r)

            # Sliding windows
            for s_start, s_end in compute_sliding_windows(
                r.event_timestamp, self.sliding_window_seconds, self.sliding_slide_seconds
            ):
                s_key = WindowKey(
                    grid_zone=r.grid_zone,
                    window_type="sliding_5m",
                    window_start=s_start,
                    window_end=s_end,
                )
                accumulators[s_key].add(r)

        metrics = [
            acc.to_zone_metric(key, computed_at)
            for key, acc in sorted(accumulators.items(), key=lambda item: item[0])
        ]

        return ProcessingResult(
            metrics=metrics,
            rejected=rejected,
            dropped_late_count=dropped_late,
        )
