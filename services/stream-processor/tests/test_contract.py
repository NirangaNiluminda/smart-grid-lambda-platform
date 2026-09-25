"""Contract-level tests for stream-processor's pipeline glue.

The windowing/validation/DLQ *logic* is exercised in `test_spark_transforms.py`
against a local SparkSession (no Kafka needed) — that's where the acceptance
criteria from `docs/PART2_ONBOARDING.md §B1` are actually checked. This file
covers the small pieces of `application/pipeline.py` that don't need Spark.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Row
from stream_processor.application.pipeline import _count_by_reason, _row_to_zone_metric


def test_row_to_zone_metric_maps_fields_and_clamps_negatives() -> None:
    row = Row(
        grid_zone="zone-north",
        window_type="tumbling_1m",
        window_start=dt.datetime(2026, 1, 1, 0, 0, tzinfo=dt.UTC),
        window_end=dt.datetime(2026, 1, 1, 0, 1, tzinfo=dt.UTC),
        total_consumption_kwh=-0.5,  # Spark shouldn't produce this, but be defensive
        total_solar_kwh=1.2,
        renewable_pct=42.0,
        active_meter_count=3,
        computed_at=dt.datetime(2026, 1, 1, 0, 1, 5, tzinfo=dt.UTC),
    )
    metric = _row_to_zone_metric(row)
    assert metric.grid_zone == "zone-north"
    assert metric.total_consumption_kwh == 0.0  # clamped
    assert metric.active_meter_count == 3


def test_count_by_reason_splits_parse_errors_from_validation_failures() -> None:
    rows = [
        Row(parse_ok=False),
        Row(parse_ok=False),
        Row(parse_ok=True),
    ]
    counts = dict(_count_by_reason(rows))
    assert counts == {"parse_error": 2, "validation_failed": 1}
