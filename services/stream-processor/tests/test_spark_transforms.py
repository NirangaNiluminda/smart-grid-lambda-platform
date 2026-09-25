"""Batch-mode tests for the speed-layer Spark transforms.

Run against a local `SparkSession` (see `conftest.py`) — no Kafka needed. These
are the load-bearing tests for the bug this rewrite fixes: window aggregation
must reflect the *full* accumulated total for a window, not one micro-batch's
partial contribution.
"""

from __future__ import annotations

import json

import pytest
from stream_processor.domain.spark_transforms import (
    aggregate_window,
    parse_kafka_readings,
    renewable_pct_column,
    split_valid_invalid,
    to_dlq_envelope,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _kafka_row(spark, payload: dict, partition: int = 0, offset: int = 0):
    value = json.dumps(payload).encode("utf-8")
    return spark.createDataFrame([(value, partition, offset)], ["value", "partition", "offset"])


def _reading(**overrides: object) -> dict:
    base = {
        "schema_version": "1.0",
        "event_id": "e1",
        "meter_id": "MTR-1",
        "household_id": "HH-1",
        "grid_zone": "zone-north",
        "power_consumption_kwh": 0.5,
        "solar_generation_kwh": 0.1,
        "reading_interval_seconds": 2,
        "event_timestamp": "2026-01-01T00:00:10Z",
    }
    base.update(overrides)
    return base


def _union(spark, dfs):
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d)
    return out


class TestParseAndValidate:
    def test_valid_reading_parses_and_passes_validation(self, spark) -> None:
        df = _kafka_row(spark, _reading())
        parsed = parse_kafka_readings(df)
        valid, invalid = split_valid_invalid(parsed)
        assert valid.count() == 1
        assert invalid.count() == 0

    def test_negative_consumption_is_invalid_not_aggregated(self, spark) -> None:
        df = _kafka_row(spark, _reading(power_consumption_kwh=-3.0))
        parsed = parse_kafka_readings(df)
        valid, invalid = split_valid_invalid(parsed)
        assert valid.count() == 0
        assert invalid.count() == 1

    def test_malformed_json_is_invalid_with_parse_error_reason(self, spark) -> None:
        df = spark.createDataFrame([(b"{not json", 0, 0)], ["value", "partition", "offset"])
        parsed = parse_kafka_readings(df)
        _, invalid = split_valid_invalid(parsed)
        assert invalid.count() == 1
        assert invalid.collect()[0]["parse_ok"] is False

    def test_dlq_envelope_carries_reason_and_raw_bytes(self, spark) -> None:
        df = _kafka_row(spark, _reading(power_consumption_kwh=-3.0))
        parsed = parse_kafka_readings(df)
        _, invalid = split_valid_invalid(parsed)
        envelope = json.loads(to_dlq_envelope(invalid).collect()[0]["value"])
        assert envelope["reason"] == "validation_failed"
        assert envelope["source_partition"] == 0
        assert envelope.get("raw")


class TestWindowedAggregation:
    def test_valid_reading_contributes_to_its_zone_window(self, spark) -> None:
        rows = [
            _reading(
                event_id="a", meter_id="m1", power_consumption_kwh=1.0, solar_generation_kwh=0.2
            ),
            _reading(
                event_id="b", meter_id="m2", power_consumption_kwh=2.0, solar_generation_kwh=0.1
            ),
        ]
        parsed = _union(spark, [_kafka_row(spark, r) for r in rows])
        valid, _ = split_valid_invalid(parse_kafka_readings(parsed))
        agg = aggregate_window(
            valid,
            window_type="tumbling_1m",
            window_duration="1 minute",
            slide_duration=None,
            watermark="2 minutes",
        )
        result = agg.collect()
        assert len(result) == 1
        row = result[0]
        assert row["grid_zone"] == "zone-north"
        assert row["total_consumption_kwh"] == pytest.approx(3.0)
        assert row["total_solar_kwh"] == pytest.approx(0.3)

    def test_accumulates_full_window_total_across_many_micro_batches(self, spark) -> None:
        """The bug this rewrite fixes: aggregating the union of several
        'micro-batches' worth of readings for one window must yield the SUM of
        all of them, not just the last batch's contribution."""
        batches = [
            [
                _reading(event_id=f"b1-{i}", meter_id=f"m{i}", power_consumption_kwh=1.0)
                for i in range(5)
            ],
            [
                _reading(event_id=f"b2-{i}", meter_id=f"m{i + 5}", power_consumption_kwh=1.0)
                for i in range(5)
            ],
            [
                _reading(event_id=f"b3-{i}", meter_id=f"m{i + 10}", power_consumption_kwh=1.0)
                for i in range(5)
            ],
        ]
        all_rows = [r for batch in batches for r in batch]
        parsed = _union(spark, [_kafka_row(spark, r) for r in all_rows])
        valid, _ = split_valid_invalid(parse_kafka_readings(parsed))
        agg = aggregate_window(
            valid,
            window_type="tumbling_1m",
            window_duration="1 minute",
            slide_duration=None,
            watermark="2 minutes",
        )
        row = agg.collect()[0]
        # 15 readings of 1.0 kWh each, spread across "3 micro-batches" -> full 15.0, not 5.0
        assert row["total_consumption_kwh"] == pytest.approx(15.0)
        assert row["active_meter_count"] == 15

    def test_active_meter_count_is_distinct_not_a_reading_count(self, spark) -> None:
        rows = [
            _reading(event_id="a", meter_id="m1", event_timestamp="2026-01-01T00:00:05Z"),
            _reading(event_id="b", meter_id="m1", event_timestamp="2026-01-01T00:00:15Z"),
            _reading(event_id="c", meter_id="m2", event_timestamp="2026-01-01T00:00:25Z"),
        ]
        parsed = _union(spark, [_kafka_row(spark, r) for r in rows])
        valid, _ = split_valid_invalid(parse_kafka_readings(parsed))
        agg = aggregate_window(
            valid,
            window_type="tumbling_1m",
            window_duration="1 minute",
            slide_duration=None,
            watermark="2 minutes",
        )
        row = agg.collect()[0]
        assert row["active_meter_count"] == 2  # not 3

    def test_separate_zones_get_separate_window_rows(self, spark) -> None:
        rows = [_reading(grid_zone="zone-north"), _reading(grid_zone="zone-south", event_id="e2")]
        parsed = _union(spark, [_kafka_row(spark, r) for r in rows])
        valid, _ = split_valid_invalid(parse_kafka_readings(parsed))
        agg = aggregate_window(
            valid,
            window_type="tumbling_1m",
            window_duration="1 minute",
            slide_duration=None,
            watermark="2 minutes",
        )
        zones = {r["grid_zone"] for r in agg.collect()}
        assert zones == {"zone-north", "zone-south"}

    def test_sliding_window_fans_one_event_into_overlapping_windows(self, spark) -> None:
        df = _kafka_row(spark, _reading(event_timestamp="2026-01-01T00:02:30Z"))
        valid, _ = split_valid_invalid(parse_kafka_readings(df))
        agg = aggregate_window(
            valid,
            window_type="sliding_5m",
            window_duration="5 minutes",
            slide_duration="1 minute",
            watermark="2 minutes",
        )
        # An event at 00:02:30 falls inside 5 overlapping 5-minute/1-minute-slide windows.
        assert agg.count() == 5

    def test_renewable_pct_matches_shared_transform(self, spark) -> None:
        from energy_common.domain.transforms import renewable_pct

        df = spark.createDataFrame([(10.0, 4.0), (0.0, 5.0), (2.0, 20.0)], ["cons", "solar"])
        out = df.withColumn("pct", renewable_pct_column("cons", "solar")).collect()
        for row in out:
            assert row["pct"] == pytest.approx(renewable_pct(row["cons"], row["solar"]))
