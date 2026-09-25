"""Spark DataFrame transforms for the speed layer.

Every function here takes and returns a `DataFrame` and has no Kafka/Postgres/
checkpoint knowledge — the streaming wiring in `application/pipeline.py` is a
thin adapter around these. That split is what makes the aggregation logic
testable in Spark's ordinary *batch* mode (`tests/test_spark_transforms.py`
runs these against a local `SparkSession`, no Kafka needed) while the same
functions run unmodified inside a real structured-streaming query.

**Why real windowed state fixes the old bug:** Spark's `groupBy(window(...))`
over a watermarked, streaming DataFrame keeps aggregation state *per window* in
its own state store across micro-batches, and only forgets a window once the
watermark has passed it. `outputMode("update")` emits the *current accumulated
total* for a window every time new data arrives for it — never a partial,
single-micro-batch total. That is the property the hand-rolled Python
accumulator (removed) got wrong: it reset its dict on every call, so each
micro-batch's upsert silently replaced the running total instead of adding to
it. Using Spark's native stateful aggregation removes the bug by construction.
"""

from __future__ import annotations

import datetime as dt

from energy_common.domain.models import MeterReading
from energy_common.domain.transforms import validate_reading
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

from stream_processor.domain.schema import READING_SCHEMA

_REQUIRED_FIELDS = (
    "event_id",
    "meter_id",
    "household_id",
    "grid_zone",
    "power_consumption_kwh",
    "solar_generation_kwh",
    "event_timestamp",
)


def parse_kafka_readings(raw: DataFrame) -> DataFrame:
    """Kafka rows (`key`, `value`, `partition`, `offset`, ...) -> parsed reading
    columns, keeping `partition`/`offset` for DLQ provenance.

    Spark's `from_json` runs in PERMISSIVE mode: malformed JSON (or a JSON
    value that isn't even an object) still produces a non-null struct with
    every field set to null, rather than a null struct — so "parse succeeded"
    has to be judged by whether the required fields actually came back
    populated, not by null-checking the struct itself.
    """
    json_col = F.col("value").cast("string")
    parsed = raw.select(
        F.from_json(json_col, READING_SCHEMA).alias("data"),
        json_col.alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    required_present = F.lit(True)
    for field in _REQUIRED_FIELDS:
        required_present = required_present & F.col(f"data.{field}").isNotNull()

    return parsed.select(
        "data.*",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        required_present.alias("parse_ok"),
    ).withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))


def _validate_row(
    power: float | None,
    solar: float | None,
    interval: int | None,
    event_ts: dt.datetime | None,
) -> bool:
    """Row-level validity check. Wraps the real
    `energy_common.domain.transforms.validate_reading` so the speed layer and
    the batch layer can never silently disagree about what counts as valid —
    see that module's docstring for the rationale."""
    if power is None or solar is None or interval is None or event_ts is None:
        return False
    reading = MeterReading(
        event_id="udf",
        meter_id="udf",
        household_id="udf",
        grid_zone="zone-north",  # placeholder: validate_reading doesn't use it
        power_consumption_kwh=float(power),
        solar_generation_kwh=float(solar),
        reading_interval_seconds=int(interval),
        event_timestamp=event_ts if event_ts.tzinfo else event_ts.replace(tzinfo=dt.UTC),
    )
    return validate_reading(reading).ok


_validate_row_udf = F.udf(_validate_row, BooleanType())


def split_valid_invalid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Structurally-parseable rows -> (business-valid, business-invalid).

    Rows that failed to parse at all (`parse_ok` false) are treated as invalid
    too, so every row lands in exactly one of the two outputs.
    """
    is_valid: Column = F.col("parse_ok") & _validate_row_udf(
        "power_consumption_kwh",
        "solar_generation_kwh",
        "reading_interval_seconds",
        "event_timestamp",
    )
    valid = parsed.filter(is_valid)
    invalid = parsed.filter(~is_valid)
    return valid, invalid


def to_dlq_envelope(invalid: DataFrame) -> DataFrame:
    """Business/parse-invalid rows -> the DLQ wire envelope
    (`docs/DATA_CONTRACTS.md §1`), ready to write to `meter.readings.dlq`."""
    reason = F.when(~F.col("parse_ok"), F.lit("parse_error")).otherwise(F.lit("validation_failed"))
    envelope = F.to_json(
        F.struct(
            reason.alias("reason"),
            F.lit("rejected by stream-processor").alias("error"),
            F.base64(F.col("raw_value").cast("binary")).alias("raw"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                "received_at"
            ),
            F.col("kafka_partition").alias("source_partition"),
            F.col("kafka_offset").alias("source_offset"),
        )
    )
    return invalid.select(envelope.cast(StringType()).alias("value"))


def renewable_pct_column(consumption_col: str, solar_col: str) -> Column:
    """Spark-column equivalent of `energy_common.domain.transforms.renewable_pct`
    (0 when consumption <= 0, else solar/consumption*100 clamped to [0, 100])."""
    cons = F.col(consumption_col)
    sol = F.col(solar_col)
    pct = F.when(cons > 0, sol * F.lit(100.0) / cons).otherwise(F.lit(0.0))
    return F.greatest(F.lit(0.0), F.least(F.lit(100.0), pct))


def aggregate_window(
    valid: DataFrame,
    *,
    window_type: str,
    window_duration: str,
    slide_duration: str | None,
    watermark: str,
) -> DataFrame:
    """Windowed per-zone aggregation. `window_duration`/`slide_duration`/
    `watermark` are Spark interval strings, e.g. ``"1 minute"``, ``"5 minutes"``.
    A `None` slide gives a tumbling window; a slide shorter than the duration
    gives an overlapping sliding window (Spark natively fans one event out to
    every window it falls in — no manual enumeration needed).
    """
    watermarked = valid.withWatermark("event_timestamp", watermark)
    window_col = (
        F.window(F.col("event_timestamp"), window_duration, slide_duration)
        if slide_duration
        else F.window(F.col("event_timestamp"), window_duration)
    )
    agg = watermarked.groupBy(F.col("grid_zone"), window_col.alias("window")).agg(
        F.sum("power_consumption_kwh").alias("total_consumption_kwh"),
        F.sum("solar_generation_kwh").alias("total_solar_kwh"),
        F.approx_count_distinct("meter_id").alias("active_meter_count"),
    )
    return (
        agg.withColumn(
            "renewable_pct", renewable_pct_column("total_consumption_kwh", "total_solar_kwh")
        )
        .withColumn("window_type", F.lit(window_type))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "grid_zone",
            "window_type",
            "window_start",
            "window_end",
            "total_consumption_kwh",
            "total_solar_kwh",
            "renewable_pct",
            "active_meter_count",
            "computed_at",
        )
    )
