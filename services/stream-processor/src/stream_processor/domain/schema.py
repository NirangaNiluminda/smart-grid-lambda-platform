"""Spark schema for the Kafka wire format.

Mirrors `energy_common.domain.models.MeterReading` field-for-field (see
`docs/DATA_CONTRACTS.md §1`). Kept separate from the pydantic model because Spark
needs its own `StructType`, not a pydantic schema — but the *fields* must match
exactly, which the round-trip test in `tests/test_spark_transforms.py` checks.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

READING_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), nullable=True),
        StructField("event_id", StringType(), nullable=True),
        StructField("meter_id", StringType(), nullable=True),
        StructField("household_id", StringType(), nullable=True),
        StructField("grid_zone", StringType(), nullable=True),
        StructField("power_consumption_kwh", DoubleType(), nullable=True),
        StructField("solar_generation_kwh", DoubleType(), nullable=True),
        StructField("reading_interval_seconds", IntegerType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),  # parsed downstream
    ]
)
