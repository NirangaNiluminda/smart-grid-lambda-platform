"""SparkSession factory.

The Kafka connector (`spark-sql-kafka-0-10`) is pulled via Maven coordinates at
startup (`spark.jars.packages`) rather than baked into the image, so the
version stays pinned to the installed PySpark version in one place
(`pyproject.toml`). The image needs outbound network access the first time a
container starts (Spark/Ivy caches the jars under `~/.ivy2` after that).
"""

from __future__ import annotations

import os
import sys

import pyspark
from pyspark.sql import SparkSession

from stream_processor.config import Settings

_KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"


def build_spark_session(settings: Settings) -> SparkSession:
    # UDFs in domain/spark_transforms.py import energy_common; the worker
    # subprocess must run the *same* interpreter the driver imported it from,
    # or it resolves `python3` off PATH and can miss the package entirely.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(settings.service_name)
        .master(settings.spark_master)
        .config("spark.jars.packages", _KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
