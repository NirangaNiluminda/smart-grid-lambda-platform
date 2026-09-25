"""SparkSession factory for the batch layer.

No Kafka connector needed here — the batch job only reads Parquet (the lake)
and CSV (the daily reference feed), both natively supported by Spark. Runs in
**local mode** inside the container; see `docs/ARCHITECTURE.md`'s
production-scale notes for the cluster-mode equivalent.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from batch_processor.config import Settings


def build_spark_session(settings: Settings) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(settings.service_name)
        .master(settings.spark_master)
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
