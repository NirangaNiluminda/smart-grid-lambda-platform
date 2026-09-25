"""Session-scoped local SparkSession for batch-mode testing of the pure
transforms in `domain/spark_transforms.py`.

These tests never touch Kafka or a streaming query — `groupBy(window(...))`
behaves identically over a static (batch) DataFrame as it does inside a
streaming one; only the incremental *emission* across micro-batches is
streaming-specific, and that is Spark's own well-tested machinery, not
project code. See that module's docstring.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("stream-processor-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
