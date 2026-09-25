"""Speed-layer streaming pipeline — composes the pure transforms in
`domain/spark_transforms.py` into three independent Structured Streaming
queries, each with its own Kafka read, state store and checkpoint:

    meter.readings ──▶ parse ──▶ split(valid, invalid) ──▶ DLQ + metrics   [query 1]
    meter.readings ──▶ parse ──▶ valid ──▶ window(1m tumbling)  ──▶ rt_zone_metrics  [query 2]
    meter.readings ──▶ parse ──▶ valid ──▶ window(5m/1m sliding) ──▶ rt_zone_metrics [query 3]

Running the aggregations as separate queries lets each keep its own state
store, so a window's upserted total is always the *full* accumulated sum for
that window so far (Spark's stateful streaming aggregation), not a single
micro-batch's partial contribution — see `spark_transforms.py`'s module
docstring for why that distinction is the whole point.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from energy_common import get_logger
from energy_common.domain.models import ZoneMetric
from energy_common.observability import counter, gauge, histogram
from pyspark.sql import DataFrame, Row, SparkSession

from stream_processor.adapters.repository import PostgresZoneMetricsRepository
from stream_processor.config import Settings
from stream_processor.domain.spark_transforms import (
    aggregate_window,
    parse_kafka_readings,
    split_valid_invalid,
    to_dlq_envelope,
)

logger = get_logger("stream-processor")

RECORDS_CONSUMED = counter(
    "records_consumed_total", "Meter readings consumed by the speed layer", ("service",)
)
RECORDS_REJECTED = counter(
    "records_rejected_total", "Readings routed to the DLQ by the speed layer", ("service", "reason")
)
MICROBATCH_DURATION = histogram(
    "microbatch_duration_seconds", "Duration of a speed-layer micro-batch", ("stage",)
)
CONSUMER_LAG = gauge(
    "consumer_lag", "Rows seen in the most recent micro-batch (proxy for backlog)", ("stage",)
)

_SERVICE = "stream-processor"


def _row_to_zone_metric(row: Row) -> ZoneMetric:
    return ZoneMetric(
        grid_zone=row["grid_zone"],
        window_type=row["window_type"],
        window_start=row["window_start"],
        window_end=row["window_end"],
        total_consumption_kwh=max(0.0, float(row["total_consumption_kwh"] or 0.0)),
        total_solar_kwh=max(0.0, float(row["total_solar_kwh"] or 0.0)),
        renewable_pct=float(row["renewable_pct"]),
        active_meter_count=int(row["active_meter_count"]),
        computed_at=row["computed_at"],
    )


class StreamPipeline:
    """Owns the three streaming queries and their lifecycle."""

    def __init__(
        self, spark: SparkSession, settings: Settings, repository: PostgresZoneMetricsRepository
    ) -> None:
        self._spark = spark
        self._settings = settings
        self._repo = repository
        # pyspark's StreamingQuery isn't a stable public export across versions;
        # `Any` avoids pinning to an internal import path.
        self._queries: list[Any] = []

    def _read_and_parse(self) -> DataFrame:
        source = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self._settings.kafka_bootstrap_servers)
            .option("subscribe", self._settings.kafka_readings_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        return parse_kafka_readings(source)

    def _validate_and_dlq_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """foreachBatch sink for query 1: count consumed/rejected, publish DLQ envelopes."""
        start = time.monotonic()
        valid, invalid = split_valid_invalid(batch_df)
        valid_count = valid.count()
        invalid_rows = invalid.collect()

        if valid_count:
            RECORDS_CONSUMED.labels(service=_SERVICE).inc(valid_count)
        if invalid_rows:
            for reason, n in _count_by_reason(invalid_rows):
                RECORDS_REJECTED.labels(service=_SERVICE, reason=reason).inc(n)
            envelopes = to_dlq_envelope(
                self._spark.createDataFrame(invalid_rows, schema=batch_df.schema)
            )
            (
                envelopes.write.format("kafka")
                .option("kafka.bootstrap.servers", self._settings.kafka_bootstrap_servers)
                .option("topic", self._settings.kafka_readings_dlq_topic)
                .save()
            )
            logger.info(
                "dlq.routed",
                batch_id=batch_id,
                count=len(invalid_rows),
                records_in=batch_df.count(),
            )

        CONSUMER_LAG.labels(stage="ingest").set(valid_count + len(invalid_rows))
        MICROBATCH_DURATION.labels(stage="ingest").observe(time.monotonic() - start)

    def _make_metrics_sink(self, window_type: str) -> Callable[[DataFrame, int], None]:
        def _sink(batch_df: DataFrame, batch_id: int) -> None:
            start = time.monotonic()
            rows = batch_df.collect()
            CONSUMER_LAG.labels(stage=window_type).set(len(rows))
            if rows:
                metrics = [_row_to_zone_metric(r) for r in rows]
                self._repo.upsert_metrics(metrics)
                logger.info(
                    "microbatch.upserted",
                    window_type=window_type,
                    batch_id=batch_id,
                    windows=len(metrics),
                )
            MICROBATCH_DURATION.labels(stage=window_type).observe(time.monotonic() - start)

        return _sink

    def start(self) -> None:
        trigger_interval = f"{self._settings.trigger_interval_seconds} seconds"
        watermark = f"{self._settings.watermark_minutes} minutes"

        ingest_query = (
            self._read_and_parse()
            .writeStream.foreachBatch(self._validate_and_dlq_batch)
            .option("checkpointLocation", str(self._settings.checkpoint_root / "ingest"))
            .trigger(processingTime=trigger_interval)
            .start()
        )
        self._queries.append(ingest_query)

        valid_tumbling, _ = split_valid_invalid(self._read_and_parse())
        tumbling = aggregate_window(
            valid_tumbling,
            window_type="tumbling_1m",
            window_duration=f"{self._settings.tumbling_window_seconds} seconds",
            slide_duration=None,
            watermark=watermark,
        )
        tumbling_query = (
            tumbling.writeStream.foreachBatch(self._make_metrics_sink("tumbling_1m"))
            .outputMode("update")
            .option("checkpointLocation", str(self._settings.checkpoint_root / "tumbling"))
            .trigger(processingTime=trigger_interval)
            .start()
        )
        self._queries.append(tumbling_query)

        valid_sliding, _ = split_valid_invalid(self._read_and_parse())
        sliding = aggregate_window(
            valid_sliding,
            window_type="sliding_5m",
            window_duration=f"{self._settings.sliding_window_seconds} seconds",
            slide_duration=f"{self._settings.sliding_slide_seconds} seconds",
            watermark=watermark,
        )
        sliding_query = (
            sliding.writeStream.foreachBatch(self._make_metrics_sink("sliding_5m"))
            .outputMode("update")
            .option("checkpointLocation", str(self._settings.checkpoint_root / "sliding"))
            .trigger(processingTime=trigger_interval)
            .start()
        )
        self._queries.append(sliding_query)

        logger.info("stream_pipeline.started", queries=len(self._queries))

    def await_termination(self) -> None:
        self._spark.streams.awaitAnyTermination()

    def stop(self) -> None:
        for q in self._queries:
            q.stop()
        logger.info("stream_pipeline.stopped")


def _count_by_reason(rows: list[Row]) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for r in rows:
        reason = "parse_error" if not r["parse_ok"] else "validation_failed"
        counts[reason] = counts.get(reason, 0) + 1
    return list(counts.items())
