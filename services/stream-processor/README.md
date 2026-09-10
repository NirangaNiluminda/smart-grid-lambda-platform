# stream-processor  ·  SPEED LAYER

**Layer:** processing (Lambda speed layer) · **Owner:** Dev B · **Status:** 🔲 Part 2 — skeleton only

Spark Structured Streaming job. Consumes Kafka `meter.readings`, maintains the
approximate, low-latency per-zone view in `rt_zone_metrics`.

> **Start with [`docs/PART2_ONBOARDING.md`](../../docs/PART2_ONBOARDING.md) §B1.**
> This is the first Part 2 service to build — it only needs Kafka + Postgres.

## Contract

| Direction | Resource | Spec |
|---|---|---|
| in | Kafka `meter.readings` (group `stream-processor-speed-layer`) | DATA_CONTRACTS §1 |
| in | Postgres `dim_zone`, `dim_household` | DATA_CONTRACTS §4 |
| out | Postgres `rt_zone_metrics` (upsert on `grid_zone, window_type, window_start`) | DATA_CONTRACTS §4 |
| out | Kafka `meter.readings.dlq` for parse-ok / validate-fail records | DATA_CONTRACTS §1 |
| out | `/metrics` on `:9111` | OBSERVABILITY §2 |

## What to build

1. `domain/windows.py` — **reuse `energy_common.domain.transforms`** for validate +
   `renewable_pct`; add the windowing spec (tumbling 1-min, sliding 5-min/1-min, watermark 2-min).
   Keep it Spark-free and unit-test it with plain rows.
2. `adapters/spark_source.py` — Kafka → parsed DataFrame; bad records → DLQ.
3. `adapters/metrics_sink.py` — `foreachBatch` upsert into `rt_zone_metrics` via a
   `ZoneMetricsRepository` (extend `energy_common.storage.BaseRepository`).
4. `application/pipeline.py` — compose source → transforms → sink; checkpoint at
   `${LAKE_ROOT}/../checkpoints/stream-processor`.
5. `composition.py` + `__main__.py`.

## Definition of Done

- `rt_zone_metrics` updates within seconds of readings arriving.
- Unit tests cover the transforms; one `-m integration` test runs the job against a test topic.
- `microbatch_duration_seconds`, `records_consumed_total`, `records_rejected_total`, `consumer_lag` exported.
- `GET /api/realtime/zones` (serving-api) reflects the rows.
