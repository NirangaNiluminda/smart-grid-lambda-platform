# ADR-0002 — Technology stack

- **Status:** Accepted
- **Date:** 2026-01 (project week 1)
- **Rubric weight:** 10 / 100
- **Depends on:** [ADR-0001](ADR-0001-lambda-vs-kappa.md) (Lambda)

Each choice is justified against *this* use case, with the alternative we rejected.

## Ingestion bus — Apache Kafka (KRaft mode)

- **Why:** We need a durable, partitioned, **replayable** log. Replay is not optional in Lambda —
  it is how the batch layer reprocesses history. Partitioning `meter.readings` by `grid_zone`
  gives per-zone ordering (the speed layer aggregates per zone) and lets consumers scale to one
  task per zone.
- **KRaft (no ZooKeeper):** one less stateful service to run in Compose; current Kafka default.
- **Rejected:** Redpanda (fewer teaching resources, brief names Kafka); RabbitMQ / Redis Streams
  (no long retention / replay story, weaker partitioning model); AWS Kinesis (not local, cost).

## Streaming source — Python + `confluent-kafka`

- **Why:** The brief requires simulated sources as Python scripts. `confluent-kafka` wraps librdkafka
  (fast, well-supported) and exposes delivery callbacks we use for producer-side metrics.
- **Rejected:** `kafka-python` (pure Python, slower, less maintained).

## Speed layer — Apache Spark Structured Streaming (PySpark)

- **Why:** Native **event-time** windowing + watermarking is exactly what "grid load per zone over
  the last minute / 5 minutes" needs. Checkpointing gives us exactly-once upserts to Postgres.
  Crucially, it is the **same engine and API** as the batch layer, so the team maintains one skset
  of transforms and one mental model.
- **Rejected:** Apache Storm (brief's alternative) — lower-level, no first-class event-time
  windowing, would need a separate batch tool with a different API, doubling the learning cost.
  Kafka Streams (JVM only; we are a Python shop). Flink (excellent fit, but heavier to run locally
  and the team already needs Spark for the batch layer — one engine is simpler).

## Batch layer — Apache Spark (batch mode)

- **Why:** Reuses the streaming transforms as pure functions; efficient full-day join of the Parquet
  master dataset with the tariff/weather feed; trivially parametrised by `business_date` for
  backfill.
- **Rejected:** pandas / DuckDB — fine at this data size, but choosing Spark keeps **one** engine
  across both layers and scales to the "what would you do at production scale" report section
  without a rewrite.

## Orchestration — Apache Airflow (LocalExecutor)

- **Why:** The batch layer needs: *wait for the daily feed* (`FileSensor`), *run with an explicit
  business date*, *retry on transient failure*, *gate on data-quality checks*, *record an auditable
  run history*, *alert on failure*. That is Airflow's job description. LocalExecutor is enough for a
  single-day-at-a-time workload.
- **Rejected:** cron (no sensing, no retries, no history, no DQ gate); Dagster/Prefect (great tools,
  but the brief names Airflow and it is the industry reference for this pattern).

## Master dataset — Parquet on the local filesystem

- **Why:** Columnar + predicate pushdown for the batch layer's per-day scans; partition by
  `business_date` so reprocessing one day reads one directory; **append-only / immutable** — the
  property Lambda's batch layer depends on. Layout mirrors an object store (`readings_raw/business_date=…/`)
  so moving to S3/HDFS is a config change.
- **Rejected:** writing raw readings straight to Postgres (loses the immutable replay substrate,
  couples ingestion to the serving store); Delta/Iceberg (valuable in production for ACID + schema
  evolution; unnecessary complexity at this scope — noted in the report's "production scale" section).

## Serving store — PostgreSQL

- **Why:** Serving queries are small: per-zone latest metrics, per-household bill for a date, per-zone
  solar summary. All are aggregates or point lookups over modest row counts — relational is the
  right tool, and SQL keeps the serving API thin. One instance also hosts Airflow's metadata DB
  (separate database), saving a container.
- **Rejected:** Cassandra (brief's alternative) — great for high-volume time-series writes, but our
  *serving* workload is low-volume reads of pre-aggregated data and sometimes needs joins; Cassandra's
  data model would fight us. We would reach for Cassandra if we were storing every raw reading for
  online serving, which we are not (that is the Parquet lake's job).

## Serving API — FastAPI

- **Why:** Pydantic request/response models come straight from `energy_common` domain models (one
  schema definition, reused); automatic OpenAPI docs for graders; first-class async; one-line
  Prometheus instrumentation. The API is a **read model** only (CQRS-style) — it never computes.
- **Rejected:** Flask (more boilerplate for typed models + docs); Django REST (far too heavy).

## Observability — structlog + prometheus-client + a rule engine

- **Why:** `structlog` gives JSON logs with bound context (service, business_date, correlation_id)
  with almost no ceremony. `prometheus-client` exposes `/metrics` from every service in a few lines.
  Alert rules are small **Strategy** objects we can unit-test — more transparent for a grader than a
  PromQL file, and demoable on command.
- **Optional:** Grafana + Prometheus Alertmanager for nicer screenshots; not on the critical path.

## Packaging — Docker Compose

- **Why:** "Docker Compose strongly recommended" in the brief; one command reproduces the whole
  platform; each service ships its own image and dependency closure, reinforcing the service
  boundaries.
- **Rejected:** a local k8s (kind/minikube) — reproducibility win not worth the overhead at this
  scope; noted as a production-scale step.
