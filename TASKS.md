# Task Tracker

Checkbox tracker for the two-person split. Detail and rationale in [`PLAN.md`](PLAN.md §7).
Acceptance criteria per task are in [`docs/PART2_ONBOARDING.md`](docs/PART2_ONBOARDING.md §4) for Part 2.

`[x]` done · `[~]` in progress · `[ ]` not started

---

## Part 1 — Developer A (Foundation, Sources & Ingestion)

### A1 · Repo scaffold & tooling
- [x] Directory layout, `.gitignore`, `LICENSE`, `.env.example`
- [x] Workspace `pyproject.toml` (ruff/black/mypy/pytest), `.pre-commit-config.yaml`
- [x] `Makefile` entrypoints
- [x] GitHub Actions CI (lint + type + unit tests)

### A2 · `energy_common` shared kernel
- [x] `domain/models.py` — frozen Pydantic models (`MeterReading`, `TariffRecord`, `WeatherForecast`, `ZoneMetric`, `HouseholdBill`, `ZoneSolarSummary`, `Alert`, `BatchRun`)
- [x] `domain/ports.py` — `Clock`, `ReadingPublisher`, `ReadingConsumer`, `ObjectStore`, `Repository`, `AlertSink`
- [x] `domain/transforms.py` — pure per-reading transforms shared by both processing layers
- [x] `domain/errors.py`
- [x] `config/settings.py` — `BaseServiceSettings` (pydantic-settings)
- [x] `observability/logging.py` — structlog JSON setup
- [x] `observability/metrics.py` — `serve_metrics`, `counter`, `histogram` helpers
- [x] `simclock/` — `SimClock`, `business_date_for`
- [x] `messaging/` — `build_producer`, `build_consumer`, `ReadingCodec`, Kafka adapters
- [x] `storage/` — `pg_engine`, base `Repository`, `ParquetObjectStore`
- [x] unit tests: models round-trip, sim-clock boundaries, codec, transforms, logging

### A3 · Database schema
- [x] `infra/postgres/init/001_dimensions.sql`
- [x] `infra/postgres/init/002_serving_tables.sql` (rt_zone_metrics, daily_household_billing, daily_zone_solar_summary)
- [x] `infra/postgres/init/003_ops_tables.sql` (alerts, batch_runs)
- [x] `infra/postgres/init/010_airflow_db.sql` (create `airflow` database)

### A4 · meter-simulator
- [x] `domain/generation.py` — diurnal load curve, time-of-day solar model, per-household profiles
- [x] `domain/anomalies.py` — anomaly Strategy (dropout / negative / spike / stuck)
- [x] `application/simulation_loop.py` — clock-driven emit loop against `ReadingPublisher` port
- [x] `adapters` — uses `energy_common` Kafka adapter; `composition.py`; `__main__.py`
- [x] `config.py`, `Dockerfile`, `README.md`
- [x] unit tests with a fake publisher + frozen clock

### A5 · batch-file-generator
- [x] `domain/reference_data.py` — build tariff + weather frames for a date (pure)
- [x] `application/generator_loop.py` — fire on simulated midnight
- [x] `adapters/landing_zone.py` — `CsvLandingZoneWriter` (atomic write + `_SUCCESS` last)
- [x] `config.py`, `Dockerfile`, `README.md`, `composition.py`, `__main__.py`
- [x] unit tests

### A6 · ingestion
- [x] `domain/envelope.py` — parse/validate result types
- [x] `application/ingest_service.py` — consume → validate → route (lake | DLQ), buffered per business_date
- [x] `adapters` — `ParquetLakeWriter` (via `energy_common`), `KafkaDlqWriter`
- [x] partition lifecycle: open → flush → `_SUCCESS` on simulated-day close; late-arrival counter
- [x] `config.py`, `Dockerfile`, `README.md`, `composition.py`, `__main__.py`
- [x] unit tests for routing + partition lifecycle (fakes for lake + DLQ)

### A7 · Seed & scripts
- [x] `data/seed/zones.csv`, `households.csv`
- [x] `scripts/create_topics.sh`
- [x] `scripts/seed_database.sh`
- [x] `scripts/demo.sh` (Part 1 portion; Dev B extends)

### A8 · Part 1 compose
- [x] `infra/docker-compose.yml` — kafka (KRaft), postgres, prometheus, the 3 Part 1 services
- [x] `infra/prometheus/prometheus.yml`

### A9 · Docs
- [x] `PLAN.md`, `README.md`, `TASKS.md`
- [x] `docs/ARCHITECTURE.md`, `docs/decisions/ADR-0001`, `ADR-0002`
- [x] `docs/DATA_CONTRACTS.md`, `docs/DEV_SETUP.md`, `docs/PART2_ONBOARDING.md`
- [x] `docs/OBSERVABILITY.md` (design + stubs), `docs/REPORT_OUTLINE.md`

### A10 · Handoff
- [ ] `make lint typecheck test` green in CI
- [ ] `make up && make topics && make seed` verified on a clean machine
- [ ] Contracts frozen; PART2_ONBOARDING walkthrough by someone other than the author
- [ ] Tag `part1-complete`

---

## Part 2 — Developer B (Processing, Serving, Orchestration, Observability)

> **Post-merge hardening pass (2026-09-25):** the first PR landed all of B1–B8
> functionally complete per the checkboxes below, on real infrastructure
> (Postgres + a live Kafka/JVM were used to verify, not just fakes). A review
> against `docs/PART2_ONBOARDING.md`'s acceptance criteria found and fixed
> four issues before merge would have been demo-safe: (1) the original
> stream-processor was a plain Python Kafka loop whose in-memory window
> accumulator reset every micro-batch, so `rt_zone_metrics` only ever held the
> *last* micro-batch's partial total for a window, not the full one — rewritten
> onto real Spark Structured Streaming, whose stateful windowed aggregation
> gets this right by construction (see `spark_transforms.py`'s docstring);
> (2) `alert-engine`'s loop never populated `RuleContext` from Postgres/
> Prometheus, so `NoDataRule` permanently false-fired and the other three
> rules could never fire — added `PostgresAlertContextRepository` +
> `PrometheusMetricsSource` + `ContextBuilder` to close that loop, and moved it
> onto simulated (not wall-clock) time; (3) the Airflow image had no build step
> installing this project's packages, so the DAG's `run_batch` task would
> `ModuleNotFoundError` — added `orchestration/airflow/Dockerfile`;
> (4) the DAG was `schedule="@daily"`, which never fires inside a
> minutes-long compressed demo — switched to externally triggered
> (`schedule=None`) via `AirflowDagTrigger`, called by batch-file-generator
> the moment a simulated day's feed closes. Also: `batch-processor` now reads
> the lake partition via Spark (was raw `pyarrow`) to match the declared Spark
> batch-layer architecture; the `serving_ro` Postgres role (used by
> serving-api and Grafana) had no `GRANT`s at all — fixed in
> `004_serving_ro_grants.sql`. All four numbered fixes plus the Spark rewrite
> are covered by tests, including a new cross-service integration suite
> (`tests/integration/`) run against real Postgres.

### B1 · stream-processor (speed layer)
- [x] Real Spark Structured Streaming (readStream/writeStream, not a polling loop) + `config.py` + composition root
- [x] transform pipeline reusing `energy_common.domain.transforms` (parse → validate → enrich → window), Spark UDFs call the *same* shared functions
- [x] tumbling 1-min + sliding 5-min per-zone aggregates via `groupBy(window(...))`, `withWatermark` — Spark's own state store, so a window's upsert is always its full accumulated total
- [x] `foreachBatch` upsert → `rt_zone_metrics`; per-query checkpointing
- [x] `/metrics`: `microbatch_duration_seconds`, `records_consumed_total`, `records_rejected_total`, `consumer_lag`
- [x] DLQ for parse-ok/validate-fail records (`writeStream` to `meter.readings.dlq`)
- [x] unit tests: `test_spark_transforms.py` (local Spark, batch-mode — includes a regression test for the accumulation bug) + `test_contract.py`
- [x] `Dockerfile` (now installs a JRE — PySpark needs a JVM), `README.md`

### B2 · batch-processor (batch layer)
- [x] `python -m batch_processor --business-date D` entrypoint (+ `--allow-open`)
- [x] read lake partition via **Spark** (`spark.read.parquet` + `groupBy`, not raw pyarrow) — require `_SUCCESS`, per-household rollup
- [x] join tariff + weather + `dim_household`
- [x] `BillingStrategy`: `FlatBilling`, `TieredBilling`, `SubsidisedBilling` (unchanged — pure, Spark-independent, already correct)
- [x] solar: self-consumed / grid-import / export + `solar_credit`
- [x] idempotent upsert `daily_household_billing` + `daily_zone_solar_summary`
- [x] report artifact `reports/out/billing_report_D.{csv,html}`
- [x] DQ assertions
- [x] unit tests (strategies + rollup, now against a real local Spark session) ; `Dockerfile` (+ JRE), `README.md`

### B3 · Airflow DAG
- [x] `daily_energy_reconciliation` DAG, parametrised `business_date`
- [x] validate feed → lake `_SUCCESS` → run_batch → dq_gate → publish_report → record_run
- [x] `on_failure_callback` → `alerts` row
- [x] retries (`default_args`) on the feed/lake checks, to absorb the small timing gap between batch-file-generator and ingestion closing a day
- [x] **externally triggered** (`schedule=None`) by `AirflowDagTrigger` the moment batch-file-generator closes a simulated day — not a wall-clock schedule, which would never fire inside a compressed demo; manual `airflow dags trigger` remains a documented fallback
- [x] custom `orchestration/airflow/Dockerfile` — the stock Airflow image had no way to import `batch_processor`
- [x] backfill verified
- [x] DAG unit test (`test_dag.py`) — now actually collected by `make test` (`orchestration` added to `testpaths`, plus a `conftest.py` fix; it was silently never running before)

### B4 · serving-api
- [x] FastAPI app, repositories, `config.py`
- [x] endpoints: realtime zones (+ by zone), daily billing (by date, by household), solar report, `/healthz`, `/metrics`
- [x] live-vs-final merge rule
- [x] response models from `energy_common`
- [x] unit tests (fake repositories) + integration test against real Postgres + `Dockerfile`, `README.md`
- [x] `serving_ro` role actually granted `SELECT` (`004_serving_ro_grants.sql`) — it previously had none, so every query would have failed with "permission denied"

### B5 · alert-engine
- [x] rule engine + `RuleContext`
- [x] `NoDataRule`, `LowRenewableRule`, `HighRejectRateRule`, `BatchFailureRule`
- [x] Observer sinks: `DbAlertSink`, `LogAlertSink`, `WebhookAlertSink` (opt)
- [x] dedupe + `resolved_at`
- [x] **`RuleContext` now built from real data every cycle** — `PostgresAlertContextRepository` (zone metrics, batch runs) + `PrometheusMetricsSource` (reject rate) via `ContextBuilder`; runs on simulated time (`SimClock`), not wall-clock
- [x] unit tests per rule + an integration test exercising a full evaluate → fire → dedupe → resolve cycle against real Postgres ; `Dockerfile`, `README.md`

### B6 · dashboard
- [x] Grafana provisioning: Postgres + Prometheus datasources, one dashboard (`infra/grafana/provisioning/`) — per-zone load/solar time series, current renewable % + active meters table, open-alerts stat, latest billing table
- [x] billing report HTML template

### B7 · full compose
- [x] `infra/docker-compose.part2.yml` — stream-processor, batch-processor, serving-api, alert-engine, Airflow (custom-built image), Grafana
- [x] Prometheus scrape targets for Part 2 services
- [x] healthchecks; `make up-full`, `make demo` full path

### B8 · docs & tests
- [x] `docs/RUNBOOK.md`
- [ ] complete `docs/OBSERVABILITY.md §5` (screenshots — needs a live run to capture)
- [x] e2e test asserting the write/read contract across services holds against real Postgres (`tests/integration/test_end_to_end.py`, `make test-integration`)
- [x] demo script (`scripts/demo.sh`, `DEMO_FULL=1`)

### B9 · report
- [ ] sections 6, 7, 8, 9 (+ shared 3, 10)
- [ ] individual-contribution statement

---

## Individual contributions (for the submission)

| Developer | Scope |
|---|---|
| **Dev A — G.W. Niranga Niluminda** | Architecture decision & ADRs, `energy_common` shared kernel, database schema, both simulated sources, Kafka ingestion → Parquet master dataset, Part 1 Docker Compose, project documentation set. |
| **Dev B — _<name>_** | Speed layer (Spark Structured Streaming), batch layer (Spark billing/solar reconciliation), Airflow orchestration, serving API, observability alert-engine + dashboards, full-stack Compose, end-to-end tests, report results sections. |
