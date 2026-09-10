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

### B1 · stream-processor (speed layer)
- [ ] Spark Structured Streaming skeleton + `config.py` + composition root
- [ ] transform pipeline reusing `energy_common.domain.transforms` (parse → validate → enrich → window)
- [ ] tumbling 1-min + sliding 5-min per-zone aggregates, watermark
- [ ] `foreachBatch` upsert → `rt_zone_metrics`; checkpointing
- [ ] `/metrics`: `microbatch_duration_seconds`, `records_consumed_total`, `records_rejected_total`, `consumer_lag`
- [ ] DLQ for parse-ok/validate-fail records
- [ ] unit tests (pure transforms) + 1 integration test (`-m integration`)
- [ ] `Dockerfile`, `README.md`

### B2 · batch-processor (batch layer)
- [ ] `python -m batch_processor --business-date D` entrypoint (+ `--allow-open`)
- [ ] read lake partition (require `_SUCCESS`), per-household rollup
- [ ] join tariff + weather + `dim_household`
- [ ] `BillingStrategy`: `FlatBilling`, `TieredBilling`, `SubsidisedBilling`
- [ ] solar: self-consumed / grid-import / export + `solar_credit`
- [ ] idempotent upsert `daily_household_billing` + `daily_zone_solar_summary`
- [ ] report artifact `reports/out/billing_report_D.{csv,html}`
- [ ] DQ assertions
- [ ] unit tests (strategies + rollup) ; `Dockerfile`, `README.md`

### B3 · Airflow DAG
- [ ] `daily_energy_reconciliation` DAG, parametrised `business_date`
- [ ] `FileSensor` on `_SUCCESS_{{ds}}` → validate → lake `_SUCCESS` → run_batch → dq_gate → publish_report → record_run
- [ ] `on_failure_callback` → `alerts` row
- [ ] backfill verified
- [ ] DAG unit test (`airflow dags test`)

### B4 · serving-api
- [ ] FastAPI app, repositories, `config.py`
- [ ] endpoints: realtime zones (+ by zone), daily billing (by date, by household), solar report, `/healthz`, `/metrics`
- [ ] live-vs-final merge rule
- [ ] response models from `energy_common`
- [ ] unit tests (fake repositories) + `Dockerfile`, `README.md`

### B5 · alert-engine
- [ ] rule engine + `RuleContext`
- [ ] `NoDataRule`, `LowRenewableRule`, `HighRejectRateRule`, `BatchFailureRule`
- [ ] Observer sinks: `DbAlertSink`, `LogAlertSink`, `WebhookAlertSink` (opt)
- [ ] dedupe + `resolved_at`
- [ ] unit tests per rule ; `Dockerfile`, `README.md`

### B6 · dashboard
- [ ] Grafana provisioning (datasource + per-zone load/renewable dashboard) OR static page on the API
- [ ] billing report HTML template

### B7 · full compose
- [ ] `infra/docker-compose.part2.yml` — spark, airflow, the 4 services, grafana
- [ ] Prometheus scrape targets for Part 2 services
- [ ] healthchecks; `make up-full`, `make demo` full path

### B8 · docs & tests
- [ ] `docs/RUNBOOK.md`
- [ ] complete `docs/OBSERVABILITY.md §5` (screenshots)
- [ ] e2e test asserting tables + report after one simulated day
- [ ] demo video / live-demo script

### B9 · report
- [ ] sections 6, 7, 8, 9 (+ shared 3, 10)
- [ ] individual-contribution statement

---

## Individual contributions (for the submission)

| Developer | Scope |
|---|---|
| **Dev A — G.W. Niranga Niluminda** | Architecture decision & ADRs, `energy_common` shared kernel, database schema, both simulated sources, Kafka ingestion → Parquet master dataset, Part 1 Docker Compose, project documentation set. |
| **Dev B — _<name>_** | Speed layer (Spark Structured Streaming), batch layer (Spark billing/solar reconciliation), Airflow orchestration, serving API, observability alert-engine + dashboards, full-stack Compose, end-to-end tests, report results sections. |
