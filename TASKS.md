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
- [x] `make lint typecheck test` green in CI
- [x] `make up-full && make topics && make seed` verified for real (2026-09-29 — see the live-verification note below; not just Part 1)
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

> **Live end-to-end verification (2026-09-29):** the 2026-09-25 hardening pass
> was validated against real infrastructure piece by piece (a temporary
> Postgres, a local Spark session) but the full `make up-full` stack had never
> actually been run together as one system. It was, this session, and found —
> and fixed — **eight further bugs that no unit or isolated-integration test
> could have caught**, each one blocking the next: (1) `python:3.11-slim`
> (unpinned) now resolves to Debian trixie, which dropped `openjdk-17-jre-headless`
> — pinned stream-processor/batch-processor to `-bookworm`; (2) the compose
> files live in `infra/`, so a bare `docker compose -f infra/...` resolves its
> project directory — and therefore `.env` lookup — to `infra/`, silently
> ignoring the repo-root `.env` project-wide since Part 1 — fixed with
> `--env-file .env` everywhere (Makefile, scripts, docs); **not**
> `--project-directory`, which also breaks the `context: ..` build paths;
> (3) `infra/docker-compose.yml` had no `restart` policy on any Part 1
> service, so `ingestion`'s expected topics-not-created-yet startup race
> killed it *permanently* rather than self-healing; (4) `scripts/seed_database.sh`
> passed `\copy` (a psql client-side meta-command) via `-c`, which can only
> ever be sent straight to the server as SQL — the seed script had never
> actually run against live data before; (5) `alert-engine`'s loop caught
> exceptions around the whole process, not per-cycle, so one bad write (e.g.
> an unseeded dimension row) crash-looped the entire service; (6) `airflow
> standalone` bootstraps its own admin user with a fresh random password every
> start *and* forces the REST API onto session-only auth at runtime regardless
> of config, so `admin`/`admin` (documented everywhere, and what
> `AirflowDagTrigger` uses) always got a 403 — switched to plain
> `webserver`+`scheduler`, which respect the configured auth backends, with an
> explicit delete-then-recreate of the admin user so a persisted `pg-data`
> volume can't leave a stale password behind; (7) even so, the API backend
> setting had the wrong config path for this Airflow version's auth manager
> (`[api] auth_backends` vs. the FAB provider's `[fab] auth_backends`, a
> different module path entirely) — **this one is fixed in code but not yet
> confirmed working**, see the open item below; (8) most fundamentally, the
> DAG had **never once parsed successfully** — every `@task` function used
> `ds` as a parameter name, a reserved Airflow context variable that the
> `@task` decorator silently rewrites into a defaulted kwarg at decoration
> time, which broke `record_run`'s signature (a non-default parameter after
> it) and failed the whole file's import. Renamed to `business_date_str`
> throughout. One more found once the DAG could finally run:
> `energy_common.storage.postgres` imported `sqlalchemy.Engine` (the
> SQLAlchemy-2.0-only top-level re-export); Airflow 2.10.2 bundles SQLAlchemy
> 1.4.x and the Airflow image installs `energy_common` with `--no-deps`
> specifically to keep that pin — switched to the version-portable
> `sqlalchemy.engine.Engine` import. And one filesystem-specific one: this dev
> machine's bind-mount doesn't honour per-uid write permission, so the
> non-root `airflow` (uid 50000) image user couldn't write the report artifact
> — added `user: "0:0"` to the airflow service (a standard, documented fix for
> exactly this bind-mount scenario). **Result:** `daily_energy_reconciliation`
> ran to completion end to end via `airflow dags trigger` — all 6 tasks green,
> 200 households billed, `daily_zone_solar_summary` populated for all 6 zones,
> `batch_runs` recorded `success`, `serving-api` correctly returned
> `"status": "final"` for the day, and the HTML/CSV report artifacts rendered
> and published. The speed layer, ingestion, and alert-engine (`NoDataRule`
> and `LowRenewableRule` both observed firing and resolving correctly) were
> independently confirmed live and healthy throughout.
>
> **Open item — not yet resolved:** `AirflowDagTrigger`'s REST API call still
> gets a 403 even with the corrected `[fab] auth_backends` config and a
> confirmed-correct password (verified via `security_manager.auth_user_db()`
> directly) and confirmed-correct RBAC permissions on the `Admin` role (92
> permissions including DAG read/create checked directly in `ab_permission_view_role`).
> The failure mode itself is unresolved — authentication succeeds, authorization
> still denies — and time-boxed rather than fully root-caused. **This does not
> block the pipeline**: the trigger call is deliberately best-effort (logs a
> warning, does not crash `batch-file-generator`), and the documented manual
> fallback (`airflow dags trigger daily_energy_reconciliation -e <date>`,
> `docs/RUNBOOK.md §3.2`) is what was actually used above and is fully
> reliable — it goes through the Airflow CLI/metadata DB directly, not the
> REST API, so it's unaffected by this issue. Before the demo recording,
> either resolve the REST API 403 for real (a newer Airflow patch version is
> an untested candidate) or plan the recording around the manual trigger step
> explicitly (already how `docs/report/DEMO_RECORDING_PLAN.md` is written).

> **Pre-recording dry run (2026-09-29, later same day) — two more bugs, this
> time in the observability config itself, not the pipeline:** (9) `grafana`
> in `infra/docker-compose.part2.yml` carried `profiles: ["dashboards"]`, a
> profile nothing in the Makefile or docs ever activates, so `localhost:3000`
> silently never came up under the documented `make up-full` flow despite
> RUNBOOK.md and the demo plan describing it as part of the normal stack —
> removed the profile gate. (10) Prometheus's scrape config had a static
> target for `batch-processor:9112`, which can never resolve: `batch-processor`
> is deliberately `profiles: ["manual"]` (a one-shot job Airflow's `run_batch`
> task invokes in-process, never a standing container), so the Targets page
> showed a permanent, misleading `DOWN` row — removed the dead scrape job and
> the matching endpoint-table row in RUNBOOK.md. Both found by actually
> opening every documented UI link before recording, not by re-reading the
> compose files; both are now fixed and verified live (Grafana's provisioned
> datasources + dashboard load correctly; all 7 remaining Prometheus targets
> report `up`). Also noted during this dry run: the Airflow UI's plain
> "Trigger DAG" button defaults to *today's real wallclock date*, which never
> matches a simulated `business_date` — not a bug, but worth knowing before
> triggering on camera (use `airflow dags trigger ... -e <business_date>`, or
> "Trigger DAG w/ config" with an explicit logical date).

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
- [x] `daily_energy_reconciliation` DAG, parametrised `business_date` — **parses and runs to completion**, verified live 2026-09-29 (see note above; it had never successfully parsed before — a `ds`-as-parameter-name bug)
- [x] validate feed → lake `_SUCCESS` → run_batch → dq_gate → publish_report → record_run — all 6 tasks confirmed green in a real run
- [x] `on_failure_callback` → `alerts` row
- [x] retries (`default_args`) on the feed/lake checks, to absorb the small timing gap between batch-file-generator and ingestion closing a day
- [~] **externally triggered** (`schedule=None`) by `AirflowDagTrigger` the moment batch-file-generator closes a simulated day — implemented and wired, but the REST API call still 403s (open item above); the **manual CLI trigger fully works** and is what was used to verify the run
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
- [x] healthchecks; `make up-full` verified live end to end 2026-09-29 (all 10 containers healthy simultaneously)

### B8 · docs & tests
- [x] `docs/RUNBOOK.md`
- [ ] complete `docs/OBSERVABILITY.md §5` (screenshots — needs a live run to capture; the run to capture from now exists, see the note above)
- [x] e2e test asserting the write/read contract across services holds against real Postgres (`tests/integration/test_end_to_end.py`, `make test-integration`) — and, beyond the test suite, the *actual* full pipeline verified live end to end 2026-09-29
- [x] demo script (`scripts/demo.sh`, `DEMO_FULL=1`)

### B9 · report
- [ ] sections 6, 7, 8, 9 (+ shared 3, 10)
- [ ] individual-contribution statement

---

## Individual contributions (for the submission)

| Developer | Scope |
|---|---|
| **Dev A — Niluminda G.W.N (EG/2021/4691)** | Architecture decision & ADRs, `energy_common` shared kernel, database schema, both simulated sources, Kafka ingestion → Parquet master dataset, Part 1 Docker Compose, project documentation set. |
| **Dev B — Niroshan I.J (EG/2021/4695)** | Jointly with Dev C: speed layer (Spark Structured Streaming), batch layer (Spark billing/solar reconciliation), Airflow orchestration, serving API, observability alert-engine + dashboards, full-stack Compose, end-to-end tests, report results sections. |
| **Dev C — Kavinda S.O (EG/2021/4608)** | Jointly with Dev B: speed layer (Spark Structured Streaming), batch layer (Spark billing/solar reconciliation), Airflow orchestration, serving API, observability alert-engine + dashboards, full-stack Compose, end-to-end tests, report results sections. |
