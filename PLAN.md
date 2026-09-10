# Project Plan — Smart Grid Energy Monitoring & Billing Platform

EC8203 *Applied Big Data Engineering* · Mini Project · 25% of module grade · 2-week, 2-person group.

This is the master plan. It states **what we are building, why the architecture is what it is,
how the code is structured, and exactly how the work is split between two developers** so the
second developer can start Part 2 without a verbal handover.

- Architecture & diagrams → [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Decision records → [`docs/decisions/`](docs/decisions/)
- Interface contract Part 1 → Part 2 → [`docs/DATA_CONTRACTS.md`](docs/DATA_CONTRACTS.md)
- Part 2 developer onboarding → [`docs/PART2_ONBOARDING.md`](docs/PART2_ONBOARDING.md)
- Task checkboxes → [`TASKS.md`](TASKS.md)

---

## 1. Objective

Architect and implement an end-to-end **Lambda** data pipeline that:

1. Ingests a **continuous** stream of smart-meter readings and a **once-a-day** tariff/weather
   reference feed.
2. Serves **real-time** grid-load and renewable-mix metrics per grid zone (low latency,
   tolerant of approximation).
3. Produces an **authoritative, recomputable** daily per-household billing and
   solar-contribution report once the day's reference feed has landed.
4. Is **observable**: structured logging across every stage, exported metrics, and
   threshold/health-check alerting.

Everything runs from a single `docker compose up`. Simulated time is compressed
(1 business day = 10 wall-clock minutes by default) so a full day can be demonstrated in a session.

## 2. Use case & business requirements

**Use Case 3 — Smart Grid Energy Monitoring & Billing** (from the brief, unchanged).

| Aspect | Interpretation for this build |
|---|---|
| **Actor** | Utility company grid-operations + billing teams |
| **Streaming source** | Smart meters emit `meter_id, household_id, power_consumption_kwh, solar_generation_kwh, grid_zone, timestamp` every ~2 simulated seconds. Includes realistic diurnal load, time-of-day solar, and injected anomalies (dropouts, negative values, spikes). |
| **Daily batch source** | One drop per simulated day into a landing zone: `tariff_reference_<date>.csv` (`household_id, tariff_rate, billing_tier, subsidy_flag, effective_date`) **and** `weather_forecast_<date>.csv` (`grid_zone, forecast_date, cloud_cover_pct, temp_c, solar_irradiance_index`). Represents the merchandising/ERP + meteorology nightly extract. |
| **Business question** | *Current grid load and renewable contribution by zone right now* **+** *what each household's bill looks like once daily tariff data is applied to their consumption.* |
| **Required outputs** | (a) API endpoint: real-time grid load / renewable-mix by zone. (b) Alert when renewable contribution for a zone is considerably low (plus other health alerts). (c) Daily consolidated billing + solar-contribution report per household. |

The two halves of the business question have **fundamentally different service levels** — this is
the crux of the architecture decision below.

| | Real-time zone view | Daily household bill |
|---|---|---|
| Latency need | seconds | "ready the morning after" |
| Correctness need | approximate is fine (late readings settle later) | exact, auditable, must reconcile to the cent |
| Input completeness | partial, unbounded stream | full closed day + the day's tariff feed |
| Recompute on correction | not meaningful | mandatory (tariff fixes, late meter data) |
| Natural trigger | continuous | "the business day closed" |

## 3. Architecture decision — Lambda vs Kappa

**Decision: Lambda architecture.** Full reasoning and the rejected-alternative write-up are in
[`docs/decisions/ADR-0001-lambda-vs-kappa.md`](docs/decisions/ADR-0001-lambda-vs-kappa.md);
summary here.

### Why Lambda fits this use case

- **Two irreducibly different computations.** The real-time zone dashboard (rough kWh/renewable %
  per zone, second-fresh) and the daily bill (exact money, audited, reconciled) are *not the same
  query at two freshnesses* — they have different inputs, different completeness guarantees and
  different consumers. Lambda's separation of a **speed layer** and a **batch layer** maps directly
  onto that.
- **The batch layer's inputs only exist once a day.** Billing cannot be computed until the tariff
  feed lands. A scheduled, orchestrated job that *waits for the file, computes the closed day, and
  publishes an immutable result* is the honest shape of this work — and it is exactly what Apache
  Airflow is for.
- **Recomputability is a hard requirement.** Money. If a tariff is corrected or late meter data
  arrives, we must reproduce business-day *D*'s bill exactly. A bounded batch job over one Parquet
  partition of an **immutable master dataset** does this cheaply and auditably.
- **Consistency vs latency split.** The dashboard needs latency and tolerates weak consistency; the
  bill needs strong consistency and tolerates latency. Serving them from one pipeline forces a
  compromise that hurts both.

### Why not Kappa (rejected alternative)

Kappa would model the daily tariff file as a log-compacted Kafka topic and compute billing
continuously as streaming state, using topic replay for reprocessing. We reject it **for this use
case** because:

- Billing is inherently **periodic and bounded** — you bill a closed period, not "continuously".
  Encoding "the day closed, emit the authoritative invoice" as watermark/allowed-lateness control
  flow is less clear and less auditable than a scheduled DAG with an explicit business-date parameter.
- **Reprocessing cost.** Correcting a historical tariff by replaying the entire meter-readings topic
  through a stateful stream job is far more expensive and slower than re-running one bounded batch
  job over `business_date=D`.
- The brief explicitly wants Airflow for "batch jobs or reporting pipelines" and wants us to
  demonstrate **both** batch and stream processing. Lambda showcases both cleanly; Kappa collapses
  the exercise to one paradigm.
- Kappa's headline advantage — a single codebase, no dual logic to keep in sync — is **weaker here**
  because the two layers do genuinely different work, not the same work twice.

> If the requirement were *only* the real-time zone dashboard (no daily billing), we would choose
> Kappa. The daily authoritative bill is what tips it to Lambda.

## 4. Technology stack & justification

Per-component justification tied to *this* use case (not generic popularity) lives in
[`docs/decisions/ADR-0002-technology-stack.md`](docs/decisions/ADR-0002-technology-stack.md).

| Layer | Choice | One-line justification |
|---|---|---|
| Ingestion / bus | **Apache Kafka** (KRaft, no ZooKeeper) | Durable, replayable, partitioned log. Partitioning by `grid_zone` gives per-zone ordering and lets the speed layer scale per zone. The retained log **is** the Lambda replay mechanism. |
| Streaming source | **Python** producer (`confluent-kafka`) | Brief requires simulated sources as Python scripts. |
| Speed layer | **Apache Spark Structured Streaming** (PySpark) | Event-time windowing + watermarking for the per-zone aggregates; same engine/API as the batch layer, so the team learns one tool; exactly-once sink semantics via checkpointing. |
| Batch layer | **Apache Spark** batch job | Reuses the streaming transforms as pure functions; handles a full-day join of the Parquet master dataset with the tariff/weather feed efficiently. |
| Orchestration | **Apache Airflow** (LocalExecutor) | `FileSensor` for the daily feed, explicit `business_date` parameter for backfill/replay, retries, failure alerting, an auditable run history — the batch layer's control plane. |
| Master dataset | **Parquet on local FS** (`data/lake/`, `s3://`-shaped layout) | Columnar, partitioned by `business_date`; immutable append-only; the batch layer's single source of truth. Stands in for HDFS/S3. |
| Serving store | **PostgreSQL** | The serving queries are small aggregates and point lookups by household/zone/date — relational fits. One instance hosts the serving DB and the Airflow metadata DB (separate databases). |
| Serving API | **FastAPI** | Typed request/response models straight from the shared domain models; auto OpenAPI docs; trivial Prometheus instrumentation. Read-only over Postgres (CQRS-style read model). |
| Observability | **structlog** (JSON logs) + **prometheus-client** + a small **rule engine** | Structured logs with correlation fields across every stage; `/metrics` on every service; explicit alert rules as testable strategy objects. Grafana optional for screenshots. |
| Packaging | **Docker Compose** | One command brings up the whole platform; each service has its own image and dependency set. |

## 5. Clean-architecture principles & design patterns

The codebase is a **monorepo of independently deployable services** around a shared kernel.
Non-negotiables:

1. **Hexagonal (ports & adapters) per service.** Business logic sits in `domain/` and `application/`
   and imports **no** framework (no Kafka, Spark, SQLAlchemy, FastAPI). I/O lives in `adapters/`
   behind interfaces (`domain/ports.py`). A thin `composition.py` / `__main__.py` wires concretions
   to the use case — the only place that knows both sides.
2. **Dependency inversion.** Services depend on `Protocol`/ABC ports from `energy_common`, never on
   concrete infrastructure. Tests inject fakes; production injects Kafka/Postgres adapters.
3. **Shared kernel, not copy-paste.** Cross-cutting code (domain models, ports, logging, sim-clock,
   Kafka/DB factories) is the versioned `energy_common` package, installed editable.
4. **Immutable domain models.** `MeterReading`, `TariffRecord`, `HouseholdBill`, `ZoneMetric`,
   `Alert`, … are frozen Pydantic v2 models with validation — they double as the wire/data contracts.
5. **12-factor config.** Every service has one `Settings` class (`pydantic-settings`), validated at
   startup, fed only from the environment. No hard-coded hosts/ports/paths.
6. **Explicit patterns where they pay for themselves:**

| Pattern | Where | Why |
|---|---|---|
| Ports & Adapters | every service | isolate the domain from infrastructure; make it testable |
| Repository | all Postgres access (`ZoneMetricsRepository`, `BillingRepository`, `AlertRepository`) | keep SQL out of business logic; swap store in tests |
| Strategy | billing calculation (flat / tiered / subsidised), alert rules, anomaly injection | open/closed — add a tariff tier or alert rule without touching the engine |
| Factory | Kafka producer/consumer, Spark session, DB engine from `Settings` | one construction path, consistent config |
| Adapter | Kafka, Parquet lake, landing zone, Prometheus | wrap third-party APIs behind our ports |
| Pipeline / chain | stream + batch transforms = ordered `Transform` steps (validate → enrich → aggregate) | each step independently unit-tested, reused across layers |
| Observer / pub-sub | `alert-engine` fan-out to alert sinks (DB, log, webhook) | add a notification channel without touching rule evaluation |
| DTO + Mapper | Kafka JSON ↔ domain model ↔ DB row ↔ API response | one conversion boundary, no leaking schemas |

## 6. System decomposition

| Service | Layer | Responsibility | Depends on |
|---|---|---|---|
| `meter-simulator` | source | Generate realistic per-household readings, inject anomalies, publish to `meter.readings`. | Kafka (out) |
| `batch-file-generator` | source | At each simulated midnight, write `tariff_reference_<date>.csv` + `weather_forecast_<date>.csv` + `_SUCCESS` to the landing zone. | FS landing zone (out) |
| `ingestion` | ingestion | Consume `meter.readings`, validate envelopes, write valid records to the Parquet master dataset partitioned by `business_date`, route unparseable messages to the DLQ. | Kafka (in), Parquet lake (out) |
| `stream-processor` | speed | Spark Structured Streaming: validate → enrich → event-time windowed aggregation per zone → upsert `rt_zone_metrics`. | Kafka (in), Postgres (out) |
| `batch-processor` | batch | Spark batch: read `business_date=D` from the lake, join tariff + weather, compute per-household bill + per-zone solar summary, write tables + report artifact. | Parquet lake (in), landing zone (in), Postgres (out) |
| `orchestration/airflow` | batch control | `daily_energy_reconciliation` DAG: sense the feed → validate → run `batch-processor` → data-quality gate → publish report → audit row. | landing zone, `batch-processor`, Postgres |
| `serving-api` | serving | Read-only HTTP over Postgres: real-time zone metrics, daily bills, solar report, health, metrics. | Postgres (in) |
| `alert-engine` | observability | Evaluate alert-rule strategies over Postgres/Prometheus on a schedule; write `alerts`; fan out to sinks. | Postgres, Prometheus |

Boundaries are enforced by: separate packages, separate images, and the fact that services only
communicate through **Kafka topics, the Parquet lake, the landing zone, and Postgres tables** — all
specified in [`docs/DATA_CONTRACTS.md`](docs/DATA_CONTRACTS.md).

## 7. Two-person work split

Split model: **Foundation & Ingestion (Dev A)** vs **Processing, Serving & Observability (Dev B)**.
Chosen because it gives a clean, one-directional handoff: Dev A produces data *into* Kafka, the
Parquet lake, the landing zone and the dimension tables; Dev B consumes exactly those and produces
the outputs. Dev B can begin as soon as the contracts + simulator are on `main` (≈ day 4), before
Dev A is fully finished.

### Part 1 — Developer A  (you)  ·  *Foundation, Data Contracts, Sources & Ingestion*

| # | Deliverable | Files |
|---|---|---|
| A1 | Repo scaffold, workspace tooling (ruff/black/mypy/pytest/pre-commit), CI, Makefile, `.env.example` | root config, `.github/` |
| A2 | **`energy_common` shared kernel** — domain models, port interfaces, `Settings` base, JSON logging, `SimClock` + `business_date_for`, Kafka factory + JSON codec + messaging adapters, Parquet `ObjectStore`, Postgres engine + base repository | `libs/common/` |
| A3 | **Database schema** — DDL migrations for dimensions (`dim_household`, `dim_zone`, `dim_date`) and the tables Part 2 fills (`rt_zone_metrics`, `daily_household_billing`, `daily_zone_solar_summary`, `alerts`, `batch_runs`) | `infra/postgres/init/` |
| A4 | **`meter-simulator`** — hexagonal service: diurnal + solar generation model, anomaly strategy, publish to `meter.readings`, `/metrics`, structured logs, unit tests with a fake publisher | `services/meter-simulator/` |
| A5 | **`batch-file-generator`** — writes tariff + weather CSV + `_SUCCESS` per simulated day; household catalogue consistent with the simulator (shared seed data); tests | `services/batch-file-generator/` |
| A6 | **`ingestion`** — Kafka → Parquet master dataset, DLQ routing, `/metrics`, structured logs, unit tests for the routing logic | `services/ingestion/` |
| A7 | Seed data + Kafka topic provisioning + DB seed script | `data/seed/`, `scripts/` |
| A8 | Part 1 Docker Compose (Kafka, Postgres, Prometheus, the three Part 1 services) | `infra/docker-compose.yml` |
| A9 | **Docs:** `PLAN.md`, `docs/ARCHITECTURE.md`, `docs/decisions/ADR-000{1,2}`, `docs/DATA_CONTRACTS.md`, `docs/DEV_SETUP.md`, `docs/PART2_ONBOARDING.md`, `docs/OBSERVABILITY.md` (design + stubs), `docs/REPORT_OUTLINE.md`, `README.md`, `TASKS.md` | `docs/`, root |
| A10 | **Handoff checkpoint** — Part 1 Definition of Done met, contracts frozen, onboarding doc verified against the code | — |

### Part 2 — Developer B  ·  *Processing, Serving, Orchestration & Observability*

| # | Deliverable | Files |
|---|---|---|
| B1 | **`stream-processor` (SPEED LAYER)** — Spark Structured Streaming; consume `meter.readings`; transform pipeline (validate → route bad → enrich with dims → event-time windowed aggregation: per-zone load kWh, solar kWh, renewable %, active meters; tumbling 1-min + sliding 5-min); upsert `rt_zone_metrics`; checkpointing; `/metrics`. Domain transforms are pure and unit-tested without Spark. | `services/stream-processor/` |
| B2 | **`batch-processor` (BATCH LAYER)** — Spark batch job (arg: `--business-date`); read `business_date=D` Parquet; per-household aggregation; join tariff + weather + subsidy; **billing Strategy** (flat / tiered / subsidised) → `HouseholdBill`; solar feed-in credit; write `daily_household_billing` + `daily_zone_solar_summary`; emit report artifact (CSV + HTML). Data-quality assertions. | `services/batch-processor/` |
| B3 | **Airflow DAG `daily_energy_reconciliation`** — `FileSensor`(tariff + weather) → schema/DQ validation → run `batch-processor` → post-run DQ gate → publish report → write `batch_runs` audit row → on-failure alert. Parametrised by `business_date` for backfill/replay. | `orchestration/airflow/dags/` |
| B4 | **`serving-api` (SERVING LAYER)** — FastAPI, read-only over Postgres via repositories: `/api/realtime/zones[/{zone}]`, `/api/billing/daily/{date}`, `/api/billing/household/{id}`, `/api/reports/solar/{date}`, `/healthz`, `/metrics`. Merges speed (today-so-far) + batch (finalised) views. Response models = shared DTOs. | `services/serving-api/` |
| B5 | **`alert-engine` (OBSERVABILITY)** — rule evaluator; each rule a Strategy (`NoDataRule`, `LowRenewableRule`, `HighRejectRateRule`, `BatchFailureRule`); scheduled evaluation; Observer fan-out to alert sinks (DB, log, optional webhook); writes `alerts`; `/metrics`. | `services/alert-engine/` |
| B6 | **Consolidated dashboard** — the daily billing/solar report rendered (HTML) + a live zone dashboard (small static page against the API, or provisioned Grafana). For the report screenshots. | `services/serving-api/` or `infra/grafana/` |
| B7 | **Full Docker Compose** — add Spark, Airflow, the four Part 2 services, Prometheus scrape targets, Grafana provisioning, healthchecks; `make up-full`, `make demo`. | `infra/docker-compose.part2.yml` |
| B8 | End-to-end + integration tests; `docs/RUNBOOK.md`; complete `docs/OBSERVABILITY.md`; demo script + recording. | `tests/`, `docs/` |
| B9 | Report sections: results & screenshots, observability design, serving-layer write-up; individual-contribution statement. | report |

### Handoff contract (what Part 1 guarantees Part 2)

Frozen and versioned in [`docs/DATA_CONTRACTS.md`](docs/DATA_CONTRACTS.md). In brief:

1. Kafka `meter.readings` — key `grid_zone`, value = JSON of `MeterReading` v1, 6 partitions; DLQ `meter.readings.dlq`.
2. Parquet master dataset — `data/lake/readings_raw/business_date=YYYY-MM-DD/*.parquet`, stated schema, `_SUCCESS` marker semantics, late-data policy.
3. Landing files — `tariff_reference_YYYY-MM-DD.csv`, `weather_forecast_YYYY-MM-DD.csv`, `_SUCCESS` marker, stated schemas.
4. Postgres — DSN via `PG_DSN`; dimensions populated; DDL present for all Part 2 output tables (Dev B evolves them via new migrations).
5. `energy_common` public API — `get_settings()`, `get_logger(component)`, `SimClock`, `business_date_for(ts)`, model classes, `build_consumer()/build_producer()`, `pg_engine()`, `serve_metrics(port)` — all docstring'd with examples.
6. Simulated-clock contract — `SIM_DAY_SECONDS`, epoch anchor, `business_date` derivation, midnight-trigger behaviour.

## 8. Timeline (14 days)

```
Day  1  2  3  4  5  6  7  8  9 10 11 12 13 14
A   [==scaffold+common==][=sources+ingestion=][handoff]
B                  [stream-proc][batch+DAG][serve+alerts][dash]
Both                                        [==integ/e2e==][demo][==report==]
```

- **Day 2** — scaffold, `energy_common`, contracts draft, compose skeleton, docs skeleton on `main`.
- **Day 5** — Part 1 Definition of Done; contracts frozen; **handoff checkpoint**.
- **Day 4** — Dev B starts `stream-processor` against the running simulator (overlap).
- **Day 9** — batch-processor + Airflow DAG green end-to-end.
- **Day 11** — alert-engine + dashboards + full compose.
- **Day 12** — integration/e2e green; demo recorded.
- **Day 14** — report finalised, submission (repo link + PDF + demo video).

## 9. Definition of Done

**Part 1**
- [ ] `make install && make lint && make typecheck && make test` all green.
- [ ] `make up && make topics` brings up Kafka + Postgres + Prometheus + the three sources/ingestion with healthy containers.
- [ ] `meter.readings` receiving records; anomalies present at the configured rate.
- [ ] At a simulated midnight, tariff + weather CSV + `_SUCCESS` appear in `data/landing/`.
- [ ] Parquet partitions appear under `data/lake/readings_raw/business_date=…/`; malformed messages land in the DLQ.
- [ ] Every Part 1 service emits JSON logs and exposes `/metrics`.
- [ ] `docs/DATA_CONTRACTS.md` matches the code; `docs/PART2_ONBOARDING.md` walked through by someone who didn't write it.

**Part 2**
- [ ] `make up-full && make demo` runs one full simulated day end-to-end unattended.
- [ ] `rt_zone_metrics` updates within seconds; `GET /api/realtime/zones` reflects it.
- [ ] Airflow DAG turns green for the closed day; `daily_household_billing` + report artifact produced; `batch_runs` row written.
- [ ] Serving API merges live + finalised views; OpenAPI docs load.
- [ ] `alert-engine` fires the low-renewable alert and the no-data alert on demand; rows in `alerts`.
- [ ] Dashboard screenshots captured for the report.
- [ ] `docs/RUNBOOK.md` + `docs/OBSERVABILITY.md` complete; e2e test green in CI.

## 10. Observability plan

Detail in [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md). Minimums:

- **Structured logging** — JSON to stdout from every service, fields: `ts, level, service, event,
  business_date, event_time, correlation_id, records_in, records_out, records_rejected, latency_ms`.
- **Metrics** — `/metrics` on every service: `*_records_produced_total`, `*_records_consumed_total`,
  `*_records_rejected_total`, `consumer_lag`, `microbatch_duration_seconds`, `batch_job_rows`,
  `http_request_duration_seconds`.
- **Alert rules** (≥1 required; we implement four):
  1. **No data** — no readings ingested for a zone in `ALERT_NO_DATA_MINUTES` → zone feed offline.
  2. **Low renewable** — zone renewable contribution < `ALERT_LOW_RENEWABLE_PCT`% for a sustained window (the brief's explicit alert).
  3. **High reject rate** — DLQ / malformed ratio > `ALERT_MAX_REJECT_RATE`.
  4. **Batch failure** — `daily_energy_reconciliation` failed or produced 0 rows.

## 11. Testing strategy

| Level | Scope | Runs |
|---|---|---|
| Unit | pure `domain/` + `application/` logic with fakes for every port | `make test`, every push, no infra |
| Contract | domain models round-trip through the JSON codec; DDL applies cleanly | `make test` |
| Integration | adapters against real Kafka/Postgres (`-m integration`) | `make up` then `make test-integration` |
| End-to-end | `make demo` asserts tables/report artifacts after one simulated day | pre-submission, CI nightly (Part 2) |

## 12. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Spark + Airflow images heavy / slow on laptops | med | med | Pin slim images; document resource needs in `DEV_SETUP.md`; `batch-processor` runnable as a plain PySpark script without Airflow for dev. |
| Contract drift between Part 1 and Part 2 | med | high | Contracts frozen at handoff; `MeterReading` is versioned; codec round-trip test; changes go through a PR that updates `DATA_CONTRACTS.md`. |
| Simulated-clock edge cases at midnight rollover | med | med | `business_date_for` unit-tested around boundaries; batch generator writes `_SUCCESS` last. |
| Stream/batch numbers don't reconcile in the demo | med | med | Both layers share the same pure transform functions; e2e test asserts speed-layer daily totals ≈ batch-layer totals within tolerance. |
| Scope creep on the dashboard | med | low | Grafana provisioned dashboard is the fallback; a bespoke UI is optional. |
| Only one laptop can run the full stack | low | med | Compose profiles; Dev B can develop `stream-processor` against Kafka alone. |

## 13. Marking rubric → where it's earned

| Criterion | Marks | Covered by |
|---|---:|---|
| Architecture Decision & Justification (Lambda vs Kappa) | 20 | §3, ADR-0001, report §2 |
| Technology Stack Selection & Justification | 10 | §4, ADR-0002, report §4 |
| Data Ingestion Implementation | 15 | `meter-simulator`, `batch-file-generator`, `ingestion` (Part 1) |
| Processing Layer Implementation | 15 | `stream-processor` + `batch-processor` (Part 2 B1–B2) |
| Storage & Serving Layer | 10 | Postgres schema (A3) + `serving-api` (B4) |
| Observability | 10 | §10, `docs/OBSERVABILITY.md`, logging in `energy_common`, `alert-engine` (B5) |
| Report | 15 | `docs/REPORT_OUTLINE.md` |
| Code Quality & Documentation | 5 | §5, hexagonal layout, CI, this doc set |

## 14. Deliverables checklist (submission)

- [ ] Git repository link (private, graders added): <https://github.com/NirangaNiluminda/smart-grid-lambda-platform>.
- [ ] Report PDF, 8–15 pages, following `docs/REPORT_OUTLINE.md`.
- [ ] 5–10 min demo video showing the pipeline end-to-end + observability, **or** a live demo ready.
- [ ] Assumptions / simplifications / simulated-time compression stated (README + report §1).
- [ ] Statement of individual contributions (Dev A = Part 1, Dev B = Part 2; see `TASKS.md`).
