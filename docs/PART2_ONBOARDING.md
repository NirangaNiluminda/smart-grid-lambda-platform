# Part 2 — Developer B Onboarding

**Read this once, top to bottom. It is written so you can start coding without a call.**

You own the **Processing, Serving, Orchestration and Observability** half of the platform. Part 1
(Dev A) has built everything *upstream* of you and frozen the interfaces you consume.

---

## 1. The 10-minute mental model

- Architecture is **Lambda**. Read [`decisions/ADR-0001`](decisions/ADR-0001-lambda-vs-kappa.md) —
  you will be asked to defend it in the viva too.
- Data already flows: `meter-simulator` → Kafka `meter.readings` → `ingestion` → immutable Parquet
  lake. A daily `tariff` + `weather` feed already lands in `data/landing/`.
- **Your job:** turn that into (a) a live per-zone view, (b) an authoritative daily bill, (c) an
  observable, alerting system, (d) an API + dashboard over it.
- Everything you consume is specified in [`DATA_CONTRACTS.md`](DATA_CONTRACTS.md). If reality
  disagrees with that doc, that is a bug — raise it, don't work around it.

```
        YOU START HERE ─────────────────────────────────────────────────┐
Kafka meter.readings ──▶ stream-processor (B1) ──▶ rt_zone_metrics ──┐   │
Parquet lake ──────────▶ batch-processor  (B2) ──▶ daily_household_billing
data/landing ─────────▶ Airflow DAG      (B3) drives B2              │   │
Postgres ─────────────▶ serving-api      (B4) ◀── merges both ───────┴───┘
Postgres + Prometheus ▶ alert-engine     (B5) ──▶ alerts
```

## 2. Get it running (5 min)

```bash
cp .env.example .env
make install
make up && make topics && make seed
# confirm upstream is alive:
docker compose --env-file .env -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic meter.readings --max-messages 3
ls -R data/lake/readings_raw/
```

If those three show data, your inputs are working. Now `make up-full` will additionally start the
skeletons of your services (they currently no-op / raise `NotImplementedError` with a pointer here).

## 3. What Part 1 guarantees you

| Interface | Where | Contract |
|---|---|---|
| `meter.readings` topic | Kafka | key = `grid_zone`, value = `MeterReading` v1 JSON, 6 partitions. §1 of DATA_CONTRACTS. |
| `meter.readings.dlq` | Kafka | you may also write here for parse-ok/validate-fail records. |
| Parquet master dataset | `data/lake/readings_raw/business_date=…/` | immutable, Hive-partitioned, `_SUCCESS` when a day closes. Schema in §2. |
| Daily feed | `data/landing/tariff_reference_*.csv`, `weather_forecast_*.csv`, `_SUCCESS_*` | schemas in §3. Marker written last — sense the marker, not the CSVs. |
| Postgres | `PG_DSN` | `dim_zone`, `dim_household`, `dim_date` populated. All your output tables already created (DDL in `infra/postgres/init/`). |
| `energy_common` | `libs/common` | models, ports, `get_settings`, `get_logger`, `SimClock`, `business_date_for`, Kafka/PG factories, `serve_metrics`. §5. |
| Simulated clock | env `SIM_*` | `business_date_for(event_timestamp)` tells you which day a reading belongs to. |

**You must not change** the `MeterReading` schema, the lake layout, the landing-file format, or the
`energy_common` public API without a PR that updates `DATA_CONTRACTS.md` and gets Dev A's review.
**You may** add new Postgres migrations (next number in `infra/postgres/init/`), new Kafka topics,
new env vars (update `.env.example`), and anything inside `services/{stream-processor,batch-processor,serving-api,alert-engine}`.

## 4. Your task list

Full breakdown with acceptance criteria in [`../TASKS.md`](../TASKS.md) (Part 2 section). Order:

### B1 · stream-processor (speed layer) — *start here, it only needs Kafka*
- Spark Structured Streaming job, consumer group `stream-processor-speed-layer`.
- Pipeline (reuse `energy_common` pure transforms — see `energy_common.domain.transforms`):
  `parse → validate (bad → DLQ) → enrich (dim_zone/dim_household) → withWatermark → windowed agg`.
- Two window types per `grid_zone`: tumbling 1-min, sliding 5-min/1-min slide.
- Aggregates: `total_consumption_kwh`, `total_solar_kwh`, `renewable_pct` (clamp 0–100),
  `active_meter_count` (approx distinct).
- Sink: `foreachBatch` → upsert `rt_zone_metrics` on `(grid_zone, window_type, window_start)`.
- Checkpoint dir: `${LAKE_ROOT}/../checkpoints/stream-processor`.
- `serve_metrics(9111)`; emit `microbatch_duration_seconds`, `records_consumed_total`, `records_rejected_total`.
- **Keep every transform pure and Spark-free where possible** — unit-test them with plain dicts;
  the Spark wiring is an adapter with one thin integration test.
- **Done when:** `rt_zone_metrics` updates within seconds of readings; `GET /api/realtime/zones`
  (once B4 exists) reflects it; unit tests cover the transforms; one `-m integration` test runs the
  job against a test topic.

### B2 · batch-processor (batch layer)
- Entrypoint: `python -m batch_processor --business-date YYYY-MM-DD` (also importable by Airflow).
- Read `readings_raw/business_date=D`; require the partition `_SUCCESS` (fail loudly if missing,
  unless `--allow-open`).
- Per-household daily rollup → join `tariff_reference_D` + `weather_forecast_D` + `dim_household`.
- **Billing Strategy** (`billing_tier` selects): `FlatBilling`, `TieredBilling`, `SubsidisedBilling`,
  each implementing `BillingStrategy.charge(usage) -> Charge`. Solar: `self_consumed`,
  `grid_import`, `solar_export`; `solar_credit` from an export rate.
- Write `daily_household_billing` + `daily_zone_solar_summary` (idempotent upsert on the PK; a
  re-run for D fully replaces D).
- Emit report artifact: `reports/out/billing_report_D.csv` + `.html`.
- DQ assertions: every active household billed exactly once; `renewable_pct` within
  `solar_irradiance_index` sanity band; no negative `net_amount` without a subsidy.
- **Done when:** re-running for the same date is idempotent and reproduces identical numbers;
  DQ assertions pass; report artifact renders.

### B3 · Airflow DAG `daily_energy_reconciliation`
- Schedule: triggered by simulated-day close (a small sensor loop) **or** a fixed wall-clock
  interval matching `SIM_DAY_SECONDS`; parametrised `business_date`.
- Tasks: `wait_for_feed` (`FileSensor` on `_SUCCESS_{{ds}}`) → `validate_feed` (schema + row counts)
  → `wait_for_lake` (partition `_SUCCESS`) → `run_batch` (`batch-processor`) → `dq_gate` →
  `publish_report` → `record_run` (`batch_runs`) . `on_failure_callback` → alert row.
- Backfill-safe: `airflow dags backfill -s D1 -e D2` re-bills a range.
- **Done when:** DAG goes green for a closed day unattended in `make demo`; a forced failure writes
  a `failed` `batch_runs` row and a `critical` alert.

### B4 · serving-api (serving layer)
- FastAPI, **read-only** over Postgres via repositories (no computation in the API).
- Endpoints: `GET /api/realtime/zones`, `/api/realtime/zones/{zone}`, `/api/billing/daily/{date}`,
  `/api/billing/household/{household_id}?date=`, `/api/reports/solar/{date}`, `/healthz`, `/metrics`.
- Response models = `energy_common` domain models (or thin DTOs mapping from them).
- Merge rule: for "today" return speed-layer numbers tagged `"status": "provisional"`; for any day
  with a `batch_runs.success` row return batch numbers tagged `"status": "final"`.
- **Done when:** `/docs` loads; every endpoint returns real data after `make demo`; `/metrics`
  scraped by Prometheus.

### B5 · alert-engine (observability)
- Scheduled evaluator. Each rule is a `Rule` strategy: `evaluate(ctx) -> list[Alert]`.
  - `NoDataRule` — no `rt_zone_metrics` for a zone in `ALERT_NO_DATA_MINUTES`.
  - `LowRenewableRule` — zone `renewable_pct` < `ALERT_LOW_RENEWABLE_PCT` sustained (brief's alert).
  - `HighRejectRateRule` — `records_rejected_total / records_consumed_total` > `ALERT_MAX_REJECT_RATE`.
  - `BatchFailureRule` — latest `batch_runs` for yesterday not `success`.
- **Observer** fan-out: `DbAlertSink` (always), `LogAlertSink` (always), `WebhookAlertSink` (optional).
- Dedupe: don't re-fire an unresolved alert with the same `(rule_name, grid_zone, business_date)`.
- **Done when:** each rule has unit tests with a fake context; `LowRenewableRule` and `NoDataRule`
  demonstrably fire in `make demo`; rows in `alerts`.

### B6 · dashboard, B7 · full compose, B8 · docs/tests, B9 · report
See TASKS.md.

## 5. Conventions you must follow

- **Hexagonal layout** per service (see `services/meter-simulator/` as the reference implementation):
  `domain/` (pure, no framework) · `application/` (use-cases) · `adapters/` (I/O) · `composition.py`
  (wiring) · `__main__.py`.
- **Ports over concretes.** Depend on `energy_common.domain.ports`; inject adapters in
  `composition.py`. Tests inject fakes.
- **One `Settings` per service** (`pydantic-settings`), validated at import of `config.py`.
- **Structured logs only** — `get_logger("stream-processor")`, bind `business_date` / `correlation_id`.
- **Every service exposes `/metrics`.**
- **Repository pattern** for all SQL — no raw SQL in `application/` or `domain/`.
- Lint/type/test green before every PR: `make lint typecheck test`.
- Update `DATA_CONTRACTS.md` in the same PR as any contract-affecting change.

## 6. FAQ

**Q: Can I run Spark without Airflow while developing B2?**
Yes — `python -m batch_processor --business-date 2026-01-01`. Airflow (B3) just calls the same entrypoint.

**Q: The lake partition has no `_SUCCESS` — is that a bug?**
Only if the simulated day has closed. Check the simulator's logged simulated clock. Use `--allow-open` for dev.

**Q: How do I get a full simulated day quickly?**
Lower `SIM_DAY_SECONDS` (e.g. 120) in `.env`, `make clean && make up-full`.

**Q: Speed-layer totals don't match batch-layer totals.**
Expected within a tolerance (speed layer drops post-watermark late data). The e2e test asserts
they're within ~2%. A large gap means a transform diverged — both layers must import the same
functions from `energy_common.domain.transforms`.

**Q: Where do I put a new dimension column?**
New migration file `infra/postgres/init/00N_*.sql`, then update `DATA_CONTRACTS.md §4` and the seed script.
