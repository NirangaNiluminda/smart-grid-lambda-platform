# Smart Grid Lambda Platform

EC8203 *Applied Big Data Engineering* — Mini Project ·
<https://github.com/NirangaNiluminda/smart-grid-lambda-platform>

Smart Grid Energy Monitoring & Billing.

An end-to-end **Lambda architecture** data platform that ingests smart-meter
readings in real time, serves live grid-load and renewable-mix metrics by zone,
and reconciles a full day of consumption against a once-a-day tariff/weather feed
to produce an authoritative per-household billing and solar-contribution report.

> **Use case:** Use Case 3 — Smart Grid Energy Monitoring & Billing (from the course brief)
> [`docs/assignment/`](docs/assignment/EC8203-MiniProject-2026.pdf)).
>
> **Business question:** *What is the current grid load and renewable contribution by
> zone, and what will each household's bill look like once daily tariff data is applied
> to their consumption?*

---

## Start here

| You are… | Read |
|---|---|
| Anyone, first time | This file, then [`PLAN.md`](PLAN.md) |
| Reviewing the architecture decision | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) + [`docs/decisions/`](docs/decisions/) |
| **Developer B, picking up Part 2** | [`docs/PART2_ONBOARDING.md`](docs/PART2_ONBOARDING.md) |
| Wiring a producer/consumer/job | [`docs/DATA_CONTRACTS.md`](docs/DATA_CONTRACTS.md) |
| Running it locally | [`docs/DEV_SETUP.md`](docs/DEV_SETUP.md) |
| Operating / demoing the running system | [`docs/RUNBOOK.md`](docs/RUNBOOK.md) *(Part 2)* |
| Writing the report | [`docs/REPORT_OUTLINE.md`](docs/REPORT_OUTLINE.md) |

## Architecture at a glance

```
                      ┌───────────────────────── SPEED LAYER (low latency, approximate) ─────────────────────────┐
 meter-simulator ──▶ Kafka ──▶ ingestion ──▶ Parquet lake        stream-processor (Spark Structured Streaming)
 (streaming source)  meter.readings   (raw, immutable            └── validate → enrich → windowed aggregation ──▶ Postgres rt_zone_metrics
                                       master dataset)                                                                    │
 batch-file-generator ──▶ landing zone (tariff + weather CSV, once per simulated day)                                     ▼
 (daily batch source)          │                                                                              serving-api (FastAPI)  ◀── dashboard
                               ▼                                                                                          ▲
                      Airflow DAG ──▶ batch-processor (Spark batch job) ──▶ Postgres daily_household_billing ──────────────┘
                      └──────────────────────── BATCH LAYER (authoritative, recomputable) ───────────────────────────────┘

 alert-engine  ── evaluates health/threshold rules over Postgres + Prometheus ──▶ alerts table + logs
```

Full diagram and data flow: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Repository layout

```
libs/common/            Shared kernel — domain models, port interfaces, config, logging, sim-clock, adapters
services/
  meter-simulator/      [Part 1] Streaming source: smart-meter readings → Kafka
  batch-file-generator/ [Part 1] Daily batch source: tariff + weather CSV → landing zone
  ingestion/            [Part 1] Kafka → raw Parquet data lake (Lambda master dataset) + DLQ
  stream-processor/     [Part 2] Speed layer: Spark Structured Streaming → rt_zone_metrics
  batch-processor/      [Part 2] Batch layer: Spark batch billing/solar reconciliation
  serving-api/          [Part 2] Read model: FastAPI over Postgres
  alert-engine/         [Part 2] Observability: threshold + health-check rules → alerts
orchestration/airflow/  [Part 2] daily_energy_reconciliation DAG
infra/                  docker-compose, Postgres schema, Prometheus/Grafana config
data/                   seed/ (committed) · landing/ + lake/ (runtime, git-ignored)
docs/                   Plan, architecture, contracts, onboarding, report outline
```

## Docker Setup: Base + Overlay Architecture

The platform uses a layered Docker Compose architecture with dedicated `Dockerfile`s per service:

1. **Independent Service `Dockerfile`s (`services/*/Dockerfile`):**
   - Each microservice has an isolated `Dockerfile` tailored to its runtime dependencies (e.g. lightweight Python for `meter-simulator`, PySpark for `batch-processor`, FastAPI for `serving-api`).
   - Keeps images lean and prevents cache invalidation across services.

2. **Base Compose (`infra/docker-compose.yml`):**
   - Spins up foundational infrastructure: Apache Kafka (KRaft), PostgreSQL 16, Prometheus, and the Part 1 producers/ingestor.
   - Run via `make up`.

3. **Part 2 Overlay Compose (`infra/docker-compose.part2.yml`):**
   - Layers downstream processing, serving, and orchestration onto the base stack using Docker Compose multi-file merging:
     ```bash
     docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.part2.yml up -d --build
     ```
   - Run via `make up-full`.

---

## How Part 2 Works

Part 2 completes the Lambda Architecture by consuming the streams and data lake produced in Part 1:

### 1. Speed Layer (`stream-processor`)
* **Technology:** Apache Spark Structured Streaming.
* **Input:** Real-time messages from Kafka topic `meter.readings`.
* **Processing:** Validates and enriches readings against dimension data (`dim_zone`, `dim_household`), applies watermarking for late-arriving data, and computes tumbling (1-min) and sliding (5-min) window aggregations.
* **Output:** Writes near-real-time metrics (`total_consumption_kwh`, `total_solar_kwh`, `renewable_pct`, `active_meter_count`) into Postgres `rt_zone_metrics`.

### 2. Batch Layer (`batch-processor`)
* **Technology:** Apache Spark Batch.
* **Input:** Immutable Parquet master dataset in `data/lake/readings_raw/business_date=YYYY-MM-DD/` once partition is closed (`_SUCCESS`), plus daily landing CSV feeds (`tariff_reference_*.csv`, `weather_forecast_*.csv`).
* **Processing:** Recomputes authoritative daily rollups per household, applies configurable tariff strategies (`FlatBilling`, `TieredBilling`, `SubsidisedBilling`), calculates net billing and solar export credits, and runs data quality assertions.
* **Output:** Idempotently upserts `daily_household_billing` and `daily_zone_solar_summary` into Postgres, and outputs report artifacts (`reports/out/billing_report_*.html` and `.csv`).

### 3. Orchestration (`orchestration/airflow`)
* **Technology:** Apache Airflow (`daily_energy_reconciliation` DAG).
* **Role:** Senses the daily simulated feed and lake partition closure, triggers the `batch-processor` job, verifies data quality, and refreshes materialized views.

### 4. Serving Layer (`serving-api`)
* **Technology:** FastAPI & Uvicorn.
* **Role:** Exposes high-performance read APIs over Postgres (`/api/realtime/zones`, `/api/billing/household/{id}`, etc.) uniting real-time speed layer data and reconciled batch records for frontends and dashboards.

### 5. Observability & Alerting (`alert-engine`, Prometheus, Grafana)
* **Technology:** Python daemon, Prometheus, and Grafana.
* **Role:** Evaluates health check and SLA rules over Postgres and Prometheus metrics (e.g. data gaps > 3 min, renewable generation < 15%, excessive reject rate), logging alerts to the `alerts` table and displaying operational dashboards.

---

## Quick start

### Option A: Part 1 Only (Core Ingestion Stack)

```bash
git clone https://github.com/NirangaNiluminda/smart-grid-lambda-platform.git
cd smart-grid-lambda-platform
cp .env.example .env
make install        # local .venv for tests + tooling
make test           # unit tests, no infrastructure needed
make up             # Kafka + Postgres + Prometheus + sources + ingestion
make topics         # create Kafka topics
make seed           # populate Postgres dimension tables
docker compose --env-file .env -f infra/docker-compose.yml logs -f meter-simulator ingestion
```

### Option B: Full Stack (Part 1 + Part 2 Layer)

```bash
make install-part2  # install heavy dependencies (pyspark, fastapi, etc.)
make up-full        # builds & runs both base + Part 2 overlay containers
make seed           # ensure dimension tables and DDLs are seeded
make topics         # ensure Kafka topics exist
make logs           # tail logs across all running services
```

To stop containers:
```bash
make down           # stop containers, preserve volume state
make clean          # stop containers and remove volumes + generated data
```

See [`docs/DEV_SETUP.md`](docs/DEV_SETUP.md) and [`docs/PART2_ONBOARDING.md`](docs/PART2_ONBOARDING.md) for detailed guides and troubleshooting.

## Status

| Layer | Component | Owner | State |
|---|---|---|---|
| Ingestion | meter-simulator | Dev A | ✅ Part 1 |
| Ingestion | batch-file-generator | Dev A | ✅ Part 1 |
| Ingestion | ingestion → data lake | Dev A | ✅ Part 1 |
| Foundation | `libs/common` | Dev A | ✅ Part 1 |
| Processing | stream-processor (speed) | Dev B | ✅ Part 2 |
| Processing | batch-processor (batch) | Dev B | ✅ Part 2 |
| Orchestration | Airflow DAG | Dev B | ✅ Part 2 |
| Serving | serving-api | Dev B | ✅ Part 2 |
| Observability | alert-engine + dashboards | Dev B | ✅ Part 2 |

Task-level tracking, including the post-merge hardening pass: [`TASKS.md`](TASKS.md).
Remaining before submission: report sections, `docs/OBSERVABILITY.md §5` screenshots, contribution statement.
