# Smart Grid Energy Monitoring & Billing — Data Platform

EC8203 *Applied Big Data Engineering* — Mini Project.

An end-to-end **Lambda architecture** data platform that ingests smart-meter
readings in real time, serves live grid-load and renewable-mix metrics by zone,
and reconciles a full day of consumption against a once-a-day tariff/weather feed
to produce an authoritative per-household billing and solar-contribution report.

> **Use case:** Use Case 3 — Smart Grid Energy Monitoring & Billing (from the brief in
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

## Quick start (Part 1 — available now)

```bash
cp .env.example .env
make install        # local .venv for tests + tooling
make test           # unit tests, no infrastructure needed
make up             # Kafka + Postgres + Prometheus + sources + ingestion
make topics         # create Kafka topics
docker compose -f infra/docker-compose.yml logs -f meter-simulator ingestion
```

See [`docs/DEV_SETUP.md`](docs/DEV_SETUP.md) for prerequisites and troubleshooting.

## Status

| Layer | Component | Owner | State |
|---|---|---|---|
| Ingestion | meter-simulator | Dev A | ✅ Part 1 |
| Ingestion | batch-file-generator | Dev A | ✅ Part 1 |
| Ingestion | ingestion → data lake | Dev A | ✅ Part 1 |
| Foundation | `libs/common` | Dev A | ✅ Part 1 |
| Processing | stream-processor (speed) | Dev B | 🔲 Part 2 |
| Processing | batch-processor (batch) | Dev B | 🔲 Part 2 |
| Orchestration | Airflow DAG | Dev B | 🔲 Part 2 |
| Serving | serving-api | Dev B | 🔲 Part 2 |
| Observability | alert-engine + dashboards | Dev B | 🔲 Part 2 |

Task-level tracking: [`TASKS.md`](TASKS.md).
