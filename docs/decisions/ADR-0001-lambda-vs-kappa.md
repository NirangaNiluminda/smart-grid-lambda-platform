# ADR-0001 — Lambda vs Kappa architecture

- **Status:** Accepted
- **Date:** 2026-01 (project week 1)
- **Deciders:** Dev A, Dev B
- **Rubric weight:** 20 / 100

## Context

Use Case 3 (Smart Grid Energy Monitoring & Billing) has two consumers with different needs:

1. **Grid operations** want *current grid load and renewable contribution by zone* — updated within
   seconds, tolerant of small inaccuracies (a late meter reading does not matter).
2. **Billing** want *each household's bill for the closed business day*, computed once the day's
   tariff and weather feed has arrived — exact, auditable, and reproducible if an input is later
   corrected.

We must choose one architecture and defend it.

### Forces

| Force | Real-time zone view | Daily household bill |
|---|---|---|
| Latency | seconds | next morning |
| Consistency | eventual / approximate acceptable | strong; must reconcile exactly |
| Input | unbounded partial stream | one bounded closed day + daily reference feed |
| Reprocessing | not meaningful | **required** (tariff fix, late data, audit) |
| Trigger | continuous | "business day D closed" + "feed for D landed" |
| Computation | rough per-zone sums over a short window | per-household join of full-day consumption × tariff × subsidy − solar credit |

## Options considered

### Option A — Lambda  ✅ chosen

A **speed layer** (Spark Structured Streaming over Kafka) maintains the approximate per-zone live
view. A **batch layer** (Spark batch orchestrated by Airflow) recomputes the authoritative daily
bill from an immutable Parquet master dataset. A **serving layer** (Postgres + FastAPI) exposes the
merge of the two.

**Pros**
- The speed and batch computations are *genuinely different* (different inputs, completeness,
  consumers). Lambda's two layers map onto that split without compromise.
- Batch layer inputs (the tariff feed) only exist once per day — a sensed, scheduled, parametrised
  job is the honest shape of the work and gives us Airflow's retries / backfill / audit history.
- Recomputability is free: re-run `batch-processor --business-date D` over the immutable partition.
- Latency-sensitive consumer and consistency-sensitive consumer are each served by the layer that
  suits them.
- Demonstrates **both** batch and stream processing, which the brief's learning outcomes and
  preferred stack (Kafka + Spark + Airflow) explicitly call for.

**Cons**
- Two code paths for anything computed in both layers → risk of drift.
  *Mitigation:* the per-reading transforms (validate, enrich, unit-normalise) live in
  `energy_common` as pure functions imported by **both** layers; an e2e test asserts the speed
  layer's rolled-up daily totals ≈ the batch layer's totals within tolerance.
- More moving parts to operate (Spark streaming job + Airflow + batch job).
  *Mitigation:* one `docker compose`; `batch-processor` also runs as a plain script without Airflow.

### Option B — Kappa  ❌ rejected

One streaming pipeline. The daily tariff/weather feed is published to a **log-compacted** Kafka
topic (`household_id → latest tariff`). Billing is maintained as **streaming state**: a stateful
job joins the meter stream with the compacted tariff table and emits per-household running charges,
finalising a day when the watermark passes midnight. Reprocessing = replay the `meter.readings`
topic from the retention log.

**Pros**
- One codebase, one engine, no batch/stream logic to keep in sync.
- Replay/reprocessing is a first-class concept, not bolted on.
- Fewer components (no Airflow, no separate batch job).
- Lower end-to-end latency for the billing numbers too (running totals always current).

**Cons — and why they are decisive here**
- **Billing is periodic and bounded by nature.** You invoice a *closed period*. Expressing
  "the day closed, emit the authoritative, immutable invoice for D" as watermark + allowed-lateness
  + trigger logic is harder to reason about and harder to audit than a DAG with an explicit
  `business_date` parameter and a recorded run.
- **Reprocessing cost.** Correcting one historical tariff means replaying the *entire*
  `meter.readings` history through a stateful job. The Lambda batch job touches only
  `business_date=D` — one Parquet partition. For an audit-driven "re-bill March 3rd" request, the
  bounded job wins decisively on time and cost.
- **The exercise wants both paradigms.** Kappa collapses a "differentiate Lambda and Kappa and
  apply batch *and* stream processing" assignment into a single-paradigm solution, leaving batch
  processing and Airflow undemonstrated.
- Kappa's central advantage — no dual logic — is **muted** here because the two layers are not the
  same computation at two freshnesses; they are different computations. There is little duplicated
  logic to eliminate.

## Decision

**Adopt Lambda.** The daily authoritative bill — bounded, auditable, recomputable, triggered by a
once-a-day feed — is a textbook batch-layer responsibility, and the real-time zone view is a
textbook speed-layer responsibility. Serving them from separate layers lets each meet its own SLA.

## Consequences

- We run a Spark Structured Streaming job **and** an Airflow-orchestrated Spark batch job.
- `ingestion` must write an **immutable, partitioned Parquet master dataset** — it is the batch
  layer's single source of truth and the replay substrate.
- Shared per-reading transforms live in `energy_common` and are imported by both layers to prevent
  drift; an e2e reconciliation test guards it.
- The serving layer must merge "live" and "finalised" views and make the boundary explicit to
  clients (a day is either *provisional* from the speed layer or *final* from the batch layer).
- If scope were reduced to only the real-time zone dashboard, this ADR should be revisited — Kappa
  would then be the better choice.
