# Architecture

Smart Grid Energy Monitoring & Billing — **Lambda architecture**.

- The decision itself (Lambda vs Kappa, rejected alternative) → [`decisions/ADR-0001-lambda-vs-kappa.md`](decisions/ADR-0001-lambda-vs-kappa.md)
- Per-component tech justification → [`decisions/ADR-0002-technology-stack.md`](decisions/ADR-0002-technology-stack.md)
- Every schema / topic / file format → [`DATA_CONTRACTS.md`](DATA_CONTRACTS.md)

---

## 1. Context

```mermaid
flowchart LR
    subgraph Sources["Simulated sources (Python)"]
        MS["meter-simulator\n(streaming, ~2s)"]
        BFG["batch-file-generator\n(daily drop)"]
    end
    subgraph Platform["Data platform"]
        K[("Kafka\nmeter.readings")]
        LZ[/"Landing zone\ntariff + weather CSV"/]
        LAKE[("Parquet master dataset\nbusiness_date=…")]
        SPEED["Speed layer\nSpark Structured Streaming"]
        BATCH["Batch layer\nSpark batch + Airflow"]
        PG[("PostgreSQL\nserving store")]
        API["serving-api (FastAPI)"]
        ALERT["alert-engine"]
    end
    Consumers["Grid ops dashboard · Billing team · Graders"]

    MS --> K
    BFG --> LZ
    K --> SPEED
    K --> ING["ingestion"] --> LAKE
    LAKE --> BATCH
    LZ --> BATCH
    SPEED --> PG
    BATCH --> PG
    PG --> API --> Consumers
    PG --> ALERT
    ALERT --> PG
```

## 2. Lambda layers

```mermaid
flowchart TD
    K[("Kafka meter.readings\nretained, replayable log")]

    subgraph SL["SPEED LAYER — low latency, approximate, transient"]
        S1["validate + route bad → DLQ"]
        S2["enrich: join dim_zone / dim_household, derive event-time parts"]
        S3["event-time windowed aggregation\ntumbling 1-min · sliding 5-min\nper grid_zone"]
        S1 --> S2 --> S3
    end

    subgraph BL["BATCH LAYER — authoritative, recomputable, immutable"]
        B0["ingestion: raw readings → Parquet\npartitioned by business_date"]
        B1["read business_date=D partition"]
        B2["per-household daily aggregation"]
        B3["join tariff_reference + weather_forecast"]
        B4["billing strategy: flat / tiered / subsidised\n+ solar feed-in credit"]
        B5["write daily_household_billing\n+ daily_zone_solar_summary\n+ report artifact"]
        B0 --> B1 --> B2 --> B3 --> B4 --> B5
    end

    subgraph SRV["SERVING LAYER — merged read model"]
        R1["rt_zone_metrics  (from speed)"]
        R2["daily_household_billing  (from batch)"]
        R3["serving-api merges:\n'today so far' (speed) + 'finalised' (batch)"]
        R1 --> R3
        R2 --> R3
    end

    K --> SL
    K --> B0
    SL --> R1
    BL --> R2
    R3 --> U["clients / dashboard"]
```

**Speed layer** answers *"grid load and renewable mix by zone right now"*. It is allowed to be
approximate: readings that arrive after their window's watermark are dropped from the live view —
the batch layer will account for them.

**Batch layer** answers *"what is each household's bill for business day D"*. It runs once the day
is closed **and** the day's tariff/weather feed has landed. It recomputes from the immutable Parquet
master dataset, so re-running it for a past day (tariff correction, late data) reproduces an exact,
auditable result.

**Serving layer** is a plain read model in Postgres. `serving-api` never computes — it reads
`rt_zone_metrics` for the live view and `daily_household_billing` for finalised days, and stitches
them so a client sees one coherent picture.

## 3. Request / data flow — a full simulated day

```mermaid
sequenceDiagram
    autonumber
    participant Sim as meter-simulator
    participant K as Kafka
    participant Ing as ingestion
    participant Lake as Parquet lake
    participant SP as stream-processor
    participant PG as Postgres
    participant BFG as batch-file-generator
    participant LZ as landing zone
    participant AF as Airflow
    participant BP as batch-processor
    participant API as serving-api

    loop every ~2 simulated seconds
        Sim->>K: MeterReading (key = grid_zone)
        K->>Ing: consume
        Ing->>Lake: append Parquet (business_date=D)
        K->>SP: consume
        SP->>PG: upsert rt_zone_metrics (windowed)
    end
    Note over BFG,LZ: at simulated midnight (D → D+1)
    BFG->>LZ: tariff_reference_D.csv, weather_forecast_D.csv, _SUCCESS
    AF->>LZ: FileSensor detects _SUCCESS
    AF->>BP: run batch-processor --business-date D
    BP->>Lake: read business_date=D
    BP->>LZ: read tariff + weather for D
    BP->>PG: write daily_household_billing, daily_zone_solar_summary
    BP->>AF: rows written, DQ checks pass
    AF->>PG: insert batch_runs audit row
    API->>PG: GET /api/billing/daily/D  → finalised
    API->>PG: GET /api/realtime/zones   → live (D+1 so far)
```

## 4. Service internal structure (hexagonal)

Every service follows the same shape so a reader who understands one understands all:

```
services/<name>/
  src/<pkg>/
    domain/        pure business rules + models + PORT interfaces   (no framework imports)
    application/   use-cases orchestrating the domain via ports
    adapters/      inbound + outbound implementations of the ports  (Kafka, Postgres, FS, HTTP)
    config.py      Settings (pydantic-settings), validated at startup
    composition.py composition root — builds adapters, injects into the use-case
    __main__.py    process entrypoint
  tests/
    unit/          domain + application, every port faked
    integration/   adapters against real infra   (marker: integration)
  Dockerfile
  pyproject.toml
  README.md
```

```mermaid
flowchart LR
    subgraph Hexagon["service"]
        direction TB
        IN["inbound adapter\n(Kafka consumer / HTTP)"]
        APP["application\nuse-case"]
        DOM["domain\nrules + ports"]
        OUT["outbound adapter\n(Postgres / Kafka / FS)"]
        IN --> APP --> DOM
        APP --> OUT
    end
    Kafka[("Kafka")] --> IN
    OUT --> PG[("Postgres")]
    DOM -. defines interfaces .-> OUT
```

The domain defines the interfaces; the adapters implement them; the composition root is the only
code that imports both. This is what lets us unit-test all business logic with zero infrastructure.

## 5. Deployment

```mermaid
flowchart TB
    subgraph compose["docker compose"]
        subgraph p1["infra/docker-compose.yml — Part 1"]
            kafka; postgres; prometheus
            meter-simulator; batch-file-generator; ingestion
        end
        subgraph p2["infra/docker-compose.part2.yml — Part 2 overlay"]
            spark; airflow
            stream-processor; batch-processor
            serving-api; alert-engine; grafana
        end
    end
```

- `make up` → Part 1 only (what exists today).
- `make up-full` → both files, the whole platform.
- One Postgres instance, two databases: `energy_serving` and `airflow`.
- Kafka in KRaft mode (no ZooKeeper container).

## 6. Simulated time

| Knob | Default | Meaning |
|---|---|---|
| `SIM_DAY_SECONDS` | 600 | wall-clock seconds per simulated business day |
| `SIM_EPOCH_WALLCLOCK` | process start | wall-clock instant mapped to simulated `SIM_START_DATE` 00:00 |
| `SIM_START_DATE` | 2026-01-01 | first simulated calendar date |

`energy_common.simclock.SimClock` converts between wall-clock and simulated time.
`business_date_for(simulated_ts)` returns the `date` a reading belongs to. The batch generator
fires its drop when the simulated clock crosses midnight; Airflow processes the day that just closed.
All simulated-time behaviour is unit-tested around the midnight boundary.
