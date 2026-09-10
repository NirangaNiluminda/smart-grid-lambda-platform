# Data Contracts

**This document is the frozen interface between Part 1 (Dev A) and Part 2 (Dev B).**
Every boundary in the system is here: Kafka topics, the Parquet master dataset, the landing-zone
files, the Postgres tables, and the `energy_common` public API. Changes go through a PR that edits
this file; `MeterReading` carries a `schema_version` so consumers can guard.

The canonical definitions in code are the frozen Pydantic models in
[`libs/common/src/energy_common/domain/models.py`](../libs/common/src/energy_common/domain/models.py).
This doc and those models must agree — a round-trip test enforces the JSON shape.

---

## 1. Kafka

### Topic `meter.readings`

| Property | Value |
|---|---|
| Partitions | 6 |
| Key | `grid_zone` (UTF-8 string) — guarantees per-zone ordering |
| Value | JSON, UTF-8, one `MeterReading` per message |
| Retention | 7 simulated days (`retention.ms` set generously; the log is the replay substrate) |
| Producer | `meter-simulator` |
| Consumers | `ingestion` (group `ingestion-raw-lake`), `stream-processor` (group `stream-processor-speed-layer`) |

**Value schema — `MeterReading` v1.0**

```jsonc
{
  "schema_version": "1.0",              // string, semver; consumers branch on major
  "event_id": "d7f3…-uuid4",            // string, unique per emitted reading
  "meter_id": "MTR-00042",              // string
  "household_id": "HH-00042",           // string, FK → dim_household
  "grid_zone": "zone-north",            // string, FK → dim_zone; one of the 6 seeded zones
  "power_consumption_kwh": 0.0417,      // float, kWh consumed during the interval (≥ 0 in normal data)
  "solar_generation_kwh": 0.0102,       // float, kWh generated during the interval (≥ 0 in normal data)
  "reading_interval_seconds": 2,        // int, simulated seconds the reading covers
  "event_timestamp": "2026-01-01T13:37:04Z"  // RFC3339 UTC, SIMULATED time
}
```

**Anomalies deliberately present in the stream** (controlled by `SIM_ANOMALY_RATE`, default 2%):

| Anomaly | Shape | Consumer expectation |
|---|---|---|
| Dropout | reading simply not emitted for a meter for a while | no-data alert should eventually fire |
| Negative value | `power_consumption_kwh < 0` | speed layer must reject → DLQ; batch DQ check flags |
| Spike | value 10–50× the household's normal | keep, but flag; do not let it break aggregates |
| Stuck meter | identical value repeated | acceptable to keep |
| Malformed JSON / missing field | not valid `MeterReading` | `ingestion` routes raw bytes to `meter.readings.dlq` |

### Topic `meter.readings.dlq`

| Property | Value |
|---|---|
| Partitions | 1 |
| Key | original key (or null) |
| Value | JSON envelope: `{ "reason": "...", "error": "...", "raw": "<base64 original bytes>", "received_at": "<RFC3339>", "source_partition": int, "source_offset": int }` |
| Producer | `ingestion` (and `stream-processor` for records that parse but fail validation) |

---

## 2. Parquet master dataset (Lambda batch-layer source of truth)

- **Root:** `${LAKE_ROOT}` (`data/lake` locally, mounted at `/data/lake` in containers)
- **Dataset:** `readings_raw`
- **Path:** `${LAKE_ROOT}/readings_raw/business_date=YYYY-MM-DD/part-*.parquet`
- **Partitioning:** Hive-style, single column `business_date` (`date`), derived by
  `energy_common.simclock.business_date_for(event_timestamp)`.
- **Write semantics:** append-only, **immutable**. `ingestion` never updates or deletes. Files are
  written to a `.../_tmp/` path and atomically renamed in.
- **Completion marker:** `ingestion` writes `_SUCCESS` into a partition directory only after the
  simulated clock has passed that day's midnight **and** the in-flight buffer for that day is
  flushed. Until then the partition is "open".
- **Late data policy:** a reading whose `business_date` partition already has `_SUCCESS` is still
  appended (new `part-*` file) and a `late_arrival_total` metric is incremented. The batch layer
  reprocesses the day if `daily_energy_reconciliation` is re-triggered for that date.

**Row schema** (superset of `MeterReading` + ingestion metadata):

| column | type | notes |
|---|---|---|
| `schema_version` | string | copied from the event |
| `event_id` | string | |
| `meter_id` | string | |
| `household_id` | string | |
| `grid_zone` | string | |
| `power_consumption_kwh` | double | as received (anomalies preserved) |
| `solar_generation_kwh` | double | as received |
| `reading_interval_seconds` | int32 | |
| `event_timestamp` | timestamp[us, UTC] | simulated |
| `ingest_timestamp` | timestamp[us, UTC] | wall-clock, set by `ingestion` |
| `kafka_partition` | int32 | provenance |
| `kafka_offset` | int64 | provenance |
| `business_date` | date32 | **partition column** |

---

## 3. Landing zone (daily batch source)

- **Root:** `${LANDING_ROOT}` (`data/landing` locally, `/data/landing` in containers)
- **Producer:** `batch-file-generator`, once per simulated day, at simulated midnight for the day
  that just **closed**.
- **Consumer:** `daily_energy_reconciliation` Airflow DAG (`FileSensor` on the marker).

| File | Format |
|---|---|
| `tariff_reference_YYYY-MM-DD.csv` | CSV, header row, UTF-8 |
| `weather_forecast_YYYY-MM-DD.csv` | CSV, header row, UTF-8 |
| `_SUCCESS_YYYY-MM-DD` | empty marker file, written **last**, after both CSVs are fully flushed |

**`tariff_reference_YYYY-MM-DD.csv`** — one row per household

| column | type | notes |
|---|---|---|
| `household_id` | string | FK → `dim_household`; every active household present |
| `tariff_rate` | decimal(8,4) | currency units per kWh of grid import |
| `billing_tier` | string | `flat` \| `tiered` \| `subsidised` — selects the billing Strategy |
| `subsidy_flag` | bool | `true`/`false`; if true, subsidised rate applies to the first block |
| `effective_date` | date | equals the file's date |

**`weather_forecast_YYYY-MM-DD.csv`** — one row per grid zone

| column | type | notes |
|---|---|---|
| `grid_zone` | string | FK → `dim_zone`; all 6 zones present |
| `forecast_date` | date | equals the file's date |
| `cloud_cover_pct` | int | 0–100 |
| `temp_c` | decimal(4,1) | |
| `solar_irradiance_index` | decimal(3,2) | 0.00–1.00; batch layer uses it to sanity-check solar totals |

---

## 4. Postgres — serving store (`energy_serving` database)

DSN via `PG_DSN`. DDL lives in [`infra/postgres/init/`](../infra/postgres/init/) and is applied on
first container start. **Part 1 creates every table below**; Part 2 fills the non-dimension ones and
may add columns via new numbered migration files.

### Dimensions — populated by `scripts/seed_database.sh` (Part 1)

`dim_zone(grid_zone PK, name, region, meter_capacity)`
`dim_household(household_id PK, grid_zone FK, dwelling_type, occupants, has_solar, base_load_kwh_day)`
`dim_date(business_date PK, year, quarter, month, day, day_of_week, is_weekend)`

### `rt_zone_metrics` — written by `stream-processor` (speed layer, Part 2)

| column | type | notes |
|---|---|---|
| `grid_zone` | text | PK part |
| `window_type` | text | `tumbling_1m` \| `sliding_5m` — PK part |
| `window_start` | timestamptz | PK part (simulated) |
| `window_end` | timestamptz | |
| `total_consumption_kwh` | double precision | |
| `total_solar_kwh` | double precision | |
| `renewable_pct` | double precision | `100 * solar / consumption`, clamped 0–100 |
| `active_meter_count` | integer | distinct meters seen in the window |
| `computed_at` | timestamptz | wall-clock upsert time |

Upsert key: `(grid_zone, window_type, window_start)`.

### `daily_household_billing` — written by `batch-processor` (batch layer, Part 2)

| column | type |
|---|---|
| `business_date` | date — PK part |
| `household_id` | text — PK part |
| `grid_zone` | text |
| `total_consumption_kwh` | double precision |
| `total_solar_kwh` | double precision |
| `self_consumed_kwh` | double precision |
| `grid_import_kwh` | double precision |
| `solar_export_kwh` | double precision |
| `tariff_rate` | numeric(8,4) |
| `billing_tier` | text |
| `subsidy_flag` | boolean |
| `energy_charge` | numeric(12,2) |
| `solar_credit` | numeric(12,2) |
| `net_amount` | numeric(12,2) |
| `currency` | text (default `LKR`) |
| `computed_at` | timestamptz |

### `daily_zone_solar_summary` — written by `batch-processor` (Part 2)

`(business_date, grid_zone)` PK · `total_consumption_kwh`, `total_solar_generation_kwh`,
`renewable_pct`, `household_count`, `computed_at`.

### `alerts` — written by `alert-engine` (Part 2)

`alert_id uuid PK`, `rule_name text`, `severity text` (`info`|`warning`|`critical`),
`grid_zone text NULL`, `business_date date NULL`, `message text`, `context jsonb`,
`fired_at timestamptz`, `resolved_at timestamptz NULL`.

### `batch_runs` — written by the Airflow DAG (Part 2)

`run_id text PK`, `dag_id text`, `business_date date`, `status text`
(`running`|`success`|`failed`), `rows_written integer`, `started_at timestamptz`,
`finished_at timestamptz NULL`, `notes text NULL`.

---

## 5. `energy_common` public API

Import surface consumers rely on (full docstrings + examples in the modules):

```python
from energy_common import (
    get_settings,  # () -> BaseServiceSettings subclass; reads env, validates, cached
    get_logger,  # (service: str) -> structlog.BoundLogger  (JSON sink)
    SimClock,  # simulated<->wall clock; .now_sim(), .sim_to_wall(), .wall_to_sim()
    business_date_for,  # (event_ts: datetime) -> datetime.date
)
from energy_common.domain.models import (
    MeterReading,
    TariffRecord,
    WeatherForecast,
    ZoneMetric,
    HouseholdBill,
    ZoneSolarSummary,
    Alert,
    BatchRun,
)
from energy_common.domain.ports import (
    ReadingPublisher,
    ReadingConsumer,
    ObjectStore,
    Repository,
    Clock,
    AlertSink,
)
from energy_common.messaging import build_producer, build_consumer, ReadingCodec
from energy_common.storage import pg_engine, ParquetObjectStore
from energy_common.observability import serve_metrics, counter, histogram
```

**Stability:** anything exported from `energy_common/__init__.py` is stable for the project's
lifetime. Anything under a submodule not re-exported there may change — raise a PR if you need it
promoted.

---

## 6. Config contract

Every service reads config from the environment via a `Settings` class. `.env.example` is the
authoritative list. A service **fails fast at startup** if a required variable is missing or
invalid. Adding a variable = update `.env.example` **and** the service `README`.
