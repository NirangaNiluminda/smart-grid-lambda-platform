# batch-file-generator

**Layer:** ingestion (daily batch source) · **Owner:** Dev A · **Status:** Part 1 complete

Drops the once-a-day reference feed the Lambda **batch layer** joins against.

## What it does

- Watches `energy_common.SimClock`. When a simulated day closes it writes, into
  `${LANDING_ROOT}`:
  - `tariff_reference_<date>.csv` — one row per household (`household_id, tariff_rate,
    billing_tier, subsidy_flag, effective_date`)
  - `weather_forecast_<date>.csv` — one row per grid zone (`grid_zone, forecast_date,
    cloud_cover_pct, temp_c, solar_irradiance_index`)
  - `_SUCCESS_<date>` — empty marker, written **last** (the Airflow `FileSensor` waits on this)
- Content is deterministic given `(business_date, SIM_SEED)` so re-billing a past
  day reproduces identical inputs.
- Household population comes from the same `data/seed/households.csv` the simulator
  uses (`energy_common.seeds`) — the two cannot drift.

Schemas: `docs/DATA_CONTRACTS.md §3`.

## Architecture

```
domain/       reference_data.py  — pure row builders, deterministic
application/  generator_loop.py  — depends on Clock + LandingZone ports
adapters/     landing_zone.py    — atomic CSV write + _SUCCESS marker last
```

## Run

```bash
docker compose -f infra/docker-compose.yml up -d batch-file-generator
# or locally:
python -m batch_file_generator
```

## Config (`.env`)

| var | default | meaning |
|---|---|---|
| `LANDING_ROOT` | `/data/landing` | where feed files are dropped |
| `SIM_DAY_SECONDS` | 600 | wall-clock seconds per simulated day |
| `SIM_SEED` | 42 | determinism seed |
| `METRICS_PORT` | 9103 | Prometheus `/metrics` |

## Metrics

`feed_files_written_total{kind="tariff"|"weather"}`.

## Tests

```bash
pytest services/batch-file-generator -q
```
