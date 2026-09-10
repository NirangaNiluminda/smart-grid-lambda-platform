# meter-simulator

**Layer:** ingestion (streaming source) · **Owner:** Dev A · **Status:** Part 1 complete

Simulates a fleet of domestic smart meters and publishes one `MeterReading` per
household per tick to Kafka `meter.readings`.

## What it does

- Loads a household population from `data/seed/households.csv` (falls back to a
  deterministic synthetic population if absent).
- Every `SIM_EMIT_INTERVAL_SECONDS` it models each household's consumption (diurnal
  curve) and solar generation (daytime bell × seasonal irradiance), adds noise,
  and publishes.
- Injects anomalies at `SIM_ANOMALY_RATE`: dropout, negative value, spike, stuck
  meter, zero reading — see `docs/DATA_CONTRACTS.md §1`.
- Simulated time is driven by `energy_common.SimClock` (`SIM_DAY_SECONDS`).

## Architecture

```
domain/       generation.py (pure model) · anomalies.py (Strategy) · profiles.py
application/  simulation_loop.py  — depends on Clock + ReadingPublisher ports only
composition.py  wires SimClock + Kafka producer + KafkaReadingPublisher
__main__.py   logging, /metrics, signal handling
```

Business logic imports no infrastructure; unit tests run it with a fake publisher
and a fake clock (`tests/`).

## Run

```bash
# via compose (normal)
docker compose -f infra/docker-compose.yml up -d meter-simulator

# locally
python -m meter_simulator          # reads .env
```

## Config (`.env`)

| var | default | meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | broker list |
| `KAFKA_READINGS_TOPIC` | `meter.readings` | output topic |
| `SIM_HOUSEHOLD_COUNT` | 200 | synthetic population size (if no seed file) |
| `SIM_EMIT_INTERVAL_SECONDS` | 2 | seconds between ticks |
| `SIM_ANOMALY_RATE` | 0.02 | P(anomaly) per reading |
| `SIM_SEED` | 42 | RNG seed (reproducible runs) |
| `SIM_DAY_SECONDS` | 600 | wall-clock seconds per simulated day |
| `METRICS_PORT` | 9101 | Prometheus `/metrics` |

## Metrics

`readings_produced_total{grid_zone,anomaly}`, `simulator_publish_errors_total`.

## Tests

```bash
pytest services/meter-simulator -q
```
