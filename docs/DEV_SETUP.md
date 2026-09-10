# Developer Setup

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Engine | 24+ | with the Compose v2 plugin (`docker compose`, not `docker-compose`) |
| Python | 3.11+ | for the local `.venv` used by tests + tooling (containers carry their own) |
| GNU Make | any | thin wrapper over the commands below |
| RAM | ~8 GB free | Part 1 is light; Part 2 adds Spark + Airflow |

## First run (Part 1)

```bash
git clone <repo-url> smart-grid-data-platform
cd smart-grid-data-platform

cp .env.example .env            # tweak SIM_DAY_SECONDS etc. if you like

make install                    # creates .venv, installs energy_common + Part 1 services editable
make lint typecheck test        # all green before you touch anything

make up                         # Kafka + Postgres + Prometheus + meter-simulator + batch-file-generator + ingestion
make topics                     # create meter.readings (6 partitions) + DLQ
make seed                       # load dim_zone / dim_household / dim_date into Postgres
```

Watch it work:

```bash
docker compose -f infra/docker-compose.yml logs -f meter-simulator ingestion

# readings landing on the topic
docker compose -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic meter.readings --max-messages 5

# Parquet partitions appearing
ls -R data/lake/readings_raw/

# at a simulated midnight, the daily feed appears
ls -l data/landing/
```

Tear down:

```bash
make down        # keep volumes
make clean       # wipe volumes + generated data/
```

## Local Python environment

`make install` builds `.venv/` with:

- `energy_common` (editable) — the shared kernel
- `meter-simulator`, `batch-file-generator`, `ingestion` (editable) — for their unit tests
- dev tools: `ruff`, `black`, `mypy`, `pytest`, `pytest-cov`

Run a single service outside Docker (useful when iterating):

```bash
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)      # or use direnv
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092   # host-mapped listener
python -m meter_simulator
```

## Ports

| Service | Port | URL |
|---|---|---|
| Kafka (host listener) | 19092 | `localhost:19092` |
| Postgres | 5432 | `postgresql://energy:energy@localhost:5432/energy_serving` |
| Prometheus | 9090 | http://localhost:9090 |
| meter-simulator `/metrics` | 9101 | http://localhost:9101/metrics |
| ingestion `/metrics` | 9102 | http://localhost:9102/metrics |
| batch-file-generator `/metrics` | 9103 | http://localhost:9103/metrics |
| serving-api *(Part 2)* | 8000 | http://localhost:8000/docs |
| Airflow *(Part 2)* | 8080 | http://localhost:8080 |
| Grafana *(Part 2)* | 3000 | http://localhost:3000 |

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make up` hangs on Kafka health | first pull is slow; `docker compose ... logs kafka`; ensure port 19092 free |
| `ingestion` logs `UNKNOWN_TOPIC_OR_PART` | run `make topics` (compose starts services before topics exist; ingestion retries) |
| No files in `data/landing/` yet | wait one `SIM_DAY_SECONDS` window; check `batch-file-generator` logs for the simulated clock |
| `mypy` fails on a fresh clone | `make install` first (needs the editable installs for imports to resolve) |
| Tests fail with `filterwarnings=error` | a dependency emitted a warning — fix the call or add a scoped `filterwarnings` marker, don't broaden globally |
| Parquet partition never gets `_SUCCESS` | the simulated day hasn't closed yet, or `SIM_DAY_SECONDS` is very large |
