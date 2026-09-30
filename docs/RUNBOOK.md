# Operational Runbook

This runbook guides operators, developers, and evaluators through running, inspecting, and troubleshooting the Smart Grid Lambda Platform.

---

## 1. Quick Start

### 1.1 Prerequisites
- Docker & Docker Compose v2+
- Python 3.11+
- `uv` (recommended) or `python3 -m venv`

### 1.2 Environment Setup
```bash
cp .env.example .env
make install
make install-part2
```

### 1.3 Launch Full Stack
```bash
# Pre-fetch pyspark once, outside the build (it's a ~320MB download; doing it
# here avoids bandwidth contention with the 6 other images pip-installing at
# the same time under `--build`, which can time out on a slower connection).
bash scripts/prefetch_pyspark.sh

# Start all containers: Kafka, Postgres, Prometheus, Grafana, Airflow, and the 5 services
make up-full

# Provision topics & seed dimensions
make topics
make seed

# Run end-to-end demo
DEMO_FULL=1 make demo
```

---

## 2. Service Endpoints

| Service | Port | Endpoint / Purpose |
|---|---|---|
| **Serving API** | `8000` | OpenAPI docs: `http://localhost:8000/docs`<br>Healthcheck: `http://localhost:8000/healthz`<br>Metrics: `http://localhost:8000/metrics` |
| **Stream Processor** | `9111` | Metrics: `http://localhost:9111/metrics` |
| **Alert Engine** | `9114` | Metrics: `http://localhost:9114/metrics` |
| **Airflow Web UI** | `8080` | DAG control: `http://localhost:8080` (`admin` / `admin`) |
| **Prometheus** | `9090` | Targets & TSDB queries: `http://localhost:9090/targets` |
| **Grafana** | `3000` | Dashboards: `http://localhost:3000` (`admin` / `admin`) |

> `batch-processor` has no standing endpoint on purpose: it's a one-shot job
> invoked in-process by Airflow's `run_batch` task, not a long-running
> container (see §3.2/§3.3), so it's not on Prometheus's scrape list either.

---

## 3. Operational Workflows

### 3.1 Inspecting Speed-Layer (Live) Metrics
```bash
# Query latest window for all grid zones
curl -s http://localhost:8000/api/realtime/zones | jq .

# Query specific zone
curl -s http://localhost:8000/api/realtime/zones/zone-north | jq .
```

### 3.2 How the Airflow DAG actually gets triggered

`daily_energy_reconciliation` is **not** on a wall-clock schedule (`schedule=None`)
— a schedule like `@daily` would never fire inside a demo session where a
simulated business day closes every `SIM_DAY_SECONDS` (minutes, not a real day).
By design, the moment `batch-file-generator` finishes writing a day's feed
(right after the `_SUCCESS` marker), it calls Airflow's REST API to trigger a
run for that exact `business_date` (`AIRFLOW_TRIGGER_ENABLED=true`, set
automatically by `infra/docker-compose.part2.yml`).

> **Known issue, as of 2026-09-29:** that REST API call currently gets a 403
> from Airflow regardless of correct credentials and confirmed-correct RBAC
> permissions — see `TASKS.md`'s live-verification note for what was ruled out.
> **Use the CLI trigger below; it is fully verified working** and is not
> affected by this (it talks to the metadata DB directly, not the REST API).
> `batch-file-generator`'s trigger attempt fails gracefully (a logged warning,
> nothing crashes) so this doesn't block anything else in the pipeline.

Watch the (currently failing) automated attempt, or just go straight to the CLI trigger:

```bash
docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.part2.yml logs -f batch-file-generator airflow
```

**Trigger it yourself** (this is the reliable path right now):

```bash
docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.part2.yml exec airflow \
  airflow dags trigger daily_energy_reconciliation -e 2026-01-01
# check it landed:
docker compose --env-file .env -f infra/docker-compose.yml -f infra/docker-compose.part2.yml exec airflow \
  airflow tasks states-for-dag-run daily_energy_reconciliation 2026-01-01
```

### 3.3 Running the Batch Reconciliation Manually
If you need to reconcile or re-bill a specific business date directly without Airflow:
```bash
# Authoritative run (requires closed partition _SUCCESS marker):
python -m batch_processor --business-date 2026-01-01

# Development run (allows unclosed in-flight partition):
python -m batch_processor --business-date 2026-01-01 --allow-open
```
Generated reports are deposited in `reports/out/`:
- `reports/out/billing_report_YYYY-MM-DD.csv`
- `reports/out/billing_report_YYYY-MM-DD.html`

### 3.4 Querying Billing & Solar Reports via API
```bash
# Daily billing reconciliation for a specific date
curl -s http://localhost:8000/api/billing/daily/2026-01-01 | jq .

# Single household bill
curl -s "http://localhost:8000/api/billing/household/HH-00042?date=2026-01-01" | jq .

# Per-zone solar contribution summary
curl -s http://localhost:8000/api/reports/solar/2026-01-01 | jq .
```

### 3.5 Monitoring Active & Resolved Alerts
Connect to Postgres to query the `alerts` table:
```bash
docker compose --env-file .env -f infra/docker-compose.yml exec postgres \
  psql -U energy -d energy_serving -c \
  "SELECT rule_name, severity, grid_zone, business_date, message, fired_at, resolved_at FROM alerts ORDER BY fired_at DESC LIMIT 10;"
```

---

## 4. Verification and Troubleshooting

### 4.1 Running All Automated Tests
```bash
make test               # unit tests: no infrastructure needed (Spark runs in local[*] mode)
make up && make test-integration   # cross-service tests against real Postgres (tests/integration/)
```
`make test` runs the full unit suite: Speed Layer and Batch Layer transforms
(against a local Spark session — see `docs/DEV_SETUP.md` if this fails with a
`PermissionError` from `spark-submit`), Serving Layer, Alert Engine, Airflow DAG
helpers. `make test-integration` proves the services agree with each other
against a real database — the upsert accumulation behaviour, the alert
context repository's SQL, the batch→serving read path, and the `serving_ro`
role's grants.

### 4.2 Code Quality & Formatting
```bash
make lint
make typecheck
```

### 4.3 Inspecting the Dead Letter Queue
To inspect invalid messages routed to the DLQ:
```bash
docker compose --env-file .env -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic meter.readings.dlq --from-beginning --max-messages 5
```
