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
| **Batch Processor** | `9112` | Metrics: `http://localhost:9112/metrics` |
| **Alert Engine** | `9114` | Metrics: `http://localhost:9114/metrics` |
| **Airflow Web UI** | `8080` | DAG control: `http://localhost:8080` (`admin` / `admin`) |
| **Prometheus** | `9090` | Targets & TSDB queries: `http://localhost:9090/targets` |
| **Grafana** | `3000` | Dashboards: `http://localhost:3000` (`admin` / `admin`) |

---

## 3. Operational Workflows

### 3.1 Inspecting Speed-Layer (Live) Metrics
```bash
# Query latest window for all grid zones
curl -s http://localhost:8000/api/realtime/zones | jq .

# Query specific zone
curl -s http://localhost:8000/api/realtime/zones/zone-north | jq .
```

### 3.2 Running the Batch Reconciliation Manually
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

### 3.3 Querying Billing & Solar Reports via API
```bash
# Daily billing reconciliation for a specific date
curl -s http://localhost:8000/api/billing/daily/2026-01-01 | jq .

# Single household bill
curl -s "http://localhost:8000/api/billing/household/HH-00042?date=2026-01-01" | jq .

# Per-zone solar contribution summary
curl -s http://localhost:8000/api/reports/solar/2026-01-01 | jq .
```

### 3.4 Monitoring Active & Resolved Alerts
Connect to Postgres to query the `alerts` table:
```bash
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U energy -d energy_serving -c \
  "SELECT rule_name, severity, grid_zone, business_date, message, fired_at, resolved_at FROM alerts ORDER BY fired_at DESC LIMIT 10;"
```

---

## 4. Verification and Troubleshooting

### 4.1 Running All Automated Tests
```bash
make test
```
Runs the complete test suite including all 25 contract specs for Speed Layer, Batch Layer, Serving Layer, Alert Engine, and Airflow DAG helpers.

### 4.2 Code Quality & Formatting
```bash
make lint
make typecheck
```

### 4.3 Inspecting the Dead Letter Queue
To inspect invalid messages routed to the DLQ:
```bash
docker compose -f infra/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic meter.readings.dlq --from-beginning --max-messages 5
```
