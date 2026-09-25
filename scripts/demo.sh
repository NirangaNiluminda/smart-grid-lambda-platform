#!/usr/bin/env bash
# End-to-end demo driver.
#
# Part 1 (available now): brings up Kafka + Postgres + Prometheus + the sources +
# ingestion, creates topics, seeds dimensions, then watches one simulated day of
# data flow into Kafka and the Parquet lake.
#
# Part 2 (Dev B) extends this to also wait for the Airflow DAG, assert the
# billing tables, and open the dashboard.
set -euo pipefail

cd "$(dirname "$0")/.."

FULL="${DEMO_FULL:-0}"
if [[ "$FULL" == "1" ]]; then
  COMPOSE=(docker compose -f infra/docker-compose.yml -f infra/docker-compose.part2.yml)
else
  COMPOSE=(docker compose -f infra/docker-compose.yml)
fi

echo "==> Starting stack"
"${COMPOSE[@]}" up -d --build

echo "==> Creating topics"
COMPOSE_FILE=infra/docker-compose.yml ./scripts/create_topics.sh

echo "==> Seeding dimensions"
COMPOSE_FILE=infra/docker-compose.yml ./scripts/seed_database.sh

echo "==> Streaming... (Ctrl-C to stop watching; the stack keeps running)"
echo "    Kafka:       docker compose -f infra/docker-compose.yml exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic meter.readings"
echo "    Lake:        watch -n2 'find data/lake -name \"*.parquet\" | wc -l'"
echo "    Landing:     watch -n2 'ls -1 data/landing'"
echo "    Prometheus:  http://localhost:9090"
if [[ "$FULL" == "1" ]]; then
  echo "    Serving API: http://localhost:8000/docs"
  echo "    Realtime:    curl http://localhost:8000/api/realtime/zones"
  echo "    Airflow:     http://localhost:8080 (admin/admin)"
  echo "    Alerts:      docker compose -f infra/docker-compose.yml exec postgres psql -U energy -d energy_serving -c 'select * from alerts;'"
  "${COMPOSE[@]}" logs -f meter-simulator ingestion stream-processor serving-api alert-engine
else
  "${COMPOSE[@]}" logs -f meter-simulator batch-file-generator ingestion
fi
