#!/usr/bin/env bash
# Create the Kafka topics with the partitioning agreed in docs/DATA_CONTRACTS.md §1.
# Safe to re-run: --if-not-exists.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.yml}"
KAFKA_SVC="${KAFKA_SVC:-kafka}"
READINGS_TOPIC="${KAFKA_READINGS_TOPIC:-meter.readings}"
DLQ_TOPIC="${KAFKA_READINGS_DLQ_TOPIC:-meter.readings.dlq}"
PARTITIONS="${KAFKA_READINGS_PARTITIONS:-6}"
RETENTION_MS="${KAFKA_READINGS_RETENTION_MS:-604800000}"  # 7 days

KT=/opt/kafka/bin/kafka-topics.sh
# --env-file .env : see Makefile's COMPOSE for why this is needed.
run() { docker compose --env-file .env -f "$COMPOSE_FILE" exec -T "$KAFKA_SVC" "$@"; }

echo "Waiting for Kafka to accept admin commands..."
for _ in $(seq 1 30); do
  if run "$KT" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then break; fi
  sleep 2
done

run "$KT" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$READINGS_TOPIC" --partitions "$PARTITIONS" --replication-factor 1 \
  --config retention.ms="$RETENTION_MS"

run "$KT" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 \
  --config retention.ms="$RETENTION_MS"

echo "Topics:"
run "$KT" --bootstrap-server localhost:9092 --describe --topic "$READINGS_TOPIC"
run "$KT" --bootstrap-server localhost:9092 --describe --topic "$DLQ_TOPIC"
