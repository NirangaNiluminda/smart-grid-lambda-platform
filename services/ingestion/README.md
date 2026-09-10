# ingestion

**Layer:** ingestion → Lambda master dataset · **Owner:** Dev A · **Status:** Part 1 complete

Consumes Kafka `meter.readings` and writes the **immutable, partitioned Parquet
master dataset** that the batch layer reprocesses. Structurally-broken messages go
to the DLQ.

## What it does

- Decodes each message via `energy_common.messaging.ReadingCodec`.
  - success → row buffered under `business_date=<date>` (from
    `energy_common.simclock.business_date_for`)
  - `ContractViolation` → `meter.readings.dlq` with a self-describing envelope
- Flushes a partition when it reaches `FLUSH_MAX_ROWS` or `FLUSH_MAX_AGE_SECONDS`.
- **Partition lifecycle:** when the simulated clock advances past a day, that
  day's partition is flushed and gets its `_SUCCESS` marker. Readings that arrive
  afterwards for that day are still written (new part file) and counted as
  `late_arrival_total`.
- Manual offset commit — offsets advance only after a poll's rows are buffered
  and any due partitions flushed.

Output schema + `_SUCCESS` / late-data semantics: `docs/DATA_CONTRACTS.md §2`.

## Architecture

```
domain/       partition_buffer.py  — pure: group by date, decide flush
application/  ingest_service.py    — consume → decode → route → lifecycle
              depends on ReadingConsumer / ObjectStore / DeadLetterSink / Clock ports
composition.py  wires Kafka consumer + ParquetObjectStore + Kafka DLQ + SimClock
```

## Config (`.env`)

| var | default | meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | brokers |
| `KAFKA_READINGS_TOPIC` | `meter.readings` | input |
| `KAFKA_READINGS_DLQ_TOPIC` | `meter.readings.dlq` | dead-letter |
| `KAFKA_CONSUMER_GROUP_INGESTION` | `ingestion-raw-lake` | consumer group |
| `LAKE_ROOT` | `/data/lake` | Parquet dataset root |
| `FLUSH_MAX_ROWS` | 500 | rows before a partition flush |
| `FLUSH_MAX_AGE_SECONDS` | 30 | age before a partition flush |
| `METRICS_PORT` | 9102 | Prometheus `/metrics` |

## Metrics

`readings_consumed_total{kafka_partition}`, `readings_rejected_total{reason}`,
`late_arrival_total{business_date}`, `lake_partition_rows{business_date}`.

## Tests

```bash
pytest services/ingestion -q      # uses fakes; no Kafka/pyarrow needed for the unit run
```
