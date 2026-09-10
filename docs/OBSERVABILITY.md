# Observability

**Rubric weight: 10 / 100.** The brief requires structured logging across all stages and at least
one alert/health-check rule. We implement structured logging everywhere, `/metrics` on every
service, and four alert rules.

- Design & the logging/metrics helpers: **owned by Dev A** (`energy_common.observability`).
- `alert-engine` service + Grafana + this doc's "Results" section: **owned by Dev B**.

---

## 1. Structured logging

Every service logs JSON to stdout via `energy_common.get_logger(service_name)` (structlog).
Docker captures stdout; `docker compose logs` / Loki (optional) aggregates.

**Mandatory fields on every line**

| field | source |
|---|---|
| `ts` | ISO-8601 UTC wall-clock |
| `level` | DEBUG/INFO/WARNING/ERROR |
| `service` | bound once at startup |
| `event` | short machine-readable string, e.g. `reading.published`, `partition.flushed`, `dlq.routed` |

**Contextual fields, bound where they apply**

`business_date`, `event_time` (simulated), `correlation_id`, `grid_zone`, `household_id`,
`records_in`, `records_out`, `records_rejected`, `latency_ms`, `kafka_partition`, `kafka_offset`.

**Stage coverage**

| Stage | Key events |
|---|---|
| meter-simulator | `sim.day_rollover`, `reading.published`, `reading.anomaly_injected`, `publish.error` |
| batch-file-generator | `sim.day_rollover`, `feed.tariff_written`, `feed.weather_written`, `feed.success_marker` |
| ingestion | `reading.consumed`, `partition.opened`, `partition.flushed`, `partition.success_marker`, `dlq.routed`, `late.arrival` |
| stream-processor *(B)* | `microbatch.start`, `microbatch.complete`, `window.upserted`, `record.rejected` |
| batch-processor *(B)* | `batch.started`, `lake.partition_read`, `feed.joined`, `billing.computed`, `tables.written`, `report.rendered`, `dq.assertion` |
| airflow DAG *(B)* | task-level via Airflow logging + a `batch_runs` row |
| serving-api *(B)* | `http.request` (method, path, status, latency_ms) |
| alert-engine *(B)* | `rule.evaluated`, `alert.fired`, `alert.resolved`, `sink.delivered` |

## 2. Metrics (Prometheus)

`energy_common.observability.serve_metrics(port)` starts a `prometheus_client` HTTP server in a
background thread. Prometheus scrapes each service (targets in
[`infra/prometheus/prometheus.yml`](../infra/prometheus/prometheus.yml)).

| metric | type | labels | emitted by |
|---|---|---|---|
| `readings_produced_total` | counter | `grid_zone`, `anomaly` | meter-simulator |
| `readings_consumed_total` | counter | `service`, `kafka_partition` | ingestion, stream-processor |
| `readings_rejected_total` | counter | `service`, `reason` | ingestion, stream-processor |
| `lake_partition_rows` | gauge | `business_date` | ingestion |
| `late_arrival_total` | counter | `business_date` | ingestion |
| `feed_files_written_total` | counter | `kind` (`tariff`/`weather`) | batch-file-generator |
| `microbatch_duration_seconds` | histogram | — | stream-processor |
| `consumer_lag` | gauge | `service`, `kafka_partition` | ingestion, stream-processor |
| `batch_job_rows` | gauge | `table` | batch-processor |
| `batch_job_duration_seconds` | histogram | `business_date` | batch-processor |
| `http_request_duration_seconds` | histogram | `method`, `path`, `status` | serving-api |
| `alerts_fired_total` | counter | `rule_name`, `severity` | alert-engine |

## 3. Alert rules

Implemented in `alert-engine` as Strategy objects (`services/alert-engine/src/alert_engine/rules/`).
Each `Rule.evaluate(ctx: RuleContext) -> list[Alert]`. Thresholds come from env (`.env.example`).

| Rule | Condition | Severity | Rubric line it satisfies |
|---|---|---|---|
| `NoDataRule` | no `rt_zone_metrics` row for a zone in the last `ALERT_NO_DATA_MINUTES` simulated minutes | `critical` | "no data received in N minutes" |
| `LowRenewableRule` | zone `renewable_pct` < `ALERT_LOW_RENEWABLE_PCT` for ≥ 3 consecutive 1-min windows | `warning` | brief: "renewable contribution considerably low" |
| `HighRejectRateRule` | `records_rejected_total / records_consumed_total` > `ALERT_MAX_REJECT_RATE` over 5 min | `warning` | "error rate above threshold" |
| `BatchFailureRule` | most recent `batch_runs` row for yesterday is not `success`, or `rows_written = 0` | `critical` | pipeline health |

**Fan-out (Observer):** `DbAlertSink` (writes `alerts`), `LogAlertSink` (ERROR log line),
`WebhookAlertSink` (optional, `ALERT_WEBHOOK_URL`).
**Dedupe:** an unresolved alert with the same `(rule_name, grid_zone, business_date)` is not re-fired;
`resolved_at` is set when the condition clears.

## 4. Health checks

- `GET /healthz` on serving-api → checks Postgres connectivity + last `rt_zone_metrics.computed_at`
  freshness; returns `200` / `503` with a JSON body.
- Compose `healthcheck:` on kafka, postgres, serving-api.
- `alert-engine` exposes `GET /healthz` (last successful evaluation timestamp).

## 5. Results (fill during Part 2)

> _Screenshots + narrative for the report go here: Prometheus targets up, a Grafana panel of
> per-zone load & renewable %, an `alerts` table row for `LowRenewableRule`, `docker compose logs`
> showing correlated `business_date` across stages, an Airflow DAG green run._
