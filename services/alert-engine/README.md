# alert-engine  ·  OBSERVABILITY

**Layer:** observability · **Owner:** Dev B · **Status:** 🔲 Part 2 — skeleton only

Scheduled rule evaluator. Reads Postgres + Prometheus, fires `Alert`s into the
`alerts` table, and fans them out to sinks.

> **Build plan:** [`docs/PART2_ONBOARDING.md`](../../docs/PART2_ONBOARDING.md) §B5 ·
> Rules & thresholds: [`docs/OBSERVABILITY.md`](../../docs/OBSERVABILITY.md) §3.

## Rules (Strategy — `rules/`)

| Rule | Condition (threshold env) | Severity |
|---|---|---|
| `NoDataRule` | no `rt_zone_metrics` for a zone in `ALERT_NO_DATA_MINUTES` | critical |
| `LowRenewableRule` | zone `renewable_pct` < `ALERT_LOW_RENEWABLE_PCT` for ≥3 windows | warning |
| `HighRejectRateRule` | `readings_rejected_total / readings_consumed_total` > `ALERT_MAX_REJECT_RATE` | warning |
| `BatchFailureRule` | latest `batch_runs` for yesterday not `success` | critical |

## Sinks (Observer — `sinks/`)

`DbAlertSink` (always) · `LogAlertSink` (always) · `WebhookAlertSink` (optional, `ALERT_WEBHOOK_URL`).

Dedupe: an unresolved alert with the same `(rule_name, grid_zone, business_date)` is not re-fired
(the `uq_alert_open` partial index enforces it). Set `resolved_at` when the condition clears.

## What to build

`domain/rule.py` (`Rule` protocol, `RuleContext`) · `rules/*.py` · `sinks/*.py` ·
`application/evaluator.py` (loop: build context → run rules → dedupe → fan out) ·
`adapters/` (`AlertRepository`, Prometheus client) · `__main__.py`.

## Definition of Done

Each rule has unit tests with a fake `RuleContext`. In `make demo`, `LowRenewableRule` and
`NoDataRule` demonstrably fire; rows appear in `alerts`; `alerts_fired_total{rule_name,severity}`
exported.
