# Report Outline (8–15 pages)

Rubric weight: 15 / 100 for the report itself; the report is also where the 20 + 10 marks for
architecture and stack justification are actually awarded. Map each section to the rubric.

| # | Section | Pages | Rubric | Source material | Owner |
|---|---|---|---|---|---|
| 1 | **Introduction & use case** — Use Case 3 restated; the business question; assumptions, simplifications, **simulated-time compression** (1 day = `SIM_DAY_SECONDS`) | 1 | — | `PLAN.md §2`, `README` | A |
| 2 | **Architecture decision: Lambda vs Kappa** — forces table; why Lambda; Kappa described fairly and the explicit reasons it was rejected *for this use case*; when we'd revisit | 2–3 | **20** | `docs/decisions/ADR-0001` | A |
| 3 | **Designed architecture** — context + layer + sequence diagrams; speed vs batch vs serving responsibilities; the immutable master dataset; simulated clock | 2–3 | 15 (report) | `docs/ARCHITECTURE.md` | A + B |
| 4 | **Technology stack** — per component: what, why *for this use case*, what was rejected | 2 | **10** | `docs/decisions/ADR-0002` | A |
| 5 | **Data ingestion** — the two simulated sources; Kafka topic/partition design; anomaly injection; raw→Parquet with `_SUCCESS`/late-data handling; DLQ | 1–2 | 15 | Part 1 code, `DATA_CONTRACTS §1–3` | A |
| 6 | **Processing layer** — speed: event-time windowing, watermarking, per-zone aggregates; batch: full-day join, billing strategies, solar credit, idempotent re-runs; how drift is prevented (shared pure transforms) | 2 | 15 | Part 2 code (B1–B2) | B |
| 7 | **Storage & serving** — Postgres schema; read-model / CQRS; the live-vs-final merge rule; API surface + sample responses | 1 | 10 | `DATA_CONTRACTS §4–5`, serving-api | B |
| 8 | **Observability design** — what is measured, how, why; logging fields; metrics catalogue; the four alert rules; health checks; screenshots | 1–2 | **10** | `docs/OBSERVABILITY.md` | B |
| 9 | **Results** — one simulated day end-to-end: dashboard screenshot, sample daily billing report, an alert firing, correlated logs, Airflow green run | 1–2 | 15 | `make demo` output | B |
| 10 | **Limitations, trade-offs, production scale** — Lambda dual-code cost & our mitigation; Parquet FS → S3/Iceberg; LocalExecutor → Celery/K8s; exactly-once nuances; schema registry; security | 1 | 15 + 20 | this file's checklist below | A + B |
| — | **Appendix** — repo link, how to reproduce, individual-contribution statement (Dev A = Part 1, Dev B = Part 2) | — | — (note: mandatory) | `TASKS.md` | both |

## Diagrams to include (render the mermaid from `docs/ARCHITECTURE.md`)

1. Context diagram (sources → platform → consumers).
2. Lambda layers (speed / batch / serving), showing the master dataset.
3. Sequence diagram of one simulated day.
4. Hexagonal service internal structure (one, as representative).
5. Deployment (compose services, two compose files).

## "Production scale" checklist to discuss in §10

- Parquet on FS → S3 + a table format (Iceberg/Delta) for ACID, schema evolution, time travel.
- Airflow LocalExecutor → Celery/Kubernetes executor; sensors → deferrable operators.
- Kafka: replication factor > 1, `min.insync.replicas`, a schema registry + Avro/Protobuf instead of JSON.
- Speed layer: real distinct-count (HLL) instead of approx; multiple stream jobs per zone group.
- Serving: read replicas / materialised views; cache layer; API auth.
- Observability: Alertmanager routing + on-call; distributed tracing (OpenTelemetry) across stages.
- Exactly-once: transactional sink or idempotent upserts everywhere (we use the latter).
- Data quality: Great Expectations / dbt tests in the DAG instead of hand-rolled assertions.
- Security: secrets manager, network policies, PII handling for household data, encryption at rest.
