# orchestration/airflow

**Owner:** Dev B · **Status:** 🔲 Part 2 skeleton

The batch layer's control plane. One DAG:

- [`dags/daily_energy_reconciliation.py`](dags/daily_energy_reconciliation.py) — sense the daily
  feed → validate → run `batch-processor` for the closed day → data-quality gate → publish the
  report → record an auditable `batch_runs` row. Failure → `critical` alert.

Build plan and task-by-task acceptance criteria: [`../../docs/PART2_ONBOARDING.md`](../../docs/PART2_ONBOARDING.md) §B3.

Airflow runs as its own container(s) in `infra/docker-compose.part2.yml`
(LocalExecutor, Postgres `airflow` metadata DB on the shared instance). This
folder is bind-mounted at `/opt/airflow/dags`.
