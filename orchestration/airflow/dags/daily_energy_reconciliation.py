"""DAG: daily_energy_reconciliation  —  Part 2 skeleton (Dev B, §B3).

Drives the Lambda **batch layer** once per closed simulated business day.

Task graph (implement the bodies):

    wait_for_feed          FileSensor on  {LANDING_ROOT}/_SUCCESS_{{ ds }}
        │
    validate_feed          tariff + weather schema & row-count checks
        │
    wait_for_lake          partition {LAKE_ROOT}/readings_raw/business_date={{ ds }}/_SUCCESS
        │
    run_batch              batch-processor --business-date {{ ds }}
        │
    dq_gate                every active household billed once; solar reconciles
        │
    publish_report         copy reports/out/billing_report_{{ ds }}.html to the served location
        │
    record_run             upsert batch_runs (status=success, rows_written=...)

on_failure_callback -> insert a 'critical' row into `alerts`.

Backfill:  airflow dags backfill -s 2026-01-01 -e 2026-01-05 daily_energy_reconciliation
"""

from __future__ import annotations

# NOTE: kept import-safe without Airflow installed so `pytest` collection and
# `ruff` work in the Part 1 environment. Dev B replaces this guard with the real
# DAG definition.
try:
    from airflow.decorators import dag, task  # type: ignore[import-not-found]

    _AIRFLOW = True
except ModuleNotFoundError:  # pragma: no cover
    _AIRFLOW = False

# Dev B: import FileSensor (airflow.sensors.filesystem) for `wait_for_feed`.


DAG_ID = "daily_energy_reconciliation"
TASK_ORDER = [
    "wait_for_feed",
    "validate_feed",
    "wait_for_lake",
    "run_batch",
    "dq_gate",
    "publish_report",
    "record_run",
]


if _AIRFLOW:  # pragma: no cover - exercised only in the Part 2 Airflow container

    @dag(
        dag_id=DAG_ID,
        schedule=None,  # Dev B: trigger per simulated-day close, or a wall-clock interval
        catchup=False,
        tags=["batch-layer", "lambda", "part2"],
    )
    def daily_energy_reconciliation() -> None:
        @task
        def not_implemented() -> None:
            raise NotImplementedError(
                "daily_energy_reconciliation: build the task graph — docs/PART2_ONBOARDING.md §B3"
            )

        not_implemented()

    daily_energy_reconciliation()
