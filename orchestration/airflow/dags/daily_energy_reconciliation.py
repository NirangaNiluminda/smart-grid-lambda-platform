"""DAG: daily_energy_reconciliation  —  Part 2 (Dev B, §B3).

Drives the Lambda batch layer once per closed simulated business day.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import shutil
import uuid
from pathlib import Path

try:
    from airflow.decorators import dag, task  # type: ignore[import-not-found]

    _AIRFLOW = True
except ModuleNotFoundError:  # pragma: no cover
    _AIRFLOW = False


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


def validate_feed_files(landing_root: Path, business_date: dt.date) -> dict[str, int]:
    date_str = business_date.isoformat()
    tariff_file = landing_root / f"tariff_reference_{date_str}.csv"
    weather_file = landing_root / f"weather_forecast_{date_str}.csv"

    if not tariff_file.exists() or not weather_file.exists():
        raise FileNotFoundError(f"Missing reference files for date {date_str}")

    with open(tariff_file, encoding="utf-8") as f:
        tariff_rows = sum(1 for _ in csv.reader(f)) - 1

    with open(weather_file, encoding="utf-8") as f:
        weather_rows = sum(1 for _ in csv.reader(f)) - 1

    if tariff_rows <= 0 or weather_rows <= 0:
        raise ValueError(
            f"Feed validation failed: tariff_rows={tariff_rows}, weather_rows={weather_rows}"
        )

    return {"tariff_rows": tariff_rows, "weather_rows": weather_rows}


def verify_lake_partition(lake_root: Path, business_date: dt.date) -> bool:
    date_str = business_date.isoformat()
    marker = lake_root / "readings_raw" / f"business_date={date_str}" / "_SUCCESS"
    if not marker.exists():
        raise FileNotFoundError(f"Lake partition _SUCCESS marker missing: {marker}")
    return True


def execute_batch_job(
    business_date: dt.date,
    lake_root: Path,
    landing_root: Path,
    pg_dsn: str,
    reports_dir: Path,
) -> tuple[int, int]:
    from batch_processor.adapters.reports import ReportRenderer
    from batch_processor.adapters.repository import PostgresBatchRepository
    from batch_processor.adapters.spark_session import build_spark_session
    from batch_processor.application.reconcile import BatchReconciliationService
    from batch_processor.config import Settings
    from energy_common.storage import pg_engine

    settings = Settings(
        pg_dsn=pg_dsn,
        lake_root=lake_root,
        landing_root=landing_root,
        report_out_dir=reports_dir,
    )
    engine = pg_engine(pg_dsn)
    repo = PostgresBatchRepository(engine)
    renderer = ReportRenderer(reports_dir)
    spark = build_spark_session(settings)
    try:
        service = BatchReconciliationService(settings, repo, renderer, spark)
        return service.run_reconciliation(business_date, allow_open=False)
    finally:
        spark.stop()


def check_dq_gate(pg_dsn: str, business_date: dt.date) -> None:
    from energy_common.storage import pg_engine
    from sqlalchemy import text

    engine = pg_engine(pg_dsn)
    with engine.connect() as conn:
        b_count = conn.execute(
            text("SELECT count(*) FROM daily_household_billing WHERE business_date = :bdate"),
            {"bdate": business_date},
        ).scalar()
        s_count = conn.execute(
            text("SELECT count(*) FROM daily_zone_solar_summary WHERE business_date = :bdate"),
            {"bdate": business_date},
        ).scalar()

    if not b_count or b_count <= 0:
        raise RuntimeError(f"DQ gate failed: no household bills found for {business_date}")
    if not s_count or s_count <= 0:
        raise RuntimeError(f"DQ gate failed: no zone solar summaries found for {business_date}")


def publish_report_artifact(
    business_date: dt.date,
    reports_out_dir: Path,
    published_dir: Path,
) -> Path:
    published_dir.mkdir(parents=True, exist_ok=True)
    src_html = reports_out_dir / f"billing_report_{business_date}.html"
    dest_html = published_dir / f"billing_report_{business_date}.html"
    if src_html.exists():
        shutil.copyfile(src_html, dest_html)
    return dest_html


def record_batch_run(
    pg_dsn: str,
    run_id: str,
    dag_id: str,
    business_date: dt.date,
    status: str,
    rows_written: int,
    notes: str | None = None,
) -> None:
    from energy_common.storage import pg_engine
    from sqlalchemy import text

    engine = pg_engine(pg_dsn)
    stmt = text(
        """
        INSERT INTO batch_runs (
            run_id, dag_id, business_date, status, rows_written, started_at, finished_at, notes
        ) VALUES (
            :run_id, :dag_id, :bdate, :status, :rows, now(), now(), :notes
        )
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            rows_written = EXCLUDED.rows_written,
            finished_at = EXCLUDED.finished_at,
            notes = EXCLUDED.notes;
        """
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "bdate": business_date,
                "status": status,
                "rows": rows_written,
                "notes": notes,
            },
        )


def on_failure_alert(context: dict) -> None:
    from energy_common.storage import pg_engine
    from sqlalchemy import text

    pg_dsn = os.getenv("PG_DSN", "postgresql://energy:energy@postgres:5432/energy_serving")
    engine = pg_engine(pg_dsn)

    dag_run = context.get("dag_run")
    bdate = getattr(dag_run, "execution_date", dt.datetime.now(dt.UTC)).date()
    exception = context.get("exception", "Unknown error")

    stmt = text(
        """
        INSERT INTO alerts (
            alert_id, rule_name, severity, grid_zone, business_date, message, context, fired_at
        ) VALUES (
            :aid, 'BatchFailureRule', 'critical', NULL, :bdate, :msg, :ctx::jsonb, now()
        )
        ON CONFLICT DO NOTHING;
        """
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "aid": str(uuid.uuid4()),
                "bdate": bdate,
                "msg": f"Airflow DAG daily_energy_reconciliation failed: {exception}",
                "ctx": '{"source": "airflow"}',
            },
        )


if _AIRFLOW:  # pragma: no cover
    landing_root = Path(os.getenv("LANDING_ROOT", "/data/landing"))
    lake_root = Path(os.getenv("LAKE_ROOT", "/data/lake"))
    pg_dsn = os.getenv("PG_DSN", "postgresql://energy:energy@postgres:5432/energy_serving")
    reports_dir = Path("/opt/airflow/reports")
    published_dir = Path("/opt/airflow/reports/published")

    @dag(
        dag_id=DAG_ID,
        # Externally triggered, not polled: batch-file-generator calls Airflow's
        # REST API the moment a simulated business day closes and writes its
        # _SUCCESS marker (see AirflowDagTrigger in batch-file-generator). A
        # wall-clock schedule like "@daily" would never line up with a demo
        # session compressed into minutes; `logical_date` on the trigger call is
        # set to the exact business_date that just closed. A manual
        # `airflow dags trigger daily_energy_reconciliation -e <date>` remains a
        # fallback (docs/RUNBOOK.md).
        schedule=None,
        start_date=dt.datetime(2026, 1, 1),
        catchup=False,
        on_failure_callback=on_failure_alert,
        default_args={
            "retries": 3,
            "retry_delay": dt.timedelta(seconds=20),
        },
        tags=["batch-layer", "lambda", "part2"],
    )
    def daily_energy_reconciliation_pipeline() -> None:
        # Every task parameter below is named `business_date_str`, deliberately
        # NOT `ds` -- `ds` is a reserved Airflow context-variable name, and the
        # @task decorator rewrites *any* parameter named `ds` into an
        # implicitly-defaulted context kwarg at decoration time, regardless of
        # whether the call site also passes it explicitly. That silently broke
        # record_run's signature (a non-default parameter after an
        # auto-defaulted `ds`), which failed the whole DAG's import -- it never
        # actually parsed successfully. The other five tasks *happened* not to
        # trip the same Python-level error (each had only the one parameter),
        # but were relying on the same fragile, unintended magic.
        @task
        def validate_feed(business_date_str: str) -> dict[str, int]:
            # Retried (default_args) so a batch-file-generator / ingestion
            # timing gap of a few seconds self-heals instead of failing the run.
            bdate = dt.date.fromisoformat(business_date_str)
            return validate_feed_files(landing_root, bdate)

        @task
        def wait_for_lake(business_date_str: str) -> bool:
            bdate = dt.date.fromisoformat(business_date_str)
            return verify_lake_partition(lake_root, bdate)

        @task
        def run_batch(business_date_str: str) -> int:
            bdate = dt.date.fromisoformat(business_date_str)
            bills, _ = execute_batch_job(bdate, lake_root, landing_root, pg_dsn, reports_dir)
            return bills

        @task
        def dq_gate(business_date_str: str) -> None:
            bdate = dt.date.fromisoformat(business_date_str)
            check_dq_gate(pg_dsn, bdate)

        @task
        def publish_report(business_date_str: str) -> str:
            bdate = dt.date.fromisoformat(business_date_str)
            path = publish_report_artifact(bdate, reports_dir, published_dir)
            return str(path)

        @task
        def record_run(business_date_str: str, rows_written: int) -> None:
            bdate = dt.date.fromisoformat(business_date_str)
            record_batch_run(
                pg_dsn=pg_dsn,
                run_id=f"run-{business_date_str}",
                dag_id=DAG_ID,
                business_date=bdate,
                status="success",
                rows_written=rows_written,
                notes="Completed successfully via Airflow DAG",
            )

        v_feed = validate_feed("{{ ds }}")
        w_lake = wait_for_lake("{{ ds }}")
        b_job = run_batch("{{ ds }}")
        dq = dq_gate("{{ ds }}")
        pub = publish_report("{{ ds }}")
        rec = record_run("{{ ds }}", b_job)

        v_feed >> w_lake >> b_job >> dq >> pub >> rec

    daily_energy_reconciliation_pipeline()
