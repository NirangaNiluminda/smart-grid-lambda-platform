"""Cross-service end-to-end test: the whole platform's write/read contract in
one place, against a real Postgres.

Requires `make up` (or at minimum a Postgres reachable at `PG_DSN`) — see
`docs/DEV_SETUP.md`. Not part of the default `make test` run (marked
`integration`); run explicitly with `make test-integration`.

What this proves that unit tests (which use fakes) cannot:
  1. stream-processor's upsert really persists the *full* window total, not a
     partial one (the bug this project's Part 2 rework fixed) — checked
     against a real `ON CONFLICT` upsert, not a fake.
  2. alert-engine's `PostgresAlertContextRepository` reads that data back
     correctly (the SQL window-function query, not just the Python model).
  3. batch-processor's repository writes a household bill and serving-api's
     `is_day_finalised` / `get_daily_bills` correctly reflect it — the
     provisional/final merge rule's data side.
  4. the `serving_ro` role (used by serving-api and Grafana) can actually read
     every table it needs — a role with no GRANTs is a silent failure mode
     unit tests never touch.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from decimal import Decimal

import pytest
from energy_common.domain.models import (
    BillingTier,
    HouseholdBill,
    ZoneMetric,
    ZoneSolarSummary,
)
from energy_common.storage import pg_engine
from sqlalchemy import text

pytestmark = pytest.mark.integration

ADMIN_DSN = os.environ.get("PG_DSN", "postgresql://energy:energy@localhost:5432/energy_serving")
READ_ONLY_DSN = os.environ.get(
    "PG_DSN_SERVING_RO", "postgresql://serving_ro:serving_ro@localhost:5432/energy_serving"
)


@pytest.fixture
def business_date() -> dt.date:
    # Unique-ish per test run so re-runs don't collide with prior leftovers.
    return dt.date(2026, 1, 1) + dt.timedelta(days=uuid.uuid4().int % 300)


@pytest.fixture(autouse=True)
def _seed_dimensions(business_date: dt.date) -> None:
    engine = pg_engine(ADMIN_DSN)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dim_zone (grid_zone, name, region, meter_capacity) "
                "VALUES ('zone-north', 'North', 'North', 100) "
                "ON CONFLICT (grid_zone) DO NOTHING"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dim_household (household_id, grid_zone, meter_id, dwelling_type, "
                "occupants, has_solar, base_load_kwh_day, solar_capacity_kwp) "
                "VALUES ('HH-E2E', 'zone-north', 'MTR-E2E', 'apartment', 2, false, 8.0, 0.0) "
                "ON CONFLICT (household_id) DO NOTHING"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dim_date (business_date, year, quarter, month, day, day_of_week, is_weekend) "
                "VALUES (:d, 2026, 1, 1, 1, 1, false) ON CONFLICT (business_date) DO NOTHING"
            ),
            {"d": business_date},
        )


def test_stream_processor_upsert_reflects_full_window_total(business_date: dt.date) -> None:
    from stream_processor.adapters.repository import PostgresZoneMetricsRepository

    engine = pg_engine(ADMIN_DSN)
    repo = PostgresZoneMetricsRepository(engine)
    window_start = dt.datetime.combine(business_date, dt.time(12, 0), tzinfo=dt.UTC)

    # Two "micro-batches" for the same window: Spark's stateful aggregation
    # means each carries the FULL accumulated total so far, not a delta.
    m1 = ZoneMetric(
        grid_zone="zone-north",
        window_type="tumbling_1m",
        window_start=window_start,
        window_end=window_start + dt.timedelta(minutes=1),
        total_consumption_kwh=5.0,
        total_solar_kwh=1.0,
        renewable_pct=20.0,
        active_meter_count=3,
        computed_at=window_start,
    )
    repo.upsert_metrics([m1])
    m2 = m1.model_copy(update={"total_consumption_kwh": 12.0, "active_meter_count": 7})
    repo.upsert_metrics([m2])

    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT total_consumption_kwh, active_meter_count FROM rt_zone_metrics "
                    "WHERE grid_zone = 'zone-north' AND window_start = :ws"
                ),
                {"ws": window_start},
            )
            .mappings()
            .one()
        )
    assert row["total_consumption_kwh"] == 12.0
    assert row["active_meter_count"] == 7


def test_alert_context_repository_reads_back_live_metrics() -> None:
    from alert_engine.adapters.context_repository import PostgresAlertContextRepository
    from stream_processor.adapters.repository import PostgresZoneMetricsRepository

    engine = pg_engine(ADMIN_DSN)
    # get_recent_zone_metrics deliberately ignores business_date and returns the
    # most-recent-by-window_start rows (that's what alert-engine needs) -- so
    # to assert on "the row we just wrote" reliably regardless of what other
    # tests left behind, anchor it far enough in the future to always sort first.
    window_start = dt.datetime.now(dt.UTC).replace(microsecond=0) + dt.timedelta(days=3650)
    metric = ZoneMetric(
        grid_zone="zone-north",
        window_type="tumbling_1m",
        window_start=window_start,
        window_end=window_start + dt.timedelta(minutes=1),
        total_consumption_kwh=9.0,
        total_solar_kwh=0.5,
        renewable_pct=5.5,
        active_meter_count=4,
        computed_at=window_start,
    )
    PostgresZoneMetricsRepository(engine).upsert_metrics([metric])

    zone_metrics = PostgresAlertContextRepository(engine).get_recent_zone_metrics(("zone-north",))
    assert zone_metrics["zone-north"][0].total_consumption_kwh == 9.0


def test_batch_bill_is_visible_through_serving_api_repository(business_date: dt.date) -> None:
    from batch_processor.adapters.repository import PostgresBatchRepository
    from serving_api.adapters.repository import PostgresServingRepository

    engine = pg_engine(ADMIN_DSN)
    bill = HouseholdBill(
        business_date=business_date,
        household_id="HH-E2E",
        grid_zone="zone-north",
        total_consumption_kwh=10.0,
        total_solar_kwh=2.0,
        self_consumed_kwh=2.0,
        grid_import_kwh=8.0,
        solar_export_kwh=0.0,
        tariff_rate=Decimal("50.00"),
        billing_tier=BillingTier.FLAT,
        subsidy_flag=False,
        energy_charge=Decimal("400.00"),
        solar_credit=Decimal("0.00"),
        net_amount=Decimal("400.00"),
        computed_at=dt.datetime.now(dt.UTC),
    )
    summary = ZoneSolarSummary(
        business_date=business_date,
        grid_zone="zone-north",
        total_consumption_kwh=10.0,
        total_solar_generation_kwh=2.0,
        renewable_pct=20.0,
        household_count=1,
        computed_at=dt.datetime.now(dt.UTC),
    )
    batch_repo = PostgresBatchRepository(engine)
    batch_repo.upsert_household_bills([bill])
    batch_repo.upsert_zone_solar_summaries([summary])
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO batch_runs (run_id, dag_id, business_date, status, rows_written, "
                "started_at, finished_at) VALUES (:rid, 'daily_energy_reconciliation', :d, "
                "'success', 1, now(), now()) ON CONFLICT (run_id) DO NOTHING"
            ),
            {"rid": f"e2e-{business_date.isoformat()}", "d": business_date},
        )

    serving_repo = PostgresServingRepository(engine)
    assert serving_repo.is_day_finalised(business_date) is True
    bills = serving_repo.get_daily_bills(business_date)
    assert any(b.household_id == "HH-E2E" and b.net_amount == Decimal("400.00") for b in bills)


def test_serving_ro_role_can_read_every_table_it_needs() -> None:
    """A role with no GRANTs is a silent failure mode unit tests never see."""
    engine = pg_engine(READ_ONLY_DSN)
    with engine.connect() as conn:
        for table in (
            "dim_zone",
            "dim_household",
            "rt_zone_metrics",
            "daily_household_billing",
            "daily_zone_solar_summary",
            "alerts",
            "batch_runs",
        ):
            conn.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
