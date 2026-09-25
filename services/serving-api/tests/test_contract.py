"""Executable spec for serving-api (Part 2 §B4)."""

from __future__ import annotations

import datetime as dt
import inspect
from decimal import Decimal

from energy_common.domain.models import BillingTier, HouseholdBill, ZoneMetric, ZoneSolarSummary
from fastapi.testclient import TestClient
from serving_api.adapters.repository import PostgresServingRepository, ServingRepositoryProtocol
from serving_api.application.api import create_app
from serving_api.config import Settings


class FakeServingRepository(ServingRepositoryProtocol):
    def __init__(self) -> None:
        self.metrics: list[ZoneMetric] = []
        self.bills: list[HouseholdBill] = []
        self.summaries: list[ZoneSolarSummary] = []
        self.finalised_days: set[dt.date] = set()
        self.latest_computed_at: dt.datetime | None = None
        self.is_healthy: bool = True

    def get_latest_zone_metrics(self, window_type: str = "tumbling_1m") -> list[ZoneMetric]:
        return [m for m in self.metrics if m.window_type == window_type]

    def get_latest_zone_metric(
        self, grid_zone: str, window_type: str = "tumbling_1m"
    ) -> ZoneMetric | None:
        matches = [
            m for m in self.metrics if m.grid_zone == grid_zone and m.window_type == window_type
        ]
        return matches[-1] if matches else None

    def get_daily_bills(self, business_date: dt.date) -> list[HouseholdBill]:
        return [b for b in self.bills if b.business_date == business_date]

    def get_household_bill(
        self, household_id: str, business_date: dt.date | None = None
    ) -> HouseholdBill | None:
        matches = [
            b
            for b in self.bills
            if b.household_id == household_id
            and (business_date is None or b.business_date == business_date)
        ]
        return matches[-1] if matches else None

    def get_solar_summaries(self, business_date: dt.date) -> list[ZoneSolarSummary]:
        return [s for s in self.summaries if s.business_date == business_date]

    def is_day_finalised(self, business_date: dt.date) -> bool:
        return business_date in self.finalised_days

    def get_latest_metric_computed_at(self) -> dt.datetime | None:
        return self.latest_computed_at

    def healthcheck(self) -> bool:
        return self.is_healthy


def _sample_metric(zone: str = "zone-north") -> ZoneMetric:
    now = dt.datetime.now(dt.UTC)
    return ZoneMetric(
        grid_zone=zone,
        window_type="tumbling_1m",
        window_start=now - dt.timedelta(minutes=1),
        window_end=now,
        total_consumption_kwh=10.5,
        total_solar_kwh=3.2,
        renewable_pct=30.48,
        active_meter_count=50,
        computed_at=now,
    )


def _sample_bill(date: dt.date) -> HouseholdBill:
    return HouseholdBill(
        business_date=date,
        household_id="HH-001",
        grid_zone="zone-north",
        total_consumption_kwh=15.0,
        total_solar_kwh=5.0,
        self_consumed_kwh=5.0,
        grid_import_kwh=10.0,
        solar_export_kwh=0.0,
        tariff_rate=Decimal("50.00"),
        billing_tier=BillingTier.FLAT,
        subsidy_flag=False,
        energy_charge=Decimal("500.00"),
        solar_credit=Decimal("0.00"),
        net_amount=Decimal("500.00"),
        currency="LKR",
        computed_at=dt.datetime.now(dt.UTC),
    )


def test_realtime_zones_returns_one_row_per_zone_latest_window() -> None:
    repo = FakeServingRepository()
    repo.metrics = [_sample_metric("zone-north"), _sample_metric("zone-south")]
    settings = Settings()
    app = create_app(repo, settings)
    client = TestClient(app)

    resp = client.get("/api/realtime/zones")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    zones = {item["grid_zone"] for item in data}
    assert zones == {"zone-north", "zone-south"}


def test_finalised_day_returns_batch_numbers_tagged_final() -> None:
    """A day with batch_runs.status='success' -> status='final'."""
    repo = FakeServingRepository()
    bdate = dt.date(2026, 1, 1)
    repo.finalised_days.add(bdate)
    repo.bills = [_sample_bill(bdate)]

    settings = Settings()
    app = create_app(repo, settings)
    client = TestClient(app)

    resp = client.get(f"/api/billing/daily/{bdate.isoformat()}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "final"
    assert len(data["bills"]) == 1
    assert data["bills"][0]["household_id"] == "HH-001"


def test_unreconciled_day_returns_speed_numbers_tagged_provisional() -> None:
    repo = FakeServingRepository()
    bdate = dt.date(2026, 1, 1)
    repo.bills = [_sample_bill(bdate)]

    settings = Settings()
    app = create_app(repo, settings)
    client = TestClient(app)

    resp = client.get(f"/api/billing/daily/{bdate.isoformat()}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "provisional"


def test_healthz_503_when_rt_metrics_stale() -> None:
    repo = FakeServingRepository()
    settings = Settings(freshness_budget_minutes=5)
    app = create_app(repo, settings)
    client = TestClient(app)

    # 1. Stale metrics (computed 10 min ago) -> 503
    repo.latest_computed_at = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=10)
    resp = client.get("/healthz")
    assert resp.status_code == 503

    # 2. Fresh metrics (computed 1 min ago) -> 200
    repo.latest_computed_at = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1)
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_api_never_computes_only_reads() -> None:
    """No aggregation SQL beyond ORDER BY / LIMIT / simple filters in the repositories."""
    source = inspect.getsource(PostgresServingRepository)
    assert "GROUP BY" not in source.upper()
    assert "SUM(" not in source.upper()
    assert "AVG(" not in source.upper()
