"""FastAPI application for serving-api."""

from __future__ import annotations

import datetime as dt

from energy_common.domain.models import HouseholdBill, ZoneMetric, ZoneSolarSummary
from fastapi import APIRouter, FastAPI, HTTPException, Query, status
from pydantic import BaseModel
from serving_api.adapters.repository import ServingRepositoryProtocol
from serving_api.config import Settings


class DailyBillingResponse(BaseModel):
    business_date: dt.date
    status: str  # "final" | "provisional"
    bills: list[HouseholdBill]


class HouseholdBillingResponse(BaseModel):
    status: str  # "final" | "provisional"
    bill: HouseholdBill


class SolarReportResponse(BaseModel):
    business_date: dt.date
    status: str  # "final" | "provisional"
    summaries: list[ZoneSolarSummary]


def create_router(
    repo: ServingRepositoryProtocol,
    settings: Settings,
) -> APIRouter:
    router = APIRouter(prefix="/api")

    @router.get("/realtime/zones", response_model=list[ZoneMetric])
    def get_realtime_zones(
        window_type: str = Query("tumbling_1m", pattern="^(tumbling_1m|sliding_5m)$"),
    ) -> list[ZoneMetric]:
        return repo.get_latest_zone_metrics(window_type=window_type)

    @router.get("/realtime/zones/{zone}", response_model=ZoneMetric)
    def get_realtime_zone(
        zone: str,
        window_type: str = Query("tumbling_1m", pattern="^(tumbling_1m|sliding_5m)$"),
    ) -> ZoneMetric:
        metric = repo.get_latest_zone_metric(zone, window_type=window_type)
        if metric is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Zone {zone} has no recent metrics",
            )
        return metric

    @router.get("/billing/daily/{date}", response_model=DailyBillingResponse)
    def get_daily_billing(date: dt.date) -> DailyBillingResponse:
        finalised = repo.is_day_finalised(date)
        bills = repo.get_daily_bills(date)
        return DailyBillingResponse(
            business_date=date,
            status="final" if finalised else "provisional",
            bills=bills,
        )

    @router.get("/billing/household/{household_id}", response_model=HouseholdBillingResponse)
    def get_household_billing(
        household_id: str,
        date: dt.date | None = Query(None),
    ) -> HouseholdBillingResponse:
        bill = repo.get_household_bill(household_id, business_date=date)
        if bill is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bill for household {household_id} not found",
            )
        finalised = repo.is_day_finalised(bill.business_date)
        return HouseholdBillingResponse(
            status="final" if finalised else "provisional",
            bill=bill,
        )

    @router.get("/reports/solar/{date}", response_model=SolarReportResponse)
    def get_solar_report(date: dt.date) -> SolarReportResponse:
        finalised = repo.is_day_finalised(date)
        summaries = repo.get_solar_summaries(date)
        return SolarReportResponse(
            business_date=date,
            status="final" if finalised else "provisional",
            summaries=summaries,
        )

    return router


def create_app(
    repo: ServingRepositoryProtocol,
    settings: Settings,
) -> FastAPI:
    app = FastAPI(
        title="Smart Grid Serving API",
        version="0.1.0",
        description="Read model merging speed-layer (provisional) and batch-layer (final) views.",
    )

    api_router = create_router(repo, settings)
    app.include_router(api_router)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        if not repo.healthcheck():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connectivity failed",
            )

        latest_metric_ts = repo.get_latest_metric_computed_at()
        if latest_metric_ts is not None:
            now = dt.datetime.now(dt.UTC)
            budget = dt.timedelta(minutes=settings.freshness_budget_minutes)
            if now - latest_metric_ts > budget:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Metrics are stale (latest was at {latest_metric_ts.isoformat()})",
                )

        return {"status": "healthy"}

    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator().instrument(app).expose(app)
    except Exception:
        pass

    return app
