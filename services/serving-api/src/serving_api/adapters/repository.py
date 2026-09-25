"""Read-only repository for serving-api."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Protocol

from energy_common.domain.models import (
    BillingTier,
    HouseholdBill,
    ZoneMetric,
    ZoneSolarSummary,
)
from energy_common.storage.postgres import BaseRepository
from sqlalchemy import text


class ServingRepositoryProtocol(Protocol):
    def get_latest_zone_metrics(self, window_type: str = "tumbling_1m") -> list[ZoneMetric]: ...
    def get_latest_zone_metric(
        self, grid_zone: str, window_type: str = "tumbling_1m"
    ) -> ZoneMetric | None: ...
    def get_daily_bills(self, business_date: dt.date) -> list[HouseholdBill]: ...
    def get_household_bill(
        self, household_id: str, business_date: dt.date | None = None
    ) -> HouseholdBill | None: ...
    def get_solar_summaries(self, business_date: dt.date) -> list[ZoneSolarSummary]: ...
    def is_day_finalised(self, business_date: dt.date) -> bool: ...
    def get_latest_metric_computed_at(self) -> dt.datetime | None: ...
    def healthcheck(self) -> bool: ...


class PostgresServingRepository(BaseRepository):
    """PostgreSQL read-only repository."""

    def get_latest_zone_metrics(self, window_type: str = "tumbling_1m") -> list[ZoneMetric]:
        stmt = text(
            """
            SELECT DISTINCT ON (grid_zone)
                grid_zone, window_type, window_start, window_end,
                total_consumption_kwh, total_solar_kwh, renewable_pct,
                active_meter_count, computed_at
            FROM rt_zone_metrics
            WHERE window_type = :wtype
            ORDER BY grid_zone, window_start DESC
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt, {"wtype": window_type}).fetchall()

        return [
            ZoneMetric(
                grid_zone=r[0],
                window_type=r[1],
                window_start=r[2],
                window_end=r[3],
                total_consumption_kwh=r[4],
                total_solar_kwh=r[5],
                renewable_pct=r[6],
                active_meter_count=r[7],
                computed_at=r[8],
            )
            for r in rows
        ]

    def get_latest_zone_metric(
        self, grid_zone: str, window_type: str = "tumbling_1m"
    ) -> ZoneMetric | None:
        stmt = text(
            """
            SELECT
                grid_zone, window_type, window_start, window_end,
                total_consumption_kwh, total_solar_kwh, renewable_pct,
                active_meter_count, computed_at
            FROM rt_zone_metrics
            WHERE grid_zone = :zone AND window_type = :wtype
            ORDER BY window_start DESC
            LIMIT 1
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt, {"zone": grid_zone, "wtype": window_type}).fetchone()

        if row is None:
            return None

        return ZoneMetric(
            grid_zone=row[0],
            window_type=row[1],
            window_start=row[2],
            window_end=row[3],
            total_consumption_kwh=row[4],
            total_solar_kwh=row[5],
            renewable_pct=row[6],
            active_meter_count=row[7],
            computed_at=row[8],
        )

    def get_daily_bills(self, business_date: dt.date) -> list[HouseholdBill]:
        stmt = text(
            """
            SELECT
                business_date, household_id, grid_zone,
                total_consumption_kwh, total_solar_kwh, self_consumed_kwh,
                grid_import_kwh, solar_export_kwh, tariff_rate,
                billing_tier, subsidy_flag, energy_charge, solar_credit,
                net_amount, currency, computed_at
            FROM daily_household_billing
            WHERE business_date = :bdate
            ORDER BY household_id
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt, {"bdate": business_date}).fetchall()

        return [
            HouseholdBill(
                business_date=r[0],
                household_id=r[1],
                grid_zone=r[2],
                total_consumption_kwh=r[3],
                total_solar_kwh=r[4],
                self_consumed_kwh=r[5],
                grid_import_kwh=r[6],
                solar_export_kwh=r[7],
                tariff_rate=Decimal(str(r[8])),
                billing_tier=BillingTier(r[9]),
                subsidy_flag=r[10],
                energy_charge=Decimal(str(r[11])),
                solar_credit=Decimal(str(r[12])),
                net_amount=Decimal(str(r[13])),
                currency=r[14],
                computed_at=r[15],
            )
            for r in rows
        ]

    def get_household_bill(
        self, household_id: str, business_date: dt.date | None = None
    ) -> HouseholdBill | None:
        if business_date is not None:
            stmt = text(
                """
                SELECT
                    business_date, household_id, grid_zone,
                    total_consumption_kwh, total_solar_kwh, self_consumed_kwh,
                    grid_import_kwh, solar_export_kwh, tariff_rate,
                    billing_tier, subsidy_flag, energy_charge, solar_credit,
                    net_amount, currency, computed_at
                FROM daily_household_billing
                WHERE household_id = :hid AND business_date = :bdate
                """
            )
            params = {"hid": household_id, "bdate": business_date}
        else:
            stmt = text(
                """
                SELECT
                    business_date, household_id, grid_zone,
                    total_consumption_kwh, total_solar_kwh, self_consumed_kwh,
                    grid_import_kwh, solar_export_kwh, tariff_rate,
                    billing_tier, subsidy_flag, energy_charge, solar_credit,
                    net_amount, currency, computed_at
                FROM daily_household_billing
                WHERE household_id = :hid
                ORDER BY business_date DESC
                LIMIT 1
                """
            )
            params = {"hid": household_id}

        with self.engine.connect() as conn:
            row = conn.execute(stmt, params).fetchone()

        if row is None:
            return None

        return HouseholdBill(
            business_date=row[0],
            household_id=row[1],
            grid_zone=row[2],
            total_consumption_kwh=row[3],
            total_solar_kwh=row[4],
            self_consumed_kwh=row[5],
            grid_import_kwh=row[6],
            solar_export_kwh=row[7],
            tariff_rate=Decimal(str(row[8])),
            billing_tier=BillingTier(row[9]),
            subsidy_flag=row[10],
            energy_charge=Decimal(str(row[11])),
            solar_credit=Decimal(str(row[12])),
            net_amount=Decimal(str(row[13])),
            currency=row[14],
            computed_at=row[15],
        )

    def get_solar_summaries(self, business_date: dt.date) -> list[ZoneSolarSummary]:
        stmt = text(
            """
            SELECT
                business_date, grid_zone, total_consumption_kwh,
                total_solar_generation_kwh, renewable_pct, household_count,
                computed_at
            FROM daily_zone_solar_summary
            WHERE business_date = :bdate
            ORDER BY grid_zone
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt, {"bdate": business_date}).fetchall()

        return [
            ZoneSolarSummary(
                business_date=r[0],
                grid_zone=r[1],
                total_consumption_kwh=r[2],
                total_solar_generation_kwh=r[3],
                renewable_pct=r[4],
                household_count=r[5],
                computed_at=r[6],
            )
            for r in rows
        ]

    def is_day_finalised(self, business_date: dt.date) -> bool:
        stmt = text(
            """
            SELECT 1 FROM batch_runs
            WHERE business_date = :bdate AND status = 'success'
            LIMIT 1
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt, {"bdate": business_date}).fetchone()
        return row is not None

    def get_latest_metric_computed_at(self) -> dt.datetime | None:
        stmt = text("SELECT MAX(computed_at) FROM rt_zone_metrics")
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
        return row[0] if row and row[0] else None
