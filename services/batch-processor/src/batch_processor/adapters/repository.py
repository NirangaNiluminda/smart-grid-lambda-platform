"""PostgreSQL repository for batch-processor."""

from __future__ import annotations

import datetime as dt

from energy_common.domain.models import HouseholdBill, ZoneSolarSummary
from energy_common.storage.postgres import BaseRepository
from sqlalchemy import text


class PostgresBatchRepository(BaseRepository):
    """Persistence operations for batch billing and reconciliation."""

    def get_active_household_ids(self) -> set[str]:
        stmt = text("SELECT household_id FROM dim_household")
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return {r[0] for r in rows}

    def get_household_zones(self) -> dict[str, str]:
        stmt = text("SELECT household_id, grid_zone FROM dim_household")
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return {r[0]: r[1] for r in rows}

    def upsert_household_bills(self, bills: list[HouseholdBill]) -> int:
        if not bills:
            return 0

        stmt = text(
            """
            INSERT INTO daily_household_billing (
                business_date,
                household_id,
                grid_zone,
                total_consumption_kwh,
                total_solar_kwh,
                self_consumed_kwh,
                grid_import_kwh,
                solar_export_kwh,
                tariff_rate,
                billing_tier,
                subsidy_flag,
                energy_charge,
                solar_credit,
                net_amount,
                currency,
                computed_at
            ) VALUES (
                :business_date,
                :household_id,
                :grid_zone,
                :total_consumption_kwh,
                :total_solar_kwh,
                :self_consumed_kwh,
                :grid_import_kwh,
                :solar_export_kwh,
                :tariff_rate,
                :billing_tier,
                :subsidy_flag,
                :energy_charge,
                :solar_credit,
                :net_amount,
                :currency,
                :computed_at
            )
            ON CONFLICT (business_date, household_id)
            DO UPDATE SET
                grid_zone = EXCLUDED.grid_zone,
                total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                total_solar_kwh = EXCLUDED.total_solar_kwh,
                self_consumed_kwh = EXCLUDED.self_consumed_kwh,
                grid_import_kwh = EXCLUDED.grid_import_kwh,
                solar_export_kwh = EXCLUDED.solar_export_kwh,
                tariff_rate = EXCLUDED.tariff_rate,
                billing_tier = EXCLUDED.billing_tier,
                subsidy_flag = EXCLUDED.subsidy_flag,
                energy_charge = EXCLUDED.energy_charge,
                solar_credit = EXCLUDED.solar_credit,
                net_amount = EXCLUDED.net_amount,
                currency = EXCLUDED.currency,
                computed_at = EXCLUDED.computed_at;
            """
        )

        rows = [
            {
                "business_date": b.business_date,
                "household_id": b.household_id,
                "grid_zone": b.grid_zone,
                "total_consumption_kwh": b.total_consumption_kwh,
                "total_solar_kwh": b.total_solar_kwh,
                "self_consumed_kwh": b.self_consumed_kwh,
                "grid_import_kwh": b.grid_import_kwh,
                "solar_export_kwh": b.solar_export_kwh,
                "tariff_rate": float(b.tariff_rate),
                "billing_tier": b.billing_tier.value,
                "subsidy_flag": b.subsidy_flag,
                "energy_charge": float(b.energy_charge),
                "solar_credit": float(b.solar_credit),
                "net_amount": float(b.net_amount),
                "currency": b.currency,
                "computed_at": b.computed_at,
            }
            for b in bills
        ]

        with self.engine.begin() as conn:
            conn.execute(stmt, rows)

        return len(bills)

    def upsert_zone_solar_summaries(self, summaries: list[ZoneSolarSummary]) -> int:
        if not summaries:
            return 0

        stmt = text(
            """
            INSERT INTO daily_zone_solar_summary (
                business_date,
                grid_zone,
                total_consumption_kwh,
                total_solar_generation_kwh,
                renewable_pct,
                household_count,
                computed_at
            ) VALUES (
                :business_date,
                :grid_zone,
                :total_consumption_kwh,
                :total_solar_generation_kwh,
                :renewable_pct,
                :household_count,
                :computed_at
            )
            ON CONFLICT (business_date, grid_zone)
            DO UPDATE SET
                total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                total_solar_generation_kwh = EXCLUDED.total_solar_generation_kwh,
                renewable_pct = EXCLUDED.renewable_pct,
                household_count = EXCLUDED.household_count,
                computed_at = EXCLUDED.computed_at;
            """
        )

        rows = [
            {
                "business_date": s.business_date,
                "grid_zone": s.grid_zone,
                "total_consumption_kwh": s.total_consumption_kwh,
                "total_solar_generation_kwh": s.total_solar_generation_kwh,
                "renewable_pct": s.renewable_pct,
                "household_count": s.household_count,
                "computed_at": s.computed_at,
            }
            for s in summaries
        ]

        with self.engine.begin() as conn:
            conn.execute(stmt, rows)

        return len(summaries)

    def delete_day(self, business_date: dt.date) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM daily_household_billing WHERE business_date = :bdate"),
                {"bdate": business_date},
            )
            conn.execute(
                text("DELETE FROM daily_zone_solar_summary WHERE business_date = :bdate"),
                {"bdate": business_date},
            )
