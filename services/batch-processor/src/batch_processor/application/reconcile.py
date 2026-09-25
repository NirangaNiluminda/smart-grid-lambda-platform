"""Batch reconciliation application service."""

from __future__ import annotations

import csv
import datetime as dt
import time
from decimal import Decimal

import pyarrow.parquet as pq
from batch_processor.adapters.reports import ReportRenderer
from batch_processor.adapters.repository import PostgresBatchRepository
from batch_processor.config import Settings
from batch_processor.domain.billing import get_billing_strategy
from batch_processor.domain.quality import (
    assert_all_households_billed,
    assert_charges_valid,
    assert_solar_reconciles,
    assert_weather_sanity,
)
from energy_common import get_logger
from energy_common.domain.models import (
    BillingTier,
    HouseholdBill,
    TariffRecord,
    WeatherForecast,
    ZoneSolarSummary,
)
from energy_common.domain.transforms import energy_balance, renewable_pct
from energy_common.observability import gauge, histogram

logger = get_logger("batch-processor")

BATCH_JOB_ROWS = gauge(
    "batch_job_rows",
    "Number of rows produced by batch job",
    labelnames=("table",),
)
BATCH_JOB_DURATION = histogram(
    "batch_job_duration_seconds",
    "Duration of batch job execution in seconds",
    labelnames=("business_date",),
)


class BatchReconciliationService:
    """Orchestrates the daily batch reconciliation pipeline."""

    def __init__(
        self,
        settings: Settings,
        repository: PostgresBatchRepository,
        report_renderer: ReportRenderer,
    ) -> None:
        self.settings = settings
        self.repo = repository
        self.report_renderer = report_renderer

    def run_reconciliation(
        self,
        business_date: dt.date,
        allow_open: bool = False,
    ) -> tuple[int, int]:
        start_time = time.monotonic()
        date_str = business_date.isoformat()
        logger.info("batch.started", business_date=date_str, allow_open=allow_open)

        # 1. Check partition and success marker
        partition_dir = self.settings.lake_root / "readings_raw" / f"business_date={date_str}"
        success_marker = partition_dir / "_SUCCESS"

        if not partition_dir.exists():
            raise FileNotFoundError(f"Lake partition directory missing: {partition_dir}")

        if not success_marker.exists() and not allow_open:
            raise FileNotFoundError(
                f"Lake partition for {date_str} is still open (missing _SUCCESS marker). "
                "Use --allow-open to process anyway."
            )

        # 2. Read Lake Parquet records and aggregate per household
        consumption_by_hh: dict[str, float] = {}
        solar_by_hh: dict[str, float] = {}
        hh_to_zone_lake: dict[str, str] = {}

        parquet_files = list(partition_dir.glob("*.parquet"))
        for pfile in parquet_files:
            table = pq.read_table(pfile)
            pydict = table.to_pydict()
            hh_col = pydict.get("household_id", [])
            zone_col = pydict.get("grid_zone", [])
            cons_col = pydict.get("power_consumption_kwh", [])
            sol_col = pydict.get("solar_generation_kwh", [])

            for hh, z, c, s in zip(hh_col, zone_col, cons_col, sol_col, strict=False):
                consumption_by_hh[hh] = consumption_by_hh.get(hh, 0.0) + (c if c > 0 else 0.0)
                solar_by_hh[hh] = solar_by_hh.get(hh, 0.0) + (s if s > 0 else 0.0)
                hh_to_zone_lake[hh] = z

        # 3. Read landing zone reference data
        tariff_file = self.settings.landing_root / f"tariff_reference_{date_str}.csv"
        weather_file = self.settings.landing_root / f"weather_forecast_{date_str}.csv"

        if not tariff_file.exists():
            raise FileNotFoundError(f"Tariff reference file missing: {tariff_file}")
        if not weather_file.exists():
            raise FileNotFoundError(f"Weather forecast file missing: {weather_file}")

        tariffs: dict[str, TariffRecord] = {}
        with open(tariff_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rec = TariffRecord(
                    household_id=row["household_id"],
                    tariff_rate=Decimal(row["tariff_rate"]),
                    billing_tier=BillingTier(row["billing_tier"].lower()),
                    subsidy_flag=row["subsidy_flag"].lower() == "true",
                    effective_date=dt.date.fromisoformat(row["effective_date"]),
                )
                tariffs[rec.household_id] = rec

        weather: list[WeatherForecast] = []
        with open(weather_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                w = WeatherForecast(
                    grid_zone=row["grid_zone"],
                    forecast_date=dt.date.fromisoformat(row["forecast_date"]),
                    cloud_cover_pct=int(row["cloud_cover_pct"]),
                    temp_c=float(row["temp_c"]),
                    solar_irradiance_index=float(row["solar_irradiance_index"]),
                )
                weather.append(w)

        # 4. Get active households & zones from repository
        active_households = self.repo.get_active_household_ids()
        household_zones = self.repo.get_household_zones()

        # If dim_household is empty (e.g. In unit tests), fall back to tariff/lake households
        if not active_households:
            active_households = set(tariffs.keys())
            household_zones = hh_to_zone_lake

        # 5. Calculate bills
        bills: list[HouseholdBill] = []
        computed_at = dt.datetime.now(dt.UTC)
        export_rate = Decimal(str(self.settings.solar_export_rate))

        for hh_id in sorted(active_households):
            tariff = tariffs.get(hh_id)
            if tariff is None:
                raise ValueError(f"Missing tariff record for household {hh_id}")

            zone = household_zones.get(hh_id, hh_to_zone_lake.get(hh_id, "zone-central"))
            cons = round(consumption_by_hh.get(hh_id, 0.0), 4)
            sol = round(solar_by_hh.get(hh_id, 0.0), 4)

            bal = energy_balance(cons, sol)
            strategy = get_billing_strategy(tariff.billing_tier)
            charge = strategy.calculate_charge(bal, tariff, export_rate)

            bill = HouseholdBill(
                business_date=business_date,
                household_id=hh_id,
                grid_zone=zone,
                total_consumption_kwh=cons,
                total_solar_kwh=sol,
                self_consumed_kwh=round(bal.self_consumed_kwh, 4),
                grid_import_kwh=round(bal.grid_import_kwh, 4),
                solar_export_kwh=round(bal.solar_export_kwh, 4),
                tariff_rate=tariff.tariff_rate,
                billing_tier=tariff.billing_tier,
                subsidy_flag=tariff.subsidy_flag,
                energy_charge=charge.energy_charge,
                solar_credit=charge.solar_credit,
                net_amount=charge.net_amount,
                currency=self.settings.currency,
                computed_at=computed_at,
            )
            bills.append(bill)

        # 6. Calculate per-zone solar summaries
        zone_groups: dict[str, list[HouseholdBill]] = {}
        for b in bills:
            zone_groups.setdefault(b.grid_zone, []).append(b)

        summaries: list[ZoneSolarSummary] = []
        for zone, z_bills in sorted(zone_groups.items()):
            z_cons = round(sum(b.total_consumption_kwh for b in z_bills), 4)
            z_sol = round(sum(b.total_solar_kwh for b in z_bills), 4)
            pct = round(renewable_pct(z_cons, z_sol), 2)
            summaries.append(
                ZoneSolarSummary(
                    business_date=business_date,
                    grid_zone=zone,
                    total_consumption_kwh=z_cons,
                    total_solar_generation_kwh=z_sol,
                    renewable_pct=pct,
                    household_count=len(z_bills),
                    computed_at=computed_at,
                )
            )

        # 7. DQ Assertions
        assert_all_households_billed(bills, active_households)
        assert_solar_reconciles(bills, summaries)
        assert_weather_sanity(summaries, weather)
        assert_charges_valid(bills)

        # 8. Persist to Postgres
        self.repo.upsert_household_bills(bills)
        self.repo.upsert_zone_solar_summaries(summaries)

        # 9. Render report artifacts
        csv_p, html_p = self.report_renderer.render(business_date, bills, summaries)
        logger.info("report.rendered", csv=str(csv_p), html=str(html_p))

        # 10. Metrics
        BATCH_JOB_ROWS.labels(table="daily_household_billing").set(len(bills))
        BATCH_JOB_ROWS.labels(table="daily_zone_solar_summary").set(len(summaries))
        duration = time.monotonic() - start_time
        BATCH_JOB_DURATION.labels(business_date=date_str).observe(duration)

        return len(bills), len(summaries)
