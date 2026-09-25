"""Executable spec for batch-processor (Part 2 §B2)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest
from batch_processor.adapters.reports import ReportRenderer
from batch_processor.application.reconcile import BatchReconciliationService
from batch_processor.config import Settings
from batch_processor.domain.billing import (
    FlatBillingStrategy,
    SubsidisedBillingStrategy,
    TieredBillingStrategy,
)
from batch_processor.domain.quality import (
    DataQualityError,
    assert_all_households_billed,
    assert_solar_reconciles,
)
from energy_common.domain.models import (
    BillingTier,
    HouseholdBill,
    TariffRecord,
    ZoneSolarSummary,
)
from energy_common.domain.transforms import EnergyBalance


def _sample_tariff(
    *,
    household_id: str = "HH-001",
    rate: Decimal = Decimal("50.00"),
    tier: BillingTier = BillingTier.FLAT,
    subsidy: bool = False,
) -> TariffRecord:
    return TariffRecord(
        household_id=household_id,
        tariff_rate=rate,
        billing_tier=tier,
        subsidy_flag=subsidy,
        effective_date=dt.date(2026, 1, 1),
    )


def test_flat_billing_charges_grid_import_at_tariff_rate() -> None:
    """net_amount = grid_import_kwh * tariff_rate - solar_export_kwh * export_rate."""
    strat = FlatBillingStrategy()
    tariff = _sample_tariff(rate=Decimal("50.00"))
    export_rate = Decimal("18.00")

    # 1. Net importer: import 8 kWh, export 0 kWh
    balance1 = EnergyBalance(self_consumed_kwh=2.0, grid_import_kwh=8.0, solar_export_kwh=0.0)
    charge1 = strat.calculate_charge(balance1, tariff, export_rate)
    assert charge1.energy_charge == Decimal("400.00")
    assert charge1.solar_credit == Decimal("0.00")
    assert charge1.net_amount == Decimal("400.00")

    # 2. Net exporter: import 0 kWh, export 8 kWh
    balance2 = EnergyBalance(self_consumed_kwh=2.0, grid_import_kwh=0.0, solar_export_kwh=8.0)
    charge2 = strat.calculate_charge(balance2, tariff, export_rate)
    assert charge2.energy_charge == Decimal("0.00")
    assert charge2.solar_credit == Decimal("144.00")
    assert charge2.net_amount == Decimal("-144.00")


def test_tiered_billing_applies_block_multipliers() -> None:
    """Tiered blocks: 0-5 kWh @ 0.8x, 5-15 kWh @ 1.0x, >15 kWh @ 1.4x."""
    strat = TieredBillingStrategy(block_kwh=(5.0, 15.0), multipliers=(0.8, 1.0, 1.4))
    tariff = _sample_tariff(rate=Decimal("10.00"), tier=BillingTier.TIERED)
    export_rate = Decimal("18.00")

    # 20 kWh grid import:
    # 5 kWh @ 8.0 = 40
    # 10 kWh @ 10.0 = 100
    # 5 kWh @ 14.0 = 70
    # Total = 210
    balance = EnergyBalance(self_consumed_kwh=0.0, grid_import_kwh=20.0, solar_export_kwh=0.0)
    charge = strat.calculate_charge(balance, tariff, export_rate)
    assert charge.energy_charge == Decimal("210.00")
    assert charge.net_amount == Decimal("210.00")


def test_subsidised_billing_discounts_first_block() -> None:
    """Subsidised billing discounts the first 5 kWh by 50%."""
    strat = SubsidisedBillingStrategy(first_block_kwh=5.0, discount_factor=0.5)
    tariff = _sample_tariff(rate=Decimal("10.00"), tier=BillingTier.SUBSIDISED, subsidy=True)
    export_rate = Decimal("18.00")

    # 10 kWh grid import:
    # 5 kWh @ 5.0 = 25
    # 5 kWh @ 10.0 = 50
    # Total = 75
    balance = EnergyBalance(self_consumed_kwh=0.0, grid_import_kwh=10.0, solar_export_kwh=0.0)
    charge = strat.calculate_charge(balance, tariff, export_rate)
    assert charge.energy_charge == Decimal("75.00")
    assert charge.net_amount == Decimal("75.00")


def test_every_active_household_billed_exactly_once() -> None:
    """DQ assertion: |daily_household_billing rows for D| == |active households|."""
    bill1 = HouseholdBill(
        business_date=dt.date(2026, 1, 1),
        household_id="HH-001",
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

    # Missing household -> raises DataQualityError
    with pytest.raises(DataQualityError, match="active households were not billed"):
        assert_all_households_billed([bill1], {"HH-001", "HH-002"})

    # Exact match -> passes
    bill2 = bill1.model_copy(update={"household_id": "HH-002"})
    assert_all_households_billed([bill1, bill2], {"HH-001", "HH-002"})


def test_rerun_for_same_date_is_idempotent() -> None:
    """Running D twice leaves identical rows (full replace, not append)."""
    stored_bills: dict[tuple[dt.date, str], HouseholdBill] = {}

    def upsert_bills(bills: list[HouseholdBill]) -> None:
        for b in bills:
            stored_bills[(b.business_date, b.household_id)] = b

    b_date = dt.date(2026, 1, 1)
    bill = HouseholdBill(
        business_date=b_date,
        household_id="HH-001",
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

    upsert_bills([bill])
    assert len(stored_bills) == 1

    # Run again for same date
    upsert_bills([bill])
    assert len(stored_bills) == 1
    assert stored_bills[(b_date, "HH-001")].net_amount == Decimal("400.00")


def test_missing_partition_success_marker_aborts_unless_allow_open(tmp_path: Path) -> None:
    """Missing partition _SUCCESS marker aborts unless --allow-open."""
    b_date = dt.date(2026, 1, 1)
    date_str = b_date.isoformat()

    lake_dir = tmp_path / "lake"
    landing_dir = tmp_path / "landing"
    out_dir = tmp_path / "out"

    part_dir = lake_dir / "readings_raw" / f"business_date={date_str}"
    part_dir.mkdir(parents=True)

    # Empty parquet file
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("household_id", pa.string()),
            ("grid_zone", pa.string()),
            ("power_consumption_kwh", pa.float64()),
            ("solar_generation_kwh", pa.float64()),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(["HH-001"]),
            pa.array(["zone-north"]),
            pa.array([5.0]),
            pa.array([1.0]),
        ],
        schema=schema,
    )
    pq.write_table(table, part_dir / "part-0.parquet")

    landing_dir.mkdir(parents=True)
    with open(landing_dir / f"tariff_reference_{date_str}.csv", "w", newline="") as f:
        f.write("household_id,tariff_rate,billing_tier,subsidy_flag,effective_date\n")
        f.write(f"HH-001,50.0,flat,false,{date_str}\n")

    with open(landing_dir / f"weather_forecast_{date_str}.csv", "w", newline="") as f:
        f.write("grid_zone,forecast_date,cloud_cover_pct,temp_c,solar_irradiance_index\n")
        f.write(f"zone-north,{date_str},10,25.0,0.8\n")

    class FakeBatchRepo:
        def get_active_household_ids(self) -> set[str]:
            return {"HH-001"}

        def get_household_zones(self) -> dict[str, str]:
            return {"HH-001": "zone-north"}

        def upsert_household_bills(self, bills: list[HouseholdBill]) -> int:
            return len(bills)

        def upsert_zone_solar_summaries(self, summaries: list[ZoneSolarSummary]) -> int:
            return len(summaries)

    settings = Settings(
        lake_root=lake_dir,
        landing_root=landing_dir,
        report_out_dir=out_dir,
    )
    service = BatchReconciliationService(
        settings=settings,
        repository=FakeBatchRepo(),  # type: ignore[arg-type]
        report_renderer=ReportRenderer(out_dir),
    )

    # Missing _SUCCESS -> raises FileNotFoundError
    with pytest.raises(FileNotFoundError, match="_SUCCESS marker"):
        service.run_reconciliation(b_date, allow_open=False)

    # With allow_open=True -> succeeds
    bills_count, summaries_count = service.run_reconciliation(b_date, allow_open=True)
    assert bills_count == 1
    assert summaries_count == 1


def test_zone_solar_summary_reconciles_to_household_rows() -> None:
    """sum(daily_household_billing.total_solar_kwh) by zone == daily_zone_solar_summary."""
    b_date = dt.date(2026, 1, 1)
    bill1 = HouseholdBill(
        business_date=b_date,
        household_id="HH-001",
        grid_zone="zone-north",
        total_consumption_kwh=10.0,
        total_solar_kwh=3.0,
        self_consumed_kwh=2.0,
        grid_import_kwh=8.0,
        solar_export_kwh=1.0,
        tariff_rate=Decimal("50.00"),
        billing_tier=BillingTier.FLAT,
        subsidy_flag=False,
        energy_charge=Decimal("400.00"),
        solar_credit=Decimal("18.00"),
        net_amount=Decimal("382.00"),
        computed_at=dt.datetime.now(dt.UTC),
    )
    bill2 = bill1.model_copy(update={"household_id": "HH-002", "total_solar_kwh": 2.0})

    # Summary matching 3.0 + 2.0 = 5.0 -> passes
    summary_ok = ZoneSolarSummary(
        business_date=b_date,
        grid_zone="zone-north",
        total_consumption_kwh=20.0,
        total_solar_generation_kwh=5.0,
        renewable_pct=25.0,
        household_count=2,
        computed_at=dt.datetime.now(dt.UTC),
    )
    assert_solar_reconciles([bill1, bill2], [summary_ok])

    # Summary mismatch -> raises DataQualityError
    summary_bad = summary_ok.model_copy(update={"total_solar_generation_kwh": 6.0})
    with pytest.raises(DataQualityError, match="does not reconcile"):
        assert_solar_reconciles([bill1, bill2], [summary_bad])
