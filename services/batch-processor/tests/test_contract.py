"""Executable spec for batch-processor (Part 2 §B2)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(reason="Part 2 skeleton — implement in §B2")


def test_flat_billing_charges_grid_import_at_tariff_rate() -> None:
    """net_amount = grid_import_kwh * tariff_rate - solar_export_kwh * export_rate."""


def test_tiered_billing_applies_block_multipliers() -> None: ...


def test_subsidised_billing_discounts_first_block() -> None: ...


def test_every_active_household_billed_exactly_once() -> None:
    """DQ assertion: |daily_household_billing rows for D| == |active households|."""


def test_rerun_for_same_date_is_idempotent() -> None:
    """Running D twice leaves identical rows (full replace, not append)."""


def test_missing_partition_success_marker_aborts_unless_allow_open() -> None: ...


def test_zone_solar_summary_reconciles_to_household_rows() -> None:
    """sum(daily_household_billing.total_solar_kwh) by zone == daily_zone_solar_summary."""
