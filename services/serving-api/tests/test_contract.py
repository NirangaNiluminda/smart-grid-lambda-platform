"""Executable spec for serving-api (Part 2 §B4)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(reason="Part 2 skeleton — implement in §B4")


def test_realtime_zones_returns_one_row_per_zone_latest_window() -> None: ...


def test_finalised_day_returns_batch_numbers_tagged_final() -> None:
    """A day with batch_runs.status='success' -> status='final', numbers from
    daily_household_billing."""


def test_unreconciled_day_returns_speed_numbers_tagged_provisional() -> None: ...


def test_healthz_503_when_rt_metrics_stale() -> None: ...


def test_api_never_computes_only_reads() -> None:
    """No aggregation SQL beyond ORDER BY / LIMIT / simple filters in the repositories."""
