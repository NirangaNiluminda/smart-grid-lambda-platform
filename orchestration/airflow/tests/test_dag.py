"""Unit tests for daily_energy_reconciliation DAG helper logic."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from dags.daily_energy_reconciliation import (
    publish_report_artifact,
    validate_feed_files,
    verify_lake_partition,
)


def test_validate_feed_files_passes_on_valid_csv(tmp_path: Path) -> None:
    bdate = dt.date(2026, 1, 1)
    date_str = bdate.isoformat()

    tariff_csv = tmp_path / f"tariff_reference_{date_str}.csv"
    weather_csv = tmp_path / f"weather_forecast_{date_str}.csv"

    tariff_csv.write_text("hh,rate,tier,sub,date\nHH1,50,flat,false,2026-01-01\n")
    weather_csv.write_text("zone,date,cloud,temp,irr\nzone-north,2026-01-01,10,25.0,0.8\n")

    res = validate_feed_files(tmp_path, bdate)
    assert res["tariff_rows"] == 1
    assert res["weather_rows"] == 1


def test_validate_feed_files_fails_on_missing(tmp_path: Path) -> None:
    bdate = dt.date(2026, 1, 1)
    with pytest.raises(FileNotFoundError):
        validate_feed_files(tmp_path, bdate)


def test_verify_lake_partition(tmp_path: Path) -> None:
    bdate = dt.date(2026, 1, 1)
    part_dir = tmp_path / "readings_raw" / "business_date=2026-01-01"
    part_dir.mkdir(parents=True)

    with pytest.raises(FileNotFoundError):
        verify_lake_partition(tmp_path, bdate)

    (part_dir / "_SUCCESS").touch()
    assert verify_lake_partition(tmp_path, bdate) is True


def test_publish_report_artifact(tmp_path: Path) -> None:
    bdate = dt.date(2026, 1, 1)
    reports_out = tmp_path / "out"
    published = tmp_path / "published"
    reports_out.mkdir()

    src = reports_out / f"billing_report_{bdate}.html"
    src.write_text("<h1>Report</h1>")

    dest = publish_report_artifact(bdate, reports_out, published)
    assert dest.exists()
    assert dest.read_text() == "<h1>Report</h1>"
