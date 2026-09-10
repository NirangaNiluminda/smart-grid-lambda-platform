from __future__ import annotations

import csv
import datetime as dt

from batch_file_generator.adapters.landing_zone import CsvLandingZoneWriter


def test_writes_readable_csv_and_marker(tmp_path) -> None:  # type: ignore[no-untyped-def]
    lz = CsvLandingZoneWriter(tmp_path)
    lz.write_csv("tariff_reference_2026-01-01.csv", ["a", "b"], [[1, 2], [3, 4]])
    lz.write_success_marker(dt.date(2026, 1, 1))

    with (tmp_path / "tariff_reference_2026-01-01.csv").open() as fh:
        rows = list(csv.reader(fh))
    assert rows == [["a", "b"], ["1", "2"], ["3", "4"]]
    assert (tmp_path / "_SUCCESS_2026-01-01").exists()


def test_no_tmp_file_left_behind(tmp_path) -> None:  # type: ignore[no-untyped-def]
    lz = CsvLandingZoneWriter(tmp_path)
    lz.write_csv("weather_forecast_2026-01-01.csv", ["x"], [["y"]])
    assert not list(tmp_path.glob("*.tmp"))


def test_overwrite_is_clean(tmp_path) -> None:  # type: ignore[no-untyped-def]
    lz = CsvLandingZoneWriter(tmp_path)
    lz.write_csv("f.csv", ["h"], [["old"]])
    lz.write_csv("f.csv", ["h"], [["new"]])
    with (tmp_path / "f.csv").open() as fh:
        assert list(csv.reader(fh)) == [["h"], ["new"]]
