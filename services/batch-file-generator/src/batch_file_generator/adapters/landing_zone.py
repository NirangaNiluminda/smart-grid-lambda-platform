"""Filesystem landing-zone adapter — implements ``ports.LandingZone``.

Write protocol matches what the Airflow ``FileSensor`` expects:
1. write each CSV to ``<name>.csv.tmp`` then atomically rename to ``<name>.csv``
2. write the ``_SUCCESS_<date>`` marker **last**, only once both CSVs are in place

so a consumer that senses the marker is guaranteed complete, readable files.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
from pathlib import Path


class CsvLandingZoneWriter:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def write_csv(self, name: str, header: list[str], rows: list[list[object]]) -> None:
        target = self._root / name
        tmp = target.with_suffix(target.suffix + ".tmp")
        with tmp.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)

    def write_success_marker(self, business_date: dt.date) -> None:
        marker = self._root / f"_SUCCESS_{business_date.isoformat()}"
        marker.touch()

    # Convenience for callers/tests.
    def path_for(self, name: str) -> Path:
        return self._root / name
