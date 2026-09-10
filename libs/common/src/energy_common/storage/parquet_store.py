"""Filesystem-backed :class:`energy_common.domain.ports.ObjectStore`.

Writes the immutable, Hive-partitioned Parquet master dataset the Lambda batch
layer reads. The path layout (``<dataset>/<partition>/part-*.parquet``) mirrors an
object store, so swapping in ``s3://`` is a root change, not a rewrite.

Write protocol (crash-safe): each append goes to a uniquely-named file inside a
per-partition ``_tmp`` directory, then is atomically ``os.rename``-d into place.
A partition is "complete" once ``_SUCCESS`` exists in it. Needs the ``parquet`` extra.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "pyarrow is not installed. Add the 'parquet' extra: pip install 'energy-common[parquet]'"
    ) from exc

from energy_common.domain.errors import StorageError

_SUCCESS = "_SUCCESS"


class ParquetObjectStore:
    """Append-only Parquet writer. Satisfies ``ports.ObjectStore``."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _partition_dir(self, dataset: str, partition: str) -> Path:
        return self._root / dataset / partition

    def append_rows(self, dataset: str, partition: str, rows: list[dict[str, object]]) -> int:
        if not rows:
            return 0
        pdir = self._partition_dir(dataset, partition)
        tmp = pdir / "_tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            table = pa.Table.from_pylist(rows)
            staged = tmp / f"part-{uuid.uuid4().hex}.parquet"
            pq.write_table(table, staged, compression="snappy")
            final = pdir / staged.name
            os.replace(staged, final)  # atomic within a filesystem
        except Exception as exc:
            raise StorageError(f"failed writing {dataset}/{partition}: {exc}") from exc
        return len(rows)

    def mark_complete(self, dataset: str, partition: str) -> None:
        pdir = self._partition_dir(dataset, partition)
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / _SUCCESS).touch(exist_ok=True)

    def partition_is_complete(self, dataset: str, partition: str) -> bool:
        return (self._partition_dir(dataset, partition) / _SUCCESS).exists()

    def partition_row_count(self, dataset: str, partition: str) -> int:
        """Total rows across the partition's parquet files (for metrics/DQ)."""
        pdir = self._partition_dir(dataset, partition)
        if not pdir.exists():
            return 0
        return sum(pq.ParquetFile(p).metadata.num_rows for p in pdir.glob("part-*.parquet"))
