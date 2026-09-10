"""Storage adapters. Each concrete class needs an extra:
``energy-common[postgres]`` for Postgres, ``energy-common[parquet]`` for the lake."""

from typing import Any

__all__ = ["BaseRepository", "ParquetObjectStore", "pg_engine"]


def __getattr__(name: str) -> Any:
    if name in {"pg_engine", "BaseRepository"}:
        from energy_common.storage import postgres

        return getattr(postgres, name)
    if name == "ParquetObjectStore":
        from energy_common.storage.parquet_store import ParquetObjectStore

        return ParquetObjectStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
