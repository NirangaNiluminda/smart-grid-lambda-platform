"""Postgres access primitives.

``pg_engine`` builds a pooled SQLAlchemy engine from a DSN. ``BaseRepository`` is
the thin superclass every concrete repository extends — it owns the engine and a
``healthcheck``; subclasses add typed query methods so the application layer never
writes SQL. Needs the ``postgres`` extra.
"""

from __future__ import annotations

from functools import lru_cache

try:
    from sqlalchemy import Engine, create_engine, text
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "SQLAlchemy is not installed. Add the 'postgres' extra: "
        "pip install 'energy-common[postgres]'"
    ) from exc

from energy_common.domain.errors import StorageError


@lru_cache(maxsize=8)
def pg_engine(dsn: str, *, pool_size: int = 5) -> Engine:
    """Cached, pooled engine. One per DSN per process."""
    try:
        return create_engine(
            dsn,
            pool_size=pool_size,
            max_overflow=2,
            pool_pre_ping=True,
            future=True,
        )
    except Exception as exc:  # pragma: no cover - misconfiguration
        raise StorageError(f"could not create engine for {dsn!r}: {exc}") from exc


class BaseRepository:
    """Superclass for concrete repositories.

    Subclasses implement query methods returning domain models; they must not
    expose SQLAlchemy result rows to callers.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    def healthcheck(self) -> bool:
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
