"""Structured logging setup (structlog).

One call — ``configure_logging(...)`` — wires JSON output to stdout with ISO
timestamps and a bound ``service`` field. ``get_logger(service)`` returns a bound
logger; downstream code binds ``business_date``, ``correlation_id`` etc. per unit
of work.

See ``docs/OBSERVABILITY.md`` for the mandatory field list.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_CONFIGURED = False


def configure_logging(*, level: str = "INFO", fmt: str = "json") -> None:
    """Idempotent global setup. Safe to call from every service's entrypoint."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        # Not cached: a cached logger pins the stdout stream it was built with,
        # which breaks pytest's per-test capture. The rebuild cost is negligible
        # at our message rates.
        cache_logger_on_first_use=False,
    )
    _CONFIGURED = True


def _reset_for_tests() -> None:
    """Undo :func:`configure_logging` so a test can reconfigure. Test-only."""
    global _CONFIGURED
    _CONFIGURED = False
    structlog.reset_defaults()


def get_logger(service: str, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger with ``service`` (and any extra context) permanently bound."""
    if not _CONFIGURED:
        configure_logging()
    logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
        service=service, **initial_context
    )
    return logger


def bind_context(**kwargs: Any) -> None:
    """Bind fields onto every subsequent log line in this async/thread context.

    Use at the start of a unit of work (a Kafka batch, an HTTP request, a DAG
    task) so correlation fields appear everywhere without threading a logger.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
