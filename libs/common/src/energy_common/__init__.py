"""energy_common — the platform's shared kernel.

Stable public API (see ``docs/DATA_CONTRACTS.md`` §5). Anything imported here is
supported for the project's lifetime; submodule internals may change.
"""

from __future__ import annotations

from energy_common.config import BaseServiceSettings, get_settings
from energy_common.observability import (
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    serve_metrics,
)
from energy_common.simclock import SimClock, business_date_for

__version__ = "1.0.0"

__all__ = [
    "BaseServiceSettings",
    "SimClock",
    "__version__",
    "bind_context",
    "business_date_for",
    "clear_context",
    "configure_logging",
    "get_logger",
    "get_settings",
    "serve_metrics",
]
