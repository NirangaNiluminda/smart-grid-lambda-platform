"""Prometheus metrics helpers.

``serve_metrics(port)`` starts a background HTTP server exposing ``/metrics``.
``counter`` / ``gauge`` / ``histogram`` are thin, deduplicating factories so two
imports of the same metric name don't blow up the default registry.
"""

from __future__ import annotations

import threading
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, start_http_server

_lock = threading.Lock()
_server_started = False


def serve_metrics(port: int) -> None:
    """Expose the default registry on ``0.0.0.0:<port>/metrics``. Idempotent."""
    global _server_started
    with _lock:
        if _server_started:
            return
        start_http_server(port)
        _server_started = True


def _existing(name: str) -> Any | None:
    return REGISTRY._names_to_collectors.get(name)


def counter(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Counter:
    existing = _existing(name)
    if isinstance(existing, Counter):
        return existing
    return Counter(name, documentation, labelnames)


def gauge(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Gauge:
    existing = _existing(name)
    if isinstance(existing, Gauge):
        return existing
    return Gauge(name, documentation, labelnames)


def histogram(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...] = (),
    buckets: tuple[float, ...] | None = None,
) -> Histogram:
    existing = _existing(name)
    if isinstance(existing, Histogram):
        return existing
    if buckets is not None:
        return Histogram(name, documentation, labelnames, buckets=buckets)
    return Histogram(name, documentation, labelnames)
