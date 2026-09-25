"""Reads ingest/reject counters from Prometheus.

`records_consumed_total` / `records_rejected_total` are Prometheus counters
exported by `ingestion` and `stream-processor` in-process (see
`energy_common.observability`) — a separate process like alert-engine can only
see their current values through Prometheus's HTTP API, not in memory. Failures
(Prometheus unreachable, no data yet) degrade to "no signal" rather than raising,
so a Prometheus hiccup doesn't take the alert loop down.
"""

from __future__ import annotations

import httpx
from energy_common import get_logger

logger = get_logger("alert-engine")

_CONSUMED_QUERY = "sum(records_consumed_total)"
_REJECTED_QUERY = "sum(records_rejected_total)"


class PrometheusMetricsSource:
    def __init__(self, base_url: str, *, timeout_s: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    def _instant_query(self, promql: str) -> float:
        try:
            resp = httpx.get(
                f"{self._base_url}/api/v1/query",
                params={"query": promql},
                timeout=self._timeout_s,
            )
            resp.raise_for_status()
            result = resp.json()["data"]["result"]
            if not result:
                return 0.0
            return float(result[0]["value"][1])
        except Exception:
            logger.warning("prometheus.query_failed", query=promql, exc_info=True)
            return 0.0

    def get_reject_rate_inputs(self) -> tuple[float, float]:
        """Returns `(records_consumed_total, records_rejected_total)` as of now."""
        return self._instant_query(_CONSUMED_QUERY), self._instant_query(_REJECTED_QUERY)
