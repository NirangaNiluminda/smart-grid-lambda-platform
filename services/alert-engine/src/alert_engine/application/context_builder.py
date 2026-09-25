"""Builds a live `RuleContext` from Postgres + Prometheus each evaluation cycle.

This is the composition of the two read adapters into what
`alert_engine.domain.rules` actually needs — kept separate from
`AlertEngine.evaluate_cycle` (which only knows about *rules*, not where their
input data comes from) so each stays independently testable.
"""

from __future__ import annotations

import datetime as dt

from energy_common.domain.models import GRID_ZONES

from alert_engine.adapters.context_repository import PostgresAlertContextRepository
from alert_engine.adapters.metrics_source import PrometheusMetricsSource
from alert_engine.domain.rules import RuleContext


class ContextBuilder:
    def __init__(
        self,
        context_repo: PostgresAlertContextRepository,
        metrics_source: PrometheusMetricsSource,
        *,
        grid_zones: tuple[str, ...] = GRID_ZONES,
    ) -> None:
        self._context_repo = context_repo
        self._metrics_source = metrics_source
        self._grid_zones = grid_zones

    def build(self, *, now: dt.datetime, current_business_date: dt.date) -> RuleContext:
        zone_metrics = self._context_repo.get_recent_zone_metrics(self._grid_zones)
        batch_runs = self._context_repo.get_recent_batch_runs(since=current_business_date)
        consumed, rejected = self._metrics_source.get_reject_rate_inputs()

        return RuleContext(
            now=now,
            current_business_date=current_business_date,
            grid_zones=self._grid_zones,
            zone_metrics=zone_metrics,
            records_consumed_total=consumed,
            records_rejected_total=rejected,
            latest_batch_runs=batch_runs,
        )
