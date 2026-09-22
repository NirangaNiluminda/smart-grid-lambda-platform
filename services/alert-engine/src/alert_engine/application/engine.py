"""Alert engine application logic: evaluation, deduplication and resolution."""

from __future__ import annotations

import datetime as dt
from typing import Protocol

from alert_engine.domain.rules import Rule, RuleContext
from energy_common import get_logger
from energy_common.domain.models import Alert
from energy_common.domain.ports import AlertSink
from energy_common.observability import counter

logger = get_logger("alert-engine")

ALERTS_FIRED = counter(
    "alerts_fired_total",
    "Total alerts fired by rule and severity",
    labelnames=("rule_name", "severity"),
)


class AlertPersistencePort(Protocol):
    def get_open_alerts(self) -> list[Alert]: ...
    def deliver(self, alert: Alert) -> None: ...
    def resolve_alert(
        self,
        rule_name: str,
        grid_zone: str | None,
        business_date: dt.date | None,
        resolved_at: dt.datetime,
    ) -> None: ...


class AlertEngine:
    """Evaluates rules, deduplicates active alerts, delivers to sinks,
    and resolves cleared alerts."""

    def __init__(
        self,
        rules: list[Rule],
        db_sink: AlertPersistencePort,
        sinks: list[AlertSink] | None = None,
    ) -> None:
        self.rules = rules
        self.db_sink = db_sink
        self.sinks = sinks or []

    def evaluate_cycle(self, ctx: RuleContext) -> tuple[list[Alert], list[Alert]]:
        # 1. Run all rules
        active_alerts: list[Alert] = []
        for rule in self.rules:
            active_alerts.extend(rule.evaluate(ctx))

        # 2. Get currently open alerts
        open_alerts = self.db_sink.get_open_alerts()
        open_keys = {a.dedupe_key(): a for a in open_alerts}

        # 3. Deliver new alerts (deduplicate against already open alerts)
        newly_fired: list[Alert] = []
        for alert in active_alerts:
            key = alert.dedupe_key()
            if key not in open_keys:
                newly_fired.append(alert)
                self.db_sink.deliver(alert)
                for sink in self.sinks:
                    if sink is not self.db_sink:
                        sink.deliver(alert)
                ALERTS_FIRED.labels(
                    rule_name=alert.rule_name,
                    severity=alert.severity.value,
                ).inc()

        # 4. Resolve cleared conditions
        active_keys = {a.dedupe_key() for a in active_alerts}
        resolved: list[Alert] = []
        for key, open_alert in open_keys.items():
            if key not in active_keys:
                self.db_sink.resolve_alert(
                    rule_name=open_alert.rule_name,
                    grid_zone=open_alert.grid_zone,
                    business_date=open_alert.business_date,
                    resolved_at=ctx.now,
                )
                resolved.append(open_alert)
                logger.info(
                    "alert.resolved",
                    rule_name=open_alert.rule_name,
                    grid_zone=open_alert.grid_zone,
                    business_date=str(open_alert.business_date),
                )

        return newly_fired, resolved
