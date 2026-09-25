"""Composition root for alert-engine."""

from __future__ import annotations

from energy_common.observability import serve_metrics
from energy_common.storage import pg_engine

from alert_engine.adapters.context_repository import PostgresAlertContextRepository
from alert_engine.adapters.metrics_source import PrometheusMetricsSource
from alert_engine.adapters.sinks import DbAlertSink, LogAlertSink, WebhookAlertSink
from alert_engine.application.context_builder import ContextBuilder
from alert_engine.application.engine import AlertEngine
from alert_engine.config import Settings
from alert_engine.domain.rules import (
    BatchFailureRule,
    HighRejectRateRule,
    LowRenewableRule,
    NoDataRule,
    Rule,
)


def build_engine(settings: Settings) -> AlertEngine:
    serve_metrics(settings.metrics_port)
    engine = pg_engine(settings.pg_dsn)
    db_sink = DbAlertSink(engine)

    sinks = [db_sink, LogAlertSink()]
    if settings.alert_webhook_url:
        sinks.append(WebhookAlertSink(settings.alert_webhook_url))

    rules: list[Rule] = [
        NoDataRule(threshold_minutes=settings.alert_no_data_minutes),
        LowRenewableRule(threshold_pct=settings.alert_low_renewable_pct),
        HighRejectRateRule(max_reject_rate=settings.alert_max_reject_rate),
        BatchFailureRule(),
    ]

    return AlertEngine(rules=rules, db_sink=db_sink, sinks=sinks)


def build_context_builder(settings: Settings) -> ContextBuilder:
    """The other half of the wiring `build_engine` doesn't cover: where the
    `RuleContext` each cycle evaluates against actually comes from."""
    engine = pg_engine(settings.pg_dsn)
    context_repo = PostgresAlertContextRepository(engine)
    metrics_source = PrometheusMetricsSource(settings.prometheus_url)
    return ContextBuilder(context_repo, metrics_source)
