"""Alert sinks and DB persistence adapter."""

from __future__ import annotations

import datetime as dt
import json

import httpx
from energy_common import get_logger
from energy_common.domain.models import Alert, AlertSeverity
from energy_common.domain.ports import AlertSink
from energy_common.storage.postgres import BaseRepository
from sqlalchemy import text

logger = get_logger("alert-engine")


class DbAlertSink(BaseRepository, AlertSink):
    """Postgres persistence for alerts and open-alert state."""

    def get_open_alerts(self) -> list[Alert]:
        stmt = text(
            """
            SELECT
                alert_id, rule_name, severity, grid_zone, business_date,
                message, context, fired_at, resolved_at
            FROM alerts
            WHERE resolved_at IS NULL
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        return [
            Alert(
                alert_id=str(r[0]),
                rule_name=r[1],
                severity=AlertSeverity(r[2]),
                grid_zone=r[3],
                business_date=r[4],
                message=r[5],
                context=r[6] if isinstance(r[6], dict) else {},
                fired_at=r[7],
                resolved_at=r[8],
            )
            for r in rows
        ]

    def deliver(self, alert: Alert) -> None:
        stmt = text(
            """
            INSERT INTO alerts (
                alert_id, rule_name, severity, grid_zone,
                business_date, message, context, fired_at, resolved_at
            ) VALUES (
                :alert_id, :rule_name, :severity, :grid_zone,
                :business_date, :message, :context, :fired_at, :resolved_at
            )
            ON CONFLICT (
                rule_name,
                COALESCE(grid_zone, ''),
                COALESCE(business_date, '1970-01-01'::date)
            )
            WHERE resolved_at IS NULL
            DO NOTHING;
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "alert_id": alert.alert_id,
                    "rule_name": alert.rule_name,
                    "severity": alert.severity.value,
                    "grid_zone": alert.grid_zone,
                    "business_date": alert.business_date,
                    "message": alert.message,
                    "context": json.dumps(alert.context),
                    "fired_at": alert.fired_at,
                    "resolved_at": alert.resolved_at,
                },
            )

    def resolve_alert(
        self,
        rule_name: str,
        grid_zone: str | None,
        business_date: dt.date | None,
        resolved_at: dt.datetime,
    ) -> None:
        stmt = text(
            """
            UPDATE alerts
            SET resolved_at = :resolved_at
            WHERE rule_name = :rule_name
              AND COALESCE(grid_zone, '') = COALESCE(:grid_zone, '')
              AND COALESCE(business_date, '1970-01-01'::date)
                  = COALESCE(:business_date, '1970-01-01'::date)
              AND resolved_at IS NULL;
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "rule_name": rule_name,
                    "grid_zone": grid_zone,
                    "business_date": business_date,
                    "resolved_at": resolved_at,
                },
            )


class LogAlertSink(AlertSink):
    """Logs fired alerts with structured context."""

    def deliver(self, alert: Alert) -> None:
        log_fn = (
            logger.critical
            if alert.severity == AlertSeverity.CRITICAL
            else (logger.warning if alert.severity == AlertSeverity.WARNING else logger.info)
        )
        log_fn(
            "alert.fired",
            alert_id=alert.alert_id,
            rule_name=alert.rule_name,
            severity=alert.severity.value,
            grid_zone=alert.grid_zone,
            business_date=alert.business_date.isoformat() if alert.business_date else None,
            message=alert.message,
            **alert.context,
        )


class WebhookAlertSink(AlertSink):
    """Optional webhook sink posting alerts to an external HTTP receiver."""

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def deliver(self, alert: Alert) -> None:
        try:
            httpx.post(self.webhook_url, json=alert.model_dump(mode="json"), timeout=3.0)
        except Exception as exc:
            logger.warning("webhook.failed", url=self.webhook_url, error=str(exc))
