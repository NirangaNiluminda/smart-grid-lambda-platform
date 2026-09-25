"""Alert rule strategies and RuleContext."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from typing import Protocol

from energy_common.domain.models import (
    GRID_ZONES,
    Alert,
    AlertSeverity,
    BatchRun,
    BatchRunStatus,
    ZoneMetric,
)


@dataclass
class RuleContext:
    now: dt.datetime
    current_business_date: dt.date
    grid_zones: tuple[str, ...] = GRID_ZONES
    zone_metrics: dict[str, list[ZoneMetric]] = field(default_factory=dict)
    records_consumed_total: float = 0.0
    records_rejected_total: float = 0.0
    latest_batch_runs: list[BatchRun] = field(default_factory=list)


class Rule(Protocol):
    rule_name: str

    def evaluate(self, ctx: RuleContext) -> list[Alert]: ...


class NoDataRule:
    """Fires critical alert when a zone has received no data past the threshold."""

    def __init__(self, threshold_minutes: int = 3) -> None:
        self.rule_name = "NoDataRule"
        self.threshold = dt.timedelta(minutes=threshold_minutes)

    def evaluate(self, ctx: RuleContext) -> list[Alert]:
        alerts: list[Alert] = []
        for zone in ctx.grid_zones:
            metrics = ctx.zone_metrics.get(zone, [])
            if not metrics:
                alerts.append(
                    Alert(
                        alert_id=str(uuid.uuid4()),
                        rule_name=self.rule_name,
                        severity=AlertSeverity.CRITICAL,
                        grid_zone=zone,
                        business_date=ctx.current_business_date,
                        message=f"No data received for zone {zone}",
                        context={"zone": zone, "status": "no_metrics"},
                        fired_at=ctx.now,
                    )
                )
            else:
                newest = metrics[0]  # assumed sorted newest first
                if ctx.now - newest.window_end > self.threshold:
                    gap_s = (ctx.now - newest.window_end).total_seconds()
                    alerts.append(
                        Alert(
                            alert_id=str(uuid.uuid4()),
                            rule_name=self.rule_name,
                            severity=AlertSeverity.CRITICAL,
                            grid_zone=zone,
                            business_date=ctx.current_business_date,
                            message=(
                                f"Zone {zone} silent for {int(gap_s)}s "
                                f"(threshold: {self.threshold})"
                            ),
                            context={"zone": zone, "gap_seconds": str(int(gap_s))},
                            fired_at=ctx.now,
                        )
                    )
        return alerts


class LowRenewableRule:
    """Fires warning alert when renewable_pct is sustained below threshold for 3 windows."""

    def __init__(self, threshold_pct: float = 15.0, consecutive_windows: int = 3) -> None:
        self.rule_name = "LowRenewableRule"
        self.threshold_pct = threshold_pct
        self.consecutive_windows = consecutive_windows

    def evaluate(self, ctx: RuleContext) -> list[Alert]:
        alerts: list[Alert] = []
        for zone, metrics in ctx.zone_metrics.items():
            tumbling = [m for m in metrics if m.window_type == "tumbling_1m"]
            if len(tumbling) >= self.consecutive_windows:
                recent = tumbling[: self.consecutive_windows]
                if all(m.renewable_pct < self.threshold_pct for m in recent):
                    avg_pct = sum(m.renewable_pct for m in recent) / len(recent)
                    alerts.append(
                        Alert(
                            alert_id=str(uuid.uuid4()),
                            rule_name=self.rule_name,
                            severity=AlertSeverity.WARNING,
                            grid_zone=zone,
                            business_date=ctx.current_business_date,
                            message=(
                                f"Renewable generation in {zone} is low ({avg_pct:.1f}%) "
                                f"for {self.consecutive_windows} consecutive windows"
                            ),
                            context={"zone": zone, "avg_renewable_pct": str(round(avg_pct, 2))},
                            fired_at=ctx.now,
                        )
                    )
        return alerts


class HighRejectRateRule:
    """Fires warning alert when reject ratio exceeds threshold."""

    def __init__(self, max_reject_rate: float = 0.05) -> None:
        self.rule_name = "HighRejectRateRule"
        self.max_reject_rate = max_reject_rate

    def evaluate(self, ctx: RuleContext) -> list[Alert]:
        if ctx.records_consumed_total <= 0:
            return []

        ratio = ctx.records_rejected_total / ctx.records_consumed_total
        if ratio > self.max_reject_rate:
            return [
                Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_name=self.rule_name,
                    severity=AlertSeverity.WARNING,
                    grid_zone=None,
                    business_date=ctx.current_business_date,
                    message=(
                        f"Stream reject rate {ratio:.2%} exceeded threshold "
                        f"{self.max_reject_rate:.2%}"
                    ),
                    context={
                        "consumed": str(ctx.records_consumed_total),
                        "rejected": str(ctx.records_rejected_total),
                        "ratio": str(round(ratio, 4)),
                    },
                    fired_at=ctx.now,
                )
            ]
        return []


class BatchFailureRule:
    """Fires critical alert if yesterday's latest batch run failed or wrote zero rows."""

    def __init__(self) -> None:
        self.rule_name = "BatchFailureRule"

    def evaluate(self, ctx: RuleContext) -> list[Alert]:
        yesterday = ctx.current_business_date - dt.timedelta(days=1)
        runs = [r for r in ctx.latest_batch_runs if r.business_date == yesterday]

        if not runs:
            return []

        # Latest run
        latest_run = sorted(runs, key=lambda r: r.started_at, reverse=True)[0]
        if latest_run.status == BatchRunStatus.FAILED or latest_run.rows_written == 0:
            return [
                Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_name=self.rule_name,
                    severity=AlertSeverity.CRITICAL,
                    grid_zone=None,
                    business_date=yesterday,
                    message=(
                        f"Batch reconciliation for {yesterday.isoformat()} status="
                        f"{latest_run.status.value}, rows_written={latest_run.rows_written}"
                    ),
                    context={
                        "run_id": latest_run.run_id,
                        "status": latest_run.status.value,
                        "rows_written": str(latest_run.rows_written),
                    },
                    fired_at=ctx.now,
                )
            ]
        return []
