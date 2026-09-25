"""Executable spec for alert-engine (Part 2 §B5)."""

from __future__ import annotations

import datetime as dt

from alert_engine.application.engine import AlertEngine
from alert_engine.domain.rules import (
    BatchFailureRule,
    HighRejectRateRule,
    LowRenewableRule,
    NoDataRule,
    RuleContext,
)
from energy_common.domain.models import (
    Alert,
    AlertSeverity,
    BatchRun,
    BatchRunStatus,
    ZoneMetric,
)
from energy_common.domain.ports import AlertSink


class InMemoryDbAlertSink:
    def __init__(self) -> None:
        self.alerts: dict[str, Alert] = {}

    def get_open_alerts(self) -> list[Alert]:
        return [a for a in self.alerts.values() if a.resolved_at is None]

    def deliver(self, alert: Alert) -> None:
        self.alerts[alert.alert_id] = alert

    def resolve_alert(
        self,
        rule_name: str,
        grid_zone: str | None,
        business_date: dt.date | None,
        resolved_at: dt.datetime,
    ) -> None:
        for alert_id, a in list(self.alerts.items()):
            if (
                a.rule_name == rule_name
                and a.grid_zone == grid_zone
                and a.business_date == business_date
                and a.resolved_at is None
            ):
                self.alerts[alert_id] = a.model_copy(update={"resolved_at": resolved_at})


class RecordingSink(AlertSink):
    def __init__(self) -> None:
        self.received: list[Alert] = []

    def deliver(self, alert: Alert) -> None:
        self.received.append(alert)


def _sample_metric(
    zone: str,
    end_time: dt.datetime,
    pct: float = 30.0,
    window_type: str = "tumbling_1m",
) -> ZoneMetric:
    return ZoneMetric(
        grid_zone=zone,
        window_type=window_type,
        window_start=end_time - dt.timedelta(minutes=1),
        window_end=end_time,
        total_consumption_kwh=10.0,
        total_solar_kwh=3.0,
        renewable_pct=pct,
        active_meter_count=20,
        computed_at=end_time,
    )


def test_no_data_rule_fires_when_zone_silent_past_threshold() -> None:
    now = dt.datetime(2026, 1, 1, 12, 10, 0, tzinfo=dt.UTC)
    bdate = dt.date(2026, 1, 1)

    rule = NoDataRule(threshold_minutes=3)

    # zone-north received data 5 min ago (> 3 min threshold)
    old_metric = _sample_metric("zone-north", now - dt.timedelta(minutes=5))
    # zone-south received data 1 min ago (< 3 min threshold)
    fresh_metric = _sample_metric("zone-south", now - dt.timedelta(minutes=1))

    ctx = RuleContext(
        now=now,
        current_business_date=bdate,
        grid_zones=("zone-north", "zone-south"),
        zone_metrics={
            "zone-north": [old_metric],
            "zone-south": [fresh_metric],
        },
    )

    alerts = rule.evaluate(ctx)
    assert len(alerts) == 1
    assert alerts[0].grid_zone == "zone-north"
    assert alerts[0].severity == AlertSeverity.CRITICAL


def test_low_renewable_rule_fires_after_sustained_low_pct() -> None:
    """Brief's explicit alert: renewable contribution considerably low."""
    now = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    bdate = dt.date(2026, 1, 1)

    rule = LowRenewableRule(threshold_pct=15.0, consecutive_windows=3)

    # 3 consecutive windows with renewable_pct < 15%
    m1 = _sample_metric("zone-north", now, pct=10.0)
    m2 = _sample_metric("zone-north", now - dt.timedelta(minutes=1), pct=12.0)
    m3 = _sample_metric("zone-north", now - dt.timedelta(minutes=2), pct=9.0)

    ctx = RuleContext(
        now=now,
        current_business_date=bdate,
        zone_metrics={"zone-north": [m1, m2, m3]},
    )

    alerts = rule.evaluate(ctx)
    assert len(alerts) == 1
    assert alerts[0].grid_zone == "zone-north"
    assert alerts[0].severity == AlertSeverity.WARNING


def test_high_reject_rate_rule_uses_ratio_not_absolute() -> None:
    now = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    bdate = dt.date(2026, 1, 1)
    rule = HighRejectRateRule(max_reject_rate=0.05)

    # 1. 100 rejects out of 1,000 (10% > 5%) -> fires
    ctx_high = RuleContext(
        now=now,
        current_business_date=bdate,
        records_consumed_total=1000,
        records_rejected_total=100,
    )
    alerts_high = rule.evaluate(ctx_high)
    assert len(alerts_high) == 1

    # 2. 100 rejects out of 10,000 (1% < 5%) -> does not fire despite same 100 rejects
    ctx_low = RuleContext(
        now=now,
        current_business_date=bdate,
        records_consumed_total=10000,
        records_rejected_total=100,
    )
    alerts_low = rule.evaluate(ctx_low)
    assert len(alerts_low) == 0


def test_batch_failure_rule_reads_latest_batch_run() -> None:
    now = dt.datetime(2026, 1, 2, 6, 0, 0, tzinfo=dt.UTC)
    today = dt.date(2026, 1, 2)
    yesterday = dt.date(2026, 1, 1)
    rule = BatchFailureRule()

    # Latest run for yesterday failed
    failed_run = BatchRun(
        run_id="run-1",
        dag_id="daily_energy_reconciliation",
        business_date=yesterday,
        status=BatchRunStatus.FAILED,
        rows_written=0,
        started_at=now - dt.timedelta(hours=1),
    )

    ctx = RuleContext(
        now=now,
        current_business_date=today,
        latest_batch_runs=[failed_run],
    )
    alerts = rule.evaluate(ctx)
    assert len(alerts) == 1
    assert alerts[0].severity == AlertSeverity.CRITICAL


def test_unresolved_duplicate_is_not_refired() -> None:
    """Same (rule_name, grid_zone, business_date) while unresolved -> no new row."""
    db_sink = InMemoryDbAlertSink()
    sink = RecordingSink()
    rule = HighRejectRateRule(max_reject_rate=0.05)
    engine = AlertEngine(rules=[rule], db_sink=db_sink, sinks=[sink])  # type: ignore[arg-type]

    now = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    ctx = RuleContext(
        now=now,
        current_business_date=dt.date(2026, 1, 1),
        records_consumed_total=100,
        records_rejected_total=20,
    )

    # First cycle -> fires
    new1, _ = engine.evaluate_cycle(ctx)
    assert len(new1) == 1
    assert len(sink.received) == 1

    # Second cycle (condition still active) -> deduplicated, does not re-fire
    new2, _ = engine.evaluate_cycle(ctx)
    assert len(new2) == 0
    assert len(sink.received) == 1


def test_alert_fans_out_to_every_registered_sink() -> None:
    db_sink = InMemoryDbAlertSink()
    sink1 = RecordingSink()
    sink2 = RecordingSink()
    rule = HighRejectRateRule(max_reject_rate=0.05)
    engine = AlertEngine(rules=[rule], db_sink=db_sink, sinks=[sink1, sink2])  # type: ignore[arg-type]

    ctx = RuleContext(
        now=dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC),
        current_business_date=dt.date(2026, 1, 1),
        records_consumed_total=100,
        records_rejected_total=20,
    )
    engine.evaluate_cycle(ctx)

    assert len(sink1.received) == 1
    assert len(sink2.received) == 1
    assert sink1.received[0].alert_id == sink2.received[0].alert_id


def test_condition_clearing_sets_resolved_at() -> None:
    db_sink = InMemoryDbAlertSink()
    sink = RecordingSink()
    rule = HighRejectRateRule(max_reject_rate=0.05)
    engine = AlertEngine(rules=[rule], db_sink=db_sink, sinks=[sink])  # type: ignore[arg-type]

    t1 = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    t2 = dt.datetime(2026, 1, 1, 12, 5, 0, tzinfo=dt.UTC)
    bdate = dt.date(2026, 1, 1)

    # Cycle 1: condition active -> fires alert
    ctx_bad = RuleContext(
        now=t1,
        current_business_date=bdate,
        records_consumed_total=100,
        records_rejected_total=20,
    )
    engine.evaluate_cycle(ctx_bad)
    open_alerts = db_sink.get_open_alerts()
    assert len(open_alerts) == 1
    assert open_alerts[0].resolved_at is None

    # Cycle 2: condition cleared -> resolved_at is set
    ctx_good = RuleContext(
        now=t2,
        current_business_date=bdate,
        records_consumed_total=1000,
        records_rejected_total=10,  # 1% < 5%
    )
    _, resolved = engine.evaluate_cycle(ctx_good)
    assert len(resolved) == 1
    assert len(db_sink.get_open_alerts()) == 0
    all_stored = list(db_sink.alerts.values())
    assert all_stored[0].resolved_at == t2
