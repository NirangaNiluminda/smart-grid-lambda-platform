"""Reads the live pipeline state a `RuleContext` needs, from Postgres.

This is the piece that was missing from the original wiring: `AlertEngine` and
the `Rule` strategies were fully implemented and unit-tested against
hand-built `RuleContext` objects, but nothing in `__main__.py` ever populated
one from real data — every cycle evaluated an empty context, so `NoDataRule`
permanently false-fired for every zone and the other three rules could never
fire at all. This repository is that missing link.
"""

from __future__ import annotations

import datetime as dt

from energy_common.domain.models import BatchRun, BatchRunStatus, ZoneMetric
from energy_common.storage.postgres import BaseRepository
from sqlalchemy import text


class PostgresAlertContextRepository(BaseRepository):
    """Read-only: everything `RuleContext` needs, one call each."""

    def get_recent_zone_metrics(
        self, zones: tuple[str, ...], *, per_zone_window_limit: int = 5
    ) -> dict[str, list[ZoneMetric]]:
        """Most recent rows per (zone, window_type), newest first — enough for
        `NoDataRule` (needs the latest row) and `LowRenewableRule` (needs the
        last few tumbling windows)."""
        stmt = text(
            """
            SELECT grid_zone, window_type, window_start, window_end,
                   total_consumption_kwh, total_solar_kwh, renewable_pct,
                   active_meter_count, computed_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY grid_zone, window_type ORDER BY window_start DESC
                       ) AS rn
                FROM rt_zone_metrics
                WHERE grid_zone = ANY(:zones)
            ) ranked
            WHERE rn <= :limit
            ORDER BY grid_zone, window_start DESC
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(
                stmt, {"zones": list(zones), "limit": per_zone_window_limit}
            ).mappings()
            out: dict[str, list[ZoneMetric]] = {zone: [] for zone in zones}
            for row in rows:
                metric = ZoneMetric(
                    grid_zone=row["grid_zone"],
                    window_type=row["window_type"],
                    window_start=row["window_start"],
                    window_end=row["window_end"],
                    total_consumption_kwh=max(0.0, float(row["total_consumption_kwh"])),
                    total_solar_kwh=max(0.0, float(row["total_solar_kwh"])),
                    renewable_pct=float(row["renewable_pct"]),
                    active_meter_count=int(row["active_meter_count"]),
                    computed_at=row["computed_at"],
                )
                out.setdefault(metric.grid_zone, []).append(metric)
        return out

    def get_recent_batch_runs(self, *, since: dt.date, lookback_days: int = 3) -> list[BatchRun]:
        stmt = text(
            """
            SELECT run_id, dag_id, business_date, status, rows_written,
                   started_at, finished_at, notes
            FROM batch_runs
            WHERE business_date >= :since_date
            ORDER BY started_at DESC
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(
                stmt, {"since_date": since - dt.timedelta(days=lookback_days)}
            ).mappings()
            return [
                BatchRun(
                    run_id=row["run_id"],
                    dag_id=row["dag_id"],
                    business_date=row["business_date"],
                    status=BatchRunStatus(row["status"]),
                    rows_written=row["rows_written"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    notes=row["notes"],
                )
                for row in rows
            ]
