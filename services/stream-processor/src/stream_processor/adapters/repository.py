"""PostgreSQL repository for rt_zone_metrics."""

from __future__ import annotations

from energy_common.domain.models import ZoneMetric
from energy_common.storage.postgres import BaseRepository
from sqlalchemy import text


class PostgresZoneMetricsRepository(BaseRepository):
    """Upserts zone metrics into rt_zone_metrics."""

    def upsert_metrics(self, metrics: list[ZoneMetric]) -> int:
        if not metrics:
            return 0

        stmt = text(
            """
            INSERT INTO rt_zone_metrics (
                grid_zone,
                window_type,
                window_start,
                window_end,
                total_consumption_kwh,
                total_solar_kwh,
                renewable_pct,
                active_meter_count,
                computed_at
            ) VALUES (
                :grid_zone,
                :window_type,
                :window_start,
                :window_end,
                :total_consumption_kwh,
                :total_solar_kwh,
                :renewable_pct,
                :active_meter_count,
                :computed_at
            )
            ON CONFLICT (grid_zone, window_type, window_start)
            DO UPDATE SET
                window_end = EXCLUDED.window_end,
                total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                total_solar_kwh = EXCLUDED.total_solar_kwh,
                renewable_pct = EXCLUDED.renewable_pct,
                active_meter_count = EXCLUDED.active_meter_count,
                computed_at = EXCLUDED.computed_at;
            """
        )

        rows = [
            {
                "grid_zone": m.grid_zone,
                "window_type": m.window_type,
                "window_start": m.window_start,
                "window_end": m.window_end,
                "total_consumption_kwh": m.total_consumption_kwh,
                "total_solar_kwh": m.total_solar_kwh,
                "renewable_pct": m.renewable_pct,
                "active_meter_count": m.active_meter_count,
                "computed_at": m.computed_at,
            }
            for m in metrics
        ]

        with self.engine.begin() as conn:
            conn.execute(stmt, rows)

        return len(metrics)
