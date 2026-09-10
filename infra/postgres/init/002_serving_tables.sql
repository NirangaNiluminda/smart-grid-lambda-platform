-- Serving-layer output tables.
-- Created by Part 1 so the contract is fixed; filled by Part 2:
--   rt_zone_metrics          <- stream-processor  (speed layer)
--   daily_household_billing  <- batch-processor   (batch layer)
--   daily_zone_solar_summary <- batch-processor   (batch layer)

\connect energy_serving

CREATE TABLE IF NOT EXISTS rt_zone_metrics (
    grid_zone              TEXT NOT NULL REFERENCES dim_zone(grid_zone),
    window_type            TEXT NOT NULL CHECK (window_type IN ('tumbling_1m', 'sliding_5m')),
    window_start           TIMESTAMPTZ NOT NULL,
    window_end             TIMESTAMPTZ NOT NULL,
    total_consumption_kwh  DOUBLE PRECISION NOT NULL,
    total_solar_kwh        DOUBLE PRECISION NOT NULL,
    renewable_pct          DOUBLE PRECISION NOT NULL CHECK (renewable_pct BETWEEN 0 AND 100),
    active_meter_count     INTEGER NOT NULL,
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_type, window_start)
);
CREATE INDEX IF NOT EXISTS ix_rt_zone_metrics_recent
    ON rt_zone_metrics (grid_zone, window_type, window_start DESC);

CREATE TABLE IF NOT EXISTS daily_household_billing (
    business_date          DATE NOT NULL REFERENCES dim_date(business_date),
    household_id            TEXT NOT NULL REFERENCES dim_household(household_id),
    grid_zone              TEXT NOT NULL REFERENCES dim_zone(grid_zone),
    total_consumption_kwh  DOUBLE PRECISION NOT NULL,
    total_solar_kwh        DOUBLE PRECISION NOT NULL,
    self_consumed_kwh      DOUBLE PRECISION NOT NULL,
    grid_import_kwh        DOUBLE PRECISION NOT NULL,
    solar_export_kwh       DOUBLE PRECISION NOT NULL,
    tariff_rate            NUMERIC(8,4) NOT NULL,
    billing_tier           TEXT NOT NULL CHECK (billing_tier IN ('flat', 'tiered', 'subsidised')),
    subsidy_flag           BOOLEAN NOT NULL,
    energy_charge          NUMERIC(12,2) NOT NULL,
    solar_credit           NUMERIC(12,2) NOT NULL,
    net_amount             NUMERIC(12,2) NOT NULL,
    currency               TEXT NOT NULL DEFAULT 'LKR',
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (business_date, household_id)
);
CREATE INDEX IF NOT EXISTS ix_billing_household ON daily_household_billing (household_id, business_date DESC);

CREATE TABLE IF NOT EXISTS daily_zone_solar_summary (
    business_date                DATE NOT NULL REFERENCES dim_date(business_date),
    grid_zone                    TEXT NOT NULL REFERENCES dim_zone(grid_zone),
    total_consumption_kwh        DOUBLE PRECISION NOT NULL,
    total_solar_generation_kwh   DOUBLE PRECISION NOT NULL,
    renewable_pct                DOUBLE PRECISION NOT NULL,
    household_count              INTEGER NOT NULL,
    computed_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (business_date, grid_zone)
);
