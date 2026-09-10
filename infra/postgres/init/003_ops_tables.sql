-- Operations / observability tables.
--   alerts      <- alert-engine   (Part 2)
--   batch_runs  <- Airflow DAG    (Part 2)
-- Created by Part 1 so alert-engine and the DAG have a stable target.

\connect energy_serving

CREATE TABLE IF NOT EXISTS alerts (
    alert_id       UUID PRIMARY KEY,
    rule_name      TEXT NOT NULL,
    severity       TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    grid_zone      TEXT REFERENCES dim_zone(grid_zone),
    business_date  DATE,
    message        TEXT NOT NULL,
    context        JSONB NOT NULL DEFAULT '{}'::jsonb,
    fired_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);
-- One open alert per (rule, zone, date). alert-engine relies on this for dedupe.
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_open
    ON alerts (rule_name, COALESCE(grid_zone, ''), COALESCE(business_date, 'epoch'::date))
    WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS batch_runs (
    run_id         TEXT PRIMARY KEY,
    dag_id         TEXT NOT NULL,
    business_date  DATE NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_written   INTEGER NOT NULL DEFAULT 0,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    notes          TEXT
);
CREATE INDEX IF NOT EXISTS ix_batch_runs_date ON batch_runs (business_date DESC, started_at DESC);
