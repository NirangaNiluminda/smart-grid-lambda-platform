-- Dimension tables. Populated by scripts/seed_database.sh (Part 1).
-- Applied automatically on first container start (docker-entrypoint-initdb.d).

\connect energy_serving

CREATE TABLE IF NOT EXISTS dim_zone (
    grid_zone       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    region          TEXT NOT NULL,
    meter_capacity  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_household (
    household_id        TEXT PRIMARY KEY,
    grid_zone           TEXT NOT NULL REFERENCES dim_zone(grid_zone),
    meter_id            TEXT NOT NULL UNIQUE,
    dwelling_type       TEXT NOT NULL,
    occupants           INTEGER NOT NULL CHECK (occupants > 0),
    has_solar           BOOLEAN NOT NULL,
    base_load_kwh_day   DOUBLE PRECISION NOT NULL,
    solar_capacity_kwp  DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_dim_household_zone ON dim_household(grid_zone);

CREATE TABLE IF NOT EXISTS dim_date (
    business_date  DATE PRIMARY KEY,
    year           INTEGER NOT NULL,
    quarter        INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    day            INTEGER NOT NULL,
    day_of_week    INTEGER NOT NULL,
    is_weekend     BOOLEAN NOT NULL
);
