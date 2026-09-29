#!/usr/bin/env bash
# Load dimension data (zones, households, calendar) into Postgres `energy_serving`.
# Idempotent: upserts on the primary key.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.yml}"
PG_SVC="${PG_SVC:-postgres}"
PGUSER="${PG_USER:-energy}"
PGDB="${PG_DATABASE:-energy_serving}"
SIM_START_DATE="${SIM_START_DATE:-2026-01-01}"
DAYS="${SEED_CALENDAR_DAYS:-120}"

# --env-file .env : see Makefile's COMPOSE for why this is needed.
COMPOSE=(docker compose --env-file .env -f "$COMPOSE_FILE")
psql() { "${COMPOSE[@]}" exec -T "$PG_SVC" psql -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDB" "$@"; }
# `\copy` is a psql *client-side* meta-command -- it only works when psql reads
# a script from stdin (or -f), never via -c (which sends the string straight
# to the server as SQL, where a leading backslash is just a syntax error).
psql_script() { "${COMPOSE[@]}" exec -T "$PG_SVC" psql -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDB"; }

echo "Waiting for Postgres..."
for _ in $(seq 1 30); do
  if psql -c 'SELECT 1' >/dev/null 2>&1; then break; fi
  sleep 2
done

echo "Loading dim_zone..."
psql_script <<'SQL'
  CREATE TEMP TABLE _z (grid_zone text, name text, region text, meter_capacity int);
  \copy _z FROM '/seed/zones.csv' WITH (FORMAT csv, HEADER true);
  INSERT INTO dim_zone SELECT * FROM _z
  ON CONFLICT (grid_zone) DO UPDATE SET name=EXCLUDED.name, region=EXCLUDED.region, meter_capacity=EXCLUDED.meter_capacity;
SQL

echo "Loading dim_household..."
psql_script <<'SQL'
  CREATE TEMP TABLE _h (household_id text, meter_id text, grid_zone text, dwelling_type text,
                        occupants int, has_solar boolean, base_load_kwh_day double precision,
                        solar_capacity_kwp double precision);
  \copy _h FROM '/seed/households.csv' WITH (FORMAT csv, HEADER true);
  INSERT INTO dim_household (household_id, grid_zone, meter_id, dwelling_type, occupants, has_solar, base_load_kwh_day, solar_capacity_kwp)
  SELECT household_id, grid_zone, meter_id, dwelling_type, occupants, has_solar, base_load_kwh_day, solar_capacity_kwp FROM _h
  ON CONFLICT (household_id) DO UPDATE SET
     grid_zone=EXCLUDED.grid_zone, meter_id=EXCLUDED.meter_id, dwelling_type=EXCLUDED.dwelling_type,
     occupants=EXCLUDED.occupants, has_solar=EXCLUDED.has_solar,
     base_load_kwh_day=EXCLUDED.base_load_kwh_day, solar_capacity_kwp=EXCLUDED.solar_capacity_kwp;
SQL

echo "Populating dim_date ($DAYS days from $SIM_START_DATE)..."
psql -c "
  INSERT INTO dim_date (business_date, year, quarter, month, day, day_of_week, is_weekend)
  SELECT d::date,
         extract(year from d)::int, extract(quarter from d)::int, extract(month from d)::int,
         extract(day from d)::int, extract(isodow from d)::int,
         extract(isodow from d)::int >= 6
  FROM generate_series('$SIM_START_DATE'::date, '$SIM_START_DATE'::date + ($DAYS - 1), '1 day') d
  ON CONFLICT (business_date) DO NOTHING;
"

psql -c "SELECT (SELECT count(*) FROM dim_zone) zones, (SELECT count(*) FROM dim_household) households, (SELECT count(*) FROM dim_date) dates;"
echo "Seed complete."
