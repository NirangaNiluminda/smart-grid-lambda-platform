-- Grants for the read-only `serving_ro` role (created in 000_databases.sql).
-- serving-api and Grafana both connect as this role — without these grants a
-- freshly created role can read nothing, and every query returns
-- "permission denied for table ...".

\connect energy_serving

GRANT CONNECT ON DATABASE energy_serving TO serving_ro;
GRANT USAGE ON SCHEMA public TO serving_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO serving_ro;

-- Cover tables created by later migrations too (owned by the role running
-- these init scripts, i.e. POSTGRES_USER).
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO serving_ro;
