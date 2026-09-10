-- Runs first (files execute in lexical order). The primary database
-- `energy_serving` is created by the image from POSTGRES_DB; here we add the
-- separate metadata database Airflow needs (Part 2) on the same instance.

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- A least-privilege role for the read-only serving API (Part 2 grants selects).
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'serving_ro') THEN
        CREATE ROLE serving_ro LOGIN PASSWORD 'serving_ro';
    END IF;
END
$$;
