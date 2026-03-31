#!/bin/bash
set -e
# Create timeseries database for TimescaleDB sensor data (Tab_Actual).
# Runs only on first postgres init (empty data dir).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
SELECT 'CREATE DATABASE timeseries OWNER "' || current_user || '"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'timeseries')\gexec
EOSQL
# Create tsdb_user if TSDB_USER env is set and different from POSTGRES_USER (for Docker)
if [ -n "${TSDB_USER:-}" ] && [ "$TSDB_USER" != "$POSTGRES_USER" ]; then
  psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${TSDB_USER}') THEN
        CREATE USER "${TSDB_USER}" WITH PASSWORD '${TSDB_PASSWORD:-tsdb_password}';
      END IF;
    END \$\$;
EOSQL
  psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "timeseries" -c "GRANT ALL ON SCHEMA public TO \"${TSDB_USER}\"; GRANT ALL ON DATABASE timeseries TO \"${TSDB_USER}\";" || true
else
  psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "timeseries" -c "GRANT ALL ON SCHEMA public TO \"$POSTGRES_USER\";" || true
fi
# Enable TimescaleDB in timeseries (optional)
psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "timeseries" -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" || true

