#!/bin/bash
set -e

# Creates the airflow metadata database alongside the main bungewatch DB.
# This script runs once on first postgres container start via docker-entrypoint-initdb.d.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
EOSQL

echo "airflow database ready"
