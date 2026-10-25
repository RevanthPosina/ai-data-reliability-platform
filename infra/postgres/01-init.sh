#!/bin/sh
# Runs once on an empty data dir (docker-entrypoint-initdb.d). Creates roles, the Lakekeeper
# catalog DB and the `shop` source DB, then applies schema/shop.sql (docs/schema.md).
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '${DEBEZIUM_PASSWORD}';
CREATE ROLE lakekeeper WITH LOGIN PASSWORD '${LAKEKEEPER_DB_PASSWORD}';
CREATE DATABASE lakekeeper OWNER lakekeeper;
CREATE DATABASE shop;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname shop -f /docker-entrypoint-initdb.d/schema/shop.sql
