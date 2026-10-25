#!/bin/sh
# Runs once on an empty data dir (docker-entrypoint-initdb.d). Creates the source DB, the
# Debezium role, the Lakekeeper catalog DB, and the G0 skeleton table. G1 adds the full domain.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '${DEBEZIUM_PASSWORD}';
CREATE ROLE lakekeeper WITH LOGIN PASSWORD '${LAKEKEEPER_DB_PASSWORD}';
CREATE DATABASE lakekeeper OWNER lakekeeper;
CREATE DATABASE shop;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname shop <<'SQL'
CREATE TABLE help_articles (
    id          bigserial PRIMARY KEY,
    title       text NOT NULL,
    body        text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- Text/PII tables: full before-images and no TOAST placeholders (PLAN G2).
ALTER TABLE help_articles REPLICA IDENTITY FULL;

-- Heartbeat target so the slot advances when tracked tables are idle (PLAN G2).
CREATE TABLE dbz_heartbeat (id int PRIMARY KEY, ts timestamptz NOT NULL);

-- Covers tables G1 adds to the schema later; Debezium's table.include.list picks what is emitted.
-- Seed rows so the initial snapshot (op='r') is exercised from G0.
INSERT INTO help_articles (title, body) VALUES
  ('Resetting your password', 'Open Settings, choose Security, then Reset password. A link is emailed to you.'),
  ('Refund timelines', 'Card refunds post within 5 to 10 business days depending on the issuing bank.');

CREATE PUBLICATION dbz_pub FOR TABLES IN SCHEMA public;

GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
GRANT INSERT, UPDATE ON dbz_heartbeat TO debezium;
SQL
