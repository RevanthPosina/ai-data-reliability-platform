# ADR-0001: Log-based CDC (Debezium) vs query-based CDC
- Status: accepted (G0)
## Context
We need every insert, update and **delete**, in commit order per key, with before-images for PII tables, and without load on the OLTP database.
## Options
1. Query-based polling (`updated_at > :watermark`): misses hard deletes, misses intermediate updates between polls, depends on application-maintained timestamps, and loads the database.
2. Log-based CDC via logical replication (Debezium): captures every committed change with its LSN and txid. Costs: a replication slot (WAL retention risk), `REPLICA IDENTITY FULL` on TOAST/PII tables.
## Decision
Debezium 3.7.0.Final Postgres connector (`pgoutput`), with a heartbeat (`heartbeat.action.query`) to keep the slot advancing when tracked tables are idle.
## Consequences
- Slot lag becomes a monitored SLO (S13). An abandoned slot can fill the disk.
- Per-key ordering by LSN is available downstream (ADR on LSN ordering in G3).
- SCD2 can be built from the full change log (G4), which polling cannot reproduce.
