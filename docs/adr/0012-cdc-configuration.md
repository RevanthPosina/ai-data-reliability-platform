# ADR-0012: Debezium Postgres connector configuration
- Status: accepted (G2)
## Decisions
| Setting | Value | Why |
|---|---|---|
| `plugin.name` | `pgoutput` | Built into Postgres 10+; no extension to install. |
| Publication | `dbz_pub FOR TABLES IN SCHEMA public`, `publication.autocreate.mode=disabled` | Created by the schema script, so the `debezium` role needs no table ownership. Tables added to `public` later are published automatically; `table.include.list` decides what Debezium emits. |
| `table.include.list` | the 7 domain tables | `dbz_heartbeat` is in the publication but not emitted. |
| `snapshot.mode` | `initial` | Snapshot (`op='r'`) then stream. Snapshot rows carry the snapshot LSN in `source.lsn`, so silver's LSN-conditional MERGE treats them as ordinary upserts (G3). |
| `REPLICA IDENTITY` | `FULL` on `customers`, `support_tickets`, `help_articles` | Without it an update that leaves a TOASTed column untouched arrives with `__debezium_unavailable_value`, and delete/update before-images carry only the PK. Cost: larger WAL records on those tables. |
| `tombstones.on.delete` | `true` | A delete emits a `d` event, then a null-value record with the same key (needed for Kafka compaction in G8). Spark filters tombstones out before parsing (G3). |
| Heartbeat | every 10 s, `heartbeat.action.query` upserts `dbz_heartbeat` | Keeps `confirmed_flush_lsn` advancing when the tracked tables are idle but the database is not, so the slot does not pin WAL. |
| Topics | `cdc.public.<table>`, 3 partitions, keyed by PK, `message.timestamp.type=LogAppendTime` | Per-key order is preserved within a partition. LogAppendTime gives a broker-side timestamp for the freshness measurement (design S1). |
| Serialization | Avro, Confluent `AvroConverter`, BACKWARD per subject | See ADR-0011. |
## Monitoring
`adrp.obs.exporter` serves `adrp_replication_slot_lag_bytes`, `..._retained_wal_bytes`, `..._active`, `adrp_connector_up` and `adrp_connector_tasks` on `:9108/metrics`. A Prometheus scrape and alert rules come with G13.
## Data contracts
`contracts/<table>.yaml` lists the columns each topic may carry (and which are PII). `contracts/baseline/*.avsc` are the registered schemas at the time of export. `scripts/contracts.py check` rewrites the registered schema to match the contract and asks the registry whether that is BACKWARD compatible; an incompatible contract fails CI. Semantic rules (ranges, referential integrity) are G5.
## Known limits
- The contract check judges compatibility of the *declared contract* against the registered schema. It does not stop someone from altering a Postgres column: that is caught by the registry rejecting the producer's new schema (connector task fails, `adrp_connector_up` drops to 0).
- Debezium's default `decimal.handling.mode=precise` is used, so numerics are Avro decimals.
