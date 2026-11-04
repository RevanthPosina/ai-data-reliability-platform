# ADR-0013: Silver merge, per-key LSN ordering and soft tombstones
- Status: accepted (G3)
## Decision
Silver holds the current state of each row. A micro-batch is applied as:
1. dedupe within the batch to the highest `(lsn, kafka_offset)` per key;
2. `MERGE ... WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`;
3. a delete becomes a **soft tombstone** `(pk, lsn, is_deleted = true, every non-key column NULL)`; it is never a hard delete.
Consumers read `WHERE NOT is_deleted`. Tombstones carry no PII.
No watermark is stored in table properties or a side table: Iceberg cannot commit two tables atomically, so a separate watermark could disagree with the data after a crash. The LSN already in each row is the watermark.
## Why per-key LSN ordering is sound
Two transactions that change the same row cannot overlap: the later one blocks on the row lock until the earlier one commits, so its WAL position is later. Debezium emits each change with its own LSN. Checked on real output during G3: 0 duplicate `(key, lsn)` pairs across 4,300+ change events, while a transaction's events carry different LSNs. LSN is therefore a total order per key.
## Why soft tombstones
With a hard delete, a replayed or late older insert/update of the same key would find no row and re-create it (resurrection). The tombstone keeps the key's last LSN, so anything older loses the `s.lsn > t.lsn` comparison.
## What this buys, and the proof
The result is independent of arrival order, duplication, replay and batch boundaries: for every key the highest-LSN event wins. This is tested (a) on a pure-Python model with Hypothesis, 1,000 random deliveries (shuffles, duplicates, replays, crash re-application, delete then re-insert); (b) on the production Spark MERGE with 50 random deliveries; (c) with a SIGKILL inside a batch at three points against the ground truth (`make g3-check`). Both property tests were mutation-checked: removing the LSN guard makes them fail.
## Limits
- A tombstone must outlive the longest possible replay of older events for that key (topic retention plus the reader replay window). Purging earlier would allow resurrection. Purge policy: [ADR-0017](0017-forget-policy.md) (kept until broker physical absence is verified, then hard-deleted).
- Two changes to one key with the same LSN would need the `kafka_offset` tie-break across batches, which silver does not store. Not observed (see above); the dedupe step does use the offset within a batch.
- Quarantined events are not applied. Silver can lag Postgres for those rows until a later compatible event for the same key arrives.
