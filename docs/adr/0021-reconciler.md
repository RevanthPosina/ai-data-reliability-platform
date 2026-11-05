# ADR-0021: Reconciler (consistent cut, canonical digests)
- Status: accepted (G10a); written before the first run, so nothing here is a result. Items marked **unverified** are not yet observed.
## Decision
Reconciliation compares each sink with Postgres, or with a sink upstream of it, at a **consistent cut**, and passes only when every comparison is clean. It is built in three parts so it can be tested without Docker: `src/adrp/recon/` holds pure logic (canonicalization, digests, comparison, checkpoint parsing, offsets, classification) and is unit-tested; `scripts/reconcile.py` holds the adapters that talk to Docker, Postgres, Kafka and Spark and also serves as a CLI for diagnosing a live stack; `scripts/g10a_check.py` injects faults and scores them (ADR-0020, ADR-0023). The reconciler never injects anything. In Milestone A it is a library plus CLI used at check time; a long-running service is left for G13 (PLAN's "Python service" is deferred).
## The cut
The caller pauses the generator and `reconcile` verifies it: no generator container is running, `gen.state.next_txn` is unchanged across the whole reconcile, and the write counters of the seven tables in `pg_stat_user_tables` are unchanged (the heartbeat table is excluded). Then:
1. `cut = pg_current_wal_lsn()`.
2. Wait for the slot `dbz_shop` to reach `confirmed_flush_lsn >= cut`. This moves with Connect's offset flush and heartbeats (design S13), so the wait is long by design.
3. Record the end offsets of every `cdc.public.*` topic.
4. Wait until the **committed offsets of silver and of bronze** reach those end offsets. The reconciler reads Spark's checkpoint directly: the highest numeric `commits/N`, then the last line of `offsets/N`. Spark's offset is the next offset to read, the same meaning as a high watermark. Files ending `.crc` and non-numeric names are ignored.
Why not the existing waiters. `g3.Drain` compares each topic's maximum LSN with the event log's `max_lsn`. A quarantined last event never reaches `max_lsn` (only good rows are carried), so after F6b it would not finish until a later event arrived. `g8_check.SafeDrain` catches `SystemExit` from `g2.read_range`, advances offsets and can leave the LSN stale, so it can declare catch-up early. Waiting on committed offsets is what the PLAN specifies and no code read them before this work.
## What is compared
Per-topic results are keyed `(topic, partition, offset)`; per-table results are keyed `(table, id)`.
| Pair | Method |
|---|---|
| Postgres vs silver | row count and the order-independent sum of `xxhash64(canonical row)` mod 2^64, per table and per pk-bucket |
| Kafka vs bronze | `compare_offsets` (ADR-0022) |
| Silver vs bronze | `rebuild.rebuild` of the table from bronze compared with the current table, both through `rebuild.checksum`: equal count and checksum. This is an equality check on this table, not a claim that anything is reproducible. |
| Qdrant vs silver, Qdrant vs Postgres | g6's `wait_consistent` and `wait_matches_postgres`: 0 stale, orphaned, duplicate, wrong-model and missing |
| Silver vs ground truth | g3's `compare_to_ground_truth`, for F4 through F6a only |
| Quarantine vs silver | for every quarantined key, silver's LSN is greater than the quarantined LSN |
| TOAST | count of `__debezium_unavailable_value` in silver string columns is 0 |
## Canonical rows
- Postgres rows come from `row_to_json` parsed with `Decimal` for numbers. `g6.pg_json` parses numerics as floats and is not used for checksums. Silver rows come from `g3.silver_rows`. `to_json` omits NULL fields, so a missing field and NULL are the same value.
- Normalization by Postgres type: timestamps with time zone to UTC at microsecond ISO form, dates to ISO, `numeric` to a normalized `Decimal`, integers, text, `character` and boolean as they are. **An unknown type raises**, so a new column type cannot slip through unhashed.
- The digest is `xxhash` on the host. Spark's `xxhash64` is typed and cannot be compared with a Postgres-side value, so `rebuild.checksum` is used only for the silver-vs-bronze pair.
- The pk-bucket is `xxh64(str(id)) mod 16`, computed the same way on both sides. It is deliberately not Iceberg's bucket transform; its job is to localize a mismatch.
- Columns come from `information_schema.columns` for the current Postgres table. Silver's meta columns are excluded. A silver-only column that is non-NULL anywhere is a mismatch; if all NULL it is only reported.
## Logical delete rules
- Checksums cover live silver rows only (`NOT is_deleted`).
- A silver tombstone (logical delete) must have no live Postgres row and no non-NULL non-key value.
- A live silver row with no Postgres row is `resurrected` if the key is in the generator's delete history and `extra_live` otherwise.
- A tombstone is **not** required for every Postgres delete: rows inserted and removed before the F4 snapshot never reached CDC.
- A positive control proves the reconciler can fail. During F6b, before the parked refund is repaired, the reconciler must report it as missing.
## Identifiers
Manifests hold counts, bucket numbers and hex checksums only. Diagnostic key lists go to `.run/g10a-diff-<label>.json` (gitignored, keys only, no payloads; inventory row 33). Hex is used so the 13-digit-number scans in `fids.ID13` and `storage.manifests_scan`, which g8 runs over all committed manifests, cannot match a checksum.
## Continuous mode (per key)
The PLAN describes comparing silver with the ground-truth log "replayed to silver's applied LSN". That is not valid per table: every topic has three partitions, so silver's applied LSN is not a clean cut at row level. Continuous mode therefore compares **per key** and never against live Postgres: for each silver row at LSN L, a bronze probe maps `(source_table, pk, lsn)` to `txid`, the run log maps `txid` to a transaction number, and the row must equal a version of that key within that transaction according to the generator's ops. This mapping is a reconciliation key, not row-level lineage.
- It runs once, during F3 while load is on.
- It catches wrong values. It does not catch lag or loss: a row that never arrives is not judged, and a stale-but-valid version still matches an older transaction.
- Rows from the snapshot or from out-of-band transactions are counted as `unjudged`. Only finished generator chunks are judged, because the run log is written at chunk end. Run directories are filtered by a start stamp because `make` clears only `runs/data/eventlog`, so xids from earlier stacks are still on disk.
- Pass requires judged > 0 and mismatches == 0. It is an open decision (D2) whether to keep it; if it is cut, `continuous.py`, one probe command and one check are removed and this ADR is amended.
## Limits
- **Zero mismatches means zero mismatches under the 6 tested faults at this cut.** Undetected faults may exist; zero detections is not zero faults.
- The reconciler compares silver with Postgres at a quiet moment. It does not see a transient wrong state between cuts; continuous mode covers wrong values only, as above.
- The retried-snapshot staleness (ADR-0020, unverified) is a case the reconciler could catch in the gate after F4, but G10a stops writers during the retry, so it is not exercised.
- Rebuilding silver from bronze is meaningful only while bronze holds the LSN-highest version of every key; on compacting topics see ADR-0022 for rebuilding from the topic.
- Hashing uses a non-cryptographic hash, so equal digests are strong evidence, not proof, of equal content; the bucket digests and the key-level diagnostics narrow any mismatch.
