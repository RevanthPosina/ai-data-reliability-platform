# ADR-0014: Schema evolution and quarantine in silver
- Status: accepted (G3)
## Decision
Each micro-batch is split by `schema_id`. For every writer schema the batch carries, `adrp.spark.evolution.classify` compares its columns with the silver table's current columns:
| Change | Action |
|---|---|
| new column | `ALTER TABLE ADD COLUMN` (nullable), then merge; no restart |
| column missing from the writer schema | merge; the column is NULL for that record |
| widening (int to bigint, float to double, decimal precision up, same scale) | `ALTER COLUMN TYPE`, then merge |
| any other type change, including narrowing | quarantine, reason `SCHEMA_INCOMPATIBLE: column <c> changed type <a> -> <b>` |
| columns removed and added together (a rename looks exactly like this) | quarantine, reason `SCHEMA_INCOMPATIBLE: possible rename ...` |
| key column removed | quarantine |
Quarantined rows go to `lake.silver.quarantine_<table>` with the full bronze-shaped record (`value_bytes` included), the reason and a timestamp, and the same `(topic, partition, offset)` can only be parked once (the stream and a replay are both safe). They contain PII (inventory row 10).
Decoding resolves each record with **its own writer schema** (looked up by schema id in the registry), then projects onto the silver table's current schema.
## Why quarantine a narrowing that would fit
A column narrowed at the source (decimal(12,2) to decimal(8,2)) would fit silver's wider column without loss. It is parked anyway: a producer-side type narrowing is a contract change that a person should look at, and the registry may well have accepted it. Same for a rename, which Avro BACKWARD compatibility accepts (drop plus add of a nullable field), so the registry will not stop it.
## Consequences
- Rows are never silently dropped or coerced. Recovery is a manual replay from quarantine after the table is fixed.
- The registry gate (G2 contracts) and this gate are independent; both exist because each catches changes the other allows.
