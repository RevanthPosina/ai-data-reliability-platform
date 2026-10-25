# ADR-0002: Spark Structured Streaming vs the Kafka Connect Iceberg sink
- Status: accepted (G0). Verified 2026-10-25 against Apache Iceberg 1.12.0.
## Context
Silver must apply upserts and deletes idempotently under replay, using an LSN guard and soft tombstones.
## Evidence
The Apache Iceberg Kafka Connect sink at 1.12.0 lists no upsert/CDC mode in its features or configuration (`docs/docs/kafka-connect.md`). Its writer package contains only `PartitionedAppendWriter` / `UnpartitionedWriter`, so it is **append-only**. The upsert/"row mutations" mode (equality deletes) existed in the earlier Tabular fork (`tabular-io/iceberg-kafka-connect`), not in the Apache connector. The Debezium SMT exists but only reshapes records.
## Decision
- Bronze: Spark query 1 with the native Iceberg streaming sink (epoch-idempotent).
- Silver: Spark query 2 with foreachBatch and an LSN-conditional `MERGE`.
- The Kafka Connect sink would have been acceptable for bronze only. One engine for both keeps decoding logic in one place.
## Consequences
- We own the MERGE logic, so it gets a property test (G3).
- Even with upsert support, an equality-delete upsert is not conditional on LSN: a replayed older event would overwrite a newer row. That is a second reason to use our own MERGE.
