# ADR-0007: Vector store: Qdrant vs pgvector
- Status: accepted (G0)
## Context
The vector index is a separate copy of customer-derived text. Deletion propagation into it is part of what we prove.
## Decision
**Qdrant v1.19.2.** Point id `uuid5(ns, entity_id|version|chunk_no)`; payload index on `entity_id`; `wait=true` on writes.
## Why not pgvector
pgvector in the source Postgres would make the index transactional with the source, which removes the propagation problem we want to measure. It would also put embedding load on the OLTP database. Qdrant is a realistic separate sink with its own deletion semantics (marked deletes until the vacuum optimizer, WAL, snapshots).
## Consequences
Physical-deletion bounds for Qdrant must be measured and reported (inventory rows 14–15).
