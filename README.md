# AI Data Reliability Platform

CDC → Iceberg → vector index + versioned training datasets.

Postgres changes flow through Debezium and Redpanda into Apache Iceberg (bronze/silver), then into a Qdrant vector index. The project's goal is to **measure** that the copies are fresh, idempotent, deletable and traceable.

> Status: **Milestone A, G1 (domain model and data generator) done.** No results yet. Every number below is a placeholder (`__`) until a run recorded in `runs/manifests/` produces it.

## Results (placeholders until measured)
| ID | Question | Result | Run manifest |
|---|---|---|---|
| R1 | Freshness p99 to the vector index | __ s at __ changes/s (achieved) | __ |
| R2 | Deletion: logical / physical absence lag | __ s / __ min | __ |
| R4 | Idempotency after replay and crash | __ dupes / __ losses / __ resurrections over __ runs | __ |

## Quick start (core profile, 16 GB laptop)
```bash
make up PROFILE=core   # infra -> bootstrap (bucket, catalog, connector) -> Spark streams, embedder, Dagster
make demo              # one insert -> topic -> bronze -> silver -> Qdrant, pass/fail per hop
make down
```
Requirements: Docker Desktop with ≥ 8 GB for the VM (10–12 GB recommended), ~15 GB free disk for images.

## Docs
- Design, SLOs, data inventory, fault matrix, version matrix: [`docs/design.md`](docs/design.md)
- Source schema, PII columns, data generator: [`docs/schema.md`](docs/schema.md)
- Decisions: [`docs/adr/`](docs/adr/)
- What was installed on the dev machine: [`docs/install-log.md`](docs/install-log.md)
- Plan: [`delegated-orbiting-garden.md`](delegated-orbiting-garden.md)

## What this project does not claim
No production-scale numbers, no "exactly-once" (only "idempotent under the tested faults"), no compliance certification, no "deleted" without saying logical or physical, and no row-level lineage via OpenLineage.
