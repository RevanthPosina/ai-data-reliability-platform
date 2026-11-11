# AI Data Reliability Platform

CDC → Iceberg → vector index + versioned training datasets.

Postgres changes flow through Debezium and Redpanda into Apache Iceberg (bronze/silver), then into a Qdrant vector index. The project's goal is to **measure** that the copies are fresh, idempotent, deletable and traceable.

> Status: **Milestone A, G6 (vector index consumer), G8 (deletion propagation) and G10a (fault subset and reconciliation) done.** Only R3 has a measured result so far; R1, R2 and R4 are placeholders (`__`) until a run recorded in `runs/manifests/` produces them (G11a).

## Results (R3 measured; the rest are placeholders until measured)
| ID | Question | Result | Run manifest |
|---|---|---|---|
| R1 | Freshness p99 to the vector index | __ s at __ changes/s (achieved) | __ |
| R2 | Deletion: logical / physical absence lag | __ s / __ min | __ |
| R3 | Fault subset (6 faults, scripted) followed by a consistent-cut reconciliation | 0 silent corruption detected by the reconciler across 7 fault rows (F6a and F6b are one fault); 1 run per topic setting on one machine: default topics 134/134 checks, 3 caught and 4 self-healed in this run; compacting topics 135/135 checks, 2 caught and 5 self-healed in this run (F5 was caught in the first, self-healed in the second). Zero mismatches under the 6 tested faults at this cut; undetected faults may exist. | [default](runs/manifests/20261106T013720Z-g10a-check.json), [compacting](runs/manifests/20261110T233326Z-g10a-check.json) |
| R4 | Idempotency after replay and crash | __ dupes / __ losses / __ resurrections over __ runs | __ |

G10a code commit `58340e0` (clean tree) for both runs. Regressions on that code: g3-check 22/22 ([manifest](runs/manifests/20261110T234612Z-g3-check.json)), g6-check 25/25 ([manifest](runs/manifests/20261110T235535Z-g6-check.json)), g8-check regression: pending. Per-fault table: [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md); measured section and attempts in [ADR-0020](docs/adr/0020-fault-injection.md).

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
- Data contracts (one YAML per CDC topic) and their CI check: [`contracts/`](contracts/), [ADR-0012](docs/adr/0012-cdc-configuration.md)
- Decisions: [`docs/adr/`](docs/adr/)
- G10a fault table (fault, detection, recovery, residual effect; generated from the manifests): [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md)
- What was installed on the dev machine: [`docs/install-log.md`](docs/install-log.md)
- Plan: [`delegated-orbiting-garden.md`](delegated-orbiting-garden.md)

## What this project does not claim
No production-scale numbers, no "exactly-once" (only "idempotent under the tested faults"), no compliance certification, no "deleted" without saying logical or physical, and no row-level lineage via OpenLineage.
