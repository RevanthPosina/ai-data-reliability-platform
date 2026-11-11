# AI Data Reliability Platform

CDC → Iceberg → vector index + versioned training datasets.

Postgres changes flow through Debezium and Redpanda into Apache Iceberg (bronze/silver), then into a Qdrant vector index. The project's goal is to **measure** that the copies are fresh, idempotent, deletable and traceable.

> Status: **Milestone A, G6 (vector index consumer), G8 (deletion propagation) and G10a (fault subset and reconciliation) done. G11a is partial: R2 and R4 are done (from existing manifests), R1 is deferred (ADR-0024).** R2, R3 and R4 have measured results; R1 is not measured yet, so its cell carries no number.

## Results (R2, R3 and R4 measured; R1 deferred)
| ID | Question | Result | Run manifest |
|---|---|---|---|
| R1 | Freshness p99 to the vector index | not measured yet (deferred, ADR-0024) | none (deferred) |
| R2 | Deletion: logical / physical absence lag | Logical lag (request to every sink verified absent by query), median over requests: run 1 503 s [min 156, max 503]; run 2 306 s [min 118, max 307]; pooled 307 s [min 118, max 503] over 12 requests. Physical lag (request to the maintenance window's verified-absent time, all sinks): run 1 43.9 min [min 31.6, max 43.9]; run 2 34.4 min [min 26.2, max 34.4]; pooled 34.4 min [min 26.2, max 43.9] over 12 requests. Physical lag is measured in this schedule (set by when the g8 check's maintenance window ran), not a minimum latency. 2 runs (6 requests each) on one machine, compacting topics (the g8 setting); each run has 5 ad-hoc requests submitted within 1.6 s of each other (not independent samples) and 1 generator-owned subject (run 1 156 s logical, 31.6 min physical; run 2 118 s logical, 26.2 min physical). pg_wal, qdrant_wal and spark_spill are bounded, not removed. No claim beyond these runs. | [run 1](runs/manifests/20261105T200430Z-g8-check.json), [run 2](runs/manifests/20261111T013825Z-g8-check.json) |
| R3 | Fault subset (6 faults, scripted) followed by a consistent-cut reconciliation | 0 silent corruption detected by the reconciler across 7 fault rows (F6a and F6b are one fault); 1 run per topic setting on one machine: default topics 134/134 checks, 3 caught and 4 self-healed in this run; compacting topics 135/135 checks, 2 caught and 5 self-healed in this run (F5 was caught in the first, self-healed in the second). Zero mismatches under the 6 tested faults at this cut; undetected faults may exist. | [default](runs/manifests/20261106T013720Z-g10a-check.json), [compacting](runs/manifests/20261110T233326Z-g10a-check.json) |
| R4 | Idempotency after replay and crash | Silver, idempotent under the tested faults: 0 duplicates, 0 losses and 0 resurrections in 3 runs (2 full fault runs, one per topic setting (default and compacting), 14 fault executions; 1 g3 kill run, 3 SIGKILLs). By group: replay (F1, F2) 0 / 0 / 0 in 4 executions (self-healed in this run 4); crash and restart (F3, F4, F5) 0 / 0 / 0 in 6 executions (caught 3, self-healed in this run 3); schema (F6a, F6b) 0 / 0 / 0 in 4 executions (caught 2, self-healed in this run 2); g3 kill 0 / 0 / 0 in 3 SIGKILLs (before_merge, mid_merge, after_merge; also 0 differing, 0 extra live rows). Counts are duplicates / losses / resurrections, from the reconciler gate after recovery (g10a) and the ground-truth compare (g3). Silver only: bronze holds duplicate rows by design (F1 replay ledgers 667 and 435). F4's retry ran with no concurrent writers. F5 is a graceful restart of a dev-container broker. One machine, code 58340e0, 1 run per topic setting; a zero here is a count in these runs, not a rate, and undetected faults may exist. Open finding (ADR-0020): Connect stalled silently once after a broker restart in an earlier non-record run; cause unverified, decision open. | [default](runs/manifests/20261106T013720Z-g10a-check.json), [compacting](runs/manifests/20261110T233326Z-g10a-check.json), [g3 kill](runs/manifests/20261110T234612Z-g3-check.json) |

R2 and R4 are regenerated from the committed manifests by `make g11a-results` ([`notebooks/g11a_results.py`](notebooks/g11a_results.py), no Docker, no new run), which also checks these cells against the rendered strings: [`docs/results/g11a-results.md`](docs/results/g11a-results.md) and [ADR-0024](docs/adr/0024-g11a-r2-r4-from-existing-runs.md), which records the scope, the denominators and the host-sleep check of the two g8 run windows. R1 is deferred and will get its own ADR before any run.

G10a code commit `58340e0` (clean tree) for both runs. Regressions on that code: g3-check 22/22 ([manifest](runs/manifests/20261110T234612Z-g3-check.json)), g6-check 25/25 ([manifest](runs/manifests/20261110T235535Z-g6-check.json)), g8-check 94/94 ([manifest](runs/manifests/20261111T013825Z-g8-check.json), at `e0fe4be`, same code as `58340e0` plus docs and manifests). Per-fault table: [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md); measured section and attempts in [ADR-0020](docs/adr/0020-fault-injection.md).

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
- G11a results, reduced to R2 and R4 (generated from the manifests by a DuckDB notebook; R1 deferred): [`docs/results/g11a-results.md`](docs/results/g11a-results.md), [ADR-0024](docs/adr/0024-g11a-r2-r4-from-existing-runs.md)
- G10a fault table (fault, detection, recovery, residual effect; generated from the manifests): [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md)
- What was installed on the dev machine: [`docs/install-log.md`](docs/install-log.md)
- Plan: [`delegated-orbiting-garden.md`](delegated-orbiting-garden.md)

## What this project does not claim
No production-scale numbers, no "exactly-once" (only "idempotent under the tested faults"), no compliance certification, no "deleted" without saying logical or physical, and no row-level lineage via OpenLineage.
