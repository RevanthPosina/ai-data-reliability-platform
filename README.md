# AI Data Reliability Platform

CDC → Iceberg → vector index + versioned training datasets.

Postgres changes flow through Debezium and Redpanda into Apache Iceberg (bronze/silver), then into a Qdrant vector index. The project's goal is to **measure** that the copies are fresh, idempotent, deletable and traceable.

> Status: **Milestone A, G6 (vector index consumer), G8 (deletion propagation) and G10a (fault subset and reconciliation) done. G11a is partial: R2 and R4 are done (from existing manifests), R1 is deferred (ADR-0024).** R2, R3 and R4 have measured results; R1 is not measured yet, so its cell carries no number. **Milestone B has started: G4 (gold layer: SCD2 dimensions, facts and a double-entry ledger, dbt on Trino) is done, g4-check 37/37 at `b3202eb`. Gold is not covered by the forget workflow until G8b.**

## Results (R2, R3 and R4 measured; R1 deferred)
| ID | Question | Result | Run manifest |
|---|---|---|---|
| R1 | Freshness p99 to the vector index | not measured yet (deferred, ADR-0024) | none (deferred) |
| R2 | Deletion: logical / physical absence lag | Logical lag (request to every sink verified absent by query), median over requests: run 1 503 s [min 156, max 503]; run 2 306 s [min 118, max 307]; pooled 307 s [min 118, max 503] over 12 requests. Physical lag (request to the maintenance window's verified-absent time, all sinks): run 1 43.9 min [min 31.6, max 43.9]; run 2 34.4 min [min 26.2, max 34.4]; pooled 34.4 min [min 26.2, max 43.9] over 12 requests. Physical lag is measured in this schedule (set by when the g8 check's maintenance window ran), not a minimum latency. 2 runs (6 requests each) on one machine, compacting topics (the g8 setting); each run has 5 ad-hoc requests submitted within 1.6 s of each other (not independent samples) and 1 generator-owned subject (run 1 156 s logical, 31.6 min physical; run 2 118 s logical, 26.2 min physical). pg_wal, qdrant_wal and spark_spill are bounded, not removed. No claim beyond these runs. | [run 1](runs/manifests/20261105T200430Z-g8-check.json), [run 2](runs/manifests/20261111T013825Z-g8-check.json) |
| R3 | Fault subset (6 faults, scripted) followed by a consistent-cut reconciliation | 0 silent corruption detected by the reconciler across 7 fault rows (F6a and F6b are one fault); 1 run per topic setting on one machine: default topics 134/134 checks, 3 caught and 4 self-healed in this run; compacting topics 135/135 checks, 2 caught and 5 self-healed in this run (F5 was caught in the first, self-healed in the second). Zero mismatches under the 6 tested faults at this cut; undetected faults may exist. | [default](runs/manifests/20261106T013720Z-g10a-check.json), [compacting](runs/manifests/20261110T233326Z-g10a-check.json) |
| R4 | Idempotency after replay and crash | Silver, idempotent under the tested faults: 0 duplicates, 0 losses and 0 resurrections in 3 runs (2 full fault runs, one per topic setting (default and compacting), 14 fault executions; 1 g3 kill run, 3 SIGKILLs). By group: replay (F1, F2) 0 / 0 / 0 in 4 executions (self-healed in this run 4); crash and restart (F3, F4, F5) 0 / 0 / 0 in 6 executions (caught 3, self-healed in this run 3); schema (F6a, F6b) 0 / 0 / 0 in 4 executions (caught 2, self-healed in this run 2); g3 kill 0 / 0 / 0 in 3 SIGKILLs (before_merge, mid_merge, after_merge; also 0 differing, 0 extra live rows). Counts are duplicates / losses / resurrections, from the reconciler gate after recovery (g10a) and the ground-truth compare (g3). Silver only: bronze holds duplicate rows by design (F1 replay ledgers 667 and 435). F4's retry ran with no concurrent writers. F5 is a graceful restart of a dev-container broker. One machine, code 58340e0, 1 run per topic setting; a zero here is a count in these runs, not a rate, and undetected faults may exist. Open finding (ADR-0020): Connect stalled silently once after a broker restart in an earlier non-record run; cause unverified, decision open. | [default](runs/manifests/20261106T013720Z-g10a-check.json), [compacting](runs/manifests/20261110T233326Z-g10a-check.json), [g3 kill](runs/manifests/20261110T234612Z-g3-check.json) |

R2 and R4 are regenerated from the committed manifests by `make g11a-results` ([`notebooks/g11a_results.py`](notebooks/g11a_results.py), no Docker, no new run), which also checks these cells against the rendered strings: [`docs/results/g11a-results.md`](docs/results/g11a-results.md) and [ADR-0024](docs/adr/0024-g11a-r2-r4-from-existing-runs.md), which records the scope, the denominators and the host-sleep check of the two g8 run windows. R1 is deferred and will get its own ADR before any run.

G10a code commit `58340e0` (clean tree) for both runs. Regressions on that code: g3-check 22/22 ([manifest](runs/manifests/20261110T234612Z-g3-check.json)), g6-check 25/25 ([manifest](runs/manifests/20261110T235535Z-g6-check.json)), g8-check 94/94 ([manifest](runs/manifests/20261111T013825Z-g8-check.json), at `e0fe4be`, same code as `58340e0` plus docs and manifests). Per-fault table: [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md); measured section and attempts in [ADR-0020](docs/adr/0020-fault-injection.md).

## Gold layer (G4, Milestone B, measured in one run)
The dbt project [`dbt_gold/`](dbt_gold/) builds SCD2 `dim_customer` and `dim_merchant` from the bronze change log, and `fct_transactions`, the append-only double-entry `fct_ledger_entries`, `dim_account` and `dim_date` from silver, on Trino over the Iceberg lake. G4 defines no R-number; S4 (freshness to gold) and R6 are not measured. Models, keys, contracts and the runbook: [`docs/gold.md`](docs/gold.md); decisions and measured sections: [ADR-0025](docs/adr/0025-gold-model-design.md) to [ADR-0030](docs/adr/0030-g4-verification-layering-trino-sizing-sleep-guard.md).

g4-check **37/37** at code commit `b3202eb` (clean tree), seed 42, one run on one machine (Apple M1 Pro, Docker Desktop VM with 4 CPUs and 8 GB), no host sleep detected (checked, drift 0.0 s): [manifest](runs/manifests/20261111T072440Z-g4-check.json). The check itself took 156.5 s of phase time. Two earlier attempts are not of record and are described in [ADR-0030](docs/adr/0030-g4-verification-layering-trino-sizing-sleep-guard.md): one aborted at P0 before any gold check, one passed 36 of 37 (its expectation for one carry-forward column was corrected afterwards, disclosed there).

| Check | Result in this run |
|---|---|
| Ledger nets to zero | 0 of 761 journals and 0 of 507 payment-plus-refund transactions failed to net at build 1; 0 of 1,129 journals and 0 of 827 transactions at build 2 |
| Balances at a quiescent cut | all 243 accounts (build 1) and all 262 accounts (build 2) matched Postgres `accounts.balance`; 0 mismatches |
| SCD2 against ground truth | the generator's customers matched the generator ops in LSN order, with `valid_from` equal to the commit time and `txid` equal to `xid32` (build 2: 232 customers, 283 versions, 0 differing); the scripted A, B and M sequence matched the oracle, including several changes between two builds |
| dbt snapshot loses intermediate changes | scripted A: 4 versions, 2 captured, 2 lost (asserted); 41 of 283 generator customer versions were not captured at 2 dbt runs in this run (a property of this schedule, not a rate) |
| One-day backfill (2026-01-01) | the checksums of `fct_transactions` (824 transactions) and `fct_ledger_entries` (2,246 entries) were unchanged, as were the rows of the two other days; the Iceberg snapshots show 2,246 position deletes and 2,246 added records, so the check is not vacuous |
| Rerun on unchanged inputs | build 3 left the checksums of all 6 gold tables and the ledger `entry_id` set unchanged in this run |
| Incremental versus from scratch | equal checksums for all six tables; the one re-keyed transaction differs only in the carry-forward columns, as designed |
| Ledger append-only | 1,522 entries from build 1 were unchanged after build 2 and after the backfill; 736 entries were added since |
| Artifact hygiene | 0 of 113 files in `dbt_gold/target` and `dbt_gold/logs` held the canary or a scripted id |

The fixture layer, `make dbt-fixture` (throwaway Trino 483, no shared stack), passed 70 of 70 FX checks and 5 `dbt_fixture` nodes (recorded in ADR-0030; no manifest). It alone covers duplicate and out-of-order bronze rows, late arrival within and beyond the 60-minute lookback with recovery, and the contract-break test. A live smoke of `make gold-build` on the existing stack (ADR-0030, not of record) built every model.

**No regression rerun.** The g3, g6, g8 and g10a results above stay at their own commits. G4 changed no pipeline code: `git diff --stat 9ef8a2c b3202eb -- src/adrp/spark src/adrp/forget src/adrp/embed src/adrp/gen src/adrp/recon src/adrp_orch docker infra/connect infra/postgres infra/redpanda infra/seaweedfs scripts/bootstrap.py scripts/g2_check.py scripts/g3_check.py scripts/g6_check.py scripts/g8_check.py scripts/g10a_check.py scripts/reconcile.py scripts/manifest.py scripts/generate.py` prints nothing, and in `docker-compose.yml` only the `trino` service (profile `transform`, started by g4-check and by no other check) changed. This is a statement about the diff, not a new measurement of those checks.

**`make ci` now needs Docker locally.** `make dbt-fixture` starts its own throwaway Trino container on port 28080 (about 2 GB of VM memory for a few minutes) and CI runs the same target; it does not touch the compose stack. Do not run `make ci` and a g-check at the same time ([ADR-0030](docs/adr/0030-g4-verification-layering-trino-sizing-sleep-guard.md)).

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
- Gold layer: models, keys, contracts and runbook: [`docs/gold.md`](docs/gold.md)
- G10a fault table (fault, detection, recovery, residual effect; generated from the manifests): [`docs/results/g10a-fault-table.md`](docs/results/g10a-fault-table.md)
- What was installed on the dev machine: [`docs/install-log.md`](docs/install-log.md)
- Plan: [`delegated-orbiting-garden.md`](delegated-orbiting-garden.md)

## What this project does not claim
No production-scale numbers, no "exactly-once" (only "idempotent under the tested faults"), no compliance certification, no "deleted" without saying logical or physical, and no row-level lineage via OpenLineage. For gold (G4): gold is not covered by deletion propagation until G8b (until then no claim is made that a forgotten customer is gone from gold, logically or physically); contracts enforce only `not_null` (Trino and Iceberg enforce no other constraint); S4 and R6 are not measured; the gold results are one run on one machine, and the point-in-time join by change LSN was tested with a single writer only.
