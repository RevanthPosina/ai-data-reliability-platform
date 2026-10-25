# AI Data Reliability Platform — Plan (v2, Staff-reviewed corrections applied)

## TL;DR
- **Build:** a lakehouse pipeline that keeps AI data **correct, fresh, deletable and traceable**. Postgres changes flow via CDC (Debezium → Redpanda) into Apache Iceberg (bronze/silver/gold), then into two AI consumers: a **Qdrant vector index** (RAG) and **versioned training/eval datasets**.
- **Proves, with measured numbers:** per-sink freshness, idempotency under replay and crashes, right-to-be-forgotten propagation (logical *and* physical) across every copy of the data, data-quality catch rate on pre-registered faults, reconciliation against source at a consistent cut, and lineage + row-level provenance.
- **Why this project:** in 21 live Oct 2026 DE postings, data quality/freshness/SLAs appeared in 14, data modeling ~13, Spark 12, streaming 11, orchestration 10, dbt 7, governance/deletion 7, training/eval data 6, lineage 6. LLM serving appeared in 0 → this replaces `goodput-gateway`.
- **Name:** "AI Data Reliability Platform — CDC → Iceberg → vector index + versioned training datasets." Not a "RAG monitor."
- **Two milestones:** **A (resume-ready):** CDC → bronze → silver → Qdrant, idempotency proof, deletion propagation, 6-fault subset, freshness/deletion/idempotency results. **B (depth):** gold dimensional + ledger models, full DQ/contracts, training datasets, lineage, full fault matrix, scale study, dashboards and write-up.
- **Hardware:** core profile needs 16 GB RAM; full stack needs 32 GB. Laptop + free tiers only.

---

## Context
- Candidate: Data Engineer (ML/AI) at Microsoft, ~5 yrs, strong Python/SQL/Spark; targeting Senior DE at big tech, fintech, AI startups, Seattle-local.
- Planned by goals with testable done conditions; user owns timing.
- Posting phrases this project answers: "data correctness, freshness, lineage, and recoverability" (Perplexity); "dataset versioning, lineage tracking, and reproducibility" (Sesame); "provenance… PII handling… auditable lineage" (Microsoft AI Data Flywheel); "dimensional, Data Vault, or ledger-style" + "data contracts" (Whatnot); "maintaining correct and complete data" (Plaid); "safe backfills and recovery, and validate data against source records" + "retention and deletion" (Remitly).
- Gaps a side project cannot close: PB-scale ownership and 7–8+ years (see "Outside the project").

## Architecture
```
Postgres 16+ (OLTP source; logical replication; track_commit_timestamp=on; REPLICA IDENTITY FULL on text/PII tables)
  └─ Debezium Postgres connector (custom Kafka Connect image + Confluent Avro converter JARs)
       └─ Redpanda topics (per table, keyed by PK) + Redpanda Schema Registry (Confluent-compatible API, BACKWARD per subject)
            ├─ Spark query 1: native Iceberg streaming sink → BRONZE (append-only change log, epoch-idempotent)
            └─ Spark query 2: foreachBatch LSN-conditional MERGE → SILVER (current state + soft tombstones)
  Storage: S3-compatible object store (SeaweedFS or Garage) + Lakekeeper Iceberg REST catalog (backed by the existing Postgres)
  dbt-core + dbt-trino on Trino → GOLD: SCD2 dimensions built from the bronze change log, facts, immutable double-entry ledger
  Consumers:
    ├─ Embedding worker (Python, small sentence-transformer; Ray as stretch), driven by incremental bronze reads → Qdrant
    └─ Dataset builder → materialized tables ml.dataset_v{n} + Parquet export + dataset card
  Dagster: orchestrates batch work (dbt, maintenance, embedding catch-up, dataset builds, forget workflows);
           models the long-running streaming services as observable assets (liveness sensors + LSN-lag asset checks)
  Quality: Dagster asset checks + dbt tests + dbt model contracts (+ GX Core only if a target posting names it)
  Lineage: Debezium OpenLineage integration + OpenLineage Spark listener + openlineage-dbt + custom emitters → Marquez
  Observability: Prometheus + Grafana with SLO alert rules and runbooks
```

## Stack (pin exact versions in G0; smoke-test the matrix in CI)
| Layer | Choice | Notes |
|---|---|---|
| Source | Postgres 16+ | `wal_level=logical`, `track_commit_timestamp=on`, `REPLICA IDENTITY FULL` on `customers`, `help_articles`, `support_tickets` |
| CDC | Debezium 3.x Postgres connector on Kafka Connect | Custom image: `quay.io/debezium/connect:<pinned 3.x>` + Confluent Avro converter JARs (not bundled since Debezium 2.0), or Apicurio converters with `as-confluent: true` |
| Broker | Redpanda single node (`--smp 1 --memory 1G --overprovisioned`) + Schema Registry | Confluent-compatible API; subject compatibility BACKWARD |
| Stream processing | Spark Structured Streaming (PySpark), two queries | Bronze via native Iceberg sink; silver via foreachBatch MERGE |
| Table format | Apache Iceberg, `format-version` pinned in an ADR (v2 position deletes vs v3 deletion vectors; verify support across Spark, Trino, pyiceberg) | |
| Catalog | Lakekeeper (Rust, light, reuses Postgres); storage profile with path-style access and STS disabled (or remote signing) for SeaweedFS/Garage | ADR notes Apache Polaris (reported Apache top-level project since Feb 2026 — verify) as the enterprise alternative; pick one |
| Object storage | SeaweedFS or Garage | ADR notes MinIO CE reportedly no longer distributes images/binaries (verify current status); a pinned last MinIO image is acceptable only if called out |
| Transformation | dbt-core + dbt-trino on Trino | dbt-trino supports snapshots and model contracts on Iceberg; only `not_null` is enforced |
| Orchestration | Dagster | Current `FreshnessPolicy` (time_window/cron) API; event-time freshness via custom asset checks |
| Data quality | Dagster asset checks + dbt tests + contracts; GX Core optional | |
| Embeddings | `sentence-transformers` small model (all-MiniLM-L6-v2 or bge-small), plain Python batching; Ray Data as stretch (cap `object_store_memory=1GB`) | |
| Vector DB | Qdrant | Point id `uuid5(ns, entity_id|version|chunk_no)`; payload index on `entity_id` |
| Lineage | Debezium OpenLineage (3.2+, needs OpenLineage SMT), OpenLineage Spark listener, `openlineage-dbt`, custom `openlineage-python` emitters → Marquez 0.50.0 | Marquez last release Oct 2024 (risk, acceptable for demo); `dagster-openlineage` community v0.2, optional |
| Reconciliation | Python service (Go optional stretch) | |
| Observability | Prometheus + Grafana | No customer IDs as metric labels |
| Packaging | Docker Compose profiles, Makefile, GitHub Actions | |

## Compose profiles and machine requirements
| Profile | Contents | Approx RAM |
|---|---|---|
| core | Postgres, Redpanda, Connect/Debezium, object store, Lakekeeper, Spark (2 queries), Qdrant, embedding worker, Dagster | 10–12 GB |
| +transform | Trino | +3 GB |
| +obs | Prometheus, Grafana | +0.5 GB |
| +lineage | Marquez | +1.5 GB |
| full | all of the above | 17–22 GB |
- Minimum: 16 GB RAM (core), 32 GB (full), 8 cores, 100 GB free SSD for G12. Raise Docker Desktop's memory limit.
- Apple Silicon: confirm arm64 images (Marquez is the likely problem); emulated images skew CPU numbers.
- Measurement runs use one fixed profile with Grafana/Marquez off; profile recorded in the run manifest.
- Metabase cut (Grafana or a Trino notebook covers the business view).

---

## Milestone A — resume-ready

### G0 — Design doc, repo, walking skeleton
- `docs/design.md`: architecture diagram (Mermaid), data flow, SLOs (freshness per sink, deletion deadline logical + physical, DQ thresholds), non-goals, threats to validity.
- **Data inventory** (every place a customer's data can live; feeds G8).
- **Timestamp capture points** (feeds G11).
- **Pre-registered fault matrix** with expected detections, committed before any checks exist (feeds G5/G10). Includes ≥ 2 faults written blind to the checks (random field corruption, dropped partition range).
- **Pinned version matrix** (Postgres, Debezium, Redpanda, Spark, Iceberg, Lakekeeper, object store, Trino, dbt-trino, Dagster, Qdrant, OpenLineage, Marquez) + CI smoke test.
- ADRs in `docs/adr/`: Debezium vs query-based CDC; Spark vs Kafka Connect Iceberg sink (check whether the Apache Iceberg Kafka Connect sink supports upsert/delete; if append-only, that is the reason for Spark silver); MoR vs CoW (measured later in G12); Iceberg format version; Lakekeeper vs Polaris; object store choice; Qdrant vs pgvector; dbt on Trino vs Spark; materialized dataset tables vs tags (G7); reproducible-vs-deletable trade-off (G7/G8).
- Repo layout, CI (ruff, mypy, pytest, `dbt build` on a fixture), `make up PROFILE=core`, `make down`, `make demo`. Run manifest: git SHA, pinned versions, seed, profile, hardware fingerprint.
- **Done:** `make up PROFILE=core` starts core; a thin slice runs end to end — one Postgres insert → topic → bronze → silver → queried (Trino or Spark SQL) → embedded in Qdrant; CI green; design doc contains every SLO with its measurement method, the data inventory, the pre-registered fault matrix and the version matrix.

### G1 — Domain model and data generator
- Domain: payments marketplace — `customers`, `merchants`, `accounts` (with transactionally maintained `balance`), `transactions`, `refunds`, plus text tables `support_tickets` and `help_articles` (RAG/training corpus, some ~10 KB to exercise TOAST). Supports dimensional + ledger modeling; has PII to delete.
- Seeded generator (Faker + numpy): inserts/updates/deletes at a configurable rate, realistic skew (some customers far more active), stable `entity_id`, per-entity `event_seq`.
- Ground-truth log (logical operations) to Parquet; separate run log for commit times and txids.
- **Done:** same seed → identical logical operation sequence (hash over (seq, op, table, pk, payload)); commit times/txids in the separate run log; maximum sustainable change rate measured and stated; schema and PII columns documented.

### G2 — CDC ingestion
- Logical replication slot + publication; Debezium snapshot then streaming; heartbeat with `heartbeat.action.query` to prevent WAL bloat when tracked tables are idle but other database traffic produces WAL.
- Serialization: Avro via `io.confluent.connect.avro.AvroConverter` → Redpanda Schema Registry; BACKWARD compatibility per subject. JSON not used — the registry is the producer-side contract.
- `REPLICA IDENTITY FULL` on text/PII tables (otherwise unchanged TOASTed columns arrive as `__debezium_unavailable_value` and delete/update before-images carry only the PK).
- Snapshot reads (`op='r'`) handled as upserts at the snapshot LSN; tombstones enabled (filtered out in Spark before parsing).
- Monitor replication-slot lag and connector status as metrics.
- CI job: fail on an incompatible Avro schema change by checking the registry compatibility against committed contract YAML.
- **Done:** snapshot + live changes arrive in Redpanda; a Postgres delete produces a delete event + tombstone; an update to a non-text column of a 10 KB help article leaves the article body intact downstream; slot lag exported.

### G3 — Iceberg bronze and silver
- **Bronze:** its own streaming query using the native Iceberg streaming sink (`writeStream.format("iceberg")`), which records query-id/epoch in the snapshot summary and skips already-committed epochs. Never write bronze and silver in one foreachBatch.
  - Bronze stores the raw Kafka record losslessly (`topic, partition, offset, kafka_ts, key_bytes, value_bytes, schema_id` parsed from the 5-byte Confluent header) plus decoded envelope columns (`op, lsn, txid, source_ts, pk, before, after`).
  - Decoding uses a registry-aware decoder (ABRiS, or a fastavro UDF keyed by schema_id) resolving to the reader schema fetched at query start; a Dagster sensor restarts bronze when a subject gets a new version. Decoded columns can always be re-derived from `value_bytes`.
  - Partition spec e.g. `days(kafka_ts), bucket(N, pk)`. Note: a forget request touches every time partition containing the customer, including FK rows in other tables.
- **Silver:** second query, foreachBatch, idempotent by construction:
  - (a) dedupe within the batch to max `source.lsn` per key with `row_number()`;
  - (b) `WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`;
  - (c) deletes are **soft tombstones** `(pk, lsn, is_deleted=true, non-key columns NULL)` — never hard deletes (a replayed older insert would resurrect the row). Consumers read `WHERE NOT is_deleted`. Tombstones carry no PII; purged after topic retention + replay window.
  - No watermarks in table properties or side tables (no multi-table atomic commit). Optional audit marker in the same commit via snapshot summary properties (`CommitMetadata.withCommitProperties` takes a Java `Callable`, awkward via py4j — implement in a small Scala helper or skip).
  - ADR: per-key LSN ordering is sound because row locks force a later writer of the same row to commit after the earlier one.
- **Schema evolution:** foreachBatch groups rows by schema_id within the batch, decodes each group with its writer schema, and resolves to the target schema, runs `ALTER TABLE … ADD COLUMN` before the MERGE; type narrowing or rename (drop + add) → `quarantine_<table>` with reason `SCHEMA_INCOMPATIBLE`.
- **Layout:** silver partition spec `bucket(N, pk)`, sort order on pk, `write.distribution-mode=hash`.
- **Maintenance (Dagster):** `rewrite_data_files` (with `partial-progress.enabled=true` + retry, since compaction can fail validation against concurrent MERGE delete files; record conflict rate), `expire_snapshots` (on bronze, retain snapshots longer than the maximum G6 reader lag), `remove_orphan_files`, `rewrite_manifests`.
- **Instrumentation now (not later):** OpenLineage Spark listener configured; foreachBatch event log `(batch_id, snapshot_id, committed_at, [(pk, lsn)])` for G11.
- **Recovery proof:** batch job rebuilds silver from bronze up to a target LSN taken from the foreachBatch event log (a batch boundary); its checksum must equal the streaming silver snapshot (via time travel) whose highest applied LSN equals that target.
- **Done:** kill/restart the stream mid-batch → silver matches ground truth (no dupes, no loss, no resurrection); added source column appears in silver without a restart; incompatible change lands in quarantine; Hypothesis property test (random per-key event sequences with shuffles, duplicates, replays → final silver = oracle) passes over 1,000 cases against a pure-Python model of the merge logic plus ~50 cases against local Spark; rebuild-from-bronze checksum = streaming checksum; maintenance reduces file count.

### G6 — Vector index consumer (Milestone A uses G3 silver directly)
- Deterministic chunker; chunk number stable per entity version.
- Incremental embedding **driven by incremental reads of bronze** (append-only; `start-snapshot-id`; `streaming-skip-delete-snapshots=true` and `streaming-skip-overwrite-snapshots=true` — bronze only gets delete/overwrite snapshots from G8; appends come only from the streaming sink), looking up each entity's current version and `is_deleted` in silver.
- Qdrant: point id `uuid5(ns, entity_id|version|chunk_no)`; payload `{entity_id, source_version, chunk_hash, embed_model_version, ingested_at}`; payload index on `entity_id`; upserts and deletes with `wait=true`. On a new version v: upsert the new points (`wait=true`), then delete-by-filter `entity_id == x AND source_version < v` (`wait=true`). On a tombstone: delete-by-filter `entity_id == x`.
- If pyiceberg reads silver, verify it supports the chosen delete-file type; otherwise read via Spark or Trino.
- "**Visible in Qdrant**" = point returned by a filtered scroll on `entity_id` with matching `source_version`, after a `wait=true` ack.
- Checks: stale chunks (payload version < silver version), orphaned chunks (entity tombstoned), duplicate chunks (same chunk_hash on multiple points), embedding-model version mismatch.
- **Done:** after a scripted burst of updates/deletes, stale = 0, orphaned = 0, duplicates = 0 within the freshness SLO; rerunning the job is a no-op.

### G8 — Deletion propagation (right to be forgotten)
- One `forget(customer_id)` request propagates across the **data inventory**:
  - Postgres row (dead tuple remains until `VACUUM` — run it and measure); Postgres WAL (gone after the slot advances and segments recycle — measure); backups if any.
  - Redpanda topics: `cleanup.policy=compact,delete`, short `retention.ms`, short `segment.ms` (the active segment is never compacted or deleted); other tables' topics carry `customer_id` in values and are removed only by `retention.ms`; measure time until no segment contains the key.
  - `quarantine_<table>` tables (raw rows with PII).
  - Bronze (append log with before/after PII): `DELETE WHERE` + rewrite.
  - Silver: soft tombstone with no PII.
  - Gold (Milestone B): immutable ledger and transactions **pseudonymize** `customer_id` (never delete accounting entries); dimensions delete or pseudonymize per documented policy (ADR).
  - Qdrant: delete-by-filter; deleted vectors are only marked until the vacuum optimizer runs (small collections may never hit `deleted_threshold`) → trigger optimization or report the bound; also Qdrant WAL (`wal_capacity_mb`) and Qdrant snapshots.
  - Dataset tables and Parquet exports (Milestone B, via G7 invalidation).
  - Generator ground-truth log and run log (Parquet, contain PII) and the foreachBatch event log (contains pks): rewrite or pseudonymize.
  - Spark streaming state; Spark local spill dirs (ephemeral; state the bound); Dagster logs (never log raw IDs); Prometheus (no customer_id labels).
- **Iceberg physical removal — applies to every Iceberg table holding the entity: bronze, silver, `quarantine_*`, gold, `ml.dataset_v*`** (MoR: the delete file hides the row but the original data file still holds it and stays referenced; `expire_snapshots` alone never removes it. `remove_orphan_files` does not remove deleted rows from referenced files, but it is required to purge orphan files left by killed or failed writes (the G10a Spark-kill fault), which can contain the entity's data; run with `older_than` ≥ the maximum write duration):
  1. `rewrite_data_files` on affected partitions with `delete-file-threshold=1`;
  2. drop/re-point any tags or branches referencing older snapshots;
  2b. drain every incremental reader of the table (G6 embedding stream, any dataset builder) until its committed offset equals the current snapshot, or pause it and restart it from the post-expiry snapshot with a silver-diff catch-up;
  3. `expire_snapshots(older_than=now, retain_last=1)`;
  4. `remove_orphan_files` (older_than ≥ max write duration);
  5. verify by object-store listing + Parquet scan of every remaining file for the entity_id.
- Audit table keyed by `hmac(customer_id)`, never the raw id: request time, per-sink logical-absence and physical-absence times, verification result.
- **Done:** for N forget requests, the verifier scans every inventory location by raw bytes or query and reports logical-absence lag and physical-absence lag per sink; any location that cannot be cleaned is listed with its bound.

### G10a — Fault subset (6 faults) + reconciliation
- Faults (seeded, scripted): duplicate events (replay an offset range), out-of-order updates, Spark kill mid-MERGE, connector crash mid-snapshot, Redpanda restart, schema drift (add column + incompatible change).
- Reconciler (Python): **at a consistent cut** — pause the generator; record `cut = pg_current_wal_lsn()`; wait until the slot's `confirmed_flush_lsn` ≥ cut (Debezium has emitted everything up to the cut); record the topic end offsets; wait until silver's committed Kafka offsets ≥ those end offsets; then compare counts and order-independent checksums (sum of xxhash64 over canonical rows, mod 2^64) per table and per pk-bucket. Continuous mode compares silver to the ground-truth log replayed to silver's applied LSN (ground-truth ops mapped to LSN via txid ↔ Debezium `source.txId`) — never to live Postgres.
- Classify each outcome: caught by named check / self-healed / silent corruption (must be zero or explained).
- Named checks in Milestone A: reconciler mismatch, quarantine count > 0, G6 stale/orphan/duplicate checks, connector-status and slot-lag alerts, `__debezium_unavailable_value` count. G5 adds the rest and re-scores the matrix.
- **Done:** fault → detection → recovery → residual effect table for the 6 faults; reconciler reports zero mismatches after recovery.

### G11a — Measurement for R1, R2, R4
- Clocks: every timestamping process (generator, prober) runs inside the Docker VM; on macOS record host–VM offset at start and end of each run, abort if > 5 ms.
- Capture points:
  - t0 = Postgres commit time (`track_commit_timestamp`; Debezium `source.ts_ms`), joined to ground truth by txid.
  - t_broker = Kafka record timestamp (topics set `message.timestamp.type=LogAppendTime`; the default is CreateTime).
  - t_silver = commit time of the Iceberg snapshot containing the change (foreachBatch event log).
  - t_gold = completion of the dbt model whose max source LSN ≥ the change's LSN (Milestone B).
  - t_qdrant = `wait=true` ack time for the point batch.
- Black-box prober inserts sentinel rows at a low fixed rate, polls each sink at ≤ SLO/20; report prober and instrumented numbers side by side.
- Report achieved change rate (not target); flag runs where the generator fell behind.
- CPU via cgroup `cpu.stat` per container; bytes via object-store deltas.
- Fixed-duration runs, warmup discarded, ≥ 5 repetitions per point, medians with min/max; ≥ 20 clean runs for false-alarm rate.
- **Done:** a DuckDB notebook over the Parquet event logs regenerates R1, R2, R4 from raw logs.

---

## Milestone B — depth

### G4 — Gold: dimensional + ledger models (dbt on Trino)
- Star schema: `fct_transactions` (grain: one row per transaction), `dim_customer` / `dim_merchant` **SCD2 built from the bronze change log** (LSN-ordered; valid_from = commit ts, valid_to = next change's ts; multiple changes to one key in the same transaction collapse to the last LSN to avoid zero-length intervals), `dim_date`.
- Comparison: dbt snapshots on silver, to show they lose intermediate changes between runs (interview point).
- Double-entry ledger `fct_ledger_entries`: immutable entries; refunds as reversing entries; sum per transaction = 0; account balances reconcile to Postgres `accounts.balance`.
- Incremental models with late-arriving data lookback; documented backfill procedure.
- Contracts: dbt model contracts on gold; ADR states only `not_null` is enforced by Trino/Iceberg, others metadata-only.
- **Done:** `dbt build` passes; ledger nets to zero per transaction; balances reconcile; SCD2 history matches ground truth for a scripted change sequence (including multiple changes between dbt runs); one-day backfill reproduces identical results.

### G5 — Data contracts and quality (full)
- Contract YAML per topic (schema + semantic rules: non-null keys, ranges, referential integrity), enforced at the registry (G2 CI) and in checks.
- Dagster asset checks on bronze/silver: nulls, uniqueness, accepted values, event-time freshness (`now − max(source_ts)`), volume anomalies, `__debezium_unavailable_value` count = 0.
- dbt tests on gold. GX Core only if a target posting names it.
- Failure routing: blocking vs warning checks; quarantine rows with reason codes.
- **Done:** each fault in the pre-registered matrix is caught by its named check or listed as uncaught; blind faults reported honestly; gold contract break fails the build.

### G7 — Versioned training/eval datasets
- Builder materializes each version into its own table `ml.dataset_v{n}` + Parquet export (does not tag live silver; tags would block expiry).
- Deterministic train/eval split by hashed entity_id; leakage check (no entity in both).
- Content hash = SHA-256 over rows sorted by (split, entity_id, chunk_no), hashing canonical row encodings (not Parquet bytes).
- Row-level provenance columns in every training row: `(entity_id, source_lsn, silver_snapshot_id, chunk_hash)`.
- Dataset card (JSON): version, source snapshot ids, retention window R, row count, content hash, excluded PII columns, generator seed, code SHA.
- Reproducibility claim: rebuilding v{n} from its source snapshot ids reproduces the hash **while those snapshots are retained (window R)**; afterwards the materialized table is the record.
- Forget interaction: any forget request touching v{n} invalidates it; rebuild as v{n}.1 with the exclusion recorded; drop v{n}. ADR documents the reproducible-vs-deletable trade-off.
- **Done:** two versions built across data changes; rebuild of v1 within R reproduces its hash; leakage check passes; a forget request produces v1.1 and removes v1; dataset cards generated automatically.

### G8b — Deletion propagation into gold and datasets
- Extend G8 to gold (delete or pseudonymize per policy) and dataset tables/exports (via G7 invalidation).
- **Done:** G8 verifier covers the full inventory, including gold and dataset versions.

### G9 — Lineage
- Postgres → topic: Debezium OpenLineage integration (with the OpenLineage SMT for output datasets).
- Topic → bronze → silver: OpenLineage Spark listener (configured in G3).
- dbt: `openlineage-dbt` (verify Trino support and behavior under `dagster-dbt`).
- Dagster: `dagster-openlineage` optional (community, v0.2).
- Qdrant and dataset builder: custom `openlineage-python` emitters with custom facets (collection, dataset version).
- Backend: Marquez 0.50.0 pinned (named risk).
- Lineage is dataset-level and partly column-level; row-level provenance comes from G7's provenance columns.
- **Done:** Marquez graph from Postgres table → topic → bronze → silver → gold → Qdrant collection and dataset version; demo traces one training row back to its source record using the graph (path) plus provenance columns (record).

### G10b — Full fault matrix
- Add: late-arriving rows, null keys, Qdrant unavailable, embedding model version change, plus the blind faults (random field corruption, dropped partition range).
- **Done:** full fault table; zero reconciler mismatches after recovery for every fault, or the residual explained.

### G11b — Full measurement
- Add R3, R5, R6, R7; training snapshots reported as **staleness at build time**, not per-change freshness.
- "Freshness goodput": % of changes visible in all sinks within SLO, as achieved change rate increases.
- **Done:** notebook regenerates R1–R7 from raw logs.

### G12 — Scale and storage-layout study
- Scale the G1 generator to the largest sustained size the machine handles (target ≥ 50M change events / tens of GB).
- Measure silver MERGE throughput, data-file and delete-file counts over time, compaction effect.
- MoR vs CoW on the same workload: latency vs read amplification vs bytes written → feeds the MoR/CoW ADR.
- Optional: TPC-DS SF10 read benchmark in Trino.
- "1000×" doc grounded in measured file and delete-file growth.
- **Done:** README states one real, reproducible size and throughput number with hardware; MoR vs CoW table.

### G13 — Observability, docs, write-up
- Prometheus alert rules tied to SLOs with error-budget burn rate; one runbook per alert.
- Grafana: slot lag, broker lag, LSN lag per sink, freshness, DQ failures, reconciler mismatches, forget-request SLA.
- README: 60-second pitch, architecture diagram, `make demo` (changes → faults → forget request → verification), results tables, threats to validity, ADR index, runbooks, "Outside scope" section.
- Blog post on the strongest finding (e.g. "deleted" rows that survive in Iceberg data files until compaction + expiry; or SCD2 snapshot loss).
- **Done:** a stranger runs `make demo` and reproduces the headline table.

---

## Results the project produces
| ID | Question | Output | Milestone |
|---|---|---|---|
| R1 | How fresh is each AI consumer? | Freshness p50/p99 per sink vs achieved change rate | A |
| R2 | Does a deletion disappear everywhere? | Per-sink logical and physical absence lag; uncleanable locations with bounds | A |
| R4 | Is the pipeline idempotent? | Dupes/loss/resurrections after replay and crash (target 0); property-test result | A |
| R3 | Which faults are caught, healed, or silent? | Fault matrix vs pre-registered expectations | A (6) / B (full) |
| R5 | Are datasets reproducible and leak-free? | Hash match within R; leakage check; invalidation on forget | B |
| R6 | Does gold reconcile to source? | Consistent-cut reconciliation; ledger nets to zero | B |
| R7 | What does it cost? | CPU-seconds and bytes per GB ingested; MoR vs CoW | B |

## Must-never-claim
- Production scale or TB/PB numbers not run.
- "Exactly-once" — say "idempotent under the tested faults."
- Compliance certification — say "deletion propagation verified in this system."
- "Deleted" without saying logical vs physical.
- "Row-level lineage via OpenLineage" (it's dataset/column-level; row provenance is via columns).
- "Reproducible" for a dataset version invalidated by a forget request, or beyond retention window R.
- That zero detected faults means no faults.

## Resume bullet shapes (fill only with measured numbers)
- "Built an AI data reliability platform (Postgres CDC → Debezium/Redpanda → Spark → Iceberg → dbt) feeding a Qdrant vector index and versioned training datasets; p99 freshness __ s to the vector index at __ changes/s (achieved rate)."
- "Implemented right-to-be-forgotten propagation across lakehouse, broker, vector index and training datasets; logical absence in all sinks within __ s, physical removal within __ min after compaction and snapshot expiry."
- "Made streaming MERGE idempotent via LSN-conditional upserts and soft tombstones; 0 duplicates, losses or resurrections across __ crash/replay runs and 1,000 property-based cases."
- "Pre-registered a __-fault injection matrix; contracts and checks caught __/__ with __ false alarms over __ clean runs; consistent-cut reconciliation showed 0 mismatches after recovery."
- "Modeled SCD2 dimensions from the CDC change log and an immutable double-entry ledger in dbt; showed snapshot-based SCD2 lost __% of intermediate changes."

## Interview questions to prepare
- How is MERGE idempotent on an at-least-once stream? Why soft tombstones? Why is per-key LSN ordering safe?
- Why can't you store the watermark in table properties?
- What physically happens to a deleted row in Iceberg MoR? What order of operations removes it? When does `remove_orphan_files` matter for deletion?
- Why do tags conflict with deletion? How did you trade off reproducibility vs deletability?
- What is TOAST and why did you need REPLICA IDENTITY FULL?
- Why drive embeddings from bronze instead of silver?
- Why build SCD2 from the change log instead of dbt snapshots?
- How did you measure freshness across components without clock skew?
- How did you avoid tuning checks to your own faults? Which faults were missed?
- Spark vs Kafka Connect Iceberg sink? MoR vs CoW, with numbers?
- What changes at 1000×?

## Risks
| Risk | Mitigation |
|---|---|
| Too many containers | Profiles; core on 16 GB; full needs 32 GB |
| Version incompatibility (Spark ↔ Iceberg ↔ catalog ↔ Trino) | Pinned matrix + CI smoke test in G0 |
| Stale components (Marquez, MinIO) | Marquez pinned and named; SeaweedFS/Garage instead of MinIO |
| Scope creep | Ship Milestone A first; cut order: Ray, GX Core, Go, G12 optional TPC-DS, Marquez column-level lineage |
| Checks tuned to faults | Pre-registered matrix + blind faults |
| Overclaiming | Must-never-claim list; numbers only from the results notebook |

## Outside the project (from the research)
- Rewrite Microsoft bullets with actual volumes, job counts, SLAs — scale is the most-demanded signal and only work history can supply it.
- Apply first where year floors are 4–5+: Plaid (Seattle), Perplexity Data Platform, Sesame, Coinbase Data Platform, Robinhood (Bellevue zone), Microsoft AI IC5. Treat 7–8+ year roles (Anthropic Product, Amazon AWS Analytics, Whatnot, Expedia Layla) as stretch.
- Many researched postings were aggregator/snippet-only; verify each is live before applying.

---
---

# Part 2 — Side Notes (for someone completely new)

## The big picture in plain words
A company's app writes to a database (Postgres). Analysts and AI systems need copies of that data. Copies go stale, get duplicated, break when the schema changes, and — worst of all — keep personal data after a customer asks to be deleted. This project builds the pipeline that makes copies, and then **proves with numbers** that the copies are fresh, correct, deletable and traceable.

## Key terms
- **CDC (change data capture):** instead of copying whole tables repeatedly, read the database's own change journal (the WAL) and stream every insert/update/delete.
- **WAL / LSN:** Postgres's write-ahead log; the LSN is a position in it, like a page number. Higher LSN = later change.
- **Debezium:** the tool that reads the WAL and turns changes into messages.
- **Redpanda:** a Kafka-compatible message broker — a durable queue of change messages.
- **Schema Registry / Avro:** stores the shape (schema) of each message and rejects incompatible changes. Avro is the compact message format.
- **Apache Iceberg:** a table format on top of files in object storage. It keeps **snapshots** (versions), which allows time travel — and means old data lingers until you clean it.
- **Bronze / silver / gold:** raw change log → current clean state → business-ready models.
- **MERGE:** "update if the row exists, insert if not" in one statement.
- **Idempotent:** doing something twice gives the same result as once. Critical because streams can replay messages.
- **Soft tombstone:** instead of deleting a row, mark it deleted. Prevents an old replayed message from bringing it back.
- **MoR vs CoW (merge-on-read vs copy-on-write):** MoR writes small "delete files" that hide rows (fast writes, slower reads). CoW rewrites whole data files without the deleted rows (slower writes, faster reads) — but the old files stay in older snapshots until those snapshots are expired, so with either mode the rows are not physically gone yet.
- **Compaction / expire snapshots:** maintenance that rewrites small files into big ones and removes old versions. Needed for physical deletion.
- **SCD2 (slowly changing dimension type 2):** keep full history of an entity (e.g. customer address) with valid-from/valid-to dates.
- **Double-entry ledger:** every money movement is recorded as matching debit and credit entries that sum to zero.
- **Data contract:** a written promise about a dataset's schema and rules that producers and consumers agree on.
- **Vector index (Qdrant):** stores text embeddings so an AI can find similar passages (RAG).
- **Embedding:** a list of numbers representing the meaning of a piece of text.
- **Lineage (OpenLineage/Marquez):** a graph of which job read which dataset and wrote which dataset.
- **Provenance:** for one specific row, exactly where it came from.
- **Freshness:** how long from a change in the source until it is visible in a copy.
- **Reconciliation:** checking a copy matches the source (counts + checksums).
- **Property-based test (Hypothesis):** the computer generates thousands of random scenarios and checks a rule always holds.

## Goal-by-goal: what and how

**G0 — Design doc and walking skeleton**
- *What:* write down every promise (SLO), every place data lives, every fault you'll test, and pin every tool version; then get one record flowing end to end.
- *How:* a markdown design doc with a diagram; Docker Compose profiles; a Makefile; CI on GitHub; insert one row in Postgres and watch it reach Qdrant.
- *Why:* writing the fault list before building the checks keeps your results honest; the thin end-to-end slice catches integration problems early.

**G1 — Domain and data generator**
- *What:* a fake payments company with customers, merchants, accounts, transactions, refunds, and text tables (support tickets, help articles).
- *How:* a Python script using Faker and a fixed random seed that performs inserts/updates/deletes and logs exactly what it did (the "answer key").
- *Why:* you can only prove correctness if you know the right answer.

**G2 — CDC ingestion**
- *What:* stream every database change into Redpanda.
- *How:* turn on Postgres logical replication; configure Debezium with the Avro converter pointing at Redpanda's schema registry; set `REPLICA IDENTITY FULL` on text tables so large text isn't replaced by a placeholder; add a CI check that blocks breaking schema changes.
- *Why:* this is the industry-standard way to move operational data without hammering the database.

**G3 — Bronze and silver in Iceberg**
- *What:* bronze keeps every change forever-ish; silver keeps the latest state of each row.
- *How:* two Spark streaming jobs. Bronze uses Iceberg's built-in streaming writer (it won't double-write on restart). Silver dedupes each batch to the latest LSN per key, then MERGEs only if the incoming LSN is newer, and marks deletes as tombstones. Test by killing the job mid-write and by generating 1,000 random replay scenarios.
- *Why:* "my pipeline is idempotent and I can prove it" is a senior-level statement.

**G6 — Vector index**
- *What:* keep Qdrant's text chunks in sync with the source.
- *How:* read new bronze changes, check silver for the current version, chunk and embed changed text with a small model, upsert to Qdrant with the source version in the payload; delete older-version chunks on update and all chunks of deleted entities.
- *Why:* stale or deleted text in an AI's search results is a real product and compliance problem.

**G8 — Right to be forgotten**
- *What:* make one customer's data disappear from every copy, and measure how long it takes.
- *How:* walk the data inventory: database, WAL, broker topics, bronze (delete + rewrite files), silver (tombstone), gold, Qdrant (delete + force cleanup), dataset versions, logs. For Iceberg, rewrite affected files, drop old tags, expire old snapshots, then scan the remaining files to prove the bytes are gone.
- *Why:* most pipelines only hide deleted data; you'll show the difference between "hidden" and "gone".

**G10 — Faults and reconciliation**
- *What:* break things on purpose and check the system notices and recovers.
- *How:* scripts that replay messages, reorder updates, kill jobs, restart the broker, change schemas. A reconciler pauses the generator, waits until every change made before the pause has passed through the broker into the copy, then compares counts and checksums.
- *Why:* interviewers ask "what happens when X fails?" — you'll have a measured table.

**G11 — Measurement**
- *What:* produce the numbers for the resume.
- *How:* record timestamps at every stage (database commit, broker, Iceberg commit, Qdrant ack), keep all clocks inside the same Docker VM, add a "prober" that inserts marker rows and watches for them, run each test 5+ times, analyze the logs with DuckDB.
- *Why:* honest, reproducible numbers are what make the project credible.

**G4 — Gold models (dbt)**
- *What:* business-ready tables: a star schema, customer history (SCD2), and a double-entry ledger.
- *How:* dbt SQL models on Trino; build SCD2 from the bronze change log so no intermediate change is lost; test that ledger entries sum to zero and balances match the source.
- *Why:* data modeling is the second most requested skill in the postings.

**G5 — Contracts and quality**
- *What:* rules that every dataset must satisfy, and what happens when they're broken.
- *How:* contract YAML files, Dagster asset checks, dbt tests; bad rows go to quarantine with a reason.
- *Why:* data quality was the #1 theme in the postings.

**G7 — Training/eval datasets**
- *What:* versioned, reproducible datasets for training and evaluating models.
- *How:* materialize each version as its own table with a content hash, a train/eval split that never leaks the same customer into both, and a dataset card describing exactly where it came from. A deletion request creates a new version and drops the old one.
- *Why:* this is the core of "AI Data Engineer" roles at Microsoft AI, Sesame, Anthropic.

**G9 — Lineage**
- *What:* a visual graph of how data flows from source to AI datasets.
- *How:* OpenLineage plugins in Debezium, Spark and dbt, plus small custom emitters, sending events to Marquez.
- *Why:* governance and auditability questions ("where did this training row come from?").

**G12 — Scale study**
- *What:* push volume as high as your machine allows and measure how the tables behave.
- *How:* scale the generator to tens of millions of changes; track file counts, delete files, MERGE throughput; compare MoR vs CoW.
- *Why:* gives you a real size number and a "what changes at 1000×" answer grounded in data.

**G13 — Docs and write-up**
- *What:* alerts, dashboards, README, blog post.
- *How:* Prometheus alert rules with runbooks; Grafana dashboards; a `make demo` that runs the whole story.
- *Why:* recruiters read the README; interviewers read the method.

## Tools glossary
- **Postgres:** relational database (the source).
- **Debezium / Kafka Connect:** CDC reader and the framework that runs it.
- **Redpanda:** Kafka-compatible broker.
- **Spark Structured Streaming:** processes the stream and writes Iceberg tables.
- **Iceberg / Lakekeeper:** table format and its catalog (where table metadata is registered).
- **SeaweedFS / Garage:** S3-compatible object storage running locally.
- **Trino:** SQL query engine for Iceberg; dbt runs on it.
- **dbt:** SQL transformation framework with tests and contracts.
- **Dagster:** orchestrator that schedules jobs and runs checks.
- **Qdrant:** vector database.
- **sentence-transformers:** small embedding models that run on CPU.
- **OpenLineage / Marquez:** lineage events and the UI that shows them.
- **Prometheus / Grafana:** metrics and dashboards.
- **DuckDB:** fast SQL on Parquet files for analysis.
- **Hypothesis:** property-based testing library for Python.
