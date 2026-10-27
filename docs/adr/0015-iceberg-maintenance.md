# ADR-0015: Iceberg maintenance, and where it runs
- Status: accepted (G3)
## Decision
`adrp.spark.maintenance` wraps Iceberg's procedures; Dagster's `iceberg_maintenance` job runs them (daily schedule defined, stopped by default). Dagster therefore runs in the **Spark image** (Python 3.10): the maintenance ops need a JVM and the Iceberg runtime, and the app image has neither. pyiceberg 0.12 cannot compact.
| Step | Setting | Reason |
|---|---|---|
| `rewrite_data_files` | `partial-progress.enabled=true`, 10 commits, `min-input-files=2`, retried up to 3 times | a rewrite can fail validation against a concurrent MERGE that adds delete files for the files being rewritten; partial progress keeps finished groups, the retry loop handles the rest, and attempts / failures are returned so the conflict rate is recorded |
| `rewrite_manifests` | default | |
| `expire_snapshots` | silver 24 h, bronze 72 h, keep at least `retain_last` | bronze must outlive the slowest incremental reader (the G6 embedder reads it by snapshot); G8 shortens this when it must remove data |
| `remove_orphan_files` | `older_than` 24 h, **`prefix_listing => true`** | must be at least the longest write so an in-flight commit's files are never removed. Without prefix listing the procedure uses a Hadoop FileSystem for `s3://`, which this image does not have, and fails with "No FileSystem for scheme s3" |
## Findings that matter for G8
`remove_orphan_files` is the only step that deletes files no snapshot references (for example the files a SIGKILLed writer left behind). It must be part of any physical-deletion procedure.
## Layout note
Bronze is partitioned `(source_table, days(kafka_ts), bucket(16, pk))`, so each micro-batch writes up to 7 x 16 small files. Compaction is what keeps the file count in check.

## Measured (G3 run of record, `runs/manifests/20261027T055029Z-g3-check.json`, clean tree `ec8f2e1`)
After roughly 6,300 generator transactions streamed through 7 silver tables and bronze, one Dagster maintenance run took the total data+delete file count from 2,044 to 335 with every table's count and checksum unchanged. A second compaction run executed while the generator was writing needed 8 attempts with 0 failures (conflict rate 0 of 8 here; one small run, not a rate to quote), and silver still matched the ground truth afterwards. Those 2,044 files are what 10-second micro-batches produce with this partition spec on this machine; they are not a general claim.
