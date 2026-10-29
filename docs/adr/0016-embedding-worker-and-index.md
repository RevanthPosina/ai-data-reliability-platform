# ADR-0016: Embedding worker and the Qdrant index
- Status: accepted (G6)
## Decisions
| Topic | Decision |
|---|---|
| What is embedded | rows of `help_articles` (title + body) and `support_tickets` (subject + body), as `entity_id = "<table>:<id>"`, in one collection `text_chunks` (cosine, 384 dims, `all-MiniLM-L6-v2@1110a243`). |
| Version | the silver row's LSN, stored as `source_version`. |
| Point id / payload | `uuid5(ns, entity_id|version|chunk_no)`; `{entity_id, source_table, source_version, chunk_no, chunk_hash, embed_model_version, ingested_at, text}`; payload indexes on `entity_id` (keyword) and `source_version` (integer). |
| Driver | **bronze**, by pyiceberg's `incremental_append_scan(from_snapshot_id_exclusive, to_snapshot_id_inclusive)`, which returns only rows added by append snapshots. Compaction (`replace`) and the delete / overwrite snapshots G8 produces are skipped, which is the behaviour the plan asks of `streaming-skip-delete-snapshots` / `streaming-skip-overwrite-snapshots`. |
| Why bronze, not silver | silver keeps only the latest row per key, so a diff against it cannot tell *which* entities changed since the worker last looked; bronze is the append-only change log with a snapshot id to resume from. |
| Current state | for each changed entity the worker reads the **current silver row** (pyiceberg `In("id", ...)` scan) for its version, `is_deleted` and text. |
| Silver behind bronze | bronze and silver are separate queries, so silver can lag. An entity whose silver LSN is below the bronze LSN stays in a persisted `pending` map and is retried every poll; the bronze pointer still advances. Entries older than 15 minutes are logged as stuck (typically: the event was quarantined). |
| New version | upsert the new points (`wait=true`), then delete this entity's points with `source_version < v` (`wait=true`). Chunks whose text hash is unchanged reuse the stored vector, so a non-text column change (an article's `view_count`) re-versions the points without calling the model. |
| Tombstone | delete-by-filter `entity_id` (`wait=true`). |
| Idempotence | if the points for `(entity, version)` already exist with the same chunk hashes and model, nothing is written. Rerunning, or restarting with the state file deleted (full bronze rescan), leaves the index byte-for-byte unchanged, including `ingested_at`. |
| Pointer | persisted only after Qdrant has acknowledged every write of the poll (at-least-once; the re-do is a no-op). If the pointer's snapshot no longer exists (expired), the worker rescans what is left in bronze. |
| Visible in Qdrant | a point returned by a filtered scroll on `entity_id` with the matching `source_version`, after the `wait=true` ack. |
| Action log | one JSON line per processed entity (`ack_ns` = time the `wait=true` call returned) under `EVENT_LOG_DIR/embedder/actions.jsonl`: the `t_qdrant` timestamp for G11. |
## Findings
- **pyiceberg reads silver correctly.** Silver is merge-on-read (format v2, position deletes). With 46 delete files present, pyiceberg 0.12.0 and Spark returned the same 237 entities, versions and tombstones. The worker therefore reads silver with pyiceberg; no Spark or Trino is needed on this path.
- **Silver layout bug found and fixed.** `ALTER TABLE ... WRITE ORDERED BY` resets `write.distribution-mode` to `range`, silently overriding the `hash` G3 had set (the plan specifies `hash`). It is now set after the sort order and asserted by the G6 check.
- A check that compares the index only with silver can pass while silver lags. The G6 check therefore also compares the whole index with Postgres entity by entity.
## Limits
- Changing the embedding model is not automatic. The `wrong_model_chunks` check detects it; re-embedding on a model change is fault F10 (Milestone B).
- Payload `text` is a copy of the source text (ticket bodies can contain PII). It is deleted with the points (G8); Qdrant's own WAL and segments need the G8 treatment.
- The worker polls (2 s). Freshness to Qdrant therefore includes silver's 10 s trigger and the worker's poll; G11 measures it.
