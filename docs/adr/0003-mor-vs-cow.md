# ADR-0003: Merge-on-read vs copy-on-write for silver
- Status: **provisional** (G0). Final decision after the G12 measurement.
## Context
Silver takes a MERGE every micro-batch. MoR writes small position-delete files (cheap writes, read amplification until compaction). CoW rewrites data files (expensive writes, clean reads).
## Decision (provisional)
MoR on silver (`write.merge.mode=merge-on-read`, `write.update.mode=merge-on-read`, `write.delete.mode=merge-on-read`), with scheduled compaction (G3 maintenance). Bronze is append-only, so the setting does not matter there.
## To be measured (G12)
Freshness latency, read amplification (files + delete files per scan), bytes written per GB ingested, compaction conflict rate, on the same workload for both modes.
## Deletion note
With either mode, rows are physically gone only after rewrite + snapshot expiry + orphan removal (G8). CoW does not remove that step.
