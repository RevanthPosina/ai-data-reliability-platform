# ADR-0022: Offsets and topic settings under reconciliation
- Status: accepted (G10a); written before the first run, so nothing here is a result. Items marked **unverified** are not yet observed.
## Decision
**No reconciler check assumes contiguous offsets.** The reconciler (ADR-0021) must work on both topic settings, and G10a is run on both.
| | Default topics (`make g10a-check`) | Compacting topics (`make g10a-check TOPICS=compact`) |
|---|---|---|
| Topic config | Debezium's defaults, no cleanup policy | `bootstrap.py --forget-ready-topics`: cluster `log_segment_ms_min` lowered, and per CDC topic the settings from `infra/redpanda/topic-config.json` (ADR-0019: `compact,delete`, short `segment.ms`, `min.cleanable.dirty.ratio=0.01`, 300 s / 600 s compaction lags, `delete.retention.ms`, 24 h `retention.ms`); topics created ahead of Debezium with its partitions, replication and timestamp type |
| Strict no-gap check | applied | not applied |
| Subset check | applied | applied |
Evidence for gaps. The g3 run on default topics showed bronze rows, distinct offsets and records equal. The g8 run of record, which applies the compacting config, showed bronze offset gaps after its forget work, none before. That figure mixes compaction with the forget workflow's deletes, so it is not a pure compaction measurement.
## How progress and presence are judged
1. **Progress compares high watermarks only**, never record counts: silver's and bronze's committed offsets must reach the topic end offsets (ADR-0021).
2. **The present-record set comes from a new reader using `enable.partition.eof=True`** that stops at EOF. `g2.read_range` waits for offset `stop-1`, which compaction may have removed, so it can hang or exit. `g2.read_range`, `g2.read_all`, `g3.Drain`, `g8.SafeDrain` and `g3.bronze_report` are not used on compacting topics.
3. **`compare_offsets` passes** when all of these hold:
   - Kafka's present offsets are a subset of bronze's distinct offsets.
   - No bronze offset is at or above the end offset.
   - `rows - distinct` equals the expected duplicate count from the F1 replay ledger.
   - Only when the topic is **not** compacting: distinct offsets equal the end offset (no gap).
4. **Compaction is read from the broker** (`broker.compaction_enabled(broker.topic_config(table))`), never inferred from the data. Inferring it from the data would hide a real loss as a "gap".
5. g3's `bronze_exact` (rows == distinct == high watermark) is not reused: it contradicts F1's by-design duplicates and the compaction gaps.
Spark's Kafka source with `failOnDataLoss=true` did not fail on compaction gaps in the G8 spike (ADR-0019), so unread intermediate versions are never ingested and bronze gaps are expected on compacting topics. They would be real loss only if `retention.ms` advanced the log start past unread offsets; the 24 h backstop is far longer than a check run.
## Limits
- **Rebuilding from the topic is not safe after F2 on a compacting topic.** F2 re-produces an older version of a key with identical bytes. That record has the highest offset for its key, so compaction may keep it and drop the newer record. Silver stays correct, because it already applied the newer LSN and the MERGE ignores the older one. A rebuild **from the topic** would apply the older value last by offset and be wrong. Bronze retains the LSN-highest version and is the record to rebuild from, so the silver-vs-bronze pair is the valid one; a topic-sourced rebuild is not offered. This ordering effect on a compacting topic is **unverified**: it follows from log compaction semantics and has not been observed in a run.
- The strict no-gap check protects only default topics. On compacting topics a record lost between Kafka and bronze that compaction had not yet removed is still caught by the subset check, but a record both lost and compacted is indistinguishable from an ordinary gap.
- Compacting topics keep records for at least the minimum compaction lag, so F2's raw read depends on the records being younger than that lag.
- Running G10a on both settings adds a second full run. Whether both runs are kept as committed manifests of record is open decision D3; the default is both.
- One machine; no claim about other brokers or settings.

## Dated notes
- **2026-11-05, what the compacting run exercises.** Kafka-side compaction gaps are exercised live: on `--topics compact` the END check "compaction physically removed records from Kafka (the offset-gap path was exercised)" requires at least one topic to hold fewer records than its end offset total, so the present-record reader and the subset check are shown to cope with a topic that has lost records. Bronze-side offset gaps (`gaps` in `compare_offsets`, which arise when forget DELETEs remove bronze rows, as in g8) are **not** exercised by `g10a-check`: bronze consumed every offset before compaction ran, and g10a runs no forget. That path is covered by unit tests only.
