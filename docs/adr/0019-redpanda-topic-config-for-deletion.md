# ADR-0019: Redpanda topic configuration for deletion
- Status: accepted (G8); values verified on Redpanda v26.2.4 in the G8 spike, forget-lag on the real topics to be measured in g8-check
## Decision
Debezium creates the `cdc.public.*` topics with no cleanup policy, so a tombstone or a deleted key's old value would stay forever. For G8 the topics get `infra/redpanda/topic-config.json`, applied by `bootstrap.py --forget-ready-topics`.
| Setting | Spike-verified | Why |
|---|---|---|
| `cleanup.policy` | `compact,delete` | compaction drops superseded values; delete is the backstop (`retention.ms` 24 h) |
| `segment.ms` | 10-30 s on spike topics | the active segment is never compacted; it rolls on this timer even with no writes |
| `min.cleanable.dirty.ratio` | 0.01 (default 0.5) | small topics otherwise never reach the ratio |
| `min.compaction.lag.ms` / `max.compaction.lag.ms` | spiked at 0 and 45 s; g8-check applies 300 s / 600 s | min lag keeps every record readable for 5 min so a stopped Spark reader is not raced by compaction during the window; Redpanda rejects max < min; the price is broker physical absence of about 5 min + `segment.ms` + housekeeping after the nudge write (to be measured in g8-check) |
| `delete.retention.ms` | 10 s | topic-level is enough; the cluster `tombstone_retention_ms` stayed unset |
| cluster `log_segment_ms_min` | lowered from 600000 to 10000, then restored | see below |
The values in `topic-config.json` are the ones g8-check applies; any value not in the table (for example the 300 s / 600 s lags) was not spiked and is to be measured in g8-check.
## Findings that drive the procedure
- **`segment.ms` is silently clamped.** With `log_segment_ms_min=600000`, `segment.ms=60000` is accepted and shown by `describe`, but the segment did not roll in 134 s. Lowering the cluster value takes effect without a restart (housekeeping tick 10 s).
- **A tombstone does not vanish by itself.** On an idle topic it stayed in the compacted segment for 5+ minutes. It went only after a later write rolled a new segment and that was compacted (about 40-45 s after the write). The workflow therefore writes a **nudge** (an update to a customer that is not a subject) to each topic, then waits. Without it the key bytes outlive the record bytes.
- Measured on idle spike topics: record bytes gone about 10 s after the roll; with a 45 s min lag, present at +41 s and gone at +51 s. The log start offset does not move with compaction.
- Because the re-key (ADR-0017) writes a newer record for the same primary key, compaction can drop the old value that carried the raw `customer_id` in `accounts` and `transactions` topics; the deleted ticket and customer keys end as tombstones that the nudge then clears.
## Why a raw byte scan is valid here
Kafka Connect's producer default is `compression.type=none` and `pg-shop.json` overrides nothing; the topic is `compression.type=producer`. A real customer email was found in plaintext in `.../cdc.public.customers/0_16/0-1-v1.log`. g8-check asserts the producer compression setting before it relies on grep. Segment files are 32 MiB preallocated and sparse; `grep -F` still finds the active segment's contents. The scan covers the whole `kafka/cdc.public.*` directory (including index files) for the canary and for the Confluent-framed key bytes (`0x00`, schema id, zigzag varint id), plus a decoding consumer scan from the earliest offset.
## Consequences for readers
- Spark's Kafka source with `failOnDataLoss=true` did **not** fail on compaction gaps, in batch and in streaming (tested); it compares against the log start offset, which compaction does not move. Unread intermediate versions are silently never ingested, so bronze offsets will have gaps and a "no offset gaps" assertion must tolerate them on compacted topics. It would fail if `retention.ms` advances the log start past unread offsets; 24 h is far longer than a g8-check run.
- Compaction also discards earlier versions of a key that g2/g3/g6 read from offset 0 and compare. Those checks therefore run on the Debezium defaults and are unchanged; only g8-check applies this config. A general deployment would not enable it without this trade-off.
## Limits
Other tables' topics carry `customer_id` in values; they are cleaned only by compaction after the re-key or by `retention.ms`. A value that is never superseded is therefore bounded by `retention.ms`; to be measured in g8-check.
