# ADR-0027: Incremental cursors, lookback, late arrival and one-day backfill
- Status: accepted (G4, 2026-11-10); written before any G4 run, so nothing here is a result. Items marked **unverified** are not yet observed. The Measured section is added after the run of record.
## Decision
**Cursors.** Models read what is new using a cursor column and a lookback window, computed from `{{ this }}` by the macro `lookback_cutoff(cursor_col)`: `max(cursor_col) - interval '<gold_lookback_minutes>' minute`.
- Bronze-derived models (the two SCD2 dimensions) use `kafka_ts`, the broker arrival time (LogAppendTime). A commit that arrives late at the broker is still picked up, which commit time alone would not guarantee.
- Silver-derived models (`fct_transactions`, the ledger) use `source_ts`, the commit time.
- `gold_lookback_minutes` defaults to **60** (decision D5).
**Window logic.** The dimensions rebuild the whole history of every key with a bronze row after the cutoff. `fct_transactions` takes transactions with `source_ts` after the cutoff, plus transactions whose refund falls in the window. The ledger selects by anti-join on `entry_id` (ADR-0026), so its cursor only limits the scan.
**Late arrival.** Rows arriving within the lookback are picked up. Rows arriving after the window are **missed**. They are caught by completeness tests, not repaired silently:
- `assert_gold_complete_vs_silver`: fact ids against silver ids, and ledger journals against the journals expected from silver.
- `assert_scd2_complete_vs_bronze`: the count of collapsed `(pk, txid)` against dimension versions plus merged no-ops.
These two tests are the named checks for design F7 (G10b). The original F7 row in design §5 is not edited; the names are a dated note.
**One-day backfill.** With `var('backfill_day')` set, `fct_transactions` and `fct_ledger_entries` select only that `business_date` (`date(created_at AT TIME ZONE 'UTC')`). A `pre_hook`, guarded by `is_incremental()`, runs `DELETE FROM {{ this }} WHERE business_date = DATE '<day>'`, which is a logical removal of the day's rows in an Iceberg snapshot. Run with `make gold-backfill DAY=<date>`; in zsh the day is typed literally and never built from one unquoted variable. Dimensions are not backfilled by day: their recovery is a rebuild of the affected keys.
**Full refresh rules.** `--full-refresh` is allowed for dimensions and facts and **forbidden for the ledger** (`full_refresh: false`). A full refresh of bronze-derived models after a forget is lossy for the subject's history (ADR-0025, ADR-0026).
**Idempotency is claimed narrowly.** Rerunning `dbt build` on unchanged inputs is expected to leave every gold checksum unchanged and the ledger's `entry_id` set unchanged. The claim is made as "idempotent under the tested faults" or as the observed fact for the run ("left every gold checksum unchanged in this run"), never as a general guarantee. An incremental build equals a from-scratch build is checked with `--target rebuild` into `gold_rebuild` (ADR-0030).
**Build log.** An `on-run-end` hook appends `(invocation_id, completed_at, model, max_src_lsn)` to `gold.build_log` (decision D7). It is the future capture point for design S4. S4 is **not measured** in G4, because there is no schedule (dbt is run from the host, decision D8; scheduling is G11b/G13).
## Limits
- "Picks up rows arriving within the 60-minute lookback; later rows are flagged by completeness tests (fixture-verified)" is the only late-data wording. G4 does not claim that it handles late data.
- The lookback is a trade-off: a longer window costs more rebuilt keys per run. The value 60 is a default, not a tuned figure; it is not tuned in G4.
- The backfill check is non-vacuous only if the backfill snapshot shows both `deleted-records > 0` and `added-records > 0` (ADR-0030).
- The checksum used for "identical results" is the sum of `xxhash64` over canonical rows (`adrp.recon.canonical.digest`). It detects differences in the compared columns on this data, and is not a proof of equality of all possible content.
- A backfill after a forget would drop the opening entries of footprint accounts (ADR-0026); that is left to G8b.
