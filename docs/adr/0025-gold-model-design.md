# ADR-0025: Gold model design (SCD2 from the bronze change log, facts from silver)
- Status: accepted (G4, 2026-11-10); written before any G4 run, so nothing here is a result. Items marked **unverified** are not yet observed. The Measured section is added after the run of record.
## Decision
Gold is a dbt project (`dbt_gold/`, dbt-core 1.12.5 with dbt-trino 1.10.6 on Trino 483, ADR-0008) that writes Iceberg format-version 2 tables (ADR-0004) through the Lakekeeper REST catalog. No dbt packages are used (no `dbt deps`). The models and their grain:
| Model | Grain | Source |
|---|---|---|
| `dim_customer`, `dim_merchant` | one row per key version (SCD2) | bronze change log |
| `dim_date` | one row per calendar day | generated sequence |
| `dim_account` | one row per account (type 1) | silver `accounts` |
| `fct_transactions` | one row per transaction id | silver `transactions`, `refunds` |
| `fct_ledger_entries` | one row per (journal, leg) | bronze openings, silver `transactions`, `refunds` (ADR-0026) |
**Dimensions come from bronze, facts from silver.** Silver keeps only the current state, so a Type 2 history needs the change log. Facts need only current state, and silver keeps the transactions after a forget re-key (ADR-0017), so facts built from silver survive the forget's bronze removal.
**SCD2 rules for `dim_customer` and `dim_merchant`:**
- Order is by LSN, per key (the per-key LSN total order of ADR-0013).
- Staging dedupes to one row per `(pk, lsn)`, keeping the lowest `(kafka_partition, kafka_offset)`. This absorbs the F1 replay and F2 re-produced duplicates that bronze holds by design.
- **Same-transaction collapse:** the max `lsn` per `(pk, txid)` is kept, so a transaction yields one version with its final values and no zero-length interval.
- Consecutive versions with an identical attribute hash (excluding `updated_at`) are merged.
- `valid_from_lsn` and `valid_to_lsn` are the authoritative interval. `valid_from` and `valid_to` are the commit time (`source_ts`, millisecond precision) and are descriptive only.
- A delete event closes the current version (`closed_by_delete`) and adds no new version. Tombstones (`op` NULL) are ignored.
- Rebuild is by affected key: the whole history of every key with new bronze rows is rebuilt from bronze (`incremental`, `delete+insert`, `unique_key` the entity id). Cursor and lookback are in ADR-0027.
- The dimension keeps the PII columns (decision D1: realistic SCD2, exact comparison with ground truth). Gold is therefore in the data inventory (ADR-0029).
**Facts and the point-in-time join.** `fct_transactions` carries `customer_sk` and `merchant_sk`, chosen as the dimension version with `valid_from_lsn <= first_lsn < coalesce(valid_to_lsn, 9e18)`, where `first_lsn` is the first LSN at which the transaction was seen. `first_lsn`, `customer_sk` and `merchant_sk` are carried forward from the existing row of `{{ this }}` with `coalesce(existing, new)`, so a forget re-key changes `customer_id` only and does not rewrite history. `description` is not carried (data minimization).
**dbt snapshot as the comparison only.** `snapshots/snap_customer_attrs` (strategy `check` on `attr_hash`, over silver customers that are not logically removed) exists to show what a snapshot taken at dbt-run frequency captures against the CDC-built SCD2. It is not used by any mart. It holds keys and a hash only.
**Staging is ephemeral.** Staging models (`stg_bronze__*`, `stg_silver__*`) are `ephemeral`, so no Trino views are created in Lakekeeper (no extra locations in the inventory).
**Snapshot-opened accounts are unsupported** (decision D9). An account whose first bronze event is `op = 'r'` has no opening entry; `assert_no_snapshot_opened_accounts` (severity `error`) documents this, and the g-checks start from empty volumes.
**Models are not partitioned** (D11; layout belongs to G12).
## Limits
- **Point-in-time join by change LSN.** The LSN is not commit order under concurrent writers, and commit times have millisecond precision. The join is exact for the single-threaded generator and only approximate with concurrent writers. This is stated wherever the join is described and is listed in the design §8 threats.
- The SCD2 claim is scoped: "matched ground truth for the scripted sequence and the generator's customers in this run", on one machine. SCD2 is not claimed lossless.
- A full refresh of a bronze-derived dimension after a forget loses the subject's history, because the forget removed that history from bronze (logical removal; physical removal follows maintenance, ADR-0018). That is intended for dimensions. It is the reason the ledger is not rebuilt from bronze (ADR-0026).
- Unverified until the fixture and the run of record: dbt-trino `delete+insert` with a subquery filter, contracts on incremental models, YAML snapshots, and unit tests with sources on Iceberg (ADR-0030).
