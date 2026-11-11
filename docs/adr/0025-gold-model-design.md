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

## Measured (G4 run of record, code commit `b3202eb`, clean tree, one machine)
Manifest: `runs/manifests/20261111T072440Z-g4-check.json`, 37 of 37 checks, seed 42, Apple M1 Pro with a Docker Desktop VM of 4 CPUs and about 8 GB (`MemTotal 8233013248`), one run. All values are read from that manifest. The data is the generator's 1,000 transactions, then 500 more, plus the scripted rows A, B and M.
- **Builds.** Five `dbt build` runs (builds 1 to 4 on target `dev`, build 5 on target `rebuild`) each ran 6 models, 36 tests and 1 snapshot with 0 non-passing nodes, in 9.8 to 12.5 s each. Unit tests are excluded from live builds (fixture only).
- **SCD2 against the generator's ground truth.** `dim_customer` versions per customer equalled the generator ops in LSN order, with `valid_from` equal to the run-log commit time truncated to ms and `txid` equal to `xid32`: build 1, 217 customers, 252 versions, 0 differing; build 2, 232 customers, 283 versions, 0 differing.
- **SCD2 against the oracle for the scripted sequence.** Build 1: A, B and M each 1 expected, 1 held, 0 differing. Build 2, after several changes between the two builds: A 4 expected, 4 held; B 2 and 2; M 3 and 3; 0 differing.
- **Structure.** In both builds every key had exactly one current row, with contiguous LSN intervals and no zero-length interval (build 2: `dim_customer` 289 rows, `dim_merchant` 31 rows, 0 violations).
- **Dedupe and collapse.** The manifest has no check that injects duplicate or out-of-order bronze rows. Duplicate absorption and same-transaction collapse are covered by the fixture (ADR-0030: 70 FX checks passed) and the oracle tests, not by this manifest.
- **Snapshot comparison.** The dbt snapshot, taken at 2 dbt runs, captured 242 of the 283 generator customer versions, so 41 were not captured in this run. The oracle predicted 41. For scripted A, 4 versions and 2 captured, so 2 were lost (asserted); for B, 2 versions and 2 captured. This is the schedule of this run, not a general rate.
- **Re-key.** After build 2, the re-keyed transaction showed the placeholder customer in `fct_transactions` while its other columns (including `customer_sk` and `first_lsn`) were unchanged and `src_lsn` had advanced.
- **Not measured.** The point-in-time join under concurrent writers (the generator is single-threaded); late arrival and the lookback (fixture only, ADR-0027); S4.
- The items listed as unverified in Limits above were exercised without a failure by the fixture and the five live builds (`delete+insert` incrementals, the YAML snapshot, contracts on the incremental marts). "Exercised without failure on this data" is the claim, not a general support statement for dbt-trino.
