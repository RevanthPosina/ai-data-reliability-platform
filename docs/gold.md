# Gold layer (G4): models, keys, contracts and runbook

Gold is the dbt project [`dbt_gold/`](../dbt_gold/) (dbt-core 1.12.5, dbt-trino 1.10.6, Trino 483) writing Iceberg format-version 2 tables through the Lakekeeper REST catalog. Decisions: [ADR-0025](adr/0025-gold-model-design.md) (models), [ADR-0026](adr/0026-double-entry-ledger.md) (ledger), [ADR-0027](adr/0027-incremental-cursors-lookback-backfill.md) (cursors, lookback, backfill), [ADR-0028](adr/0028-model-contracts-trino-iceberg.md) (contracts), [ADR-0029](adr/0029-gold-in-the-inventory-until-g8b.md) (inventory), [ADR-0030](adr/0030-g4-verification-layering-trino-sizing-sleep-guard.md) (verification). Measured values are in those ADRs and in the run of record `runs/manifests/20261111T072440Z-g4-check.json` (one run, one machine); none are repeated here.

**Gold is not covered by the forget workflow until G8b.** Gold tables hold PII (`dim_customer`, every version) and customer ids; see design §3 rows 35 to 44.

## Models

| Model | Grain | Key | Source | Materialization |
|---|---|---|---|---|
| `dim_customer` | one row per customer version (SCD2) | `customer_sk` = hex xxhash64 of `customer_id:valid_from_lsn`; natural key `customer_id` | bronze change log | incremental, `delete+insert` on `customer_id` |
| `dim_merchant` | one row per merchant version (SCD2) | `merchant_sk`; natural key `merchant_id` | bronze change log | incremental, `delete+insert` on `merchant_id` |
| `dim_date` | one row per calendar day (default 2025-12-01 to 2027-12-31) | `date_key` (yyyymmdd) | generated sequence | table |
| `dim_account` | one row per account (type 1) | `account_id` | silver `accounts` | table |
| `fct_transactions` | one row per transaction id | `transaction_id` | silver `transactions` and `refunds` | incremental, `delete+insert` on `transaction_id` |
| `fct_ledger_entries` | one row per (journal, leg) | `entry_id` = `journal_type:source_id:leg` | bronze account openings, silver `transactions` and `refunds` | incremental `append`, `full_refresh: false` |
| `gold_snapshots.snap_customer_attrs` | one row per customer and attribute-hash change seen by dbt | `customer_id` | silver `customers` (hash of email, phone, address) | dbt snapshot, `check` strategy; the comparison with SCD2 only |
| `gold.build_log` | one row per model per dbt invocation | none | `on-run-end` hook | appended; LSNs and times only |

Staging models are ephemeral (no Trino views in the catalog).

**SCD2 rules.** Per key, versions are ordered by change LSN. Bronze duplicates are collapsed to one row per `(pk, lsn)`. Within one transaction only the last change is kept, so there is no zero-length interval. Consecutive versions with an identical attribute hash (excluding `updated_at`) are merged. `valid_from` is the commit time (millisecond precision), `valid_to` is the next version's `valid_from` or the delete's commit time, and `valid_from_lsn` and `valid_to_lsn` are the authoritative interval. `is_current` marks the open version.

**Point-in-time join.** `fct_transactions.customer_sk` is the `dim_customer` version whose LSN interval contains the transaction's first change LSN. `first_lsn`, `customer_sk` and `merchant_sk` are carried forward from the existing row, so a forget re-key changes only `customer_id`. The LSN is not commit order under concurrent writers, so the join is exact only for a single writer (design §8).

**Ledger.** Amounts are signed (a positive amount raises the account's balance). Two external contra accounts: `-1` (`external:funding`) and `-2` (`external:opening_equity`). Journal types: `opening` (only for a non-zero opening balance), `topup`, `payment`, `refund` (reversing legs linked to the transaction by `transaction_id`). There is no `customer_id`; join through `dim_account`. Accounts opened by a snapshot read (`op = 'r'`) are unsupported, and `assert_no_snapshot_opened_accounts` documents it.

## Contracts

Every mart has `contract: {enforced: true}` and a `data_type` on every column. Keys and amounts carry `not_null` constraints, **the only constraint Trino and Iceberg enforce**. `primary_key`, `unique` and foreign keys are metadata, backed by the dbt tests for the rows present at build time. The snapshot has no enforced contract (ADR-0028 amendment).

## Tests

Generic tests in `models/marts/schema.yml` and singular tests in `dbt_gold/tests/`: ledger nets to zero per journal and per transaction; ledger balance equals silver `accounts.balance` (an `error` only with `--vars '{strict_reconcile: true}'`, which is valid only at a quiescent cut, because silver tables are merged one after another; otherwise `warn`); SCD2 no overlap, contiguous, one current row, no zero-length LSN interval; `customer_sk` as-of join; refund not above the payment; and the two completeness tests below. `store_failures` is never set.

## Runbook

Prerequisite for all commands: Trino is running on `:8080` (`docker compose --env-file .run/compose.env --profile transform up -d trino`, after the core stack is up), and `make` targets use the project venv.

**Build.** `make gold-build` runs `dbt build` (models, tests and the snapshot; unit tests are excluded) against target `dev`, schema `gold`. A rerun on unchanged inputs is expected to leave the gold tables unchanged; the run of record observed that for its inputs, and it is not a general guarantee. At a quiescent cut (generator stopped, silver caught up) add `--vars '{strict_reconcile: true}'` to run the balance reconciliation as an error.

**Incremental window and late arrival.** Each incremental model reads rows newer than the table's own maximum cursor minus `gold_lookback_minutes` (default 60): `kafka_ts` for the bronze-derived dimensions, `source_ts` for silver-derived facts. Rows arriving within the lookback are picked up. Rows arriving later are missed and flagged by `assert_gold_complete_vs_silver` (facts and ledger against silver) and `assert_scd2_complete_vs_bronze` (dimension versions against bronze). Both are verified in the fixture only (ADR-0030).

**One-day backfill.** `make gold-backfill DAY=2026-01-01` (type the date literally; in zsh never build it from one unquoted variable). It reselects only that `business_date` for `fct_transactions` and `fct_ledger_entries`; the ledger's `pre_hook` first deletes that day's entries, a logical removal in an Iceberg snapshot. It needs existing tables and no `--full-refresh` (the build fails otherwise). A backfill day that contains a re-keyed transaction is not checksum-identical, because the incremental run keeps first-seen `first_lsn`, `customer_sk` and `merchant_sk` and a reselect derives them from the current row.

**Full refresh.** Allowed for the dimensions and the facts. **Never for the ledger** (`full_refresh: false`): it is built from bronze account openings, and after a forget the subject's bronze history is gone, so a rebuild would lose opening entries. A full refresh of the bronze-derived dimensions after a forget is likewise lossy for the subject's history. Gold has no forget handling until G8b.

**Recovery from a late arrival beyond the lookback.**
1. A completeness test fails. Do not ignore it, and do not `--full-refresh` the ledger.
2. Facts and ledger: identify the missed rows' `business_date` values and run `make gold-backfill DAY=<date>` for each.
3. Dimensions: they are not backfilled by day. Rebuild them with `dbt build --full-refresh --select dim_customer dim_merchant` (same project and profile flags as `make gold-build`), or widen the window for one run with `--vars '{gold_lookback_minutes: <minutes>}'`.
4. Rerun `make gold-build` and confirm the completeness tests pass. The fixture shows a backfill recovering a late fact on scripted data (ADR-0030); the dimension options in step 3 were not run, and none of this has been run on the live lake.

**Incremental versus from-scratch.** `--target rebuild` builds the same models into schema `gold_rebuild` (and `gold_rebuild_snapshots`) for comparison; drop those tables afterwards (inventory row 44).

**Not covered.** Scheduling (dbt runs from the host; S4 is not measured), partitioning and file layout (G12), concurrent writers for the point-in-time join, and any deletion of gold data (G8b).
