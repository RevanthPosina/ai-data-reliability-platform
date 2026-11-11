# ADR-0026: Double-entry ledger (`fct_ledger_entries`)
- Status: accepted (G4, 2026-11-10); written before any G4 run, so nothing here is a result. Items marked **unverified** are not yet observed. The Measured section is added after the run of record.
## Decision
**Source semantics modelled** (`src/adrp/gen/model.py`, `infra/postgres/schema/shop.sql`): a customer account opens with a non-zero `balance` and no transaction; a merchant account opens at 0.00. `topup` has a NULL payer (external funding) and raises the payee. `payment` lowers the payer and raises the payee (a merchant). `refund` (one per transaction, full or half of a payment) lowers the merchant payee and raises the customer payer. Postgres changes the balance in the same transaction as each money movement. The generator never updates merchants, never updates or removes transactions or refunds, and never removes customers; customers are removed only by forget (logical removal, ADR-0017).
**Entries** have a signed `amount decimal(14,2)` where a plus sign raises the account's balance. `entry_id = journal_type || ':' || source_id || ':' || leg` and `journal_id = journal_type || ':' || source_id`. Two external contra accounts carry negative ids: `-1` is `external:funding` and `-2` is `external:opening_equity`.
| Journal | Leg 1 | Leg 2 |
|---|---|---|
| `opening` (only when the balance is non-zero) | (account, +balance) | (-2, -balance) |
| `topup` | (payee, +amount) | (-1, -amount) |
| `payment` | (payer, -amount) | (payee, +amount) |
| `refund` (reversing, linked by `transaction_id`) | (original payee, -refund amount) | (original payer, +refund amount) |
A refund is a separate journal of reversing entries. The original payment's entries are never edited.
**Opening entries come from bronze.** The first event per account key (min LSN) with `op = 'c'` gives the opening balance (`after.balance`). Silver has no opening event, because it holds current state only. Accounts whose first event is `op = 'r'` are unsupported (ADR-0025, D9).
**Transactions and refunds come from silver** (decision D2), so a forget re-key flows into gold on the next run without rewriting ledger history.
**No `customer_id` in the ledger.** Entries carry account ids only. A customer is reached through `dim_account`, which is re-keyed when silver is re-keyed. This keeps the ledger out of the forget workflow: accounts survive a forget re-keyed to the placeholder customer.
**Append-only by construction.** `incremental_strategy='append'`; the model selects only rows whose `entry_id` is not in `{{ this }}` (anti-join); and `full_refresh: false` is set, so `dbt build --full-refresh` cannot rebuild it. Tests do not use `store_failures`.
**Reconciliation is defined** as equality of the ledger's per-account sum (`account_id > 0`) with Postgres `accounts.balance`, for every account, at a quiescent cut. A quiescent cut is: the generator finished, scripted writes done, silver drained (`Drain.wait_silver()` true), then a build. Silver tables are merged one after another and are mutually consistent only at such a cut, so the singular test `assert_ledger_balance_matches_silver_accounts` has severity `error` only with `--vars '{strict_reconcile: true}'` and `warn` otherwise. Two further singular tests: every journal nets to zero, and every transaction (the payment plus its refund reversal) nets to zero.
**A backfill touches the ledger by business date** (ADR-0027): a pre-hook removes (logically, with an Iceberg `DELETE`) the day's rows and the model reselects them.
## Limits
- "Append-only by construction" is a design property, not a proof of immutability. The run of record reports that entries were unchanged across the builds and the backfill in that run. The words "immutable ledger" are not used as a result.
- "Balances reconcile" is scoped: all n accounts matched Postgres `accounts.balance` at a quiescent cut, in one run on one machine. It is not an always-true claim, and the test is `warn` outside the cut.
- After a forget, a ledger rebuild from bronze would lose opening entries of footprint accounts (ADR-0017 removes the footprint's bronze rows). `full_refresh: false` guards this, and a backfill after a forget would also drop opening entries of affected accounts. Whether to guard or repair this is a G8b decision (ADR-0029).
- The ledger does not model fees, currencies other than the row's `currency`, or concurrent writers.

## Measured (G4 run of record, code commit `b3202eb`, clean tree, one machine)
Manifest: `runs/manifests/20261111T072440Z-g4-check.json`, 37 of 37 checks. One run on one machine; values are counts in that run.
- **Nets to zero.** Build 1: 761 journals and 507 transactions (a payment plus its refund reversal, grouped by `transaction_id`), 0 not netting in either grouping. Build 2: 1,129 journals and 827 transactions, 0 not netting.
- **Balances at a quiescent cut** (generator finished, scripted writes done, silver drained, then a build). Every Postgres `accounts.balance` was compared with the ledger sum of `account_id > 0`. Build 1: 243 accounts, 0 mismatches, 0 ledger-only accounts, 0 accounts without entries. Build 2: 262 accounts, 0 mismatches, 0 ledger-only accounts, 1 account without entries (compared as a sum of 0.00 and matched). The strict test (`strict_reconcile`) was an error in all five builds, and none failed.
- **Append-only.** 1,522 entries existed after build 1; after build 2 and again after build 4 (the backfill), 0 of them were changed or missing, and 736 had been added since. Build 3 (no new data) left the `entry_id` set unchanged. At build 3 the ledger held 2,258 entries. "Append-only by construction" is the design; these are the observed counts in this run.
- **Re-key.** The ledger entries of the re-keyed transaction (4) were unchanged by the re-key, since the ledger carries no `customer_id`.
- **Not measured.** A ledger rebuild or backfill after a forget (no forget runs in G4; ADR-0029); snapshot-opened accounts (unsupported, and the run starts from empty volumes).
