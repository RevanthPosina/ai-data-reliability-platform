# ADR-0029: Gold is in the data inventory; the forget workflow does not cover it until G8b
- Status: accepted (G4, 2026-11-10); written before any G4 run, so nothing here is a result. Items marked **unverified** are not yet observed. The Measured section is added after the run of record.
## Decision
**Gold is tracked now and is not covered by deletion propagation until G8b.** G4 adds inventory rows 35 to 44 to design §3 (all Milestone B). The forget workflow (ADR-0017, `src/adrp/spark/forget.py`) is **not changed in G4** (decision D10). A change would force a g8 rerun of about 50 minutes and is out of G4's scope.
**Consequence for a forget on a stack where gold has been built.** The physical verifier scans the whole `s3://lake/` bucket (ADR-0018). Gold files that still hold the subject's attributes would therefore make that verifier fail, which is the correct signal. g8-check starts from clean volumes and never builds gold, so the G8 regression is unaffected.
**Inventory rows** (all Milestone B; G4 creates them, G8b changes what they hold). The wording is logical or physical in every row.
| Row | Location | What it holds | Deletion story |
|---|---|---|---|
| 35 | `gold.dim_customer` | PII, all versions | logical: G8b removes the subject's versions; physical: the ADR-0018 procedure extended to gold in G8b; the whole-bucket scan already sees its files |
| 36 | `gold.fct_transactions` | `customer_id`, `customer_sk` | `customer_id` is pseudonymized by the re-key on the next dbt run (checked in the fixture and the live run); `customer_sk` is re-pointed in G8b |
| 37 | `gold.fct_ledger_entries` | account ids only | never removed; accounts survive, re-keyed |
| 38 | `gold.dim_account` | `customer_id` | re-keyed via silver |
| 39 | `gold.dim_merchant` | no customer data | none |
| 40 | `gold_snapshots.snap_customer_attrs` | `customer_id` plus attribute hash (pseudonymous) | G8b removes by key (logical, then physical) |
| 41 | `gold.build_log` | LSNs and times only | none |
| 42 | `dbt_gold/target`, `dbt_gold/logs` | no row data, by rule | canary grep in g4-check |
| 43 | Trino | no persistent storage; query history in memory until restart | spill disabled by default |
| 44 | `gold_rebuild` (test-run only) | copy of gold | dropped (logical removal, then expired by maintenance) at the end of g4-check |
The fixture `fx` catalog holds synthetic data in a throwaway container and gets a note, not a row.
**G4 designs gold so that G8b is mechanical:**
- no `customer_id` in the ledger (ADR-0026);
- a forget re-key flows into `fct_transactions` and `dim_account` on the next run;
- staging is ephemeral, so there are no gold views or extra locations;
- physical removal uses Lakekeeper hard-delete of the table or an expire-and-remove procedure (ADR-0018), not a gold-specific mechanism;
- the snapshot holds hashes and keys only;
- no `store_failures`, so no failure-row copies (ADR-0028).
## Limits
- Until G8b, no claim is made that a forgotten customer is gone from gold, logically or physically. `dim_customer` holds PII for every version (ADR-0025, D1).
- G8b must decide how a backfill or rebuild after a forget treats the opening entries of footprint accounts (ADR-0026).
- The ADR-0017 policy is "re-key, then remove"; G4 only checks that a re-key flows into `fct_transactions` and leaves the ledger unchanged. It does not run a forget.

## Measured (G4 run of record, code commit `b3202eb`, clean tree, one machine)
Manifest: `runs/manifests/20261111T072440Z-g4-check.json`, 37 of 37 checks. Rows 35 to 44 are now in design §3. G4 ran no forget, so nothing here measures deletion.
- **Re-key flows to gold.** After a psql `UPDATE` re-keying one transaction from A to B and build 2, `fct_transactions` showed the placeholder customer, the row's other columns were unchanged, and the 4 ledger entries of that transaction were unchanged (row 36 and row 37, logical re-key only; no physical removal is claimed).
- **Artifact hygiene (row 42).** 113 files under `dbt_gold/target` and `dbt_gold/logs` were scanned for the canary email and the scripted raw ids: 0 files with a hit.
- **`gold_rebuild` (row 44).** Dropped at the end of P5 (8 tables). That is a logical removal in the catalog; physical removal follows maintenance (ADR-0018) and was not measured.
- **Smoke on a used stack (Step D, ADR-0030).** The one warning there (1 account whose bronze opening event was physically removed by an earlier forget) is the limit above in action, not a model defect.
- **Open limit.** Gold is not covered by the forget workflow until G8b. `dim_customer` holds PII for every version, and no claim is made that a forgotten customer is gone from gold, logically or physically.
