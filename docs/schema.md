# Source schema, PII columns and the data generator (G1)

DDL: [`infra/postgres/schema/shop.sql`](../infra/postgres/schema/shop.sql). Database `shop`, schema `public`.

## Tables

| Table | Grain | Key columns | PII columns | Replica identity | Changes the generator makes |
|---|---|---|---|---|---|
| `customers` | one customer | `id` | `full_name`, `email`, `phone`, `address`, `date_of_birth` | FULL | insert; update of email/phone/address |
| `merchants` | one merchant | `id` | none | default | insert |
| `accounts` | one ledger account (one per customer, one per merchant) | `id`; FK `customer_id` / `merchant_id` | `customer_id` links to a person | default | insert; `balance` update in the same transaction as every money movement |
| `transactions` | one money movement (`payment` or `topup`) | `id`; FKs to customer, merchant, payer/payee account | `customer_id` links to a person | default | insert only (immutable) |
| `refunds` | at most one refund per payment | `id`; `transaction_id` unique | none directly (joins to a customer via the transaction) | default | insert only (immutable) |
| `support_tickets` | one ticket | `id`; FK `customer_id` | `customer_id`; `body` is free text and ~30% of tickets contain the customer's name and email | FULL | insert; status/body update; delete |
| `help_articles` | one article (RAG corpus) | `id` | none | FULL | insert (~20% with a ~10 KB body, stored out of line by TOAST); update (70% non-text `view_count`, 30% body edit); delete |
| `dbz_heartbeat` | Debezium heartbeat target | `id` | none | default | Debezium only |
| `gen.state` | generator progress (one row) | `id = 1` | none | — (schema `gen`, outside the publication) | `next_txn` updated in every generator transaction |

**Ids:** the generator supplies ids 1, 2, … per table. Ad-hoc inserts (smoke test, G11 prober) use the identity default, which starts at 10^12, so the two never collide.

**Money:** `numeric(12,2)` / `numeric(14,2)`, exact `Decimal` arithmetic in the generator. A payment debits the customer account and credits the merchant account. A refund reverses part or all of a payment. A top-up credits a customer account from outside (no payer account). Customer balances never go negative: an unaffordable payment becomes a top-up.

## Data generator

Code: [`src/adrp/gen/model.py`](../src/adrp/gen/model.py) (pure, seeded) and [`runner.py`](../src/adrp/gen/runner.py) (applies to Postgres).

- **Determinism:** the generator never reads the database. NumPy `default_rng(seed)` + `Faker.seed_instance(seed)` + an in-memory state model fully determine every id, value and logical timestamp. The same seed gives the same sequence for the same pinned package versions (Faker 40.43.0, NumPy 2.5.4; recorded in each manifest).
- **Logical transactions:** bootstrap (20 merchants, 200 customers, 30 articles), then a weighted mix (see `MIX`): payment 58, ticket insert 10, ticket update 8, top-up 6, refund 5, customer update 5, customer insert 3, article update 3, ticket delete 2, article insert 1, merchant insert 0.5, article delete 0.5.
- **Skew:** customers are picked by Zipf(a = 1.3) over creation order, so early customers are far more active (unit test: the top 10% of customers account for > 50% of transactions).
- **Stable ids and per-entity sequence:** every op carries `event_seq` (1 for the insert, +1 per later change).
- **Resume:** each logical transaction commits together with `gen.state.next_txn`. A later run replays the model to that point (no DB writes) and continues the same sequence.
- **Pacing:** `--rate` in row changes per second (0 = unthrottled). Every run reports the **achieved** rate after warmup and `fell_behind` (achieved < 95% of target).

### Logs (Parquet, under `runs/data/<run_id>/`, git-ignored, contain synthetic PII)
| Log | Columns | Purpose |
|---|---|---|
| `ground_truth/` | `seq, txn_no, op, table, pk, event_seq, row_json` | logical operations; the oracle for G3/G10 |
| `run_log/` | `txn_no, kind, n_ops, txid (xid8), xid32, client_begin_ns, client_commit_ns, commit_ts` | physical execution: Postgres txid and commit time per logical transaction. `xid32` joins to Debezium `source.txId` (G11). |

Both are inventory row 16 in [design.md](design.md) and are cleaned by the forget workflow (G8).

### Determinism check (G1 done condition)
`make gen-determinism` applies 5,000 transactions, then checks that: two dry runs give the same digest; the DB run's digest equals the dry run's; the Parquet ground truth recomputes to the same digest; seed + 1 differs; every run-log row has a commit timestamp; and DB row counts and every account balance match the model. The digest is SHA-256 over `(seq, op, table, pk, payload)` per op.
Run of record: all 6 checks passed, txns [103938, 108938), 11,400 ops, clean tree at `e06a844`. Manifest: `runs/manifests/20261025T221413Z-gen-determinism.json`. (An earlier run on uncommitted code, txns [5000, 10000), also passed: `20261025T220706Z-gen-determinism.json`.)

### Maximum sustainable change rate
Definition: generator → Postgres, unthrottled, one connection, one logical transaction per DB transaction, **core profile running** (CDC, Spark and the embedder consuming at the same time), Docker VM 8 GB / 4 CPU on an Apple M1 Pro.
Protocol: 5 repetitions × 30 s, first 5 s of each discarded.

Run of record (clean tree at `801b56b`, manifest `runs/manifests/20261025T222234Z-gen-max-rate.json`):

| Metric | Median | Min | Max |
|---|---|---|---|
| Row changes / s | 1350.9 | 1086.2 | 1379.4 |
| Logical transactions / s | 593.8 | 478.2 | 604.9 |

Variance across runs is large. Two earlier runs with the same protocol, on uncommitted code, gave medians of 1489.9 and 1565.9 changes/s (`20261025T221023Z-gen-max-rate.json`, `20261025T221754Z-gen-max-rate.json`). Within the run of record the per-rep range was 1086.2–1379.4. Likely causes, not isolated: contention from the concurrently running pipeline on 4 VM CPUs, and the database growing across runs (the run of record covered txns 212,905–301,107). Treat the ceiling as roughly 1.1k–1.6k changes/s on this machine, and always report the achieved rate per run.

This is the generator's ceiling into Postgres. Whether the *pipeline* keeps up at a given rate is a separate question, measured in G11 (freshness vs achieved rate).
