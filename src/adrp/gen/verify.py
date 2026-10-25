"""Check the database against the generator model replayed to gen.state.next_txn.

Compares, for generator-owned rows (id < 1e12), the row count per table and every account balance.
Prints `VERIFY {json}`.
"""

from __future__ import annotations

import json
import os
from decimal import Decimal

import psycopg

from adrp.gen.model import Generator

AD_HOC_ID_START = 1_000_000_000_000


def main() -> None:
    conn = psycopg.connect(os.environ["SHOP_DSN"])
    with conn.cursor() as cur:
        cur.execute("SELECT seed, next_txn FROM gen.state WHERE id = 1")
        row = cur.fetchone()
        assert row is not None, "generator has not run"
        seed, next_txn = int(row[0]), int(row[1])
        gen = Generator(seed)
        gen.skip(next_txn)
        s = gen.s
        expected = {
            "customers": len(s.customers),
            "merchants": len(s.merchants),
            "accounts": len(s.accounts),
            "transactions": s.next_id.get("transactions", 1) - 1,
            "refunds": s.next_id.get("refunds", 1) - 1,
            "support_tickets": len(s.tickets),
            "help_articles": len(s.articles),
        }
        actual: dict[str, int] = {}
        for table in expected:
            cur.execute(f"SELECT count(*) FROM {table} WHERE id < %s", (AD_HOC_ID_START,))
            got = cur.fetchone()
            actual[table] = int(got[0]) if got else -1
        cur.execute("SELECT id, balance FROM accounts WHERE id < %s", (AD_HOC_ID_START,))
        db_bal = {int(i): Decimal(b) for i, b in cur.fetchall()}
    model_bal = {i: a["balance"] for i, a in s.accounts.items()}
    mismatched = sorted(i for i in model_bal.keys() | db_bal.keys() if model_bal.get(i) != db_bal.get(i))
    out = {
        "seed": seed,
        "next_txn": next_txn,
        "counts_match": expected == actual,
        "expected": expected,
        "actual": actual,
        "balance_mismatches": len(mismatched),
        "ok": expected == actual and not mismatched,
    }
    print("VERIFY " + json.dumps(out))


if __name__ == "__main__":
    main()
