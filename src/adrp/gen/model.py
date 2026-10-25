"""Deterministic logical-operation generator for the payments marketplace (PLAN G1).

The generator never reads the database. Every id, timestamp and value comes from the seeded RNGs
and an in-memory model of the state, so `(seed, txn range)` fully determines the operation stream:
the same seed gives the same sequence, and the ground truth can always be regenerated.

One logical transaction = the ops one Postgres transaction applies (e.g. a payment inserts a
transaction row and updates two account balances). Ops carry the full row after the change.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
from faker import Faker

from adrp.common import canonical_json

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
CENT = Decimal("0.01")

# Logical transaction mix (weights, normalized at load). Bootstrap transactions come first.
MIX: dict[str, float] = {
    "payment": 58.0,
    "topup": 6.0,
    "refund": 5.0,
    "new_customer": 3.0,
    "update_customer": 5.0,
    "new_merchant": 0.5,
    "new_ticket": 10.0,
    "update_ticket": 8.0,
    "delete_ticket": 2.0,
    "new_article": 1.0,
    "update_article": 3.0,
    "delete_article": 0.5,
}
BOOTSTRAP = {"merchants": 20, "customers": 200, "articles": 30}
ZIPF_A = 1.3  # customer activity skew: low ranks (early customers) are far more active
TICKET_FLOW = {"open": "pending", "pending": "resolved", "resolved": "closed", "closed": "closed"}


@dataclass(frozen=True)
class Op:
    seq: int
    txn_no: int
    op: str  # I | U | D
    table: str
    pk: int
    event_seq: int  # per-entity sequence number: 1 for the insert, +1 per later change
    row: dict[str, Any] | None  # full row after the change; None for deletes

    def identity(self) -> str:
        """The hashed identity of one op: (seq, op, table, pk, payload) (PLAN G1 done condition)."""
        return canonical_json([self.seq, self.op, self.table, self.pk, self.row])


@dataclass(frozen=True)
class Txn:
    txn_no: int
    kind: str
    ops: tuple[Op, ...]


@dataclass
class _State:
    next_id: dict[str, int] = field(default_factory=dict)
    event_seq: dict[tuple[str, int], int] = field(default_factory=dict)
    customers: dict[int, dict[str, Any]] = field(default_factory=dict)
    customer_order: list[int] = field(default_factory=list)  # creation order = activity rank
    merchants: dict[int, dict[str, Any]] = field(default_factory=dict)
    merchant_ids: list[int] = field(default_factory=list)
    accounts: dict[int, dict[str, Any]] = field(default_factory=dict)
    customer_account: dict[int, int] = field(default_factory=dict)
    merchant_account: dict[int, int] = field(default_factory=dict)
    refundable: list[int] = field(default_factory=list)  # recent payment ids without a refund
    payments: dict[int, dict[str, Any]] = field(default_factory=dict)
    tickets: dict[int, dict[str, Any]] = field(default_factory=dict)
    ticket_ids: list[int] = field(default_factory=list)
    articles: dict[int, dict[str, Any]] = field(default_factory=dict)
    article_ids: list[int] = field(default_factory=list)


class Generator:
    """Yields logical transactions in a fixed order for a seed."""

    REFUND_WINDOW = 5000

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.s = _State()
        self.clock = EPOCH
        self.txn_no = 0
        self.seq = 0
        kinds = list(MIX)
        weights = np.array([MIX[k] for k in kinds], dtype=float)
        self._kinds = kinds
        self._p = weights / weights.sum()
        self._bootstrap = (
            ["new_merchant"] * BOOTSTRAP["merchants"]
            + ["new_customer"] * BOOTSTRAP["customers"]
            + ["new_article"] * BOOTSTRAP["articles"]
        )

    # ---- public API -------------------------------------------------------------------------

    def next_txn(self) -> Txn:
        if self.txn_no < len(self._bootstrap):
            kind = self._bootstrap[self.txn_no]
        else:
            kind = self._kinds[int(self.rng.choice(len(self._kinds), p=self._p))]
        self.clock += timedelta(microseconds=int(self.rng.exponential(200_000)) + 1)
        self._ops: list[Op] = []
        handler = getattr(self, f"_t_{kind}")
        applied = handler()
        if not applied:  # precondition not met (e.g. nothing to refund) -> deterministic fallback
            kind = "payment" if self.s.customer_order and self.s.merchant_ids else "new_customer"
            getattr(self, f"_t_{kind}")()
        txn = Txn(self.txn_no, kind, tuple(self._ops))
        self.txn_no += 1
        return txn

    def txns(self, count: int) -> Iterator[Txn]:
        for _ in range(count):
            yield self.next_txn()

    def skip(self, count: int) -> None:
        """Advance the model without keeping the ops (resume from a later txn)."""
        for _ in range(count):
            self.next_txn()

    # ---- op helpers ---------------------------------------------------------------------------

    def _new_id(self, table: str) -> int:
        nid = self.s.next_id.get(table, 1)
        self.s.next_id[table] = nid + 1
        return nid

    def _emit(self, op: str, table: str, pk: int, row: dict[str, Any] | None) -> None:
        key = (table, pk)
        es = self.s.event_seq.get(key, 0) + 1
        self.s.event_seq[key] = es
        self._ops.append(Op(self.seq, self.txn_no, op, table, pk, es, dict(row) if row is not None else None))
        self.seq += 1

    def _money(self, mean_log: float, sigma: float) -> Decimal:
        value = float(self.rng.lognormal(mean_log, sigma))
        return max(Decimal(str(round(value, 2))), CENT).quantize(CENT)

    def _pick_customer(self) -> int:
        n = len(self.s.customer_order)
        for _ in range(16):
            rank = int(self.rng.zipf(ZIPF_A)) - 1
            if rank < n:
                return self.s.customer_order[rank]
        return self.s.customer_order[int(self.rng.integers(n))]

    def _pick(self, ids: list[int]) -> int:
        return ids[int(self.rng.integers(len(ids)))]

    def _touch_account(self, account_id: int, delta: Decimal) -> None:
        acct = self.s.accounts[account_id]
        acct["balance"] = (acct["balance"] + delta).quantize(CENT)
        acct["updated_at"] = self.clock
        self._emit("U", "accounts", account_id, acct)

    def _new_account(self, owner_type: str, owner_id: int, opening: Decimal) -> int:
        aid = self._new_id("accounts")
        row = {
            "id": aid,
            "owner_type": owner_type,
            "customer_id": owner_id if owner_type == "customer" else None,
            "merchant_id": owner_id if owner_type == "merchant" else None,
            "currency": "USD",
            "balance": opening,
            "created_at": self.clock,
            "updated_at": self.clock,
        }
        self.s.accounts[aid] = row
        self._emit("I", "accounts", aid, row)
        return aid

    # ---- logical transactions (return False when a precondition is not met) -----------------

    def _t_new_customer(self) -> bool:
        cid = self._new_id("customers")
        dob = date(1950, 1, 1) + timedelta(days=int(self.rng.integers(0, 365 * 55)))
        row = {
            "id": cid,
            "full_name": self.fake.name(),
            "email": self.fake.email(),
            "phone": self.fake.phone_number(),
            "address": self.fake.address().replace("\n", ", "),
            "date_of_birth": dob,
            "created_at": self.clock,
            "updated_at": self.clock,
        }
        self.s.customers[cid] = row
        self.s.customer_order.append(cid)
        self._emit("I", "customers", cid, row)
        self.s.customer_account[cid] = self._new_account("customer", cid, self._money(4.5, 0.6))
        return True

    def _t_update_customer(self) -> bool:
        if not self.s.customer_order:
            return False
        cid = self._pick_customer()
        row = self.s.customers[cid]
        field_name = ("email", "phone", "address")[int(self.rng.integers(3))]
        if field_name == "email":
            row["email"] = self.fake.email()
        elif field_name == "phone":
            row["phone"] = self.fake.phone_number()
        else:
            row["address"] = self.fake.address().replace("\n", ", ")
        row["updated_at"] = self.clock
        self._emit("U", "customers", cid, row)
        return True

    def _t_new_merchant(self) -> bool:
        mid = self._new_id("merchants")
        row = {
            "id": mid,
            "name": self.fake.company(),
            "category": ("grocery", "travel", "electronics", "dining", "fuel", "apparel")[int(self.rng.integers(6))],
            "country": "US",
            "created_at": self.clock,
            "updated_at": self.clock,
        }
        self.s.merchants[mid] = row
        self.s.merchant_ids.append(mid)
        self._emit("I", "merchants", mid, row)
        self.s.merchant_account[mid] = self._new_account("merchant", mid, Decimal("0.00"))
        return True

    def _t_topup(self) -> bool:
        if not self.s.customer_order:
            return False
        cid = self._pick_customer()
        return self._topup(cid, self._money(4.6, 0.5))

    def _topup(self, cid: int, amount: Decimal) -> bool:
        acct = self.s.customer_account[cid]
        tid = self._new_id("transactions")
        row = {
            "id": tid,
            "txn_type": "topup",
            "customer_id": cid,
            "merchant_id": None,
            "payer_account_id": None,
            "payee_account_id": acct,
            "amount": amount,
            "currency": "USD",
            "status": "settled",
            "description": "Account top-up",
            "created_at": self.clock,
        }
        self._emit("I", "transactions", tid, row)
        self._touch_account(acct, amount)
        return True

    def _t_payment(self) -> bool:
        if not (self.s.customer_order and self.s.merchant_ids):
            return False
        cid = self._pick_customer()
        mid = self._pick(self.s.merchant_ids)
        amount = self._money(3.2, 0.9)
        payer = self.s.customer_account[cid]
        if self.s.accounts[payer]["balance"] < amount:
            return self._topup(cid, (amount * 2).quantize(CENT))  # insufficient funds -> top up instead
        payee = self.s.merchant_account[mid]
        tid = self._new_id("transactions")
        row = {
            "id": tid,
            "txn_type": "payment",
            "customer_id": cid,
            "merchant_id": mid,
            "payer_account_id": payer,
            "payee_account_id": payee,
            "amount": amount,
            "currency": "USD",
            "status": "settled",
            "description": f"Purchase at {self.s.merchants[mid]['name']}",
            "created_at": self.clock,
        }
        self._emit("I", "transactions", tid, row)
        self._touch_account(payer, -amount)
        self._touch_account(payee, amount)
        self.s.payments[tid] = row
        self.s.refundable.append(tid)
        if len(self.s.refundable) > self.REFUND_WINDOW:
            old = self.s.refundable.pop(0)
            self.s.payments.pop(old, None)
        return True

    def _t_refund(self) -> bool:
        if not self.s.refundable:
            return False
        idx = int(self.rng.integers(len(self.s.refundable)))
        tid = self.s.refundable[idx]
        pay = self.s.payments[tid]
        full = self.rng.random() < 0.7
        amount = pay["amount"] if full else max((pay["amount"] / 2).quantize(CENT), CENT)
        payee = pay["payee_account_id"]
        if self.s.accounts[payee]["balance"] < amount:
            return False  # merchant cannot fund the refund
        self.s.refundable.pop(idx)
        self.s.payments.pop(tid)
        rid = self._new_id("refunds")
        row = {
            "id": rid,
            "transaction_id": tid,
            "amount": amount,
            "reason": ("damaged", "not_received", "duplicate", "changed_mind")[int(self.rng.integers(4))],
            "created_at": self.clock,
        }
        self._emit("I", "refunds", rid, row)
        self._touch_account(payee, -amount)
        self._touch_account(pay["payer_account_id"], amount)
        return True

    def _t_new_ticket(self) -> bool:
        if not self.s.customer_order:
            return False
        cid = self._pick_customer()
        cust = self.s.customers[cid]
        body = self.fake.paragraph(nb_sentences=4)
        if self.rng.random() < 0.3:  # free text that carries PII, as real tickets do
            body += f" Contact me at {cust['email']}. - {cust['full_name']}"
        tkid = self._new_id("support_tickets")
        row = {
            "id": tkid,
            "customer_id": cid,
            "subject": self.fake.sentence(nb_words=6),
            "body": body,
            "status": "open",
            "priority": ("low", "normal", "high")[int(self.rng.integers(3))],
            "created_at": self.clock,
            "updated_at": self.clock,
        }
        self.s.tickets[tkid] = row
        self.s.ticket_ids.append(tkid)
        self._emit("I", "support_tickets", tkid, row)
        return True

    def _t_update_ticket(self) -> bool:
        if not self.s.ticket_ids:
            return False
        tkid = self._pick(self.s.ticket_ids)
        row = self.s.tickets[tkid]
        row["status"] = TICKET_FLOW[row["status"]]
        if self.rng.random() < 0.5:
            row["body"] = row["body"] + "\n\nUpdate: " + self.fake.sentence()
        row["updated_at"] = self.clock
        self._emit("U", "support_tickets", tkid, row)
        return True

    def _t_delete_ticket(self) -> bool:
        if not self.s.ticket_ids:
            return False
        idx = int(self.rng.integers(len(self.s.ticket_ids)))
        tkid = self.s.ticket_ids.pop(idx)
        del self.s.tickets[tkid]
        self._emit("D", "support_tickets", tkid, None)
        return True

    def _t_new_article(self) -> bool:
        aid = self._new_id("help_articles")
        large = self.rng.random() < 0.2  # ~10 KB bodies exercise TOAST (PLAN G1/G2)
        body = self.fake.text(max_nb_chars=10_000) if large else self.fake.text(max_nb_chars=800)
        row = {
            "id": aid,
            "title": self.fake.sentence(nb_words=5).rstrip("."),
            "body": body,
            "category": ("account", "payments", "refunds", "security", "merchants")[int(self.rng.integers(5))],
            "view_count": 0,
            "created_at": self.clock,
            "updated_at": self.clock,
        }
        self.s.articles[aid] = row
        self.s.article_ids.append(aid)
        self._emit("I", "help_articles", aid, row)
        return True

    def _t_update_article(self) -> bool:
        if not self.s.article_ids:
            return False
        aid = self._pick(self.s.article_ids)
        row = self.s.articles[aid]
        if self.rng.random() < 0.7:  # non-text column change: the TOASTed body must survive downstream
            row["view_count"] = row["view_count"] + int(self.rng.integers(1, 50))
        else:
            row["body"] = row["body"] + "\n\n" + self.fake.paragraph(nb_sentences=2)
        row["updated_at"] = self.clock
        self._emit("U", "help_articles", aid, row)
        return True

    def _t_delete_article(self) -> bool:
        if len(self.s.article_ids) <= 5:  # keep a corpus
            return False
        idx = int(self.rng.integers(len(self.s.article_ids)))
        aid = self.s.article_ids.pop(idx)
        del self.s.articles[aid]
        self._emit("D", "help_articles", aid, None)
        return True


class StreamHash:
    """Order-sensitive SHA-256 over op identities; identical streams give identical digests."""

    def __init__(self) -> None:
        self._h = hashlib.sha256()
        self.ops = 0

    def update(self, op: Op) -> None:
        self._h.update(op.identity().encode())
        self._h.update(b"\n")
        self.ops += 1

    def hexdigest(self) -> str:
        return self._h.hexdigest()


def stream_digest(seed: int, start: int, count: int) -> tuple[str, int]:
    """Digest of txns [start, start+count) for a seed, without touching a database."""
    gen = Generator(seed)
    gen.skip(start)
    h = StreamHash()
    for txn in gen.txns(count):
        for op in txn.ops:
            h.update(op)
    return h.hexdigest(), h.ops
