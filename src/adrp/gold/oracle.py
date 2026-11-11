"""Pure-Python oracle for the gold layer (PLAN G4): SCD2 versions, snapshot capture loss, double-entry ledger.

Nothing here talks to a database. The dbt models are checked against these functions, and the functions are
checked against the generator's own ground truth, so the gold semantics are pinned down without Docker.

SCD2 (dim_customer, dim_merchant) is built from the bronze change log:
  (a) duplicates collapse: one event per (pk, lsn). Bronze holds duplicates by design (replay, re-produce).
  (b) same-transaction collapse: per (pk, txid) only the event with the highest LSN counts, so one transaction
      yields at most one version, with its final values, and no zero-length interval exists.
  (c) events are ordered by LSN per key. A delete closes the open version; it does not start one.
  (d) consecutive versions whose tracked attributes are identical are merged (no-op updates, e.g. a change to
      an untracked column such as updated_at). A version is never merged across a delete.
  The LSN interval [valid_from_lsn, valid_to_lsn) is authoritative; commit times are ms precision and descriptive.

Ledger (fct_ledger_entries) is double-entry with signed amounts (+ raises the account's balance). Every journal
has two legs that sum to zero. External contra accounts have negative ids: -1 funding, -2 opening equity.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from adrp.common import canonical_json
from adrp.gen.model import Generator

EXTERNAL_FUNDING = -1
EXTERNAL_OPENING_EQUITY = -2

UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)
_KAFKA_LAG_MS = 50  # synthetic broker LogAppendTime lag behind the commit time
_PARTITIONS = 3
_OP_CODE = {"I": "c", "U": "u", "D": "d"}
_FULL_IDENTITY = frozenset({"customers", "support_tickets", "help_articles"})  # REPLICA IDENTITY FULL (shop.sql)
_SILVER_TABLES = ("customers", "merchants", "accounts", "transactions", "refunds")
_ZONED = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


# ---- timestamps ---------------------------------------------------------------------------------


def epoch_ms(dt: datetime) -> int:
    """Milliseconds since the Unix epoch, by integer arithmetic (no float rounding)."""
    return (dt - UNIX_EPOCH) // _MS


def format_zoned(dt: datetime) -> str:
    """Debezium ZonedTimestamp style: UTC, trailing zeros of the fraction trimmed, 'Z' suffix."""
    u = dt.astimezone(timezone.utc)
    frac = f".{u.microsecond:06d}".rstrip("0") if u.microsecond else ""
    return u.strftime("%Y-%m-%dT%H:%M:%S") + frac + "Z"


def parse_zoned(s: str) -> datetime:
    """Inverse of format_zoned; accepts 0-9 fraction digits and 'Z' or an offset (3.10's fromisoformat cannot)."""
    m = _ZONED.match(s)
    if m is None:
        raise ValueError(f"not an ISO-8601 timestamp with zone: {s!r}")
    day, clock, frac, zone = m.groups()
    micro = int((frac or "0")[:6].ljust(6, "0"))
    base = datetime.fromisoformat(f"{day}T{clock}")
    tz = timezone.utc if zone == "Z" else datetime.fromisoformat(f"2000-01-01T00:00:00{zone}").tzinfo
    return base.replace(microsecond=micro, tzinfo=tz).astimezone(timezone.utc)


# ---- SCD2 ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ChangeEvent:
    """One bronze change (op c/u/r/d; tombstones are excluded). `after` is the parsed after-image (None for d)."""

    pk: int
    lsn: int
    txid: int
    op: str
    source_ts_ms: int
    kafka_ts_ms: int
    after: Mapping[str, Any] | None


@dataclass(frozen=True)
class Version:
    """One SCD2 version. Interval is [valid_from_lsn, valid_to_lsn); valid_to_* is None while current."""

    pk: int
    valid_from_lsn: int
    valid_to_lsn: int | None
    valid_from_ms: int
    valid_to_ms: int | None
    txid: int
    attrs: tuple[Any, ...]  # values of the tracked columns, in `tracked` order
    is_current: bool
    closed_by_delete: bool


def _tie(e: ChangeEvent) -> tuple[int, str, int, int, str]:
    # Duplicates of one (pk, lsn) differ only in broker metadata; the earliest arrival wins, with a total
    # order on the rest so the result never depends on input order.
    return (e.kafka_ts_ms, e.op, e.txid, e.source_ts_ms, canonical_json(e.after) if e.after is not None else "")


def _versions_for_key(pk: int, events: Sequence[ChangeEvent], tracked: Sequence[str]) -> list[Version]:
    done: list[Version] = []
    cur: Version | None = None
    for e in events:  # ascending LSN, one event per transaction
        if e.op == "d":
            if cur is not None:
                done.append(
                    replace(
                        cur, valid_to_lsn=e.lsn, valid_to_ms=e.source_ts_ms, is_current=False, closed_by_delete=True
                    )
                )
                cur = None
            continue
        if e.after is None:
            raise ValueError(f"op {e.op!r} without an after-image (pk={pk}, lsn={e.lsn})")
        attrs = tuple(e.after.get(t) for t in tracked)
        if cur is not None and cur.attrs == attrs:
            continue  # no-op for the tracked columns: the open version simply extends
        if cur is not None:
            done.append(replace(cur, valid_to_lsn=e.lsn, valid_to_ms=e.source_ts_ms, is_current=False))
        cur = Version(pk, e.lsn, None, e.source_ts_ms, None, e.txid, attrs, True, False)
    if cur is not None:
        done.append(cur)
    return done


def scd2_versions(events: Iterable[ChangeEvent], tracked: Sequence[str]) -> list[Version]:
    """Expected SCD2 versions, sorted by (pk, valid_from_lsn). Independent of input order and duplicates."""
    best: dict[tuple[int, int], ChangeEvent] = {}
    for e in events:
        cur = best.get((e.pk, e.lsn))
        if cur is None or _tie(e) < _tie(cur):
            best[(e.pk, e.lsn)] = e
    final: dict[tuple[int, int], ChangeEvent] = {}
    for e in best.values():
        cur = final.get((e.pk, e.txid))
        if cur is None or e.lsn > cur.lsn:
            final[(e.pk, e.txid)] = e
    by_pk: dict[int, list[ChangeEvent]] = {}
    for e in final.values():
        by_pk.setdefault(e.pk, []).append(e)
    out: list[Version] = []
    for pk in sorted(by_pk):
        out.extend(_versions_for_key(pk, sorted(by_pk[pk], key=lambda ev: ev.lsn), tracked))
    return out


def _in_force(v: Version, cut: int) -> bool:
    return v.valid_from_lsn <= cut and (v.valid_to_lsn is None or cut < v.valid_to_lsn)


def snapshot_captures(versions: Iterable[Version], run_cut_lsns: Iterable[int]) -> list[Version]:
    """The versions a dbt `check` snapshot records when it runs once per cut LSN.

    At a run the snapshot sees each key as of the cut (silver merged up to `cut`, deleted keys filtered out) and
    stores a row only when the tracked attributes differ from the last row it stored for that key. So a version
    that began and ended between two runs is never seen, and a version equal to the last captured state is not
    stored again. Sorted by (pk, valid_from_lsn).
    """
    by_pk: dict[int, list[Version]] = {}
    for v in versions:
        by_pk.setdefault(v.pk, []).append(v)
    cuts = sorted(set(run_cut_lsns))
    out: list[Version] = []
    for pk in sorted(by_pk):
        vs = sorted(by_pk[pk], key=lambda v: v.valid_from_lsn)
        last: tuple[Any, ...] | None = None
        taken = False
        for cut in cuts:
            seen = next((v for v in vs if _in_force(v, cut)), None)
            if seen is None or (taken and seen.attrs == last):
                continue
            out.append(seen)
            last, taken = seen.attrs, True
    return out


def lost_intermediate(versions: Iterable[Version], run_cut_lsns: Iterable[int]) -> int:
    """Number of SCD2 versions the snapshot never stored: len(versions) - len(snapshot_captures)."""
    vs = list(versions)
    return len(vs) - len(snapshot_captures(vs, run_cut_lsns))


# ---- ledger -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Opening:
    """An account's opening balance (first bronze event of the account, op 'c')."""

    account_id: int
    balance: Decimal
    currency: str
    created_at: datetime
    lsn: int


@dataclass(frozen=True)
class LedgerTxn:
    transaction_id: int
    txn_type: str  # payment | topup
    payer_account_id: int | None  # None for topup (external funding)
    payee_account_id: int
    amount: Decimal
    currency: str
    created_at: datetime
    lsn: int


@dataclass(frozen=True)
class LedgerRefund:
    refund_id: int
    transaction_id: int
    amount: Decimal
    created_at: datetime
    lsn: int


@dataclass(frozen=True)
class Entry:
    """One ledger leg. entry_id = journal_type:source_id:leg, journal_id = journal_type:source_id.

    Legs by journal type: opening = account, equity; topup = payee, funding; payment = payer, payee;
    refund = payee, payer (reversal of the original payment, linked by transaction_id).
    """

    entry_id: str
    journal_id: str
    journal_type: str  # opening | topup | payment | refund
    transaction_id: int | None
    refund_id: int | None
    account_id: int
    amount: Decimal  # signed: + raises the account's balance
    currency: str
    business_date: date
    source_lsn: int


def _bdate(dt: datetime) -> date:
    return dt.astimezone(timezone.utc).date()


def _journal(
    journal_type: str,
    source_id: int,
    legs: tuple[tuple[str, int, Decimal], tuple[str, int, Decimal]],
    *,
    transaction_id: int | None,
    refund_id: int | None,
    currency: str,
    created_at: datetime,
    lsn: int,
) -> list[Entry]:
    jid = f"{journal_type}:{source_id}"
    return [
        Entry(
            f"{jid}:{leg}", jid, journal_type, transaction_id, refund_id, acct, amt, currency, _bdate(created_at), lsn
        )
        for leg, acct, amt in legs
    ]


def ledger_entries(
    openings: Iterable[Opening], transactions: Iterable[LedgerTxn], refunds: Iterable[LedgerRefund]
) -> list[Entry]:
    """All ledger entries, sorted by (source_lsn, entry_id). A zero opening balance emits no entry."""
    txns = {t.transaction_id: t for t in transactions}
    out: list[Entry] = []
    for o in openings:
        if o.balance == 0:
            continue
        out += _journal(
            "opening", o.account_id,
            (("account", o.account_id, o.balance), ("equity", EXTERNAL_OPENING_EQUITY, -o.balance)),
            transaction_id=None, refund_id=None, currency=o.currency, created_at=o.created_at, lsn=o.lsn,
        )  # fmt: skip
    for t in txns.values():
        if t.txn_type == "topup":
            legs = (("payee", t.payee_account_id, t.amount), ("funding", EXTERNAL_FUNDING, -t.amount))
        elif t.txn_type == "payment":
            if t.payer_account_id is None:
                raise ValueError(f"payment {t.transaction_id} has no payer account")
            legs = (("payer", t.payer_account_id, -t.amount), ("payee", t.payee_account_id, t.amount))
        else:
            raise ValueError(f"unknown txn_type {t.txn_type!r}")
        out += _journal(
            t.txn_type, t.transaction_id, legs,
            transaction_id=t.transaction_id, refund_id=None, currency=t.currency, created_at=t.created_at, lsn=t.lsn,
        )  # fmt: skip
    for r in refunds:
        orig = txns.get(r.transaction_id)
        if orig is None or orig.txn_type != "payment" or orig.payer_account_id is None:
            raise ValueError(f"refund {r.refund_id} does not refer to a known payment")
        out += _journal(
            "refund", r.refund_id,
            (("payee", orig.payee_account_id, -r.amount), ("payer", orig.payer_account_id, r.amount)),
            transaction_id=r.transaction_id, refund_id=r.refund_id, currency=orig.currency,
            created_at=r.created_at, lsn=r.lsn,
        )  # fmt: skip
    return sorted(out, key=lambda e: (e.source_lsn, e.entry_id))


def balances(entries: Iterable[Entry], *, include_external: bool = False) -> dict[int, Decimal]:
    """Sum of signed entries per account. External contra accounts (negative ids) only with include_external.
    An account with no entries is absent."""
    out: dict[int, Decimal] = {}
    for e in entries:
        if e.account_id > 0 or include_external:
            out[e.account_id] = out.get(e.account_id, Decimal(0)) + e.amount
    return out


# ---- synthetic bronze from the generator ----------------------------------------------------------


@dataclass(frozen=True)
class BronzeRow:
    """The bronze columns that matter to gold (key_bytes, value_bytes, schema_id omitted: NULL here).

    source_ts_ms is the commit time; kafka_ts_ms the (synthetic) broker append time. before/after are
    canonical-JSON strings exactly as adrp.spark.decode produces them.
    """

    topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_ts_ms: int
    source_table: str  # 'public.customers'
    op: str | None  # c | u | d (None = tombstone)
    lsn: int
    txid: int
    source_ts_ms: int
    pk: str  # '{"id":N}'
    before: str | None
    after: str | None


def _debezium_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Generator row -> the value types decode_change yields: timestamptz as ISO 'Z' strings, dates as ISO strings."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = format_zoned(v)
        elif isinstance(v, date):
            out[k] = v.isoformat()
        else:
            out[k] = v  # Decimal -> str happens in canonical_json, as for the real decoder
    return out


def events_from_generator(seed: int, n_txns: int) -> tuple[list[BronzeRow], dict[str, dict[int, dict[str, Any]]]]:
    """Run the generator for n_txns and return (bronze_rows, silver_state), with no database.

    Synthetic envelope: lsn = op.seq * 100, txid = txn_no + 1000, commit time = the generator clock of the
    transaction (ms), broker time = commit + 50 ms, partition = pk % 3, one offset counter per partition.
    Every table is emitted. REPLICA IDENTITY FULL tables carry a before-image, as in shop.sql.

    silver_state[table][pk] is the final row of customers, merchants, accounts, transactions and refunds
    (generator types) plus lsn, is_deleted, op and source_ts_ms. None of these tables is ever deleted.
    """
    gen = Generator(seed)
    rows: list[BronzeRow] = []
    silver: dict[str, dict[int, dict[str, Any]]] = {t: {} for t in _SILVER_TABLES}
    last_row: dict[tuple[str, int], dict[str, Any]] = {}
    offsets = [0] * _PARTITIONS
    for _ in range(n_txns):
        txn = gen.next_txn()
        commit_ms = epoch_ms(gen.clock)
        for op in txn.ops:
            part = op.pk % _PARTITIONS
            after = _debezium_row(op.row) if op.row is not None else None
            prev = last_row.get((op.table, op.pk))
            before = prev if op.table in _FULL_IDENTITY and op.op != "I" else None
            code = _OP_CODE[op.op]
            rows.append(
                BronzeRow(
                    topic=f"cdc.public.{op.table}",
                    kafka_partition=part,
                    kafka_offset=offsets[part],
                    kafka_ts_ms=commit_ms + _KAFKA_LAG_MS,
                    source_table=f"public.{op.table}",
                    op=code,
                    lsn=op.seq * 100,
                    txid=op.txn_no + 1000,
                    source_ts_ms=commit_ms,
                    pk=canonical_json({"id": op.pk}),
                    before=canonical_json(before) if before is not None else None,
                    after=canonical_json(after) if after is not None else None,
                )
            )
            offsets[part] += 1
            if after is None:
                last_row.pop((op.table, op.pk), None)
            else:
                last_row[(op.table, op.pk)] = after
            if op.table in silver and op.row is not None:
                silver[op.table][op.pk] = {
                    **op.row,
                    "lsn": op.seq * 100,
                    "is_deleted": False,
                    "op": code,
                    "source_ts_ms": commit_ms,
                }
    return rows, silver


def events_from_bronze(rows: Iterable[BronzeRow], table: str) -> list[ChangeEvent]:
    """ChangeEvents of one table (e.g. 'customers') from bronze rows; tombstones (op None) are skipped."""
    out: list[ChangeEvent] = []
    for r in rows:
        if r.source_table != f"public.{table}" or r.op is None:
            continue
        after = json.loads(r.after) if r.after is not None else None
        out.append(ChangeEvent(int(json.loads(r.pk)["id"]), r.lsn, r.txid, r.op, r.source_ts_ms, r.kafka_ts_ms, after))
    return out


def ledger_inputs_from_bronze(rows: Iterable[BronzeRow]) -> tuple[list[Opening], list[LedgerTxn], list[LedgerRefund]]:
    """Parse ledger inputs out of the bronze JSON. Openings are the first event of each account (op 'c' only;
    snapshot-opened accounts, op 'r', are unsupported). Transactions and refunds are their insert events."""
    first: dict[int, BronzeRow] = {}
    txns: dict[int, LedgerTxn] = {}
    refunds: dict[int, LedgerRefund] = {}
    for r in rows:
        if r.after is None or r.op is None:
            continue
        a = json.loads(r.after)
        pk = int(json.loads(r.pk)["id"])
        if r.source_table == "public.accounts":
            cur = first.get(pk)
            if cur is None or r.lsn < cur.lsn:
                first[pk] = r
        elif r.source_table == "public.transactions" and r.op == "c":
            payer = a["payer_account_id"]
            txns[pk] = LedgerTxn(
                pk, a["txn_type"], None if payer is None else int(payer), int(a["payee_account_id"]),
                Decimal(a["amount"]), a["currency"], parse_zoned(a["created_at"]), r.lsn,
            )  # fmt: skip
        elif r.source_table == "public.refunds" and r.op == "c":
            refunds[pk] = LedgerRefund(
                pk, int(a["transaction_id"]), Decimal(a["amount"]), parse_zoned(a["created_at"]), r.lsn
            )
    openings: list[Opening] = []
    for pk, r in sorted(first.items()):
        if r.op != "c":
            raise ValueError(f"account {pk} was not opened by a change event (op {r.op!r}): unsupported")
        assert r.after is not None
        a = json.loads(r.after)
        openings.append(Opening(pk, Decimal(a["balance"]), a["currency"], parse_zoned(a["created_at"]), r.lsn))
    return openings, list(txns.values()), list(refunds.values())
