"""Tests of the gold oracle (PLAN G4 A1): SCD2 semantics, snapshot loss, double-entry ledger, and a
no-Docker reconciliation of the ledger against the generator's own balances."""

import dataclasses
import itertools
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from adrp.common import canonical_json
from adrp.gen.model import Generator
from adrp.gold.oracle import (
    EXTERNAL_FUNDING,
    EXTERNAL_OPENING_EQUITY,
    ChangeEvent,
    Entry,
    LedgerRefund,
    LedgerTxn,
    Opening,
    balances,
    events_from_bronze,
    events_from_generator,
    format_zoned,
    ledger_entries,
    ledger_inputs_from_bronze,
    lost_intermediate,
    parse_zoned,
    scd2_versions,
    snapshot_captures,
)

TRACKED = ("email", "phone")
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ev(pk: int, lsn: int, txid: int, op: str, email: str = "e", phone: str = "p", *, ts: int = 0) -> ChangeEvent:
    after = None if op == "d" else {"id": pk, "email": email, "phone": phone, "updated_at": f"t{lsn}"}
    return ChangeEvent(pk, lsn, txid, op, ts or lsn, ts or lsn + 5, after)


# ---- scripted SCD2 cases --------------------------------------------------------------------------


def test_multi_change_between_runs_and_same_transaction_collapse() -> None:
    events = [
        ev(1, 100, 10, "c", "a"),
        ev(1, 200, 11, "u", "b"),
        ev(1, 300, 12, "u", "c"),
        ev(1, 400, 13, "u", "d"),  # one transaction, two updates to the same key:
        ev(1, 410, 13, "u", "e"),  # only the last (410) counts
    ]
    vs = scd2_versions(events, TRACKED)
    assert [(v.valid_from_lsn, v.valid_to_lsn, v.attrs[0]) for v in vs] == [
        (100, 200, "a"),
        (200, 300, "b"),
        (300, 410, "c"),
        (410, None, "e"),
    ]
    assert [v.is_current for v in vs] == [False, False, False, True]
    assert vs[-1].txid == 13 and vs[-1].valid_from_ms == 410 and vs[0].valid_to_ms == 200


def test_untracked_change_extends_the_open_version() -> None:
    events = [ev(1, 100, 10, "c", "a", "x"), ev(1, 200, 11, "u", "a", "x"), ev(1, 300, 12, "u", "a", "y")]
    vs = scd2_versions(events, TRACKED)
    assert [(v.valid_from_lsn, v.valid_to_lsn) for v in vs] == [(100, 300), (300, None)]
    assert vs[0].txid == 10


def test_delete_closes_and_reinsert_starts_a_new_version() -> None:
    events = [ev(1, 100, 10, "c", "a"), ev(1, 200, 11, "d"), ev(1, 300, 12, "c", "a")]  # same attrs after reinsert
    vs = scd2_versions(events, TRACKED)
    assert [(v.valid_from_lsn, v.valid_to_lsn, v.closed_by_delete, v.is_current) for v in vs] == [
        (100, 200, True, False),
        (300, None, False, True),
    ]


def test_insert_and_delete_in_one_transaction_leaves_no_version() -> None:
    assert scd2_versions([ev(1, 100, 10, "c", "a"), ev(1, 110, 10, "d")], TRACKED) == []


def test_duplicates_and_arrival_order_do_not_matter() -> None:
    base = [ev(1, 100, 10, "c", "a"), ev(1, 200, 11, "u", "b"), ev(2, 150, 10, "c", "z")]
    dup = [dataclasses.replace(e, kafka_ts_ms=e.kafka_ts_ms + 9_000) for e in base]  # re-produced at a later offset
    assert scd2_versions(reversed(base + dup), TRACKED) == scd2_versions(base, TRACKED)


def test_snapshot_loses_exactly_the_intermediate_versions() -> None:
    # A has 4 versions; the snapshot runs at lsn 150 (sees v1) and 500 (sees v4): v2 and v3 are never captured.
    events = [ev(1, 100, 10, "c", "a"), ev(1, 200, 11, "u", "b"), ev(1, 300, 12, "u", "c"), ev(1, 400, 13, "u", "d")]
    vs = scd2_versions(events, TRACKED)
    assert len(vs) == 4
    cap = snapshot_captures(vs, [150, 500])
    assert [v.valid_from_lsn for v in cap] == [100, 400]
    assert lost_intermediate(vs, [150, 500]) == 2
    assert lost_intermediate(vs, [150, 250, 350, 500]) == 0  # a run after every change misses nothing


def test_snapshot_skips_unchanged_revisits_and_deleted_keys() -> None:
    events = [ev(1, 100, 10, "c", "a"), ev(1, 200, 11, "u", "b"), ev(1, 300, 12, "u", "a"), ev(2, 100, 10, "c", "q")]
    events += [ev(2, 250, 11, "d")]
    vs = scd2_versions(events, TRACKED)
    # key 1: seen as a (150) and a again (350): second is not stored. key 2: gone at the second run.
    cap = snapshot_captures(vs, [150, 350])
    assert [(v.pk, v.valid_from_lsn) for v in cap] == [(1, 100), (2, 100)]
    assert lost_intermediate(vs, [150, 350]) == 2


# ---- Hypothesis: SCD2 ------------------------------------------------------------------------------


@st.composite
def histories(draw: st.DrawFn) -> list[ChangeEvent]:
    """Per-key valid histories (insert, updates, deletes, re-inserts) with globally increasing LSNs, several
    events sharing a txid, and a tiny value alphabet so that no-op updates are common."""
    n = draw(st.integers(1, 40))
    alive: set[int] = set()
    out: list[ChangeEvent] = []
    lsn, txid = 100, 1000
    for _ in range(n):
        pk = draw(st.integers(1, 4))
        op = "c" if pk not in alive else draw(st.sampled_from(["u", "u", "d"]))
        if op == "d":
            alive.discard(pk)
        else:
            alive.add(pk)
        lsn += draw(st.integers(1, 5))
        if draw(st.booleans()):
            txid += 1
        email = draw(st.sampled_from("ab"))
        phone = draw(st.sampled_from("xy"))
        out.append(ev(pk, lsn, txid, op, email, phone, ts=lsn * 3))
    return out


@st.composite
def deliveries(draw: st.DrawFn) -> tuple[list[ChangeEvent], list[ChangeEvent]]:
    """(history, the same history shuffled, with duplicates re-produced at later broker times)."""
    history = draw(histories())
    extra = draw(st.lists(st.sampled_from(history), max_size=len(history)))
    later = draw(st.lists(st.integers(0, 10_000), min_size=len(extra), max_size=len(extra)))
    dups = [dataclasses.replace(e, kafka_ts_ms=e.kafka_ts_ms + d) for e, d in zip(extra, later, strict=True)]
    return history, draw(st.permutations(history + dups))


def expected_versions(history: list[ChangeEvent]) -> list[tuple[int, int, int | None, tuple[Any, ...], bool]]:
    """Second implementation: state after each transaction, compress equal consecutive states, read runs."""
    out: list[tuple[int, int, int | None, tuple[Any, ...], bool]] = []
    for pk in sorted({e.pk for e in history}):
        evs = sorted((e for e in history if e.pk == pk), key=lambda e: e.lsn)
        finals = [list(g)[-1] for _, g in itertools.groupby(evs, key=lambda e: e.txid)]
        runs: list[tuple[int, tuple[Any, ...] | None]] = []
        for e in finals:
            state = None if e.after is None else tuple(e.after[t] for t in TRACKED)
            if (runs and runs[-1][1] == state) or (not runs and state is None):
                continue
            runs.append((e.lsn, state))
        for i, (lsn, state) in enumerate(runs):
            if state is None:
                continue
            nxt = runs[i + 1] if i + 1 < len(runs) else None
            out.append((pk, lsn, None if nxt is None else nxt[0], state, nxt is not None and nxt[1] is None))
    return out


@settings(max_examples=1000, deadline=None)
@given(deliveries())
def test_scd2_invariant_to_shuffle_and_duplicates(data: tuple[list[ChangeEvent], list[ChangeEvent]]) -> None:
    history, delivered = data
    got = scd2_versions(delivered, TRACKED)
    assert got == scd2_versions(history, TRACKED)
    assert [(v.pk, v.valid_from_lsn, v.valid_to_lsn, v.attrs, v.closed_by_delete) for v in got] == expected_versions(
        history
    )


@settings(max_examples=1000, deadline=None)
@given(histories())
def test_scd2_contiguous_no_zero_length_one_current(history: list[ChangeEvent]) -> None:
    vs = scd2_versions(history, TRACKED)
    for pk in {v.pk for v in vs}:
        mine = [v for v in vs if v.pk == pk]
        assert sum(v.is_current for v in mine) <= 1
        for v in mine:
            assert v.is_current == (v.valid_to_lsn is None) == (v.valid_to_ms is None)
            assert v.valid_to_lsn is None or v.valid_from_lsn < v.valid_to_lsn  # no zero-length LSN interval
            assert v.valid_to_ms is None or v.valid_from_ms <= v.valid_to_ms
        for a, b in itertools.pairwise(mine):
            assert a.valid_to_lsn is not None
            if a.closed_by_delete:
                assert a.valid_to_lsn < b.valid_from_lsn  # a gap while the key was deleted
            else:
                assert a.valid_to_lsn == b.valid_from_lsn  # contiguous, no overlap
                assert a.attrs != b.attrs  # no-op changes were merged
        if mine[-1].closed_by_delete:
            assert not mine[-1].is_current


@settings(max_examples=1000, deadline=None)
@given(histories(), st.lists(st.integers(0, 600), max_size=5))
def test_snapshot_captures_are_versions_in_order(history: list[ChangeEvent], cuts: list[int]) -> None:
    vs = scd2_versions(history, TRACKED)
    cap = snapshot_captures(vs, cuts)
    assert set(cap) <= set(vs)
    assert 0 <= lost_intermediate(vs, cuts) == len(vs) - len(cap)
    if vs:  # a run after the last change captures every key that is still alive, so nothing current is lost
        final = snapshot_captures(vs, [*cuts, 10**9])
        assert {v.pk for v in vs if v.is_current} <= {v.pk for v in final}


# ---- ledger ----------------------------------------------------------------------------------------

money = st.decimals(min_value="0.01", max_value="500", places=2)


@st.composite
def ledger_inputs(draw: st.DrawFn) -> tuple[list[Opening], list[LedgerTxn], list[LedgerRefund]]:
    accounts = list(range(1, 7))
    openings = [Opening(a, draw(st.one_of(st.just(Decimal("0.00")), money)), "USD", T0, lsn=a) for a in accounts]
    txns: list[LedgerTxn] = []
    for i in range(1, draw(st.integers(0, 25)) + 1):
        payee = draw(st.sampled_from(accounts))
        kind = draw(st.sampled_from(["topup", "payment"]))
        payer = None if kind == "topup" else draw(st.sampled_from(accounts))
        txns.append(LedgerTxn(i, kind, payer, payee, draw(money), "USD", T0, 100 + i))
    payments = [t for t in txns if t.txn_type == "payment"]
    refunded = draw(st.lists(st.sampled_from(payments), unique_by=lambda t: t.transaction_id)) if payments else []
    refunds = [
        LedgerRefund(
            j, t.transaction_id, draw(st.decimals(min_value="0.01", max_value=t.amount, places=2)), T0, 1000 + j
        )
        for j, t in enumerate(refunded, start=1)
    ]
    return openings, txns, refunds


def sums_by(entries: list[Entry], attr: str) -> dict[Any, Decimal]:
    out: dict[Any, Decimal] = {}
    for e in entries:
        k = getattr(e, attr)
        if k is not None:
            out[k] = out.get(k, Decimal(0)) + e.amount
    return out


@settings(max_examples=1000, deadline=None)
@given(ledger_inputs(), st.randoms(use_true_random=False))
def test_ledger_nets_to_zero_and_ignores_input_order(
    data: tuple[list[Opening], list[LedgerTxn], list[LedgerRefund]], rnd: Any
) -> None:
    openings, txns, refunds = data
    entries = ledger_entries(openings, txns, refunds)
    assert all(s == 0 for s in sums_by(entries, "journal_id").values())
    assert all(s == 0 for s in sums_by(entries, "transaction_id").values())  # payment + its refund reversal
    assert len({e.entry_id for e in entries}) == len(entries)
    assert sum((e.amount for e in entries), Decimal(0)) == 0
    o2, t2, r2 = list(openings), list(txns), list(refunds)
    rnd.shuffle(o2)
    rnd.shuffle(t2)
    rnd.shuffle(r2)
    assert ledger_entries(o2, t2, r2) == entries
    # Customer-visible balance = openings + topups - payments + refunds, computed without the ledger.
    expect: dict[int, Decimal] = {}
    for o in openings:
        expect[o.account_id] = expect.get(o.account_id, Decimal(0)) + o.balance
    for t in txns:
        expect[t.payee_account_id] = expect.get(t.payee_account_id, Decimal(0)) + t.amount
        if t.payer_account_id is not None:
            expect[t.payer_account_id] = expect.get(t.payer_account_id, Decimal(0)) - t.amount
    by_id = {t.transaction_id: t for t in txns}
    for r in refunds:
        orig = by_id[r.transaction_id]
        assert orig.payer_account_id is not None
        expect[orig.payee_account_id] -= r.amount
        expect[orig.payer_account_id] += r.amount
    assert {a: b for a, b in balances(entries).items() if b} == {a: b for a, b in expect.items() if b}


def test_ledger_legs_signs_and_external_accounts() -> None:
    openings = [Opening(1, Decimal("50.00"), "USD", T0, 10), Opening(9, Decimal("0.00"), "USD", T0, 20)]
    txns = [
        LedgerTxn(5, "topup", None, 1, Decimal("20.00"), "USD", T0, 30),
        LedgerTxn(6, "payment", 1, 9, Decimal("30.00"), "USD", T0, 40),
    ]
    refunds = [LedgerRefund(2, 6, Decimal("15.00"), datetime(2026, 1, 2, tzinfo=timezone.utc), 50)]
    es = ledger_entries(openings, txns, refunds)
    assert [(e.entry_id, e.account_id, e.amount) for e in es] == [
        ("opening:1:account", 1, Decimal("50.00")),
        ("opening:1:equity", EXTERNAL_OPENING_EQUITY, Decimal("-50.00")),  # no entries for the zero opening of 9
        ("topup:5:funding", EXTERNAL_FUNDING, Decimal("-20.00")),
        ("topup:5:payee", 1, Decimal("20.00")),
        ("payment:6:payee", 9, Decimal("30.00")),
        ("payment:6:payer", 1, Decimal("-30.00")),
        ("refund:2:payee", 9, Decimal("-15.00")),
        ("refund:2:payer", 1, Decimal("15.00")),
    ]
    assert es[-1].transaction_id == 6 and es[-1].refund_id == 2 and es[-1].business_date.isoformat() == "2026-01-02"
    assert balances(es) == {1: Decimal("55.00"), 9: Decimal("15.00")}
    assert balances(es, include_external=True)[EXTERNAL_FUNDING] == Decimal("-20.00")


# ---- generator ground truth (no Docker) -------------------------------------------------------------


def test_zoned_timestamp_round_trip() -> None:
    dt = datetime(2026, 1, 1, 0, 0, 3, 120_000, tzinfo=timezone.utc)
    assert format_zoned(dt) == "2026-01-01T00:00:03.12Z"
    assert parse_zoned("2026-01-01T00:00:03.12Z") == dt
    assert parse_zoned("2026-01-01T01:00:03+01:00") == dt.replace(second=3, microsecond=0)
    assert format_zoned(dt.replace(microsecond=0)) == "2026-01-01T00:00:03Z"


def test_events_from_generator_envelope() -> None:
    rows, silver = events_from_generator(42, 300)
    gen = Generator(42)
    ops = [op for t in gen.txns(300) for op in t.ops]
    assert len(rows) == len(ops)
    for r, op in zip(rows, ops, strict=True):
        assert (r.lsn, r.txid, r.source_table) == (op.seq * 100, op.txn_no + 1000, f"public.{op.table}")
        assert r.pk == canonical_json({"id": op.pk}) and r.op == {"I": "c", "U": "u", "D": "d"}[op.op]
        assert (r.after is None) == (op.row is None)
    assert len({(r.kafka_partition, r.kafka_offset) for r in rows}) == len(rows)
    cust = next(r for r in rows if r.source_table == "public.customers" and r.op == "c")
    assert cust.before is None and cust.after is not None and '"date_of_birth":"19' in cust.after
    assert '"created_at":"2026-01-01T00:00:' in cust.after
    acct = next(r for r in rows if r.source_table == "public.accounts" and r.op == "c")
    assert acct.after is not None and '"balance":"' in acct.after  # decimals as strings
    upd = next(r for r in rows if r.source_table == "public.customers" and r.op == "u")
    assert upd.before is not None and upd.after is not None and upd.before != upd.after  # REPLICA IDENTITY FULL
    assert all(r.before is None for r in rows if r.source_table == "public.accounts")
    assert set(silver) == {"customers", "merchants", "accounts", "transactions", "refunds"}
    assert silver["accounts"][1]["balance"] == gen.s.accounts[1]["balance"] and silver["accounts"][1]["lsn"] % 100 == 0


def test_customer_scd2_matches_generator_updates() -> None:
    rows, silver = events_from_generator(42, 2000)
    gen = Generator(42)
    history: dict[int, list[tuple[Any, ...]]] = {}
    for op in (o for t in gen.txns(2000) for o in t.ops if o.table == "customers"):
        assert op.row is not None
        state = (op.row["email"], op.row["phone"], op.row["address"])
        seq = history.setdefault(op.pk, [])
        if not seq or seq[-1] != state:  # one change per transaction in the generator
            seq.append(state)
    vs = scd2_versions(events_from_bronze(rows, "customers"), ("email", "phone", "address"))
    got: dict[int, list[tuple[Any, ...]]] = {}
    for v in vs:
        got.setdefault(v.pk, []).append(v.attrs)
    assert got == history
    assert sum(len(h) > 1 for h in history.values()) > 20  # the comparison is not vacuous
    assert len(silver["customers"]) == len(history)
    assert sum(v.is_current for v in vs) == len(history)


def test_ledger_reconciles_with_generator_balances() -> None:
    rows, silver = events_from_generator(42, 2000)
    openings, txns, refunds = ledger_inputs_from_bronze(rows)
    entries = ledger_entries(openings, txns, refunds)
    gen = Generator(42)
    for _ in gen.txns(2000):
        pass
    expected = {a: row["balance"] for a, row in gen.s.accounts.items() if row["balance"] != 0}
    assert balances(entries) == expected
    assert {a: row["balance"] for a, row in silver["accounts"].items() if row["balance"] != 0} == expected
    assert len(refunds) > 10 and any(t.txn_type == "topup" for t in txns)  # refunds and topups are exercised
    assert all(s == 0 for s in sums_by(entries, "journal_id").values())
    assert all(s == 0 for s in sums_by(entries, "transaction_id").values())
