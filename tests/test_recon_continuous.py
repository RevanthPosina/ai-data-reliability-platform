"""Continuous mode: per-key judging at the row's own LSN (G10a)."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from adrp.recon.continuous import judge_at_lsn

COLS = [("id", "bigint"), ("name", "text"), ("amount", "numeric")]


@dataclass(frozen=True)
class FakeOp:
    txn_no: int
    op: str
    table: str
    pk: int
    row: dict[str, Any] | None


OPS = [
    FakeOp(1, "I", "accounts", 1, {"id": 1, "name": "a", "amount": Decimal("1.0")}),
    FakeOp(2, "U", "accounts", 1, {"id": 1, "name": "b", "amount": Decimal("2.00")}),
    FakeOp(2, "U", "accounts", 1, {"id": 1, "name": "c", "amount": Decimal("3")}),  # two versions in one txn
    FakeOp(3, "D", "accounts", 1, None),
    FakeOp(2, "I", "customers", 1, {"id": 1, "name": "other", "amount": Decimal("9")}),
]
LSN_TXID = {100: 7001, 200: 7002, 300: 7003, 400: 7004}
XID_TXN = {7001: 1, 7002: 2, 7003: 3}  # 7004 is not a generator transaction


def silver(lsn: int, name: str, amount: str, deleted: bool = False) -> dict[str, Any]:
    return {"id": 1, "name": name, "amount": Decimal(amount), "lsn": lsn, "is_deleted": deleted}


def judge(rows: list[dict[str, Any]], table: str | None = "accounts") -> dict[str, int]:
    return judge_at_lsn(rows, LSN_TXID, XID_TXN, OPS, COLS, table)


def test_match_including_either_version_in_a_txn_and_scale() -> None:
    res = judge([silver(100, "a", "1.00"), silver(200, "b", "2"), silver(200, "c", "3.0")])
    assert res == {"judged": 3, "unjudged": 0, "mismatches": 0}


def test_tombstone_matches_a_delete_op() -> None:
    row = {"id": 1, "name": None, "amount": None, "lsn": 300, "is_deleted": True}
    assert judge([row]) == {"judged": 1, "unjudged": 0, "mismatches": 0}


def test_mismatch() -> None:
    res = judge([silver(100, "WRONG", "1"), silver(200, "b", "99"), silver(300, "a", "1")])
    assert res == {"judged": 3, "unjudged": 0, "mismatches": 3}  # the last: live row where the txn deleted


def test_tombstone_where_txn_did_not_delete_is_a_mismatch() -> None:
    row = {"id": 1, "name": None, "amount": None, "lsn": 100, "is_deleted": True}
    assert judge([row])["mismatches"] == 1


def test_unjudged() -> None:
    rows = [
        silver(400, "a", "1"),  # txid not in the run log (snapshot or out-of-band)
        silver(999, "a", "1"),  # lsn unknown to bronze
        {"id": 1, "name": "a", "amount": Decimal("1"), "lsn": None, "is_deleted": False},
    ]
    assert judge(rows) == {"judged": 0, "unjudged": 3, "mismatches": 0}


def test_txn_without_this_key_is_a_mismatch() -> None:
    row = {"id": 2, "name": "a", "amount": Decimal("1"), "lsn": 100, "is_deleted": False}
    assert judge([row])["mismatches"] == 1


def test_table_filter_and_xid_wraparound() -> None:
    assert judge([silver(200, "other", "9")], table="customers")["mismatches"] == 0
    assert judge([silver(200, "other", "9")], table="accounts")["mismatches"] == 1
    res = judge_at_lsn([silver(100, "a", "1")], {100: 7001 + 2**32}, XID_TXN, OPS, COLS, "accounts")
    assert res["judged"] == 1 and res["mismatches"] == 0
