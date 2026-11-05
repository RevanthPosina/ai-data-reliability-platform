"""Postgres vs silver TableResult counts (G10a)."""

import json
import re
from decimal import Decimal
from typing import Any

from adrp.recon.compare import UNAVAILABLE, TableResult, compare_table

COLS = [("id", "bigint"), ("name", "text"), ("amount", "numeric"), ("created_at", "timestamp with time zone")]


def pg(i: int, name: str = "n", amount: str = "1.50") -> dict[str, Any]:
    return {"id": i, "name": name, "amount": Decimal(amount), "created_at": "2026-01-02T03:04:05.12+00:00"}


def sv(i: int, name: str = "n", amount: str = "1.5", lsn: int = 10, **kw: Any) -> dict[str, Any]:
    return {
        "id": i,
        "name": name,
        "amount": Decimal(amount),
        "created_at": "2026-01-02T03:04:05.120000+00:00",
        "lsn": lsn,
        "is_deleted": False,
        "op": "u",
        "source_ts": "x",
        **kw,
    }


def tomb(i: int, lsn: int = 20, **kw: Any) -> dict[str, Any]:
    return {
        "id": i,
        "name": None,
        "amount": None,
        "created_at": None,
        "lsn": lsn,
        "is_deleted": True,
        "op": "d",
        "source_ts": "x",
        **kw,
    }


def run(pg_rows: list[dict[str, Any]], silver: list[dict[str, Any]], **kw: Any) -> TableResult:
    return compare_table("accounts", pg_rows, silver, COLS, **kw)


def test_clean_table_is_ok() -> None:
    r = run([pg(1), pg(2)], [sv(1), sv(2)])
    assert r.ok
    assert (r.pg_count, r.silver_live, r.silver_tombstones) == (2, 2, 0)
    assert r.digest_equal and r.bucket_mismatches == 0 and r.diff_keys == {}


def test_missing() -> None:
    r = run([pg(1), pg(2)], [sv(1)])
    assert r.missing == 1 and not r.ok and not r.digest_equal
    assert r.diff_keys == {"missing": [2]}


def test_tombstone_over_a_live_postgres_row_is_missing() -> None:
    r = run([pg(1)], [tomb(1)])
    assert r.missing == 1 and r.silver_tombstones == 1 and not r.ok


def test_differing() -> None:
    r = run([pg(1), pg(2)], [sv(1), sv(2, name="other")])
    assert r.differing == 1 and not r.ok and r.bucket_mismatches >= 1


def test_resurrected_vs_extra_live() -> None:
    r = run([pg(1)], [sv(1), sv(7), sv(8)], deleted_ever={7})
    assert (r.resurrected, r.extra_live) == (1, 1)
    assert r.diff_keys["resurrected"] == [7] and r.diff_keys["extra_live"] == [8]
    assert not r.ok


def test_tombstone_with_values_is_counted() -> None:
    r = run([], [tomb(3, name="pii"), tomb(4)])
    assert r.tombstone_nonnull == 1 and not r.ok
    assert r.diff_keys == {"tombstone_nonnull": [3]}


def test_tombstone_with_only_meta_columns_is_clean() -> None:
    assert run([], [tomb(3)]).ok


def test_no_tombstone_required_for_a_delete_never_seen() -> None:
    # a row inserted and deleted before the snapshot is in neither Postgres nor silver
    r = run([pg(1)], [sv(1)], deleted_ever={99})
    assert r.ok


def test_duplicates() -> None:
    r = run([pg(1)], [sv(1, lsn=5), sv(1, lsn=9)])
    assert r.duplicates == 1 and not r.ok
    assert r.differing == 0  # judged on the highest-LSN row


def test_duplicate_judged_on_highest_lsn() -> None:
    r = run([pg(1)], [sv(1, lsn=5), sv(1, lsn=9, name="stale")])
    assert r.duplicates == 1 and r.differing == 1


def test_unavailable_value() -> None:
    r = run([pg(1, name=UNAVAILABLE)], [sv(1, name=UNAVAILABLE)])
    assert r.unavailable_values == 1 and not r.ok and r.differing == 0


def test_silver_only_column_nonnull_is_a_mismatch_and_null_is_reported() -> None:
    r = run([pg(1)], [sv(1, extra="v", empty=None)])
    assert r.silver_only_columns == 1 and r.silver_only_null_columns == 1 and not r.ok
    r2 = run([pg(1)], [sv(1, empty=None)])
    assert r2.ok and r2.silver_only_null_columns == 1


def test_unreconciled_quarantine() -> None:
    assert run([pg(1)], [sv(1, lsn=10)], quarantined={1: 10}).unreconciled_quarantine == 1
    assert run([pg(1)], [sv(1, lsn=11)], quarantined={1: 10}).unreconciled_quarantine == 0
    assert run([], [], quarantined={1: 10}).unreconciled_quarantine == 1
    assert run([], [tomb(1, lsn=30)], quarantined={1: 10}).ok  # a later tombstone is an applied version


def test_to_dict_has_counts_buckets_and_hex_only() -> None:
    ids = [1000000000123, 1000000000456, 1000000000789]
    r = run([pg(i) for i in ids], [sv(ids[0]), sv(ids[1], name="x"), sv(555555555555), tomb(ids[2], name="pii")])
    d = r.to_dict()
    text = json.dumps(d)
    assert not re.search(r"(?<!\d)\d{13}(?!\d)", text)
    for i in ids:
        assert str(i) not in text
    assert "diff_keys" not in d and str(ids[0]) not in repr(r)
    assert re.fullmatch(r"[0-9a-f]{8}_[0-9a-f]{8}", d["pg_checksum"])
    assert isinstance(d["mismatch_buckets"], list) and all(isinstance(b, int) for b in d["mismatch_buckets"])
    assert d["ok"] is False and d["digest_equal"] is False
