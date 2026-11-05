"""Canonical hashing: Postgres form and Spark form of one value must hash equal (G10a)."""

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from adrp.recon.canonical import (
    MASK,
    canonical_row,
    digest,
    hex64,
    normalize,
    pk_bucket,
    row_hash,
)

COLS = [
    ("id", "bigint"),
    ("name", "text"),
    ("amount", "numeric"),
    ("created_at", "timestamp with time zone"),
    ("born", "date"),
    ("active", "boolean"),
]


def h(row: dict[str, Any]) -> int:
    return row_hash(canonical_row(row, COLS))


def test_timestamp_postgres_form_equals_spark_form() -> None:
    assert normalize("2026-01-02T03:04:05.12+00:00", "timestamp with time zone") == normalize(
        "2026-01-02T03:04:05.120000+00:00", "timestamp with time zone"
    )
    assert normalize("2026-01-02T03:04:05.12+00:00", "timestamp with time zone") == normalize(
        "2026-01-02T03:04:05.120000Z", "timestamp with time zone"
    )


def test_timestamp_offsets_and_python_datetimes_normalize_to_utc() -> None:
    a = normalize("2026-01-02T05:04:05+02:00", "timestamp with time zone")
    assert a == normalize("2026-01-02T03:04:05+00", "timestamp with time zone")
    assert a == normalize(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "timestamp with time zone")
    assert a == "2026-01-02T03:04:05.000000+00:00"


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError):
        normalize("2026-01-02T03:04:05", "timestamp with time zone")


def test_numeric_scale_does_not_matter() -> None:
    assert normalize(Decimal("10.00"), "numeric") == normalize(Decimal("10.0"), "numeric") == "10"
    assert normalize("10.50", "numeric") == normalize(Decimal("10.5"), "numeric") == "10.5"
    assert normalize(Decimal("1E+2"), "numeric") == "100"
    assert normalize(Decimal("-0.00"), "numeric") == "0"
    assert normalize(7, "numeric") == "7"


def test_date_forms() -> None:
    assert normalize("2026-03-04", "date") == normalize(date(2026, 3, 4), "date") == "2026-03-04"


def test_null_equals_absent_field() -> None:
    base = {"id": 1, "name": "a", "amount": Decimal("1.0"), "created_at": None, "born": None, "active": True}
    absent = {k: v for k, v in base.items() if k not in ("created_at", "born")}
    assert h(base) == h(absent)


def test_null_differs_from_empty_string() -> None:
    assert h({"id": 1, "name": None}) != h({"id": 1, "name": ""})


def test_same_value_postgres_and_spark_forms_hash_equal() -> None:
    pg = {
        "id": 5,
        "name": "x",
        "amount": Decimal("10.00"),
        "created_at": "2026-01-02T03:04:05.12+00:00",
        "born": "2000-01-01",
        "active": False,
    }
    spark = {
        "id": 5,
        "name": "x",
        "amount": Decimal("10.0"),
        "created_at": "2026-01-02T03:04:05.120000+00:00",
        "born": "2000-01-01",
        "active": False,
        "lsn": 99,  # silver meta columns are not in `columns`
        "is_deleted": False,
    }
    assert h(pg) == h(spark)
    spark["amount"] = Decimal("10.01")
    assert h(pg) != h(spark)


def test_unknown_pg_type_raises() -> None:
    with pytest.raises(ValueError):
        normalize("x", "uuid")
    with pytest.raises(ValueError):
        canonical_row({"a": None}, [("a", "jsonb")])  # even a NULL: the type itself is unknown


def test_type_mismatch_raises() -> None:
    with pytest.raises(TypeError):
        normalize(True, "bigint")
    with pytest.raises(TypeError):
        normalize(5, "text")


def test_pk_bucket_range_and_stability() -> None:
    assert all(0 <= pk_bucket(i) < 16 for i in range(200))
    assert pk_bucket(1000000000042) == pk_bucket(1000000000042)
    assert {pk_bucket(i, 4) for i in range(200)} == {0, 1, 2, 3}


def test_hex64_never_holds_a_13_digit_run() -> None:
    assert hex64(0) == "00000000_00000000"
    assert hex64(MASK + 1) == "00000000_00000001"
    assert len(hex64(2**64 - 1)) == 17


ROWS = st.lists(
    st.builds(
        lambda i, n, a: {"id": i, "name": n, "amount": Decimal(a) / 100},
        st.integers(1, 10**13),
        st.text(max_size=8),
        st.integers(-(10**6), 10**6),
    ),
    max_size=40,
)
SMALL = [("id", "bigint"), ("name", "text"), ("amount", "numeric")]


@given(ROWS, st.randoms(use_true_random=False))
def test_digest_is_order_independent_and_mod_2_64(rows: list[dict[str, Any]], rnd: Any) -> None:
    shuffled = list(rows)
    rnd.shuffle(shuffled)
    d1, d2 = digest(rows, SMALL), digest(shuffled, SMALL)
    assert d1 == d2
    assert d1.count == len(rows)
    assert d1.checksum == sum(row_hash(canonical_row(r, SMALL)) for r in rows) % MASK
    assert d1.checksum < MASK
    assert sum(n for n, _ in d1.per_bucket.values()) == len(rows)
    assert sum(s for _, s in d1.per_bucket.values()) % MASK == d1.checksum


def test_checksum_actually_wraps() -> None:
    rows = [{"id": i, "name": "n", "amount": Decimal(i)} for i in range(1, 200)]
    raw = sum(row_hash(canonical_row(r, SMALL)) for r in rows)
    assert raw >= MASK  # the sum overflows 2**64, so the mod is exercised
    assert digest(rows, SMALL).checksum == raw % MASK


@given(ROWS.filter(lambda r: len({x["id"] for x in r}) == len(r) and len(r) > 0), st.data())
def test_one_changed_row_changes_table_and_exactly_one_bucket(rows: list[dict[str, Any]], data: st.DataObject) -> None:
    i = data.draw(st.integers(0, len(rows) - 1))
    changed = [dict(r) for r in rows]
    changed[i]["name"] = changed[i]["name"] + "!"
    before, after = digest(rows, SMALL), digest(changed, SMALL)
    assert before.count == after.count
    assert before.checksum != after.checksum
    diff = [
        b for b in set(before.per_bucket) | set(after.per_bucket) if before.per_bucket.get(b) != after.per_bucket.get(b)
    ]
    assert diff == [pk_bucket(int(rows[i]["id"]))]
