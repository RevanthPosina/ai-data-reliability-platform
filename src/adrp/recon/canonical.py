"""Canonical row form and order-independent digests (design S10).

A Postgres row (row_to_json, floats parsed as Decimal) and a silver row (Spark to_json) must hash equal when they
hold the same values, whatever their textual form: `...12+00:00` vs `...120000+00:00`, `10.00` vs `10.0`, a NULL
vs a field the JSON omitted. Every value is normalized by its Postgres type, serialized with canonical_json and
hashed with xxh64. A table digest is the count and the sum of the row hashes mod 2**64, in total and per pk
bucket, so it does not depend on row order."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import xxhash

from adrp.common import canonical_json

PK = "id"
MASK = 2**64

_TS = re.compile(r"^(\d{4})-(\d\d)-(\d\d)[T ](\d\d):(\d\d):(\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d(?::?\d\d)?)?$")
_INT_TYPES = frozenset({"bigint", "integer", "smallint", "int"})
_TEXT_TYPES = frozenset({"text", "character", "character varying"})


def _parse_tz(tz: str) -> timezone:
    if tz == "Z":
        return timezone.utc
    sign = -1 if tz[0] == "-" else 1
    digits = tz[1:].replace(":", "")
    hours, minutes = int(digits[:2]), int(digits[2:4] or 0)
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _timestamptz(value: Any) -> str:
    if isinstance(value, str):
        m = _TS.match(value)
        if not m:
            raise ValueError(f"unparseable timestamptz: {value!r}")
        y, mo, d, h, mi, s, frac, tz = m.groups()
        if tz is None:
            raise ValueError(f"timestamptz without an offset: {value!r}")
        micro = int((frac or "0").ljust(6, "0")[:6])  # Postgres holds microseconds; finer digits are dropped
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), micro, tzinfo=_parse_tz(tz))
    elif isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("naive datetime for a timestamptz column")
        dt = value
    else:
        raise TypeError(f"timestamptz expects str or datetime, got {type(value).__name__}")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return date.fromisoformat(value).isoformat()
    raise TypeError(f"date expects str or date, got {type(value).__name__}")


def _numeric(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float, str)):
        raise TypeError(f"numeric expects a number, got {type(value).__name__}")
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    if not d.is_finite():
        raise ValueError(f"non-finite numeric: {value!r}")
    if d == 0:
        return "0"
    return format(d.normalize(), "f")


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("integer expects an int, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, (Decimal, float)) and value == int(value):
        return int(value)
    raise TypeError(f"integer expects an int, got {value!r}")


def normalize(value: Any, pg_type: str) -> Any:
    """The canonical form of one value of Postgres type `pg_type` (information_schema data_type).

    None stays None. Raises ValueError for an unknown type (a new column type must be decided on, not guessed)
    and TypeError when the value's Python type does not fit the column type."""
    if pg_type == "timestamp with time zone":
        return None if value is None else _timestamptz(value)
    if pg_type == "date":
        return None if value is None else _date(value)
    if pg_type == "numeric":
        return None if value is None else _numeric(value)
    if pg_type in _INT_TYPES:
        return None if value is None else _integer(value)
    if pg_type in _TEXT_TYPES:
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{pg_type} expects str, got {type(value).__name__}")
        return value
    if pg_type == "boolean":
        if value is not None and not isinstance(value, bool):
            raise TypeError(f"boolean expects bool, got {type(value).__name__}")
        return value
    raise ValueError(f"unknown pg_type: {pg_type!r}")


def canonical_row(row: Mapping[str, Any], columns: Sequence[tuple[str, str]]) -> str:
    """Canonical JSON of the listed (name, pg_type) columns. A missing field and a NULL are the same."""
    return canonical_json({name: normalize(row.get(name), pg_type) for name, pg_type in columns})


def row_hash(canonical: str) -> int:
    return int(xxhash.xxh64_intdigest(canonical.encode("utf-8")))


def pk_bucket(pk: int, buckets: int = 16) -> int:
    """Bucket of a primary key. Deliberately not Iceberg's bucket: it only localizes a mismatch."""
    return int(xxhash.xxh64_intdigest(str(pk).encode("utf-8"))) % buckets


def hex64(value: int) -> str:
    """16 hex digits split 8_8, so a manifest scan for 13-digit ids can never match inside a checksum."""
    h = f"{value % MASK:016x}"
    return f"{h[:8]}_{h[8:]}"


@dataclass
class TableDigest:
    count: int = 0
    checksum: int = 0
    # bucket -> (row count, checksum); only non-empty buckets are present
    per_bucket: dict[int, tuple[int, int]] = field(default_factory=dict)


def digest(rows: Iterable[Mapping[str, Any]], columns: Sequence[tuple[str, str]], buckets: int = 16) -> TableDigest:
    """Count and order-independent checksum (sum of row hashes mod 2**64) of `rows`, in total and per bucket.
    Each row must carry its primary key in `id`."""
    out = TableDigest()
    for row in rows:
        h = row_hash(canonical_row(row, columns))
        b = pk_bucket(int(row[PK]), buckets)
        n, s = out.per_bucket.get(b, (0, 0))
        out.per_bucket[b] = (n + 1, (s + h) % MASK)
        out.count += 1
        out.checksum = (out.checksum + h) % MASK
    return out
