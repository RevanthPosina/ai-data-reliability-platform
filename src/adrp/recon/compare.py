"""Postgres vs silver comparison for one table (design S10).

Silver is judged against the Postgres table read at a consistent cut. Live silver rows (NOT is_deleted) must equal
the Postgres rows; a tombstone must have no live Postgres row and no non-NULL non-key value. A tombstone is not
required for every Postgres delete: rows inserted and deleted before the snapshot never reached CDC.

Diff ids are kept on `TableResult.diff_keys` (for the gitignored .run/ diff file) and never in `to_dict()`, which
holds counts, bucket numbers and hex checksums only."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from adrp.recon.canonical import PK, canonical_row, digest, hex64

META = frozenset({"lsn", "is_deleted", "op", "source_ts"})
UNAVAILABLE = "__debezium_unavailable_value"

# the counts that must be 0 for a table to be clean (the others are informational)
FAILING = (
    "missing",
    "extra_live",
    "resurrected",
    "differing",
    "duplicates",
    "tombstone_nonnull",
    "unavailable_values",
    "bucket_mismatches",
    "silver_only_columns",
    "unreconciled_quarantine",
)


@dataclass
class TableResult:
    table: str
    pg_count: int = 0  # informational
    silver_live: int = 0  # informational
    silver_tombstones: int = 0  # informational
    missing: int = 0  # Postgres row with no live silver row
    extra_live: int = 0  # live silver row absent from Postgres, never deleted by the generator
    resurrected: int = 0  # live silver row absent from Postgres, key in deleted_ever
    differing: int = 0  # both sides live, canonical rows differ
    duplicates: int = 0  # keys with more than one silver row
    tombstone_nonnull: int = 0  # tombstones that still hold a non-NULL non-key value
    unavailable_values: int = 0  # cells equal to __debezium_unavailable_value
    digest_equal: bool = True  # count and checksum, total and per bucket
    bucket_mismatches: int = 0
    silver_only_columns: int = 0  # silver-only columns holding a non-NULL value on a live row
    unreconciled_quarantine: int = 0  # quarantined keys without a later applied silver version
    silver_only_null_columns: int = 0  # informational: silver-only columns that are NULL everywhere
    pg_checksum: int = 0
    silver_checksum: int = 0
    mismatch_buckets: list[int] = field(default_factory=list)
    diff_keys: dict[str, list[int]] = field(default_factory=dict, repr=False, compare=False)

    @property
    def ok(self) -> bool:
        return self.digest_equal and all(getattr(self, name) == 0 for name in FAILING)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"table": self.table, "ok": self.ok}
        for name in ("pg_count", "silver_live", "silver_tombstones", *FAILING, "silver_only_null_columns"):
            out[name] = getattr(self, name)
        out["digest_equal"] = self.digest_equal
        out["pg_checksum"] = hex64(self.pg_checksum)
        out["silver_checksum"] = hex64(self.silver_checksum)
        out["mismatch_buckets"] = sorted(self.mismatch_buckets)
        return out


def _lsn(row: Mapping[str, Any]) -> int:
    v = row.get("lsn")
    return int(v) if v is not None else -1


def compare_table(
    table: str,
    pg_rows: Iterable[Mapping[str, Any]],
    silver_rows: Iterable[Mapping[str, Any]],
    columns: Sequence[tuple[str, str]],
    deleted_ever: Iterable[int] = frozenset(),
    quarantined: Mapping[int, int] | None = None,
) -> TableResult:
    """Compare one table. `columns` are the Postgres (name, pg_type) pairs and must include `id`.
    `deleted_ever`: keys the generator deleted at some point. `quarantined`: key -> LSN of its quarantined event."""
    res = TableResult(table)
    deleted = set(deleted_ever)
    pg = {int(r[PK]): r for r in pg_rows}
    res.pg_count = len(pg)

    by_key: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    all_silver: list[Mapping[str, Any]] = []
    for r in silver_rows:
        all_silver.append(r)
        by_key[int(r[PK])].append(r)
    res.unavailable_values = sum(1 for r in all_silver for v in r.values() if v == UNAVAILABLE)

    keys: dict[str, set[int]] = defaultdict(set)
    chosen: dict[int, Mapping[str, Any]] = {}
    for k, rows in by_key.items():
        if len(rows) > 1:
            keys["duplicates"].add(k)
        chosen[k] = max(rows, key=_lsn)  # the highest LSN wins, as in the MERGE
    live = {k: r for k, r in chosen.items() if not r.get("is_deleted")}
    tombstones = {k: r for k, r in chosen.items() if r.get("is_deleted")}
    res.silver_live, res.silver_tombstones = len(live), len(tombstones)

    for k, pg_row in pg.items():
        s = live.get(k)
        if s is None:
            keys["missing"].add(k)
        elif canonical_row(pg_row, columns) != canonical_row(s, columns):
            keys["differing"].add(k)
    for k in live:
        if k not in pg:
            keys["resurrected" if k in deleted else "extra_live"].add(k)
    for k, t in tombstones.items():
        if any(v is not None for c, v in t.items() if c != PK and c not in META):
            keys["tombstone_nonnull"].add(k)

    names = {c for c, _ in columns}
    nonnull: set[str] = set()
    seen: set[str] = set()
    for r in live.values():
        for c, v in r.items():
            if c not in names and c not in META:
                seen.add(c)
                if v is not None:
                    nonnull.add(c)
    res.silver_only_columns = len(nonnull)
    res.silver_only_null_columns = len(seen - nonnull)

    for k, qlsn in (quarantined or {}).items():
        s = chosen.get(k)
        if s is None or _lsn(s) <= qlsn:
            keys["unreconciled_quarantine"].add(k)

    pg_d = digest(pg.values(), columns)
    sv_d = digest(live.values(), columns)
    res.pg_checksum, res.silver_checksum = pg_d.checksum, sv_d.checksum
    bad = sorted(
        b
        for b in set(pg_d.per_bucket) | set(sv_d.per_bucket)
        if pg_d.per_bucket.get(b, (0, 0)) != sv_d.per_bucket.get(b, (0, 0))
    )
    res.mismatch_buckets, res.bucket_mismatches = bad, len(bad)
    res.digest_equal = pg_d.count == sv_d.count and pg_d.checksum == sv_d.checksum and not bad

    for name in (
        "missing",
        "extra_live",
        "resurrected",
        "differing",
        "duplicates",
        "tombstone_nonnull",
        "unreconciled_quarantine",
    ):
        setattr(res, name, len(keys[name]))
    res.diff_keys = {name: sorted(ids) for name, ids in keys.items() if ids}
    return res
