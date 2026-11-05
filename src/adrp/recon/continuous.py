"""Continuous mode (design S10): judge silver rows per key against the generator's ground truth at their own LSN.

A silver row at LSN L was written by exactly one Postgres transaction. The bronze probe gives LSN -> txid, the
generator run log gives xid32 -> txn_no, and the generator's ops give the versions that transaction wrote for the
key. Silver must equal one of them. This catches wrong values; it does not detect lag or loss, and it is a
reconciliation key, not row-level lineage. Rows whose transaction is not in the run log (snapshot rows,
out-of-band writes) are unjudged."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol

from adrp.recon.canonical import PK, canonical_row

XID_MOD = 2**32  # Debezium reports the 32-bit xid in source.txId


class OpLike(Protocol):
    """The generator's `adrp.gen.model.Op`."""

    @property
    def txn_no(self) -> int: ...
    @property
    def op(self) -> str: ...  # I | U | D
    @property
    def table(self) -> str: ...
    @property
    def pk(self) -> int: ...
    @property
    def row(self) -> Mapping[str, Any] | None: ...


def judge_at_lsn(
    silver_rows: Iterable[Mapping[str, Any]],
    lsn_to_txid: Mapping[int, int],
    xid_to_txn: Mapping[int, int],
    ops: Sequence[OpLike],
    columns: Sequence[tuple[str, str]],
    table: str | None = None,
) -> dict[str, int]:
    """{judged, unjudged, mismatches} for one table's silver rows (live and tombstones).

    A row is judged when its LSN maps to a txid and that xid to a generator txn. It matches when that txn has, for
    the key, a delete (silver tombstone) or an insert/update whose `columns` equal the silver row. `table`
    restricts `ops` to one table; leave it None when `ops` is already that table's."""
    versions: dict[tuple[int, int], list[OpLike]] = defaultdict(list)
    for o in ops:
        if table is None or o.table == table:
            versions[(o.txn_no, o.pk)].append(o)
    judged = unjudged = mismatches = 0
    for r in silver_rows:
        lsn = r.get("lsn")
        txid = lsn_to_txid.get(int(lsn)) if lsn is not None else None
        txn = xid_to_txn.get(txid % XID_MOD) if txid is not None else None
        if txn is None:
            unjudged += 1
            continue
        judged += 1
        cands = versions.get((txn, int(r[PK])), [])
        if r.get("is_deleted"):
            ok = any(o.op == "D" for o in cands)
        else:
            mine = canonical_row(r, columns)
            ok = any(o.op != "D" and o.row is not None and canonical_row(o.row, columns) == mine for o in cands)
        if not ok:
            mismatches += 1
    return {"judged": judged, "unjudged": unjudged, "mismatches": mismatches}
