"""Schema-evolution rules for silver (PLAN G3), pure so they can be unit tested.

Given the silver table's current data columns and the columns of the writer schema a record was
serialized with, decide whether the record can be applied and what must change in the table first.

  identical column types                          ok
  new columns                                     ok -> ALTER TABLE ADD COLUMN (nullable)
  columns missing from the writer schema          ok -> they become NULL for that record
  widening (int->bigint, float->double,
            decimal(p,s)->decimal(p',s), p'>p)    ok -> ALTER COLUMN TYPE (Iceberg type promotion)
  anything else (narrowing, string<->number...)   SCHEMA_INCOMPATIBLE -> quarantine
  columns removed AND columns added together      SCHEMA_INCOMPATIBLE (a rename looks exactly like drop + add)
  a key column removed or retyped                 SCHEMA_INCOMPATIBLE
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

REASON = "SCHEMA_INCOMPATIBLE"
_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)
_PROMOTE = {("INT", "BIGINT"), ("FLOAT", "DOUBLE")}


@dataclass(frozen=True)
class Verdict:
    compatible: bool
    adds: tuple[tuple[str, str], ...] = ()
    promotions: tuple[tuple[str, str], ...] = ()
    detail: str = ""
    reason: str = field(default="", compare=False)

    @property
    def quarantine_reason(self) -> str:
        return f"{REASON}: {self.detail}"


def norm(ddl: str) -> str:
    return re.sub(r"\s+", "", ddl).upper()


def is_widening(old: str, new: str) -> bool:
    old, new = norm(old), norm(new)
    if (old, new) in _PROMOTE:
        return True
    mo, mn = _DECIMAL.match(old), _DECIMAL.match(new)
    return bool(mo and mn and mo.group(2) == mn.group(2) and int(mn.group(1)) > int(mo.group(1)))


def classify(table_cols: list[tuple[str, str]], writer_cols: list[tuple[str, str]], keys: list[str]) -> Verdict:
    """table_cols / writer_cols: [(column, spark ddl type)] (data columns only)."""
    table = dict(table_cols)
    writer = dict(writer_cols)
    added = [c for c in writer if c not in table]
    removed = [c for c in table if c not in writer]
    promotions: list[tuple[str, str]] = []
    for col in writer.keys() & table.keys():
        if norm(table[col]) == norm(writer[col]):
            continue
        if is_widening(table[col], writer[col]):
            promotions.append((col, writer[col]))
            continue
        return Verdict(False, detail=f"column {col} changed type {table[col]} -> {writer[col]}")
    key_gone = [k for k in keys if k in removed]
    if key_gone:
        return Verdict(False, detail=f"key column(s) removed: {key_gone}")
    if removed and added:
        return Verdict(False, detail=f"possible rename: removed {removed}, added {added}")
    return Verdict(True, adds=tuple((c, writer[c]) for c in added), promotions=tuple(promotions))
