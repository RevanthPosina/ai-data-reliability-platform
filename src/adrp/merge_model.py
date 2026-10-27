"""Pure-Python model of the silver merge (PLAN G3). It is the executable spec of the Spark MERGE and the
oracle for the property tests.

State per key: (lsn, is_deleted, values). One batch is applied as:
  (a) dedupe within the batch to the max (lsn, offset) per key
  (b) WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE; WHEN NOT MATCHED THEN INSERT
  (c) a delete is a soft tombstone (is_deleted=True, values=None), never a hard delete
Applying the same batch again, or any batch in any order, cannot change the result: per key the row with
the highest LSN wins, and LSNs are unique per change (checked on real Debezium output in G3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

Values = dict[str, Any] | None


@dataclass(frozen=True)
class Event:
    key: int
    lsn: int
    op: str  # r | c | u | d
    values: Values
    offset: int = 0  # Kafka offset: tie-break for identical LSN within one batch (snapshot rows)


@dataclass(frozen=True)
class Row:
    lsn: int
    is_deleted: bool
    values: Values


State = dict[int, Row]


def to_row(e: Event) -> Row:
    return Row(e.lsn, e.op == "d", None if e.op == "d" else e.values)


def dedupe(batch: list[Event]) -> list[Event]:
    best: dict[int, Event] = {}
    for e in batch:
        cur = best.get(e.key)
        if cur is None or (e.lsn, e.offset) > (cur.lsn, cur.offset):
            best[e.key] = e
    return list(best.values())


def apply_batch(state: State, batch: list[Event]) -> State:
    out = dict(state)
    for e in dedupe(batch):
        cur = out.get(e.key)
        if cur is None or e.lsn > cur.lsn:
            out[e.key] = to_row(e)
    return out


def oracle(events: list[Event]) -> State:
    """The expected final state: for each key, the event with the highest LSN."""
    return apply_batch({}, events)
