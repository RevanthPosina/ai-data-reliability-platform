"""Kafka vs bronze offset comparison (design S10, ADR-0022).

No check here assumes contiguous offsets, because a compacting topic drops records. Bronze holds duplicate rows by
design (a replayed range is appended again), so the duplicate count is compared with the replay ledger exactly."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass
class OffsetResult:
    rows: int
    distinct: int
    kafka_present: int
    missing_in_bronze: int  # offsets Kafka still holds that bronze does not
    beyond_end: int  # bronze offsets at or above the end offset
    gaps: int  # offsets in [0, end) bronze does not hold (allowed when compacting)
    dup_rows: int  # rows - distinct
    expected_dups: int
    compacting: bool

    @property
    def dups_equal(self) -> bool:
        return self.dup_rows == self.expected_dups

    @property
    def ok(self) -> bool:
        return (
            self.missing_in_bronze == 0
            and self.beyond_end == 0
            and self.dups_equal
            and (self.compacting or self.gaps == 0)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "distinct": self.distinct,
            "kafka_present": self.kafka_present,
            "missing_in_bronze": self.missing_in_bronze,
            "beyond_end": self.beyond_end,
            "gaps": self.gaps,
            "dup_rows": self.dup_rows,
            "expected_dups": self.expected_dups,
            "dups_equal": self.dups_equal,
            "compacting": self.compacting,
            "ok": self.ok,
        }


def compare_offsets(
    kafka_present: Mapping[int, Collection[int]],
    bronze_rows_by_partition: Mapping[int, int],
    bronze_distinct: Mapping[int, Collection[int]],
    end: Mapping[int, int],
    compacting: bool,
    expected_dups: int,
) -> OffsetResult:
    """One topic. All maps are keyed by partition. `kafka_present`: offsets a reader found in Kafka below `end`.
    `bronze_distinct`: the distinct offsets bronze holds. `end`: high watermarks. `expected_dups`: the replay
    ledger. Passes when every Kafka-present offset is in bronze, no bronze offset is at or above `end`, the
    duplicate rows equal the ledger, and (only when not compacting) bronze has no gap below `end`."""
    missing = beyond = gaps = distinct = present = 0
    for p in set(kafka_present) | set(bronze_distinct) | set(end):
        e = end.get(p, 0)
        have = set(bronze_distinct.get(p, ()))
        kp = set(kafka_present.get(p, ()))
        distinct += len(have)
        present += len(kp)
        missing += len(kp - have)
        beyond += sum(1 for o in have if o >= e)
        gaps += sum(1 for o in range(e) if o not in have)
    rows = sum(bronze_rows_by_partition.values())
    return OffsetResult(rows, distinct, present, missing, beyond, gaps, rows - distinct, expected_dups, compacting)
