"""Kafka vs bronze offsets, with and without compaction (G10a)."""

from adrp.recon.offsets import OffsetResult, compare_offsets


def run(
    present: dict[int, set[int]],
    rows: dict[int, int],
    distinct: dict[int, set[int]],
    end: dict[int, int],
    compacting: bool = False,
    dups: int = 0,
) -> OffsetResult:
    return compare_offsets(present, rows, distinct, end, compacting, dups)


def test_contiguous_clean() -> None:
    r = run({0: {0, 1, 2}}, {0: 3}, {0: {0, 1, 2}}, {0: 3})
    assert r.ok and r.gaps == 0 and r.dup_rows == 0


def test_gap_passes_when_compacting_and_fails_when_not() -> None:
    args = ({0: {0, 2}}, {0: 2}, {0: {0, 2}}, {0: 3})
    assert run(*args, compacting=True).ok
    strict = run(*args, compacting=False)
    assert not strict.ok and strict.gaps == 1 and strict.missing_in_bronze == 0


def test_duplicate_ledger_must_be_exact() -> None:
    present, distinct, end = {0: {0, 1}}, {0: {0, 1}}, {0: 2}
    assert run(present, {0: 5}, distinct, end, dups=3).ok
    assert not run(present, {0: 5}, distinct, end, dups=2).ok
    assert not run(present, {0: 5}, distinct, end, dups=4).ok
    assert not run(present, {0: 2}, distinct, end, dups=1).ok


def test_offset_beyond_end_fails() -> None:
    r = run({0: {0}}, {0: 2}, {0: {0, 5}}, {0: 3}, compacting=True)
    assert r.beyond_end == 1 and not r.ok
    assert not run({}, {1: 1}, {1: {0}}, {0: 3}, compacting=True).ok  # partition unknown to `end`


def test_kafka_present_missing_from_bronze_fails_in_both_modes() -> None:
    for compacting in (True, False):
        r = run({0: {0, 1, 2}}, {0: 2}, {0: {0, 2}}, {0: 3}, compacting=compacting)
        assert r.missing_in_bronze == 1 and not r.ok


def test_multiple_partitions_and_to_dict() -> None:
    r = run({0: {0, 1}, 1: {0}}, {0: 3, 1: 1}, {0: {0, 1}, 1: {0}}, {0: 2, 1: 1}, dups=1)
    assert r.ok and (r.rows, r.distinct, r.kafka_present) == (4, 3, 3)
    d = r.to_dict()
    assert d["ok"] is True and d["dups_equal"] is True and all(not isinstance(v, (list, set)) for v in d.values())
