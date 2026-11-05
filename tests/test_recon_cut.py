"""Spark checkpoint parsing and coverage (G10a)."""

import pytest

from adrp.recon.cut import latest_committed, parse_offsets_file, uncovered

OFFSETS_FILE = (
    "v1\n"
    '{"batchWatermarkMs":0,"batchTimestampMs":1772200000000,"conf":{"spark.sql.shuffle.partitions":"4"}}\n'
    '{"cdc.public.customers":{"0":120,"1":98,"2":131},"cdc.public.accounts":{"0":7,"1":9,"2":12}}'
)


def test_parse_offsets_file_three_lines() -> None:
    assert parse_offsets_file(OFFSETS_FILE) == {
        "cdc.public.customers": {0: 120, 1: 98, 2: 131},
        "cdc.public.accounts": {0: 7, 1: 9, 2: 12},
    }
    assert parse_offsets_file(OFFSETS_FILE + "\n")["cdc.public.accounts"][2] == 12


@pytest.mark.parametrize("bad", ["", "v1\n{}\n-", 'v1\n{"batchWatermarkMs":0}', "v1\n[1]"])
def test_parse_offsets_file_rejects_other_shapes(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_offsets_file(bad)


def test_latest_committed_ignores_crc_and_non_numeric() -> None:
    names = ["0", "1", "2", ".2.crc", ".10.crc", "9.crc", "10.tmp", "2", "foo"]
    assert latest_committed(names) == 2
    assert latest_committed(["1", "9", "10"]) == 10  # numeric, not lexical
    assert latest_committed([".0.crc", "metadata"]) is None
    assert latest_committed([]) is None


def test_uncovered_empty_means_covered() -> None:
    end = {"t": {0: 10, 1: 0}}
    assert uncovered({"t": {0: 10, 1: 0}}, end) == {}
    assert uncovered({"t": {0: 12}}, end) == {}  # ahead of an earlier end read
    assert uncovered({}, {"t": {0: 0}}) == {}  # an empty partition needs no commit


def test_uncovered_reports_lag_and_missing_partition() -> None:
    end = {"t": {0: 10, 1: 5, 2: 7}, "u": {0: 3}}
    committed = {"t": {0: 10, 1: 4}}
    assert uncovered(committed, end) == {"t": {1: 1, 2: 7}, "u": {0: 3}}
