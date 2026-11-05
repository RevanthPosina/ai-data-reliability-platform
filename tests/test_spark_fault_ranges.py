"""Pure parts of adrp.spark.fault (G10a): range parsing and the Kafka batch-read options. No Spark import."""

import json

import pytest

from adrp.spark import fault


def test_parse_ranges_keeps_non_empty_and_types_the_keys() -> None:
    ranges = fault.parse_ranges(json.dumps({"cdc.public.customers": {"0": [5, 9], "1": [3, 3], "2": [0, 1]}}))
    assert ranges == {"cdc.public.customers": {0: (5, 9), 2: (0, 1)}}


def test_parse_ranges_drops_topics_with_only_empty_ranges() -> None:
    assert fault.parse_ranges('{"cdc.public.accounts": {"0": [4, 4]}}') == {}


@pytest.mark.parametrize(
    "text",
    [
        "[]",
        '{"t": []}',
        '{"t": {"0": [1]}}',
        '{"t": {"0": [1, 2, 3]}}',
        '{"t": {"0": [2, 1]}}',
        '{"t": {"0": [-1, 1]}}',
        '{"t": {"0": ["1", 2]}}',
        '{"t": {"0": [1.5, 2]}}',
        '{"t": {"0": [true, 2]}}',
    ],
)
def test_parse_ranges_rejects_malformed(text: str) -> None:
    with pytest.raises(ValueError):
        fault.parse_ranges(text)


def test_kafka_options_cover_exactly_the_assigned_partitions() -> None:
    opts = fault.kafka_options({"cdc.public.b": {2: (7, 9), 0: (1, 4)}, "cdc.public.a": {1: (0, 2)}})
    assert json.loads(opts["assign"]) == {"cdc.public.a": [1], "cdc.public.b": [0, 2]}
    assert json.loads(opts["startingOffsets"]) == {"cdc.public.a": {"1": 0}, "cdc.public.b": {"0": 1, "2": 7}}
    assert json.loads(opts["endingOffsets"]) == {"cdc.public.a": {"1": 2}, "cdc.public.b": {"0": 4, "2": 9}}
    assert " " not in opts["assign"]
