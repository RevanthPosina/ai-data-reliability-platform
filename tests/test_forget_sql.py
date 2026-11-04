import json

import pytest

from adrp.spark import forget_sql as fs


def test_sql_str_escapes_quotes_and_backslashes() -> None:
    assert fs.sql_str("a'b\\c") == "'a\\'b\\\\c'"


def test_pk_for_id_matches_bronze_canonical_json() -> None:
    from adrp.common import canonical_json

    assert fs.pk_for_id(1000000000123) == canonical_json({"id": 1000000000123}) == '{"id":1000000000123}'


def test_footprint_roundtrip_dedupes_and_drops_empty() -> None:
    fp = fs.parse_footprint(json.dumps({"public.customers": ['{"id":2}', '{"id":1}', '{"id":2}'], "public.x": []}))
    assert fp == {"public.customers": ['{"id":1}', '{"id":2}']}
    assert fs.parse_footprint(fs.dump_footprint(fp)) == fp
    assert fs.footprint_size(fp) == 2


@pytest.mark.parametrize("bad", ["[]", '{"t": "x"}', '{"t": [1]}'])
def test_parse_footprint_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError):
        fs.parse_footprint(bad)


def test_events_where_pairs_source_table_with_pks() -> None:
    fp = {"public.support_tickets": ['{"id":1}', '{"id":2}'], "public.customers": ['{"id":9}']}
    w = fs.events_where(fp)
    assert w == (
        "(source_table = 'public.customers' AND pk IN ('{\"id\":9}')) OR "
        "(source_table = 'public.support_tickets' AND pk IN ('{\"id\":1}', '{\"id\":2}'))"
    )
    assert fs.events_where({}) is None


def test_silver_where_single_key_and_missing_table() -> None:
    fp = {"public.support_tickets": ['{"id":1}', '{"id":2}']}
    assert fs.silver_where(fp, "support_tickets") == "`id` IN (1, 2)"
    assert fs.silver_where(fp, "accounts") is None


def test_silver_where_composite_key() -> None:
    fp = {"public.t": ['{"a":1,"b":"x"}']}
    assert fs.silver_where(fp, "t") == "(`a` = 1 AND `b` = 'x')"


def test_json_customer_where_and_merge() -> None:
    w = fs.json_customer_where([5, 3, 5])
    assert "get_json_object(after, '$.customer_id') IN ('3', '5')" in w
    assert "get_json_object(before, '$.customer_id') IN ('3', '5')" in w
    assert fs.merge_footprints({"t": ["b"]}, {"t": ["a", "b"], "u": ["c"]}) == {"t": ["a", "b"], "u": ["c"]}
