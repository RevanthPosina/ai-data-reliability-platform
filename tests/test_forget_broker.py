import pytest

from adrp.forget import broker, ids
from adrp.forget.rawscan import Subject

CID = 2_000_000_000_123
CANARY = "g8c-" + "cd" * 16
SUBJ = Subject({"customers": [CID], "support_tickets": [7, 8]}, canaries=(CANARY,))


def test_references_by_key() -> None:
    assert broker.references("support_tickets", 7, None, SUBJ)  # tombstone of a footprint key
    assert not broker.references("support_tickets", 9, None, SUBJ)
    assert not broker.references("accounts", 7, None, SUBJ)  # the same pk in another table is another entity


def test_references_by_customer_id_in_either_image() -> None:
    env = {"before": {"id": 5, "customer_id": CID}, "after": None}
    assert broker.references("accounts", 5, env, SUBJ)
    assert broker.references("accounts", 5, {"before": None, "after": {"customer_id": CID}}, SUBJ)
    assert broker.references("customers", 99, {"after": {"id": CID}}, SUBJ)
    assert not broker.references("accounts", 5, {"after": {"customer_id": 1_000_000_000_001}}, SUBJ)  # the placeholder


def test_references_by_canary_in_any_string_field() -> None:
    env = {"after": {"id": 50, "body": f"hello {CANARY} bye", "n": 3}}
    assert broker.references("support_tickets", 50, env, SUBJ)
    assert not broker.references("support_tickets", 50, {"after": {"id": 50, "body": "hello"}}, SUBJ)


def test_references_ignores_non_mapping_images() -> None:
    assert not broker.references("accounts", 5, {"before": None, "after": None}, SUBJ)


def test_scan_count_absent_requires_complete_and_clean() -> None:
    assert broker.ScanCount(100, 0, 0, 0, True).absent
    assert not broker.ScanCount(100, 0, 0, 0, False).absent
    assert not broker.ScanCount(100, 1, 1, 0, True).absent
    assert not broker.ScanCount(100, 0, 0, 2, True).absent


def test_raw_count_absent() -> None:
    assert broker.RawCount(0, 0, 0, 0).absent
    assert not broker.RawCount(1, 0, 0, 0).absent and not broker.RawCount(0, 1, 0, 0).absent
    assert not broker.RawCount(0, 0, 1, 0).absent  # a grep error is not evidence of absence


def test_nudge_sql_excludes_subject_rows_and_uses_a_noop_column() -> None:
    sql = broker.nudge_sql("support_tickets", [8, 7], 30)
    assert "SET updated_at = updated_at" in sql and "ARRAY[7,8]::bigint[]" in sql and "LIMIT 30" in sql
    assert "ARRAY[]::bigint[]" in broker.nudge_sql("customers", [], 3)
    assert "SET status = status" in broker.nudge_sql("transactions", [], 3)
    with pytest.raises(ValueError):
        broker.nudge_sql("customers; drop", [], 3)


def test_removal_bound_and_statement() -> None:
    cfg = {"cleanup.policy": "compact,delete", "min.compaction.lag.ms": "300000", "segment.ms": "30000"}
    assert broker.removal_bound_s(cfg) == 350.0
    s = broker.broker_bound(cfg, segment_min_ms=10000)
    assert "~350 s" in s and "tombstone needs a later write" in s.lower()
    clamped = broker.broker_bound({**cfg, "segment.ms": "30000"}, segment_min_ms=600000)
    assert "does not roll on time" in clamped


def test_broker_bound_without_compaction_is_unbounded() -> None:
    cfg = {"cleanup.policy": "delete", "retention.ms": "604800000"}
    assert broker.removal_bound_s(cfg) is None
    assert "does not compact" in broker.broker_bound(cfg) and "Not removed" in broker.broker_bound(cfg)


def test_key_patterns_for_a_topic_find_a_framed_key() -> None:
    from adrp.forget.rawscan import kafka_key_patterns

    pats = kafka_key_patterns(SUBJ.entities["support_tickets"], [11], SUBJ.customer_ids)
    assert ids.confluent_key_bytes(11, 7) in pats and ids.zigzag_varint(CID) in pats
