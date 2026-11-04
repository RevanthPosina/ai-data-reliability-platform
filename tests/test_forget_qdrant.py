from adrp.forget import qdrant as q
from adrp.forget.rawscan import GrepCount, Subject

BIG = 2_000_000_000_123
SUBJ = Subject({"support_tickets": [12, BIG]}, canaries=("g8c-abc",))


def test_entity_filter_matches_any_of_the_ids() -> None:
    f = q.entity_filter(["support_tickets:1", "support_tickets:2"])
    assert f == {"must": [{"key": "entity_id", "match": {"any": ["support_tickets:1", "support_tickets:2"]}}]}


def test_batches_split_at_the_batch_size() -> None:
    items = [f"support_tickets:{i}" for i in range(q.BATCH * 2 + 1)]
    sizes = [len(b) for b in q._batches(items)]
    assert sizes == [q.BATCH, q.BATCH, 1]


def test_subject_patterns_quote_small_ids_and_bare_for_big_ones() -> None:
    pats = q.subject_patterns(SUBJ)
    assert b'"support_tickets:12"' in pats and b"support_tickets:12" not in pats  # would match :123
    assert f'"support_tickets:{BIG}"'.encode() in pats and f"support_tickets:{BIG}".encode() in pats
    assert b"g8c-abc" in pats


def test_classify_splits_hits_by_location() -> None:
    g = GrepCount(
        frozenset(
            {
                "/qdrant/storage/collections/c/0/wal/open-1",
                "/qdrant/storage/collections/c/0/segments/u1/payload_storage/page_0.dat",
                "/qdrant/storage/collections/c/0/segments/u2/payload_index/x/page_0.dat",
                "/qdrant/storage/collections/c/payload_index.json",
            }
        ),
        errors=0,
    )
    r = q.classify(g)
    assert (r.segment_files, r.wal_files, r.other_files, r.total) == (2, 1, 1, 4) and not r.absent


def test_bound_strings_only_describe_what_is_present() -> None:
    assert q.bound_strings(0, 32, q.QdrantRaw(0, 0, 0, 0)) == []
    wal = q.bound_strings(0, 32, q.QdrantRaw(0, 1, 0, 0))
    assert len(wal) == 1 and "2 x wal_capacity_mb = 64 MiB" in wal[0] and "no API to force" in wal[0]
    seg = q.bound_strings(2, 32, q.QdrantRaw(3, 0, 0, 0))
    assert len(seg) == 1 and "2 segment(s) hold fewer than 100 vectors" in seg[0]
    no_small = q.bound_strings(0, 32, q.QdrantRaw(1, 0, 0, 0))
    assert "no segment is below the vacuum floor" in no_small[0]


def test_vacuum_config_respects_qdrants_floor() -> None:
    assert q.VACUUM_CONFIG["vacuum_min_vector_number"] >= 100 and q.VACUUM_CONFIG["deleted_threshold"] < 0.001


def test_bad_collection_name_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        q._name("a/../b")
