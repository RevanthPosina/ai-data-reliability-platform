import pytest

from adrp.forget import ids
from adrp.forget import rawscan as rs

CANARY = "g8c-" + "ab" * 16
BIG = 2_000_000_000_123


def test_needles_text_skips_small_ids_and_includes_canaries() -> None:
    n = rs.Needles(canaries=(CANARY,), ids=(5, BIG))
    assert n.text() == [CANARY.encode(), str(BIG).encode()]
    assert bool(n) and not bool(rs.Needles())


def test_pg_int8_has_trailing_zero_bytes_stripped_and_small_ids_skipped() -> None:
    (pat,) = rs.Needles(ids=(BIG, 7)).pg_int8()
    assert pat == BIG.to_bytes(8, "little").rstrip(b"\0")
    assert not pat.endswith(b"\0") and len(pat) >= 5


def test_kafka_key_patterns_match_the_matcher_and_the_framed_layout() -> None:
    pats = rs.kafka_key_patterns([BIG, 42], schema_ids=[9], bare_ids=[3_000_000_000_001])
    assert ids.confluent_key_bytes(9, BIG) in pats and ids.confluent_key_bytes(9, 42) in pats
    assert ids.zigzag_varint(BIG) in pats and ids.zigzag_varint(3_000_000_000_001) in pats
    assert ids.zigzag_varint(42) not in pats  # a short varint matches anything
    data = b"junk" + ids.confluent_key_bytes(9, BIG) + b"more"
    assert ids.Matcher([BIG], schema_ids=[9]).hits(data).key_bytes >= 1
    assert any(p in data for p in pats)


def test_pattern_blob_drops_empty_and_splits_newline_patterns() -> None:
    blob, approx, dropped = rs.pattern_blob([b"", b"abcdef", b"xx\nlonger-piece\nyy", b"a\nb"])
    assert blob == b"abcdef\nlonger-piece\n"
    assert (approx, dropped) == (1, 1)  # one cut at a newline, one too short to search


def test_pattern_blob_cuts_nul_for_busybox_and_counts_it() -> None:
    blob, approx, dropped = rs.pattern_blob([b"abcdef\0\x01\x02", b"\0abc", b"plain"], cut_at_nul=True)
    assert blob == b"abcdef\nplain\n" and (approx, dropped) == (1, 1)
    blob2, a2, d2 = rs.pattern_blob([b"abcdef\0\x01"], cut_at_nul=False)
    assert blob2 == b"abcdef\0\x01\n" and (a2, d2) == (0, 0)


def test_pattern_blob_empty_input_gives_empty_blob() -> None:
    assert rs.pattern_blob([]) == (b"", 0, 0)


def test_parse_grep_result_counts_files_and_flags_errors() -> None:
    g = rs.parse_grep_result(0, "/a/1\n/a/2\n\n")
    assert g.files == 2 and g.errors == 0 and bool(g)
    assert rs.parse_grep_result(1, "").files == 0 and not rs.parse_grep_result(1, "")
    assert rs.parse_grep_result(2, "/a/1\n").errors == 1


def test_grep_count_merge_is_a_union_of_paths() -> None:
    a = rs.parse_grep_result(0, "/x\n/y\n", approximated=1)
    b = rs.parse_grep_result(0, "/y\n/z\n", dropped=2)
    m = a.merge(b)
    assert m.files == 3 and m.approximated == 1 and m.dropped == 2
    assert m.locations == ("/x", "/y", "/z")


def test_locations_are_capped() -> None:
    g = rs.parse_grep_result(0, "\n".join(f"/f/{i:03d}" for i in range(50)))
    assert g.files == 50 and len(g.locations) == rs.MAX_LOCATIONS


@pytest.mark.parametrize("p", ["/var/lib/redpanda/data/kafka/cdc.public.*", "/qdrant/storage", "/data"])
def test_safe_path_accepts_plain_paths_and_globs(p: str) -> None:
    assert rs.safe_path(p) == p


@pytest.mark.parametrize("p", ["/a b", "/a;rm", "$(x)", "/a'b", ""])
def test_safe_path_rejects_shell_metacharacters(p: str) -> None:
    with pytest.raises(ValueError):
        rs.safe_path(p)


def test_subject_entity_strings_and_needles() -> None:
    s = rs.Subject({"customers": [BIG], "support_tickets": [30, 20]}, canaries=(CANARY,))
    assert s.customer_ids == (BIG,)
    assert s.entity_strings(["support_tickets"]) == ["support_tickets:20", "support_tickets:30"]
    assert s.entity_strings() == [f"customers:{BIG}", "support_tickets:20", "support_tickets:30"]
    assert s.needles().canaries == (CANARY,) and s.needles().ids == (BIG,)  # tickets are not searched as numbers
    led = rs.Subject({"customers": [BIG], "accounts": [11], "transactions": [12], "support_tickets": [30]})
    assert led.removed_ids("accounts") == () and led.removed_ids("transactions") == ()
    assert led.removed_ids("support_tickets") == (30,)


def test_wait_until_reports_elapsed_and_timeout() -> None:
    calls: list[int] = []

    def third_time() -> bool:
        calls.append(1)
        return len(calls) >= 3

    ok, el = rs.wait_until(third_time, timeout_s=5, every_s=0.01)
    assert ok and len(calls) == 3 and el < 1
    ok2, _ = rs.wait_until(lambda: False, timeout_s=0.05, every_s=0.01)
    assert not ok2
