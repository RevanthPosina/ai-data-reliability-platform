import random
import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from adrp.common import parse_confluent_header
from adrp.forget import ids

KEY = "forget_key_for_tests"


@pytest.mark.parametrize(
    ("n", "enc"),
    [(0, b"\x00"), (-1, b"\x01"), (1, b"\x02"), (-2, b"\x03"), (63, b"\x7e"), (64, b"\x80\x01"), (-65, b"\x81\x01")],
)
def test_zigzag_varint_matches_avro_spec_vectors(n: int, enc: bytes) -> None:
    assert ids.zigzag_varint(n) == enc


@given(st.integers(min_value=-(2**63), max_value=2**63 - 1))
def test_zigzag_varint_roundtrip(n: int) -> None:
    enc = ids.zigzag_varint(n)
    assert ids.read_zigzag_varint(b"xx" + enc + b"yy", 2) == (n, 2 + len(enc))


def test_confluent_key_bytes_layout() -> None:
    key = ids.confluent_key_bytes(7, 1_234_567_890_123)
    schema_id, body = parse_confluent_header(key)  # the pipeline's own parser agrees with the layout
    assert schema_id == 7
    assert ids.read_zigzag_varint(body) == (1_234_567_890_123, len(body))
    assert key[:5] == b"\x00\x00\x00\x00\x07"


def test_subject_hmac_is_stable_keyed_and_hides_the_id() -> None:
    h = ids.subject_hmac(KEY, 2_000_000_000_001)
    assert h == ids.subject_hmac(KEY, "2000000000001")
    assert re.fullmatch(r"[0-9a-f]{64}", h)
    assert h != ids.subject_hmac("another key", 2_000_000_000_001)
    assert h != ids.subject_hmac(KEY, 2_000_000_000_002)
    assert "2000000000001" not in h


def test_empty_key_is_rejected() -> None:
    for f in (ids.subject_hmac, ids.pseudonym, ids.pseudonym_int):
        with pytest.raises(ValueError):
            f("", "support_tickets:1")


def test_pseudonym_format_and_determinism() -> None:
    p = ids.pseudonym(KEY, "support_tickets:1000000000001")
    assert re.fullmatch(r"p:[0-9a-f]{16}", p)
    assert p == ids.pseudonym(KEY, "support_tickets:1000000000001")
    assert p != ids.pseudonym(KEY, "support_tickets:1000000000002")
    assert p != ids.pseudonym("another key", "support_tickets:1000000000001")


@given(st.text(max_size=40))
def test_pseudonym_int_is_negative_int63(entity: str) -> None:
    n = ids.pseudonym_int(KEY, entity)
    assert -(2**63 - 1) <= n <= -1
    assert n == ids.pseudonym_int(KEY, entity)


def test_pseudonym_int_differs_per_entity_and_key() -> None:
    vals = {ids.pseudonym_int(KEY, f"transactions:{i}") for i in range(500)}
    assert len(vals) == 500
    assert ids.pseudonym_int(KEY, "transactions:1") != ids.pseudonym_int("another key", "transactions:1")


def test_adhoc_ids_are_13_digit_distinct_and_avoid_taken() -> None:
    rng = random.Random(1)
    got = ids.adhoc_subject_ids(50, rng, taken=[ids.ADHOC_ID_MIN])
    assert len(set(got)) == 50 and ids.ADHOC_ID_MIN not in got
    assert all(10**12 <= i <= 9_999_999_999_999 for i in got)
    assert all(ids.ID13.fullmatch(str(i)) for i in got)
    assert got == ids.adhoc_subject_ids(50, random.Random(1), taken=[ids.ADHOC_ID_MIN])


def test_canaries_have_prefix_and_are_unique() -> None:
    a, b = ids.new_canary(), ids.new_canary()
    assert a != b and re.fullmatch(r"g8c-[0-9a-f]{32}", a)
    assert ids.new_canary(random.Random(5)) == ids.new_canary(random.Random(5))


def test_matcher_on_text() -> None:
    subj, other = 2_000_000_000_001, 2_000_000_000_002
    canary = ids.new_canary()
    m = ids.Matcher([subj], canaries=[canary])
    assert m.hits(f"email={canary}@example.com id={subj}").canary == 1
    assert m.hits(f"id={subj} id={subj}").subject_id == 2
    assert not m.hits(f"id={other} {ids.new_canary()}")  # another subject's id and canary are not ours
    assert not m.hits(f"id=1{subj} id={subj}0")  # digit boundaries
    assert ids.Matcher().hits(f"x {ids.new_canary()} y").canary == 1  # no tokens given: any g8c- prefix


def test_matcher_on_bytes_finds_text_and_key_patterns() -> None:
    subj, small = 2_000_000_000_001, 17
    canary = ids.new_canary()
    m = ids.Matcher([subj, small], canaries=[canary], schema_ids=[3])
    blob = b"\xff\x00junk" + canary.encode() + b"\x01" + ids.confluent_key_bytes(3, small) + b"tail"
    h = m.hits(blob)
    assert h.canary == 1 and h.key_bytes == 1 and h.subject_id == 0 and h
    assert m.hits(b"junk" + ids.confluent_key_bytes(4, small)).key_bytes == 0  # another schema id
    # a large id is also found as a bare varint (a key that lost its Confluent header, e.g. inside a segment batch)
    assert m.hits(b"\x10" + ids.zigzag_varint(subj) + b"\x10").key_bytes == 1
    # a small id's bare varint is not searched: it would match anywhere
    assert m.hits(ids.zigzag_varint(small)).key_bytes == 0
    assert m.hits(f"{subj}".encode()).subject_id == 1


def test_empty_matcher_matches_nothing_but_prefix() -> None:
    m = ids.Matcher(canaries=[])
    assert not m.hits(b"anything g8c-abc 1234567890123") and not m.hits("anything g8c-abc")
