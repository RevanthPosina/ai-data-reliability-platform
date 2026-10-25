import struct

import pytest
from hypothesis import given
from hypothesis import strategies as st

from adrp.common import canonical_json, chunk_text, parse_confluent_header, point_id


def test_confluent_header_roundtrip() -> None:
    payload = b"\x00" + struct.pack(">I", 42) + b"avro-body"
    assert parse_confluent_header(payload) == (42, b"avro-body")


@pytest.mark.parametrize("bad", [b"", b"\x00\x00\x00", b"\x01\x00\x00\x00\x01x"])
def test_confluent_header_rejects_non_confluent(bad: bytes) -> None:
    with pytest.raises(ValueError):
        parse_confluent_header(bad)


def test_point_id_is_deterministic_and_distinct() -> None:
    assert point_id("7", 100, 0) == point_id("7", 100, 0)
    assert len({point_id("7", 100, 0), point_id("7", 101, 0), point_id("7", 100, 1), point_id("8", 100, 0)}) == 4


def test_canonical_json_sorts_keys() -> None:
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


@given(st.text(alphabet=st.characters(codec="utf-8"), max_size=5000), st.integers(min_value=1, max_value=900))
def test_chunker_deterministic_bounded_lossless(text: str, max_chars: int) -> None:
    chunks = chunk_text(text, max_chars)
    assert chunks == chunk_text(text, max_chars)
    assert all(0 < len(c) <= max_chars for c in chunks)
    # Whitespace-normalized content is preserved (hard splits can break a word across chunks).
    assert "".join(chunks).replace(" ", "") == "".join(text.split())
