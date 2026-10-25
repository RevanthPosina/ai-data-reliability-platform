"""Pure helpers shared by Spark jobs, the embedding worker and tests (no heavy imports)."""

from __future__ import annotations

import hashlib
import json
import struct
import uuid
from typing import Any

# Fixed namespace for Qdrant point ids: uuid5(ns, "entity_id|version|chunk_no") (PLAN G6).
POINT_NAMESPACE = uuid.UUID("6f1c7d2e-3b5a-4e8f-9a0b-1c2d3e4f5a6b")

CONFLUENT_MAGIC = 0


def parse_confluent_header(payload: bytes) -> tuple[int, bytes]:
    """Split a Confluent wire-format message into (schema_id, avro_body)."""
    if len(payload) < 5 or payload[0] != CONFLUENT_MAGIC:
        raise ValueError("not Confluent wire format (missing magic byte 0 + 4-byte schema id)")
    (schema_id,) = struct.unpack(">I", payload[1:5])
    return schema_id, payload[5:]


def canonical_json(obj: Any) -> str:
    """Stable JSON used for pk strings and hashing: sorted keys, no whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=_json_default)


def _json_default(o: Any) -> Any:
    from decimal import Decimal

    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (bytes, bytearray)):
        return o.hex()
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def chunk_text(text: str, max_chars: int = 800) -> list[str]:
    """Deterministic chunker: greedy split on whitespace, each chunk <= max_chars.

    Same input -> same chunks, so chunk_no is stable per entity version (PLAN G6).
    A single token longer than max_chars is hard-split.
    """
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    chunks: list[str] = []
    current = ""
    for word in text.split():
        while len(word) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(word[:max_chars])
            word = word[max_chars:]
        if not word:
            continue
        candidate = f"{current} {word}" if current else word
        if len(candidate) > max_chars:
            chunks.append(current)
            current = word
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def point_id(entity_id: str, version: int, chunk_no: int) -> str:
    return str(uuid.uuid5(POINT_NAMESPACE, f"{entity_id}|{version}|{chunk_no}"))


def chunk_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
