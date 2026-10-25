"""Registry-aware Avro decoding for Debezium records (fastavro, keyed by schema id)."""

from __future__ import annotations

import io
import json
import os
import urllib.request
from typing import Any

import fastavro

from adrp.common import canonical_json, parse_confluent_header

_SCHEMAS: dict[int, Any] = {}


def registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def schema_by_id(schema_id: int) -> Any:
    """Writer schema for an id, parsed once per Python worker."""
    if schema_id not in _SCHEMAS:
        raw = _get_json(f"{registry_url()}/schemas/ids/{schema_id}")["schema"]
        _SCHEMAS[schema_id] = fastavro.parse_schema(json.loads(raw))
    return _SCHEMAS[schema_id]


def latest_schema(subject: str) -> dict[str, Any]:
    """Latest registered schema for a subject (reader schema fetched at query start)."""
    raw = _get_json(f"{registry_url()}/subjects/{subject}/versions/latest")["schema"]
    return json.loads(raw)


def decode(payload: bytes) -> tuple[int, dict[str, Any]]:
    schema_id, body = parse_confluent_header(payload)
    record = fastavro.schemaless_reader(io.BytesIO(body), schema_by_id(schema_id))
    return schema_id, record  # type: ignore[return-value]


def decode_change(key: bytes | None, value: bytes | None) -> tuple[Any, ...]:
    """Decode one Kafka record into the bronze envelope columns.

    Returns (schema_id, pk, op, lsn, txid, source_ts_ms, before_json, after_json).
    A tombstone (value None) keeps only pk.
    """
    pk = None
    if key is not None:
        _, key_rec = decode(key)
        pk = canonical_json(key_rec)
    if value is None:
        return (None, pk, None, None, None, None, None, None)
    schema_id, env = decode(value)
    src = env["source"]
    before = canonical_json(env["before"]) if env.get("before") is not None else None
    after = canonical_json(env["after"]) if env.get("after") is not None else None
    return (schema_id, pk, env["op"], src.get("lsn"), src.get("txId"), src.get("ts_ms"), before, after)
