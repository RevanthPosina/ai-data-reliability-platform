"""Registry-aware Avro decoding for Debezium records (fastavro, keyed by schema id)."""

from __future__ import annotations

import io
import json
import os
import urllib.request
from datetime import date, timedelta
from typing import Any

import fastavro

from adrp.common import canonical_json, parse_confluent_header
from adrp.spark.avro_types import find_record

_SCHEMAS: dict[int, Any] = {}
_RAW: dict[int, dict[str, Any]] = {}
_DATE_FIELDS: dict[int, frozenset[str]] = {}
EPOCH = date(1970, 1, 1)


def registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def raw_schema_by_id(schema_id: int) -> dict[str, Any]:
    """Writer schema as registered (JSON), cached per Python worker."""
    if schema_id not in _RAW:
        raw = json.loads(_get_json(f"{registry_url()}/schemas/ids/{schema_id}")["schema"])
        _RAW[schema_id] = raw
        _DATE_FIELDS[schema_id] = frozenset(_date_fields(raw))
    return _RAW[schema_id]


def _date_fields(envelope: dict[str, Any]) -> list[str]:
    if not any(f["name"] == "after" for f in envelope.get("fields", [])):
        return []  # a key schema, not a change envelope
    out = []
    for f in find_record(envelope, "after")["fields"]:
        t = f["type"]
        if isinstance(t, list):
            t = next(x for x in t if x != "null")
        if isinstance(t, dict) and t.get("connect.name") == "io.debezium.time.Date":
            out.append(f["name"])
    return out


def schema_by_id(schema_id: int) -> Any:
    """Writer schema parsed for fastavro, once per Python worker."""
    if schema_id not in _SCHEMAS:
        _SCHEMAS[schema_id] = fastavro.parse_schema(raw_schema_by_id(schema_id))
    return _SCHEMAS[schema_id]


def _with_iso_dates(row: dict[str, Any] | None, schema_id: int) -> dict[str, Any] | None:
    """Debezium dates are ints (days since epoch); Spark reads them as ISO strings and casts to DATE."""
    if row is None:
        return None
    fields = _DATE_FIELDS.get(schema_id, frozenset())
    return {
        k: ((EPOCH + timedelta(days=v)).isoformat() if k in fields and v is not None else v) for k, v in row.items()
    }


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
    before_row = _with_iso_dates(env.get("before"), schema_id)
    after_row = _with_iso_dates(env.get("after"), schema_id)
    before = canonical_json(before_row) if before_row is not None else None
    after = canonical_json(after_row) if after_row is not None else None
    return (schema_id, pk, env["op"], src.get("lsn"), src.get("txId"), src.get("ts_ms"), before, after)
