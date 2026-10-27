"""Data contracts for CDC topics: derive column types from Debezium Avro and apply a YAML contract
to a registered schema so the *registry* can judge compatibility (PLAN G2 CI check).

A contract lists the columns a topic's `after` record is allowed to have. `apply_contract` rewrites
the latest registered schema so its row record matches the contract, keeping every unchanged field
verbatim; the registry's `/compatibility/subjects/<s>/versions/latest` endpoint then decides whether
that rewrite is BACKWARD compatible. Pure functions only (no network) so they are unit-testable.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from adrp.spark.avro_types import _named_records, find_record

PRIMITIVES = {"long", "int", "string", "bytes", "boolean", "double", "float"}
DEBEZIUM_LOGICAL = {
    "io.debezium.time.ZonedTimestamp": "timestamptz",
    "io.debezium.time.Date": "date",
    "io.debezium.time.MicroTimestamp": "timestamp",
}
AVRO_FOR_LOGICAL = {
    "timestamptz": {"type": "string", "connect.version": 1, "connect.name": "io.debezium.time.ZonedTimestamp"},
    "date": {"type": "int", "connect.version": 1, "connect.name": "io.debezium.time.Date"},
    "timestamp": {"type": "long", "connect.version": 1, "connect.name": "io.debezium.time.MicroTimestamp"},
}


def contract_type(avro_type: Any) -> tuple[str, bool]:
    """(contract type string, nullable) for one Avro field type."""
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) != 1:
            raise ValueError(f"unsupported union: {avro_type}")
        base, _ = contract_type(non_null[0])
        return base, "null" in avro_type
    if isinstance(avro_type, dict):
        name = avro_type.get("connect.name")
        if name in DEBEZIUM_LOGICAL:
            return DEBEZIUM_LOGICAL[name], False
        if avro_type.get("logicalType") == "decimal":
            params = avro_type.get("connect.parameters", {})
            precision = params.get("connect.decimal.precision", avro_type["precision"])
            return f"decimal({precision},{avro_type.get('scale', 0)})", False
        return contract_type(avro_type["type"])
    if avro_type in PRIMITIVES:
        return str(avro_type), False
    raise ValueError(f"unsupported Avro type: {avro_type!r}")


def avro_type_for(ctype: str) -> Any:
    """A plain Avro type for a contract type (used only when a column is new or its type changed)."""
    if ctype in PRIMITIVES:
        return ctype
    if ctype in AVRO_FOR_LOGICAL:
        return copy.deepcopy(AVRO_FOR_LOGICAL[ctype])
    if ctype.startswith("decimal(") and ctype.endswith(")"):
        precision, scale = (int(x) for x in ctype[len("decimal(") : -1].split(","))
        return {"type": "bytes", "logicalType": "decimal", "precision": precision, "scale": scale}
    raise ValueError(f"unknown contract type: {ctype}")


def row_record(envelope: dict[str, Any]) -> dict[str, Any]:
    """The Debezium row record (shared by `before` and `after`)."""
    return find_record(envelope, "after")


def columns_of(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    cols = []
    for f in row_record(envelope)["fields"]:
        ctype, nullable = contract_type(f["type"])
        cols.append({"name": f["name"], "type": ctype, "nullable": nullable})
    return cols


def apply_contract(latest: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of `latest` whose row record has exactly the contract's columns."""
    candidate = copy.deepcopy(latest)
    record = row_record(candidate)
    existing = {f["name"]: f for f in record["fields"]}
    fields = []
    for col in contract["columns"]:
        old = existing.get(col["name"])
        if old is not None and contract_type(old["type"]) == (col["type"], col["nullable"]):
            fields.append(old)
            continue
        avro_t = avro_type_for(col["type"])
        if col["nullable"]:
            fields.append({"name": col["name"], "type": ["null", avro_t], "default": None})
        else:
            fields.append({"name": col["name"], "type": avro_t})
    record["fields"] = fields
    return candidate


def canonical(schema: dict[str, Any]) -> str:
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))


def diff_columns(registered: list[dict[str, Any]], contract_cols: list[dict[str, Any]]) -> dict[str, list[str]]:
    reg = {c["name"]: c for c in registered}
    con = {c["name"]: c for c in contract_cols}
    return {
        "added": sorted(con.keys() - reg.keys()),
        "removed": sorted(reg.keys() - con.keys()),
        "changed": sorted(
            n
            for n in reg.keys() & con.keys()
            if (reg[n]["type"], reg[n]["nullable"]) != (con[n]["type"], con[n]["nullable"])
        ),
    }


def named_record_count(envelope: dict[str, Any]) -> int:
    """Sanity helper for tests: number of named record types in a schema."""
    out: dict[str, dict[str, Any]] = {}
    _named_records(envelope, out)
    return len(out)
