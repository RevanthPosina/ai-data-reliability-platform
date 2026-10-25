"""Map Debezium Avro record schemas to Spark SQL DDL (pure; no pyspark import).

G0 covers primitives, nullable unions and the logical types the skeleton table uses.
G3 extends this with schema-evolution handling and quarantine on incompatible changes.
"""

from __future__ import annotations

from typing import Any

_PRIMITIVES = {
    "boolean": "BOOLEAN",
    "int": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "bytes": "BINARY",
    "string": "STRING",
}


def avro_to_spark(avro_type: Any) -> tuple[str, bool]:
    """Return (spark_ddl_type, nullable) for one Avro field type."""
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) != 1:
            raise ValueError(f"unsupported union: {avro_type}")
        ddl, _ = avro_to_spark(non_null[0])
        return ddl, "null" in avro_type
    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical == "decimal":
            return f"DECIMAL({avro_type['precision']},{avro_type.get('scale', 0)})", False
        # Debezium time types (io.debezium.time.*) keep their physical type in G0:
        # ZonedTimestamp is an ISO string, MicroTimestamp a long of epoch micros.
        return avro_to_spark(avro_type["type"])
    if avro_type in _PRIMITIVES:
        return _PRIMITIVES[avro_type], False
    raise ValueError(f"unsupported Avro type: {avro_type!r}")


def _named_records(schema: Any, out: dict[str, dict[str, Any]]) -> None:
    if isinstance(schema, list):
        for t in schema:
            _named_records(t, out)
    elif isinstance(schema, dict):
        if schema.get("type") == "record":
            out[schema["name"]] = schema
            if "namespace" in schema:
                out[f"{schema['namespace']}.{schema['name']}"] = schema
            for f in schema["fields"]:
                _named_records(f["type"], out)


def find_record(envelope_schema: dict[str, Any], field_name: str) -> dict[str, Any]:
    """Return the record schema of a Debezium envelope field ('before'/'after').

    Debezium defines the row record once (in 'before') and 'after' refers to it by name.
    """
    named: dict[str, dict[str, Any]] = {}
    _named_records(envelope_schema, named)
    for field in envelope_schema["fields"]:
        if field["name"] == field_name:
            t = field["type"]
            if isinstance(t, list):
                t = next(x for x in t if x != "null")
            return named[t] if isinstance(t, str) else t
    raise KeyError(field_name)


def record_columns(record_schema: dict[str, Any]) -> list[tuple[str, str, bool]]:
    """[(column, spark_type, nullable)] for a record schema, in field order."""
    return [(f["name"], *avro_to_spark(f["type"])) for f in record_schema["fields"]]


def struct_ddl(columns: list[tuple[str, str, bool]]) -> str:
    return ", ".join(f"`{name}` {ddl}" for name, ddl, _ in columns)
