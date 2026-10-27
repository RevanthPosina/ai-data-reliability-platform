"""Map Debezium Avro record schemas to Spark SQL DDL (pure; no pyspark import)."""

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


DEBEZIUM_SPARK = {
    "io.debezium.time.ZonedTimestamp": "TIMESTAMP",  # ISO-8601 string with zone
    "io.debezium.time.Date": "DATE",  # int days since epoch; decode.py renders it as an ISO date string
    "io.debezium.time.MicroTimestamp": "TIMESTAMP",
}
# Spark types that from_json reads as STRING and the projection casts (exact decimals, ISO times).
STRING_ON_WIRE = ("DECIMAL", "TIMESTAMP", "DATE", "BINARY")


def avro_to_spark(avro_type: Any) -> tuple[str, bool]:
    """Return (spark_ddl_type, nullable) for one Avro field type."""
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) != 1:
            raise ValueError(f"unsupported union: {avro_type}")
        ddl, _ = avro_to_spark(non_null[0])
        return ddl, "null" in avro_type
    if isinstance(avro_type, dict):
        name = avro_type.get("connect.name")
        if name in DEBEZIUM_SPARK:
            return DEBEZIUM_SPARK[name], False
        if avro_type.get("logicalType") == "decimal":
            # The Connect converter writes precision 64 in the Avro type; the column's real precision
            # is in connect.parameters.
            params = avro_type.get("connect.parameters", {})
            precision = int(params.get("connect.decimal.precision", avro_type["precision"]))
            return f"DECIMAL({precision},{avro_type.get('scale', 0)})", False
        return avro_to_spark(avro_type["type"])
    if avro_type in _PRIMITIVES:
        return _PRIMITIVES[avro_type], False
    raise ValueError(f"unsupported Avro type: {avro_type!r}")


def json_type(spark_ddl: str) -> str:
    """The type from_json should use for a column: exact decimals and times travel as strings."""
    return "STRING" if spark_ddl.upper().startswith(STRING_ON_WIRE) else spark_ddl


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
    return ", ".join(f"`{name}` {json_type(ddl)}" for name, ddl, _ in columns)
