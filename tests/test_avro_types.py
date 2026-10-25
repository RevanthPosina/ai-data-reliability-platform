import pytest

from adrp.spark.avro_types import avro_to_spark, find_record, record_columns, struct_ddl

ENVELOPE = {
    "type": "record",
    "name": "Envelope",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "title", "type": "string"},
                        {"name": "body", "type": ["null", "string"]},
                        {
                            "name": "updated_at",
                            "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"},
                        },
                        {
                            "name": "amount",
                            "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2},
                        },
                    ],
                },
            ],
        },
        {"name": "after", "type": ["null", "Value"]},
        {"name": "op", "type": "string"},
    ],
}


def test_find_record_and_columns() -> None:
    cols = record_columns(find_record(ENVELOPE, "before"))
    assert cols == [
        ("id", "BIGINT", False),
        ("title", "STRING", False),
        ("body", "STRING", True),
        ("updated_at", "STRING", False),
        ("amount", "DECIMAL(12,2)", False),
    ]
    assert struct_ddl(cols[:2]) == "`id` BIGINT, `title` STRING"


def test_after_resolves_named_reference() -> None:
    assert find_record(ENVELOPE, "after") is find_record(ENVELOPE, "before")


def test_rejects_multi_type_union() -> None:
    with pytest.raises(ValueError):
        avro_to_spark(["null", "string", "long"])
