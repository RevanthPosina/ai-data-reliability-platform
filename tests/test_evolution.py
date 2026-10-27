import pytest

from adrp.spark.avro_types import avro_to_spark, json_type, struct_ddl
from adrp.spark.evolution import classify, is_widening

TABLE = [("id", "bigint"), ("title", "string"), ("balance", "decimal(12,2)"), ("note", "string")]


def w(**kw: str) -> list[tuple[str, str]]:
    base = dict(TABLE)
    base.update(kw)
    return list(base.items())


def test_identical_schema_is_compatible_and_changes_nothing() -> None:
    v = classify(TABLE, TABLE, ["id"])
    assert v.compatible and not v.adds and not v.promotions


def test_added_column_is_added() -> None:
    v = classify(TABLE, [*TABLE, ("locale", "string")], ["id"])
    assert v.compatible and v.adds == (("locale", "string"),)


def test_removed_column_alone_is_compatible() -> None:
    assert classify(TABLE, [c for c in TABLE if c[0] != "note"], ["id"]).compatible


def test_rename_is_drop_plus_add_and_is_quarantined() -> None:
    v = classify(
        TABLE, [("id", "bigint"), ("title", "string"), ("balance", "decimal(12,2)"), ("remark", "string")], ["id"]
    )
    assert not v.compatible and "rename" in v.detail
    assert v.quarantine_reason.startswith("SCHEMA_INCOMPATIBLE")


def test_type_narrowing_is_quarantined() -> None:
    v = classify(TABLE, w(balance="decimal(8,2)"), ["id"])
    assert not v.compatible and "balance" in v.detail


@pytest.mark.parametrize("new", ["boolean", "int", "double"])
def test_unrelated_type_change_is_quarantined(new: str) -> None:
    assert not classify(TABLE, w(title=new), ["id"]).compatible


def test_widening_is_promoted() -> None:
    v = classify(TABLE, w(balance="decimal(14,2)"), ["id"])
    assert v.compatible and v.promotions == (("balance", "decimal(14,2)"),)
    assert is_widening("int", "bigint") and is_widening("float", "double")
    assert not is_widening("decimal(12,2)", "decimal(14,4)") and not is_widening("bigint", "int")


def test_key_removal_is_quarantined() -> None:
    assert not classify(TABLE, [c for c in TABLE if c[0] != "id"], ["id"]).compatible


def test_debezium_logical_types_map_to_spark() -> None:
    assert avro_to_spark({"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}) == ("TIMESTAMP", False)
    assert avro_to_spark({"type": "int", "connect.name": "io.debezium.time.Date"}) == ("DATE", False)
    dec = {
        "type": "bytes", "logicalType": "decimal", "precision": 64, "scale": 2,
        "connect.parameters": {"scale": "2", "connect.decimal.precision": "12"},
    }  # fmt: skip
    assert avro_to_spark(dec) == ("DECIMAL(12,2)", False)
    assert avro_to_spark(["null", "long"]) == ("BIGINT", True)


def test_json_types_read_exact_values_as_strings() -> None:
    assert [json_type(t) for t in ("DECIMAL(12,2)", "TIMESTAMP", "DATE", "BIGINT", "STRING")] == [
        "STRING", "STRING", "STRING", "BIGINT", "STRING",
    ]  # fmt: skip
    assert struct_ddl([("a", "DECIMAL(12,2)", False), ("b", "INT", True)]) == "`a` STRING, `b` INT"
