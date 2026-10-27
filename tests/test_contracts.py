import copy

import pytest

from adrp.contracts import apply_contract, avro_type_for, canonical, columns_of, contract_type, diff_columns

ROW = {
    "type": "record",
    "name": "Value",
    "namespace": "cdc.public.things",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "label", "type": ["null", "string"], "default": None},
        {
            "name": "amount",
            "type": {
                "type": "bytes", "scale": 2, "precision": 64, "logicalType": "decimal",
                "connect.name": "org.apache.kafka.connect.data.Decimal",
                "connect.parameters": {"scale": "2", "connect.decimal.precision": "12"},
            },
        },
        {"name": "created_at", "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}},
        {"name": "born", "type": {"type": "int", "connect.name": "io.debezium.time.Date"}},
    ],
}  # fmt: skip
ENVELOPE = {
    "type": "record",
    "name": "Envelope",
    "namespace": "cdc.public.things",
    "fields": [
        {"name": "before", "type": ["null", ROW], "default": None},
        {"name": "after", "type": ["null", "cdc.public.things.Value"], "default": None},
        {"name": "op", "type": "string"},
    ],
}


def contract_from(envelope: dict) -> dict:  # type: ignore[type-arg]
    return {"columns": columns_of(envelope)}


def test_contract_types() -> None:
    cols = {c["name"]: (c["type"], c["nullable"]) for c in columns_of(ENVELOPE)}
    assert cols == {
        "id": ("long", False),
        "label": ("string", True),
        "amount": ("decimal(12,2)", False),
        "created_at": ("timestamptz", False),
        "born": ("date", False),
    }


def test_contract_type_rejects_multi_type_union() -> None:
    with pytest.raises(ValueError):
        contract_type(["null", "string", "long"])


def test_avro_type_for_roundtrips_through_contract_type() -> None:
    for ctype in ["long", "string", "decimal(12,2)", "timestamptz", "date", "timestamp"]:
        assert contract_type(avro_type_for(ctype))[0] == ctype


def test_unchanged_contract_leaves_schema_identical() -> None:
    assert canonical(apply_contract(ENVELOPE, contract_from(ENVELOPE))) == canonical(ENVELOPE)


def test_changed_type_new_column_and_removed_column() -> None:
    contract = contract_from(ENVELOPE)
    contract["columns"] = [c for c in contract["columns"] if c["name"] != "born"]
    for c in contract["columns"]:
        if c["name"] == "id":
            c["type"] = "int"  # narrowing
    contract["columns"].append({"name": "note", "type": "string", "nullable": False})
    candidate = apply_contract(ENVELOPE, contract)
    names = [f["name"] for f in candidate["fields"][0]["type"][1]["fields"]]
    assert names == ["id", "label", "amount", "created_at", "note"]
    assert candidate["fields"][0]["type"][1]["fields"][0]["type"] == "int"
    # 'after' references the same record, so it changed too
    assert columns_of(candidate) == [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "label", "type": "string", "nullable": True},
        {"name": "amount", "type": "decimal(12,2)", "nullable": False},
        {"name": "created_at", "type": "timestamptz", "nullable": False},
        {"name": "note", "type": "string", "nullable": False},
    ]


def test_apply_contract_does_not_mutate_input() -> None:
    before = copy.deepcopy(ENVELOPE)
    contract = contract_from(ENVELOPE)
    contract["columns"].pop()
    apply_contract(ENVELOPE, contract)
    assert ENVELOPE == before


def test_diff_columns() -> None:
    reg = columns_of(ENVELOPE)
    con = [c for c in reg if c["name"] != "born"] + [{"name": "x", "type": "long", "nullable": True}]
    con[0] = {**con[0], "type": "int"}
    assert diff_columns(reg, con) == {"added": ["x"], "removed": ["born"], "changed": ["id"]}
