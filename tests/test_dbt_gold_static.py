"""Static checks on dbt_gold (ADR-0025..0030): contracts, keys, materialization, ledger rules. No Docker, no dbt run."""

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "dbt_gold"
MARTS = GOLD / "models/marts"
STAGING = GOLD / "models/staging"
SCHEMA = MARTS / "schema.yml"
PROJECT = GOLD / "dbt_project.yml"
EXCLUDED_DIRS = {"target", "logs", "dbt_packages"}

# Key columns: each must be not_null by constraint or by a not_null test (ADR-0028).
KEYS = {
    "dim_customer": ["customer_sk", "customer_id"],
    "dim_merchant": ["merchant_sk", "merchant_id"],
    "dim_date": ["date_key"],
    "dim_account": ["account_id"],
    "fct_transactions": ["transaction_id"],
    "fct_ledger_entries": ["entry_id", "journal_id"],
}


def _schema_models() -> dict[str, dict[str, Any]]:
    doc = yaml.safe_load(SCHEMA.read_text())
    return {m["name"]: m for m in doc["models"]}


def _mart_names() -> list[str]:
    return sorted(p.stem for p in MARTS.glob("*.sql"))


def _sql_config(path: Path) -> dict[str, Any]:
    """Keyword arguments of the `{{ config(...) }}` block, read with a regex (quoted strings and booleans)."""
    block = re.search(r"\{\{\s*config\((.*?)^\)\s*\}\}", path.read_text(), re.DOTALL | re.MULTILINE)
    if block is None:
        return {}
    out: dict[str, Any] = {}
    for key, value in re.findall(r"(\w+)\s*=\s*('[^']*'|\"[^\"]*\"|true|false)", block.group(1)):
        out[key] = {"true": True, "false": False}.get(value, value[1:-1] if value[0] in "'\"" else value)
    return out


def _is_not_null(column: dict[str, Any]) -> bool:
    constraints = column.get("constraints") or []
    tests = column.get("data_tests") or []
    return any(c.get("type") == "not_null" for c in constraints) or "not_null" in tests


def _gold_files() -> list[Path]:
    return [
        p
        for p in GOLD.rglob("*")
        if p.is_file() and p.suffix in {".yml", ".yaml", ".sql"} and not EXCLUDED_DIRS & set(p.parts)
    ]


def test_marts_are_exactly_the_schema_models() -> None:
    assert _mart_names()
    assert sorted(_schema_models()) == _mart_names()


def test_every_mart_has_enforced_contract() -> None:
    models = _schema_models()
    for name in _mart_names():
        enforced = (models[name].get("config") or {}).get("contract", {}).get("enforced") is True
        if not enforced:
            enforced = re.search(r"enforced\s*[:=]\s*true", (MARTS / f"{name}.sql").read_text()) is not None
        assert enforced, f"{name}: contract.enforced is not true"


def test_every_mart_column_has_data_type() -> None:
    for name, model in _schema_models().items():
        assert model.get("columns"), f"{name}: no columns declared"
        for column in model["columns"]:
            assert column.get("data_type"), f"{name}.{column.get('name')}: no data_type"


def test_key_columns_are_not_null() -> None:
    models = _schema_models()
    for name, keys in KEYS.items():
        columns = {c["name"]: c for c in models[name]["columns"]}
        for key in keys:
            assert key in columns, f"{name}: key column {key} missing"
            assert _is_not_null(columns[key]), f"{name}.{key}: no not_null constraint or test"
    for name, model in models.items():
        for column in model["columns"]:
            if any(c.get("type") == "primary_key" for c in column.get("constraints") or []):
                assert _is_not_null(column), f"{name}.{column['name']}: primary_key without not_null"


def test_no_store_failures_anywhere() -> None:
    for path in _gold_files():
        assert "store_failures" not in path.read_text(), f"store_failures in {path.relative_to(ROOT)}"


def test_no_materialized_view_anywhere() -> None:
    pattern = re.compile(r"materialized[ _]view|materialized\s*[:=]\s*['\"]?view\b", re.IGNORECASE)
    for path in _gold_files():
        assert not pattern.search(path.read_text()), f"materialized view in {path.relative_to(ROOT)}"


def test_fct_ledger_entries_full_refresh_false() -> None:
    config = _sql_config(MARTS / "fct_ledger_entries.sql")
    assert config.get("full_refresh") is False, f"fct_ledger_entries full_refresh is {config.get('full_refresh')!r}"


def test_staging_is_ephemeral() -> None:
    folder = yaml.safe_load(PROJECT.read_text())["models"]["adrp_gold"]["staging"]
    files = sorted(STAGING.glob("*.sql"))
    assert files
    for path in files:
        materialized = _sql_config(path).get("materialized", folder.get("+materialized"))
        assert materialized == "ephemeral", f"{path.name} is {materialized!r}, not ephemeral"


def test_fct_ledger_entries_has_no_customer_id() -> None:
    columns = {c["name"] for c in _schema_models()["fct_ledger_entries"]["columns"]}
    assert "customer_id" not in columns
