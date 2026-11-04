"""Pure helpers behind adrp.spark.forget (G8): footprint parsing and SQL predicates. No Spark import, so the host
test suite covers them.

A footprint maps a bronze `source_table` (`public.support_tickets`) to the canonical pk strings bronze stores
(`{"id":1000000000123}`). The same strings address quarantine tables (same columns); silver tables are addressed by
their key columns, parsed back out of the pk string.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

Footprint = dict[str, list[str]]

BRONZE_TABLE = "lake.bronze.cdc_events"
# Domain tables whose rows name a customer (customer_id); customers themselves are addressed by pk.
CUSTOMER_FK_TABLES = ("support_tickets", "accounts", "transactions")


def sql_str(value: str) -> str:
    """Spark SQL string literal (backslash escapes are on by default)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def parse_footprint(text: str) -> Footprint:
    raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("footprint must be a JSON object {source_table: [pk, ...]}")
    fp: Footprint = {}
    for table, pks in raw.items():
        if not isinstance(pks, list) or not all(isinstance(p, str) for p in pks):
            raise ValueError(f"footprint[{table!r}] must be a list of pk strings")
        if pks:
            fp[str(table)] = sorted(set(pks))
    return fp


def dump_footprint(fp: Footprint) -> str:
    return json.dumps({t: sorted(set(p)) for t, p in sorted(fp.items())}, separators=(",", ":"))


def footprint_size(fp: Footprint) -> int:
    return sum(len(p) for p in fp.values())


def merge_footprints(*parts: Footprint) -> Footprint:
    out: dict[str, set[str]] = {}
    for fp in parts:
        for t, pks in fp.items():
            out.setdefault(t, set()).update(pks)
    return {t: sorted(p) for t, p in sorted(out.items())}


def pk_values(pk: str) -> dict[str, Any]:
    key = json.loads(pk)
    if not isinstance(key, dict) or not key:
        raise ValueError("pk is not a key record")
    return key


def silver_name(source_table: str) -> str:
    """public.support_tickets -> support_tickets"""
    return source_table.split(".", 1)[-1]


def lit(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return sql_str(str(value))


def events_where(fp: Footprint) -> str | None:
    """Predicate over (source_table, pk) for bronze / quarantine tables."""
    parts = [
        f"(source_table = {sql_str(t)} AND pk IN ({', '.join(sql_str(p) for p in pks)}))"
        for t, pks in sorted(fp.items())
        if pks
    ]
    return " OR ".join(parts) if parts else None


def silver_where(fp: Footprint, table: str) -> str | None:
    """Predicate over the key columns of silver table `table` (short name, e.g. `support_tickets`)."""
    pks = fp.get(f"public.{table}", [])
    if not pks:
        return None
    keys = [pk_values(p) for p in pks]
    names = sorted(keys[0])
    if len(names) == 1:
        col = names[0]
        return f"`{col}` IN ({', '.join(lit(k[col]) for k in keys)})"
    return " OR ".join("(" + " AND ".join(f"`{c}` = {lit(k[c])}" for c in names) + ")" for k in keys)


def key_tuples(fp: Footprint, table: str) -> Iterator[dict[str, Any]]:
    for pk in fp.get(f"public.{table}", []):
        yield pk_values(pk)


def customer_in(column: str, customer_ids: Iterable[int]) -> str:
    return f"{column} IN ({', '.join(sql_str(str(int(i))) for i in sorted(set(customer_ids)))})"


def json_customer_where(customer_ids: Iterable[int]) -> str:
    """Bronze/quarantine predicate: before or after image names one of the customers (JSON string columns)."""
    ids = list(customer_ids)
    after = customer_in("get_json_object(after, '$.customer_id')", ids)
    before = customer_in("get_json_object(before, '$.customer_id')", ids)
    return f"({after} OR {before})"


def pk_for_id(row_id: int) -> str:
    return json.dumps({"id": int(row_id)}, sort_keys=True, separators=(",", ":"))
