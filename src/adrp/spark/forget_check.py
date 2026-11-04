"""Spark-side verification helpers for the forget workflow (G8). Same conventions as forget.py: one JVM per call,
output lines `ROW\\t...` and `RESULT {json}`; no RESULT carries an id, pk or canary.

footprints --customers 1,2     ROW <customer> <source_table> <pk json>   per customer, the union of the rows of
                               RESULT {customers: n}                      bronze/quarantine history and live silver
logical --groups JSON          RESULT {label: {bronze_rows, quarantine_rows, bronze_txn_rows, silver: {table: {...}}}}
                               groups = {label: {footprint, customers, txid, lsn}}; counts for each group's footprint

`logical` answers, per group: how many footprint rows are left in bronze and in quarantine; how many bronze rows
belong to the forget transaction (txid and lsn >= the begin lsn: delivery check); per silver table with footprint
rows: live rows, tombstones, tombstones with lsn >= the begin lsn, tombstones that still hold a non-key value, and
rows whose customer_id is still one of the group's customers.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from pyspark.sql import SparkSession

from adrp.spark import forget, silver
from adrp.spark.forget_sql import BRONZE_TABLE, events_where, parse_footprint, pk_values, silver_where
from adrp.spark.probe import out


def non_key_columns(columns: list[str], key_cols: list[str]) -> list[str]:
    """Columns of a silver table that a tombstone must leave NULL: everything except keys and metadata."""
    skip = set(key_cols) | set(silver.META_COLUMNS)
    return [c for c in columns if c not in skip]


def silver_counts(spark: SparkSession, fq: str, fp: dict[str, list[str]], customers: list[int], lsn: int | None) -> Any:
    name = fq.split(".")[-1]
    where = silver_where(fp, name)
    cols = spark.table(fq).columns
    res: dict[str, Any] = {}
    if where:
        keys = sorted(pk_values(fp[f"public.{name}"][0]))
        n = forget.count_where
        res["footprint_rows"] = n(spark, fq, where)
        res["live_rows"] = n(spark, fq, f"({where}) AND NOT is_deleted")
        res["tombstones"] = n(spark, fq, f"({where}) AND is_deleted")
        res["tombstones_at_lsn"] = (
            n(spark, fq, f"({where}) AND is_deleted AND lsn >= {int(lsn)}") if lsn is not None else None
        )
        nk = non_key_columns(cols, keys)
        pii = " OR ".join(f"`{c}` IS NOT NULL" for c in nk)
        res["tombstones_with_values"] = n(spark, fq, f"({where}) AND is_deleted AND ({pii})") if nk else 0
    if customers and "customer_id" in cols:
        ints = ", ".join(str(int(c)) for c in sorted(set(customers)))
        res["customer_id_rows"] = forget.count_where(spark, fq, f"customer_id IN ({ints})")
    return res


def logical_group(spark: SparkSession, g: dict[str, Any]) -> dict[str, Any]:
    fp = parse_footprint(json.dumps(g.get("footprint", {})))
    customers = [int(c) for c in g.get("customers", [])]
    lsn = g.get("lsn")
    res: dict[str, Any] = {"bronze_rows": 0, "quarantine_rows": 0, "bronze_txn_rows": None, "silver": {}}
    where = events_where(fp)
    for fq in forget.event_tables(spark):
        n = forget.count_where(spark, fq, where) if where else 0
        key = "bronze_rows" if fq == BRONZE_TABLE else "quarantine_rows"
        res[key] += n
    if g.get("txid") is not None:
        cond = f"txid = {int(g['txid'])}" + (f" AND lsn >= {int(lsn)}" if lsn is not None else "")
        res["bronze_txn_rows"] = forget.count_where(spark, BRONZE_TABLE, cond)
    for fq in forget.silver_tables(spark):
        r = silver_counts(spark, fq, fp, customers, lsn)
        if r:
            res["silver"][fq.split(".")[-1]] = r
    return res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["footprints", "logical"])
    ap.add_argument("--customers", default="")
    ap.add_argument("--groups", default="")
    a = ap.parse_args()
    spark = forget.build_session_for_cli()
    if a.cmd == "footprints":
        customers = forget.read_ints(a.customers, "")
        for c in customers:
            for table, pks in forget.compute_footprint(spark, [c]).items():
                for pk in pks:
                    print(f"ROW\t{c}\t{table}\t{pk}", flush=True)
        out("RESULT", {"customers": len(customers)})
    else:
        groups = json.loads(a.groups)
        out("RESULT", {label: logical_group(spark, g) for label, g in groups.items()})


if __name__ == "__main__":
    main()
