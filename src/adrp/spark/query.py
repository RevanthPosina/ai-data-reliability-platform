"""One-off Spark SQL probe used by `make demo`: find one pk in bronze and silver.

Prints a single line `RESULT {json}` once both are found (or on timeout).
"""

from __future__ import annotations

import argparse
import json
import time

from adrp.spark.session import build_session


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="help_articles")
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--min-lsn", type=int, default=0)
    ap.add_argument("--timeout", type=float, default=180)
    args = ap.parse_args()

    spark = build_session("adrp-query")
    spark.sparkContext.setLogLevel("ERROR")
    pk = json.dumps({"id": args.id}, separators=(",", ":"))
    deadline = time.monotonic() + args.timeout
    out: dict[str, object] = {"found_bronze": False, "found_silver": False}
    while time.monotonic() < deadline:
        bronze = spark.sql(
            "SELECT op, lsn, kafka_ts FROM lake.bronze.cdc_events "
            f"WHERE source_table = 'public.{args.table}' AND pk = '{pk}' AND lsn >= {args.min_lsn} "
            "ORDER BY lsn"
        ).collect()
        silver = spark.sql(
            f"SELECT id, title, lsn, is_deleted FROM lake.silver.{args.table} "
            f"WHERE id = {args.id} AND lsn >= {args.min_lsn}"
        ).collect()
        out = {
            "found_bronze": bool(bronze),
            "found_silver": bool(silver),
            "bronze_ops": [r.op for r in bronze],
            "silver": silver[0].asDict() if silver else None,
        }
        if bronze and silver:
            break
        time.sleep(3)
    print("RESULT " + json.dumps(out, default=str), flush=True)


if __name__ == "__main__":
    main()
