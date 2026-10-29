"""Spark-side probes used by the G3 check (one JVM per call). Output lines start with a tag.

dump --tables a,b                       ROW <table> <json>      every silver row
bronze-stats                            RESULT {...}            rows and distinct (topic,partition,offset) per table
quarantine                              RESULT {...}            quarantined rows per table and reason
columns --tables a,b                    RESULT {...}            current silver columns
files --tables a,b                      RESULT {...}            data/delete file counts and bytes, snapshot counts
rebuild-check --tables a,b --pick k     RESULT {...}            rebuild-from-bronze vs time-travelled silver snapshots
checksum --tables a,b                   RESULT {...}            count + checksum of the current silver tables
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from adrp.spark import rebuild, silver
from adrp.spark.session import build_session


def event_log_dir() -> str:
    return os.path.join(os.environ.get("EVENT_LOG_DIR", "/eventlog"), "silver_batches")


def out(tag: str, payload: Any) -> None:
    print(f"{tag} {json.dumps(payload, default=str)}", flush=True)


def table_obj(spark: SparkSession, name: str) -> silver.SilverTable:
    fq = f"lake.silver.{name}"
    # keys are the leading identity column of every domain table (`id`); read from the key subject for generality
    from adrp.spark.decode import latest_schema

    key = latest_schema(f"cdc.public.{name}-key")
    return silver.SilverTable(fq, [f["name"] for f in key["fields"]])


def cmd_dump(spark: SparkSession, tables: list[str]) -> None:
    for t in tables:
        fmt = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"  # microseconds: to_json defaults to milliseconds
        for r in spark.sql(
            f"SELECT to_json(struct(*), map('timestampFormat', \"{fmt}\")) AS j FROM lake.silver.{t}"
        ).collect():
            print(f"ROW\t{t}\t{r['j']}", flush=True)


def cmd_bronze_stats(spark: SparkSession) -> None:
    df = spark.table("lake.bronze.cdc_events")
    rows = df.groupBy("topic").agg(
        F.count("*").alias("rows"),
        F.countDistinct("kafka_partition", "kafka_offset").alias("distinct_offsets"),
        F.sum(F.col("value_bytes").isNull().cast("int")).alias("tombstones"),
        F.max("lsn").alias("max_lsn"),
    )
    out("RESULT", {r["topic"]: r.asDict() for r in rows.collect()})


def cmd_quarantine(spark: SparkSession) -> None:
    res: dict[str, Any] = {}
    for ns in spark.sql("SHOW TABLES IN lake.silver").collect():
        if ns["tableName"].startswith("quarantine_"):
            rows = spark.table(f"lake.silver.{ns['tableName']}").groupBy("reason", "schema_id").count().collect()
            res[ns["tableName"]] = [r.asDict() for r in rows]
    out("RESULT", res)


def cmd_columns(spark: SparkSession, tables: list[str]) -> None:
    out(
        "RESULT",
        {
            t: [f"{f.name}:{f.dataType.simpleString()}" for f in spark.table(f"lake.silver.{t}").schema.fields]
            for t in tables
        },
    )


def file_stats(spark: SparkSession, fq: str) -> dict[str, Any]:
    query = f"SELECT content, count(*) AS n, sum(file_size_in_bytes) AS bytes FROM {fq}.files GROUP BY content"
    by = {r["content"]: r for r in spark.sql(query).collect()}
    snaps = spark.sql(f"SELECT count(*) AS n FROM {fq}.snapshots").first()
    return {
        "data_files": int(by[0]["n"]) if 0 in by else 0,
        "data_bytes": int(by[0]["bytes"]) if 0 in by else 0,
        "delete_files": sum(int(by[c]["n"]) for c in (1, 2) if c in by),
        "snapshots": int(snaps["n"]) if snaps else 0,
    }


def cmd_props(spark: SparkSession, tables: list[str]) -> None:
    keys = ("write.merge.mode", "write.update.mode", "write.delete.mode", "write.distribution-mode", "format-version")
    res = {}
    for t in tables:
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES lake.silver.{t}").collect()}
        res[t] = {k: props.get(k) for k in keys}
    out("RESULT", res)


def cmd_files(spark: SparkSession, tables: list[str]) -> None:
    out("RESULT", {t: file_stats(spark, t if t.startswith("lake.") else f"lake.silver.{t}") for t in tables})


def data_columns(spark: SparkSession, fq: str) -> list[str]:
    return sorted(f.name for f in spark.table(fq).schema.fields)


def cmd_checksum(spark: SparkSession, tables: list[str]) -> None:
    res = {}
    for t in tables:
        fq = f"lake.silver.{t}"
        n, h = rebuild.checksum(spark.table(fq), data_columns(spark, fq))
        res[t] = {"rows": n, "checksum": h}
    out("RESULT", res)


def cmd_rebuild_check(spark: SparkSession, tables: list[str], picks: int) -> None:
    """For each table, take batch boundaries from the event log: target LSN T = highest LSN carried by batches
    up to and including that one; S = the snapshot that batch left. Compare rebuild(T) with silver at S."""
    log = spark.read.parquet(event_log_dir())
    res: dict[str, Any] = {}
    for t in tables:
        entries = (
            log.where((F.col("table") == t) & F.col("max_lsn").isNotNull() & F.col("snapshot_after").isNotNull())
            .select("batch_id", "snapshot_before", "snapshot_after", "max_lsn", "written_at")
            .orderBy("batch_id", "written_at")
            .collect()
        )
        # a batch replayed after a crash appears twice: keep the last write per batch_id
        by_batch = {e["batch_id"]: e for e in entries}
        ordered = [by_batch[b] for b in sorted(by_batch)]
        committed = [
            e for e in ordered if e["snapshot_after"] != e["snapshot_before"]
        ]  # batches that changed the table
        cumulative, hi = [], 0
        for e in ordered:
            hi = max(hi, e["max_lsn"])
            cumulative.append((e, hi))
        chosen_idx = sorted(
            {len(cumulative) - 1, *[int(len(cumulative) * (i + 1) / (picks + 1)) for i in range(picks - 1)]}
        )
        table = table_obj(spark, t)
        checks = []
        for idx in chosen_idx:
            if not (0 <= idx < len(cumulative)):
                continue
            e, target = cumulative[idx]
            snap = rebuild.snapshot_df(spark, table.name, e["snapshot_after"])
            cols = sorted(snap.columns)
            n_s, h_s = rebuild.checksum(snap, cols)
            top = snap.agg(F.max("lsn")).first()
            snap_max_lsn = int(top[0]) if top and top[0] is not None else None
            rebuilt = rebuild.rebuild(spark, table, target).select(*[F.col(f"`{c}`") for c in cols])
            n_r, h_r = rebuild.checksum(rebuilt, cols)
            checks.append({
                "batch_id": e["batch_id"], "target_lsn": target, "snapshot_id": e["snapshot_after"],
                "snapshot_max_lsn": snap_max_lsn, "snapshot_rows": n_s, "rebuilt_rows": n_r,
                "snapshot_checksum": h_s, "rebuilt_checksum": h_r,
                "equal": (n_s, h_s) == (n_r, h_r),
            })  # fmt: skip
        res[t] = {"batches_logged": len(ordered), "batches_that_committed": len(committed), "checks": checks}
    out("RESULT", res)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "cmd", choices=["dump", "bronze-stats", "quarantine", "columns", "props", "files", "checksum", "rebuild-check"]
    )
    ap.add_argument("--tables", default="")
    ap.add_argument("--pick", type=int, default=3)
    a = ap.parse_args()
    spark = build_session("adrp-inspect")
    spark.sparkContext.setLogLevel("ERROR")
    tables = [t for t in a.tables.split(",") if t]
    if a.cmd == "dump":
        cmd_dump(spark, tables)
    elif a.cmd == "bronze-stats":
        cmd_bronze_stats(spark)
    elif a.cmd == "quarantine":
        cmd_quarantine(spark)
    elif a.cmd == "columns":
        cmd_columns(spark, tables)
    elif a.cmd == "props":
        cmd_props(spark, tables)
    elif a.cmd == "files":
        cmd_files(spark, tables)
    elif a.cmd == "checksum":
        cmd_checksum(spark, tables)
    else:
        cmd_rebuild_check(spark, tables, a.pick)


if __name__ == "__main__":
    main()
