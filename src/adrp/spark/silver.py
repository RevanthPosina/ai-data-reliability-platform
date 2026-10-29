"""Silver tables: creation, schema evolution, quarantine and the LSN-conditional MERGE (PLAN G3).

This module is the Spark implementation of adrp.merge_model; the property tests drive both.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from adrp.spark import avro_types
from adrp.spark.avro_types import json_type
from adrp.spark.evolution import Verdict

META_COLUMNS = ("lsn", "is_deleted", "op", "source_ts")
BUCKETS = 16
FORMAT_VERSION = os.environ.get("ICEBERG_FORMAT_VERSION", "2")

QUARANTINE_DDL = """
    topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_ts TIMESTAMP,
    key_bytes BINARY, value_bytes BINARY, schema_id INT, source_table STRING,
    op STRING, lsn BIGINT, txid BIGINT, source_ts TIMESTAMP, pk STRING,
    before STRING, after STRING, reason STRING, quarantined_at TIMESTAMP
"""


@dataclass
class SilverTable:
    name: str  # fully qualified, e.g. lake.silver.customers
    keys: list[str]

    def data_columns(self, spark: SparkSession) -> list[tuple[str, str]]:
        """Current data columns (name, spark ddl type) read from the catalog, excluding bookkeeping columns."""
        return [
            (f.name, f.dataType.simpleString())
            for f in spark.table(self.name).schema.fields
            if f.name not in META_COLUMNS
        ]

    def key_ddl(self, spark: SparkSession) -> str:
        cols = dict(self.data_columns(spark))
        return ", ".join(f"`{k}` {json_type(cols[k])}" for k in self.keys)


def create_silver(
    spark: SparkSession, name: str, keys: list[str], columns: list[tuple[str, str, bool]], target_file_mb: int = 128
) -> SilverTable:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    cols = ", ".join(f"`{n}` {t}" for n, t, _ in columns)
    fq = f"lake.silver.{name}"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            {cols}, lsn BIGINT, is_deleted BOOLEAN, op STRING, source_ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (bucket({BUCKETS}, `{keys[0]}`))
        TBLPROPERTIES (
            'format-version'='{FORMAT_VERSION}',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read',
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='{target_file_mb * 1024 * 1024}')
        """
    )
    spark.sql(f"ALTER TABLE {fq} WRITE ORDERED BY {', '.join(f'`{k}`' for k in keys)}")
    # WRITE ORDERED BY resets the distribution mode to `range`; the plan specifies `hash` (re-set it after).
    spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('write.distribution-mode'='hash')")
    return SilverTable(fq, keys)


def apply_evolution(spark: SparkSession, table: SilverTable, verdicts: list[Verdict]) -> list[str]:
    """Run the ALTER TABLEs the compatible verdicts need. Returns the statements for logging."""
    stmts: list[str] = []
    existing = {n for n, _ in table.data_columns(spark)}
    for v in verdicts:
        for col, ddl in v.adds:
            if col not in existing:
                stmts.append(f"ALTER TABLE {table.name} ADD COLUMN `{col}` {ddl}")
                existing.add(col)
        for col, ddl in v.promotions:
            stmts.append(f"ALTER TABLE {table.name} ALTER COLUMN `{col}` TYPE {ddl}")
    for stmt in dict.fromkeys(stmts):  # de-duplicate, keep order
        spark.sql(stmt)
    return list(dict.fromkeys(stmts))


def ensure_quarantine(spark: SparkSession, name: str) -> str:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    fq = f"lake.silver.quarantine_{name}"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fq} ({QUARANTINE_DDL}) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{FORMAT_VERSION}')"
    )
    return fq


def quarantine(spark: SparkSession, name: str, rows: DataFrame, reasons: dict[int, str]) -> int:
    """Park rows whose schema cannot be applied. Idempotent per (topic, partition, offset)."""
    fq = ensure_quarantine(spark, name)
    reason_col = F.create_map(*[x for sid, r in reasons.items() for x in (F.lit(sid), F.lit(r))])[F.col("schema_id")]
    out = rows.select(
        "topic", "kafka_partition", "kafka_offset", "kafka_ts", "key_bytes", "value_bytes", "schema_id",
        "source_table", "op", "lsn", "txid", "source_ts", "pk", "before", "after",
        reason_col.alias("reason"), F.current_timestamp().alias("quarantined_at"),
    )  # fmt: skip
    n = out.count()
    view = f"q_{name}"
    out.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {fq} t USING {view} s
        ON t.topic = s.topic AND t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return n


def project_changes(spark: SparkSession, table: SilverTable, changes: DataFrame) -> DataFrame:
    """Decode `pk`/`after` JSON into typed columns of the table's *current* schema (one row per change).

    Columns the record does not carry become NULL; deletes become soft tombstones (non-key columns NULL).
    """
    cols = table.data_columns(spark)
    after_ddl = ", ".join(f"`{n}` {json_type(t)}" for n, t in cols)
    k = F.from_json("pk", table.key_ddl(spark)).alias("k")
    a = F.from_json("after", after_ddl).alias("a")
    rows = changes.select(F.col("pk").alias("_pk"), k, a, "op", "lsn", "source_ts", "kafka_offset")
    is_del = F.col("op") == "d"
    types = dict(cols)
    exprs = []
    for n, t in cols:
        if n in table.keys:
            exprs.append(F.col(f"k.{n}").cast(t).alias(n))
        else:
            exprs.append(F.when(is_del, F.lit(None)).otherwise(F.col(f"a.{n}")).cast(types[n]).alias(n))
    return rows.select("_pk", *exprs, "lsn", is_del.alias("is_deleted"), "op", "source_ts", "kafka_offset")


def latest_per_key(projected: DataFrame, keys: list[str]) -> DataFrame:
    """(a) dedupe within a set of changes to the max (lsn, kafka_offset) per key."""
    latest = Window.partitionBy(*keys).orderBy(F.col("lsn").desc(), F.col("kafka_offset").desc())
    return projected.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn", "kafka_offset")


def merge_changes(spark: SparkSession, table: SilverTable, changes: DataFrame) -> list[tuple[str, int]]:
    """(a) dedupe, (b) LSN-conditional MERGE. Returns [(pk, lsn)] it carried."""
    src = latest_per_key(project_changes(spark, table, changes), table.keys).persist()
    try:
        carried = [(r["_pk"], int(r["lsn"])) for r in src.select("_pk", "lsn").collect()]
        view = f"src_{table.name.split('.')[-1]}"
        src.drop("_pk").createOrReplaceTempView(view)
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in table.keys)
        spark.sql(
            f"""
            MERGE INTO {table.name} t USING {view} s ON {on}
            WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        src.unpersist()
    return carried


def current_snapshot(spark: SparkSession, fq: str) -> tuple[int | None, Any]:
    row = spark.sql(f"SELECT snapshot_id, committed_at FROM {fq}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return (int(row["snapshot_id"]), row["committed_at"]) if row else (None, None)


def columns_from_registry(envelope: dict[str, Any]) -> list[tuple[str, str, bool]]:
    return avro_types.record_columns(avro_types.find_record(envelope, "after"))
