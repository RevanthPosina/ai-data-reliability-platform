"""Rebuild silver from bronze up to a target LSN and checksum it (PLAN G3 recovery proof).

The rebuild applies the same projection and dedupe as the streaming MERGE, but as one batch job over
the append-only change log, skipping the rows silver quarantined. If the streaming silver table is
correct, the rebuild at the LSN of a batch boundary equals the silver snapshot committed by that batch.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from adrp.spark import silver

MOD = 1 << 64


def checksum(df: DataFrame, columns: list[str]) -> tuple[int, int]:
    """(row count, order-independent checksum): sum of xxhash64 over the columns, mod 2^64."""
    row = (
        df.select(F.xxhash64(*[F.col(f"`{c}`") for c in columns]).alias("h"))
        .agg(F.count("*").alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s"))
        .first()
    )
    assert row is not None
    return int(row["n"]), int(row["s"] or 0) % MOD


def rebuild(spark: SparkSession, table: silver.SilverTable, max_lsn: int) -> DataFrame:
    name = table.name.rsplit(".", 1)[-1]
    bronze = spark.table("lake.bronze.cdc_events").where(
        (F.col("source_table") == f"public.{name}") & F.col("value_bytes").isNotNull() & (F.col("lsn") <= max_lsn)
    )
    quarantine = f"lake.silver.quarantine_{name}"
    if spark.catalog.tableExists(quarantine):
        parked = spark.table(quarantine).select("topic", "kafka_partition", "kafka_offset")
        bronze = bronze.join(parked, ["topic", "kafka_partition", "kafka_offset"], "left_anti")
    state = silver.latest_per_key(silver.project_changes(spark, table, bronze), table.keys)
    return state.drop("_pk")


def snapshot_df(spark: SparkSession, fq: str, snapshot_id: int) -> DataFrame:
    return spark.read.option("versionAsOf", str(snapshot_id)).table(fq)
