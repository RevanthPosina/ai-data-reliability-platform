"""Two streaming queries in one Spark application (PLAN G3, G0 thin slice).

Query 1 (bronze): native Iceberg streaming sink, append-only change log, epoch-idempotent.
Query 2 (silver): foreachBatch, dedupe to max LSN per key, LSN-conditional MERGE, soft tombstones.
They never share a foreachBatch. Each has its own checkpoint.
"""

from __future__ import annotations

import os
import time
import urllib.error
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from adrp.spark import avro_types
from adrp.spark.decode import decode_change, latest_schema
from adrp.spark.session import build_session

# topic -> silver table. G1 adds the rest of the domain.
TABLES = {"cdc.public.help_articles": "help_articles"}
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
FORMAT_VERSION = os.environ.get("ICEBERG_FORMAT_VERSION", "2")
BUCKETS = 16

ENVELOPE = T.StructType(
    [
        T.StructField("schema_id", T.IntegerType()),
        T.StructField("pk", T.StringType()),
        T.StructField("op", T.StringType()),
        T.StructField("lsn", T.LongType()),
        T.StructField("txid", T.LongType()),
        T.StructField("source_ts_ms", T.LongType()),
        T.StructField("before", T.StringType()),
        T.StructField("after", T.StringType()),
    ]
)
decode_udf = F.udf(decode_change, ENVELOPE)


def read_topics(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", ",".join(TABLES))
        .option("startingOffsets", "earliest")
        .load()
    )


def decoded(raw: DataFrame) -> DataFrame:
    return raw.withColumn("e", decode_udf("key", "value")).select(
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").alias("key_bytes"),
        F.col("value").alias("value_bytes"),
        F.col("e.schema_id").alias("schema_id"),
        F.regexp_replace("topic", r"^cdc\.", "").alias("source_table"),
        F.col("e.op").alias("op"),
        F.col("e.lsn").alias("lsn"),
        F.col("e.txid").alias("txid"),
        F.timestamp_millis(F.col("e.source_ts_ms")).alias("source_ts"),
        F.col("e.pk").alias("pk"),
        F.col("e.before").alias("before"),
        F.col("e.after").alias("after"),
    )


def ensure_bronze(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lake.bronze.cdc_events (
            topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_ts TIMESTAMP,
            key_bytes BINARY, value_bytes BINARY, schema_id INT, source_table STRING,
            op STRING, lsn BIGINT, txid BIGINT, source_ts TIMESTAMP, pk STRING,
            before STRING, after STRING)
        USING iceberg
        PARTITIONED BY (source_table, days(kafka_ts), bucket({BUCKETS}, pk))
        TBLPROPERTIES ('format-version'='{FORMAT_VERSION}')
        """
    )


def wait_for_subject(subject: str, poll_s: float = 5.0) -> dict[str, Any]:
    """Debezium registers a subject with its first record; wait instead of crash-looping."""
    while True:
        try:
            return latest_schema(subject)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            print(f"waiting for schema subject {subject}", flush=True)
            time.sleep(poll_s)


class SilverTable:
    """Target schema for one silver table, resolved from the registry at query start."""

    def __init__(self, spark: SparkSession, topic: str, name: str) -> None:
        self.name = f"lake.silver.{name}"
        value_schema = wait_for_subject(f"{topic}-value")
        key_schema = wait_for_subject(f"{topic}-key")
        self.columns = avro_types.record_columns(avro_types.find_record(value_schema, "after"))
        self.keys = [f["name"] for f in key_schema["fields"]]
        self.after_ddl = avro_types.struct_ddl(self.columns)
        self.key_ddl = avro_types.struct_ddl([c for c in self.columns if c[0] in self.keys])
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
        cols = ", ".join(f"`{n}` {t}" for n, t, _ in self.columns)
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                {cols}, lsn BIGINT, is_deleted BOOLEAN, op STRING, source_ts TIMESTAMP)
            USING iceberg
            PARTITIONED BY (bucket({BUCKETS}, `{self.keys[0]}`))
            TBLPROPERTIES (
                'format-version'='{FORMAT_VERSION}',
                'write.merge.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.delete.mode'='merge-on-read',
                'write.distribution-mode'='hash')
            """
        )
        spark.sql(f"ALTER TABLE {self.name} WRITE ORDERED BY {', '.join(self.keys)}")

    def merge(self, changes: DataFrame) -> None:
        nonkey = [n for n, _, _ in self.columns if n not in self.keys]
        rows = changes.select(
            F.from_json("pk", self.key_ddl).alias("k"),
            F.from_json("after", self.after_ddl).alias("a"),
            "op",
            "lsn",
            "source_ts",
            "kafka_offset",
        )
        is_del = F.col("op") == "d"
        projected = rows.select(
            *[F.col(f"k.{k}").alias(k) for k in self.keys],
            # Soft tombstone: non-key columns NULL, so tombstones carry no PII.
            *[F.when(is_del, F.lit(None)).otherwise(F.col(f"a.{c}")).alias(c) for c in nonkey],
            "lsn",
            is_del.alias("is_deleted"),
            "op",
            "source_ts",
            "kafka_offset",
        )
        latest = Window.partitionBy(*self.keys).orderBy(F.col("lsn").desc(), F.col("kafka_offset").desc())
        src = projected.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn", "kafka_offset")
        view = f"src_{self.name.split('.')[-1]}"
        src.createOrReplaceTempView(view)
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in self.keys)
        changes.sparkSession.sql(
            f"""
            MERGE INTO {self.name} t USING {view} s ON {on}
            WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )


def main() -> None:
    spark = build_session("adrp-streams")
    spark.sparkContext.setLogLevel("WARN")
    ensure_bronze(spark)
    silver = {topic: SilverTable(spark, topic, name) for topic, name in TABLES.items()}
    root = os.environ["CHECKPOINT_ROOT"]

    bronze_q = (
        decoded(read_topics(spark))
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{root}/bronze")
        .option("fanout-enabled", "true")
        .queryName("bronze")
        .toTable("lake.bronze.cdc_events")
    )

    def merge_batch(batch: DataFrame, batch_id: int) -> None:
        # Tombstones (null value) are dropped before parsing; the delete event already carries op='d'.
        changes = batch.where(F.col("value_bytes").isNotNull()).persist()
        try:
            for topic, table in silver.items():
                part = changes.where(F.col("topic") == topic)
                if not part.isEmpty():
                    table.merge(part)
        finally:
            changes.unpersist()

    silver_q = (
        decoded(read_topics(spark))
        .writeStream.foreachBatch(merge_batch)
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{root}/silver")
        .queryName("silver")
        .start()
    )
    _ = (bronze_q, silver_q)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
