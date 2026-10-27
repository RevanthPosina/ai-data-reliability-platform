"""Two streaming queries in one Spark application (PLAN G3).

Query 1 (bronze): native Iceberg streaming sink, append-only change log, epoch-idempotent.
Query 2 (silver): foreachBatch, per table: classify the writer schemas in the batch, quarantine the
incompatible ones, evolve the table for the compatible ones, then dedupe to max LSN per key and run the
LSN-conditional MERGE with soft tombstones. They never share a foreachBatch and have separate checkpoints.

Every silver batch appends one row per table to the batch event log (Parquet, EVENT_LOG_DIR/silver_batches):
(batch_id, table, snapshot ids, committed_at, counts, [(pk, lsn)]). It feeds the rebuild proof (G3) and the
freshness measurement (G11).
"""

from __future__ import annotations

import os
import time
import urllib.error
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from adrp.spark import evolution, silver
from adrp.spark.decode import decode_change, latest_schema, raw_schema_by_id
from adrp.spark.session import build_session

TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
FORMAT_VERSION = os.environ.get("ICEBERG_FORMAT_VERSION", "2")
BUCKETS = 16
TOPIC_PATTERN = r"cdc\.public\..*"

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

EVENT_LOG_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.LongType()),
        T.StructField("table", T.StringType()),
        T.StructField("snapshot_before", T.LongType()),
        T.StructField("snapshot_after", T.LongType()),
        T.StructField("committed_at", T.TimestampType()),
        T.StructField("n_events", T.IntegerType()),
        T.StructField("n_quarantined", T.IntegerType()),
        T.StructField("max_lsn", T.LongType()),
        T.StructField(
            "changes",
            T.ArrayType(T.StructType([T.StructField("pk", T.StringType()), T.StructField("lsn", T.LongType())])),
        ),
        T.StructField("written_at", T.TimestampType()),
    ]
)


def read_topics(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribePattern", TOPIC_PATTERN)
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


def fault_point(point: str, batch_id: int) -> None:
    """Test-only: park the driver at a named point so a test can SIGKILL the container mid-batch.

    Enabled only with ADRP_FAULT_HOOKS=1 and a file CHECKPOINT_ROOT/fault naming the point.
    """
    if os.environ.get("ADRP_FAULT_HOOKS") != "1":
        return
    try:
        with open(os.path.join(os.environ["CHECKPOINT_ROOT"], "fault")) as fh:
            wanted = fh.read().strip()
    except FileNotFoundError:
        return
    if wanted == point:
        print(f"FAULT_HOOK point={point} batch={batch_id}", flush=True)
        time.sleep(600)


def wait_for_subject(subject: str, poll_s: float = 5.0) -> dict[str, Any]:
    while True:
        try:
            return latest_schema(subject)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            time.sleep(poll_s)


class Silver:
    """Silver tables seen so far, created on first sight of a topic (an empty table has no schema yet)."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.tables: dict[str, silver.SilverTable] = {}

    def get(self, topic: str) -> tuple[str, silver.SilverTable]:
        name = topic.rsplit(".", 1)[-1]
        if name not in self.tables:
            value = wait_for_subject(f"{topic}-value")
            key = wait_for_subject(f"{topic}-key")
            self.tables[name] = silver.create_silver(
                self.spark, name, [f["name"] for f in key["fields"]], silver.columns_from_registry(value)
            )
        return name, self.tables[name]


def writer_columns(schema_id: int) -> list[tuple[str, str]]:
    cols = silver.columns_from_registry(raw_schema_by_id(schema_id))
    return [(n, t) for n, t, _ in cols]


def process_table(spark: SparkSession, batch_id: int, topic: str, part: DataFrame, registry: Silver) -> dict[str, Any]:
    name, table = registry.get(topic)
    ids = [r["schema_id"] for r in part.select("schema_id").distinct().collect() if r["schema_id"] is not None]
    current = table.data_columns(spark)
    verdicts = {sid: evolution.classify(current, writer_columns(sid), table.keys) for sid in ids}
    bad = {sid: v.quarantine_reason for sid, v in verdicts.items() if not v.compatible}
    n_quarantined = 0
    good = part
    if bad:
        n_quarantined = silver.quarantine(spark, name, part.where(F.col("schema_id").isin(list(bad))), bad)
        good = part.where(~F.col("schema_id").isin(list(bad)))
        print(
            f"QUARANTINE batch={batch_id} table={name} rows={n_quarantined} reasons={sorted(set(bad.values()))}",
            flush=True,
        )
    stmts = silver.apply_evolution(spark, table, [v for v in verdicts.values() if v.compatible])
    for stmt in stmts:
        print(f"EVOLVE batch={batch_id} {stmt}", flush=True)
    before_id, _ = silver.current_snapshot(spark, table.name)
    carried: list[tuple[str, int]] = []
    if good.take(1):
        carried = silver.merge_changes(spark, table, good)
    after_id, committed_at = silver.current_snapshot(spark, table.name)
    return {
        "batch_id": batch_id, "table": name, "snapshot_before": before_id, "snapshot_after": after_id,
        "committed_at": committed_at, "n_events": len(carried), "n_quarantined": n_quarantined,
        "max_lsn": max((lsn for _, lsn in carried), default=None),
        "changes": [{"pk": pk, "lsn": lsn} for pk, lsn in carried],
        "written_at": datetime.now(timezone.utc),
    }  # fmt: skip


def make_batch_handler(registry: Silver) -> Any:
    event_dir = os.path.join(os.environ.get("EVENT_LOG_DIR", "/eventlog"), "silver_batches")

    def handle(batch: DataFrame, batch_id: int) -> None:
        # Tombstones (null value) are dropped before parsing; the delete event already carries op='d'.
        # Temp views and SQL must use the micro-batch's own (cloned) session, not the application session.
        session = batch.sparkSession
        rows = batch.where(F.col("value_bytes").isNotNull()).persist()
        try:
            fault_point("before_merge", batch_id)
            topics = sorted(r["topic"] for r in rows.select("topic").distinct().collect())
            entries = []
            for i, topic in enumerate(topics):
                entries.append(process_table(session, batch_id, topic, rows.where(F.col("topic") == topic), registry))
                if i == 0 and len(topics) > 1:
                    fault_point("mid_merge", batch_id)
            fault_point("after_merge", batch_id)
            if entries:
                session.createDataFrame(entries, EVENT_LOG_SCHEMA).write.mode("append").parquet(event_dir)
        finally:
            rows.unpersist()

    return handle


def main() -> None:
    spark = build_session("adrp-streams")
    spark.sparkContext.setLogLevel("WARN")
    ensure_bronze(spark)
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
    silver_q = (
        decoded(read_topics(spark))
        .writeStream.foreachBatch(make_batch_handler(Silver(spark)))
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{root}/silver")
        .queryName("silver")
        .start()
    )
    _ = (bronze_q, silver_q)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
