"""Fault-injection helpers that need Spark (G10a). One JVM per call; output lines start with a tag, like probe.py. Run
it the way the G3 check runs probe.py (spark-query container, tools profile). Test-only: nothing here is part of a
pipeline.

replay-bronze --bootstrap host:port --ranges JSON   RESULT {read, by_partition: {topic: {partition: n}}}
                                                    batch-read the Kafka ranges, decode them as the streaming query does
                                                    and append them to lake.bronze.cdc_events: bronze then holds those
                                                    records twice, which is the by-design effect of a replayed offset
                                                    range. Ranges: {"cdc.public.customers": {"0": [start, end], ...}},
                                                    start inclusive, end exclusive. `read` is what was actually read
                                                    and appended, which can be fewer than requested (compacted offsets).
orphans [--tables ..] [--grace-s N]                 RESULT {orphans_removed: {table: n}, total, errors: {table: type}}
                                                    forget.remove_orphans_now per table (default grace 0: run it with
                                                    the writers stopped). --tables is as for forget.py, default all.

The pure parts (range parsing, Kafka option strings) import no Spark, so the host test suite covers them.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

BRONZE_TABLE = "lake.bronze.cdc_events"

Ranges = dict[str, dict[int, tuple[int, int]]]


def emit(tag: str, payload: Any) -> None:
    print(f"{tag} {json.dumps(payload, default=str)}", flush=True)


def parse_ranges(text: str) -> Ranges:
    """{topic: {partition: [start, end]}} with start inclusive and end exclusive. Empty ranges are dropped."""
    raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("ranges must be a JSON object {topic: {partition: [start, end]}}")
    ranges: Ranges = {}
    for topic, parts in raw.items():
        if not isinstance(parts, dict):
            raise ValueError(f"ranges[{topic!r}] must be an object {{partition: [start, end]}}")
        for part, bounds in parts.items():
            if not (isinstance(bounds, list) and len(bounds) == 2 and all(type(b) is int for b in bounds)):
                raise ValueError(f"ranges[{topic!r}][{part!r}] must be [start, end] integers")
            start, end = bounds
            if start < 0 or end < start:
                raise ValueError(f"ranges[{topic!r}][{part!r}] needs 0 <= start <= end")
            if end > start:
                ranges.setdefault(str(topic), {})[int(part)] = (start, end)
    return ranges


def kafka_options(ranges: Ranges) -> dict[str, str]:
    """The `assign`, `startingOffsets` and `endingOffsets` options of a Kafka batch read of exactly these ranges.
    `assign` names the partitions, because with `subscribe` the offset JSON would have to cover every partition."""
    return {
        "assign": json.dumps({t: sorted(p) for t, p in sorted(ranges.items())}, separators=(",", ":")),
        "startingOffsets": json.dumps(
            {t: {str(p): se[0] for p, se in sorted(parts.items())} for t, parts in sorted(ranges.items())},
            separators=(",", ":"),
        ),
        "endingOffsets": json.dumps(
            {t: {str(p): se[1] for p, se in sorted(parts.items())} for t, parts in sorted(ranges.items())},
            separators=(",", ":"),
        ),
    }


def replay_bronze(spark: SparkSession, bootstrap: str, ranges: Ranges) -> dict[str, Any]:
    from adrp.spark import streams  # decoded() is the projection of the streaming bronze query

    if not ranges:
        return {"read": 0, "by_partition": {}}
    reader = spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap)
    for key, value in kafka_options(ranges).items():
        reader = reader.option(key, value)
    # Compaction may have removed offsets inside a range: read what is there instead of failing.
    df = streams.decoded(reader.option("failOnDataLoss", "false").load()).persist()
    try:
        by_partition: dict[str, dict[str, int]] = {}
        for r in df.groupBy("topic", "kafka_partition").count().collect():
            by_partition.setdefault(r["topic"], {})[str(r["kafka_partition"])] = int(r["count"])
        read = sum(n for parts in by_partition.values() for n in parts.values())
        if read:
            df.writeTo(BRONZE_TABLE).append()
    finally:
        df.unpersist()
    return {"read": read, "by_partition": by_partition}


def cmd_orphans(spark: SparkSession, spec: str, grace_s: float) -> None:
    from adrp.spark.forget import remove_orphans_now, resolve_tables

    removed: dict[str, int] = {}
    errors: dict[str, str] = {}
    for fq in resolve_tables(spark, spec):
        try:
            removed[fq] = remove_orphans_now(spark, fq, grace_s)
        except Exception as e:  # noqa: BLE001 - one table failing must not hide the others' counts
            errors[fq] = type(e).__name__
    emit("RESULT", {"orphans_removed": removed, "total": sum(removed.values()), "errors": errors})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["replay-bronze", "orphans"])
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", ""))
    ap.add_argument("--ranges", default="")
    ap.add_argument("--tables", default="")
    ap.add_argument("--grace-s", type=float, default=0)
    a = ap.parse_args()
    if a.cmd == "replay-bronze":
        if not a.bootstrap or not a.ranges:
            ap.error("replay-bronze needs --bootstrap and --ranges")
        ranges = parse_ranges(a.ranges)
    from adrp.spark.session import build_session

    spark = build_session("adrp-fault")
    spark.sparkContext.setLogLevel("ERROR")
    if a.cmd == "replay-bronze":
        emit("RESULT", replay_bronze(spark, a.bootstrap, ranges))
    else:
        cmd_orphans(spark, a.tables, a.grace_s)


if __name__ == "__main__":
    main()
