"""Iceberg table maintenance (PLAN G3): compaction, manifest rewrite, snapshot expiry, orphan removal.

Each step returns a plain dict (before/after file counts, attempts, failures) so Dagster can log it and the
G3 check can assert on it. Compaction runs with partial progress and a retry loop, because it can fail
validation when a concurrent MERGE commits delete files for the files it is rewriting; attempts and
failures are returned so the conflict rate is recorded, not hidden.

Defaults are conservative for deletion (G8 tightens them): bronze keeps snapshots longer than the maximum
reader lag of the embedding worker (G6), and orphan removal only touches files older than the longest
possible write so it cannot delete files of an in-flight commit.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import SparkSession

from adrp.spark.probe import file_stats

COMPACTION_OPTIONS = {
    "partial-progress.enabled": "true",
    "partial-progress.max-commits": "10",
    "min-input-files": "2",
    "max-concurrent-file-group-rewrites": "2",
}
SILVER_RETAIN_HOURS = 24
BRONZE_RETAIN_HOURS = 72  # > the maximum G6 reader lag (SLO 120 s) by orders of magnitude, on purpose
ORPHAN_OLDER_THAN_HOURS = 24  # >= the maximum write duration


def short(fq: str) -> str:
    """lake.silver.customers -> silver.customers (the procedure's `table` argument is catalog-relative)."""
    return fq.split(".", 1)[1]


def ts_literal(older_than: datetime) -> str:
    return older_than.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def compact(
    spark: SparkSession,
    fq: str,
    attempts: int = 3,
    options: dict[str, str] | None = None,
    where: str | None = None,
) -> dict[str, Any]:
    """rewrite_data_files with the default options; `options` overrides them key by key (G8 passes
    delete-file-threshold=1, min-input-files=1, ...), `where` is a Spark SQL filter selecting the files to rewrite."""
    before = file_stats(spark, fq)
    merged = {**COMPACTION_OPTIONS, **(options or {})}
    opts = ", ".join(f"'{k}', '{v}'" for k, v in merged.items())
    where_arg = ""
    if where:
        escaped = where.replace("\\", "\\\\").replace("'", "\\'")
        where_arg = f", where => '{escaped}'"
    failures: list[str] = []
    result: dict[str, Any] = {}
    for attempt in range(1, attempts + 1):
        try:
            row = spark.sql(
                f"CALL lake.system.rewrite_data_files(table => '{short(fq)}'{where_arg}, options => map({opts}))"
            ).first()
            result = row.asDict() if row else {}
            break
        except Exception as e:  # noqa: BLE001 - commit/validation conflicts surface as various Java exceptions
            failures.append(type(e).__name__ + ": " + str(e).splitlines()[0][:200])
            time.sleep(2 * attempt)
    ran = len(failures) + (1 if result else 0)
    return {
        "step": "compact", "table": fq, "before": before, "after": file_stats(spark, fq), "result": result,
        "attempts": ran, "failed_attempts": len(failures), "conflict_rate": len(failures) / ran if ran else None,
        "failures": failures,
    }  # fmt: skip


def rewrite_manifests(spark: SparkSession, fq: str) -> dict[str, Any]:
    row = spark.sql(f"CALL lake.system.rewrite_manifests('{short(fq)}')").first()
    return {"step": "rewrite_manifests", "table": fq, "result": row.asDict() if row else {}}


def expire(spark: SparkSession, fq: str, retain_hours: float, retain_last: int = 5) -> dict[str, Any]:
    before = file_stats(spark, fq)
    older = datetime.now(timezone.utc) - timedelta(hours=retain_hours)
    row = spark.sql(
        f"CALL lake.system.expire_snapshots(table => '{short(fq)}', older_than => TIMESTAMP '{ts_literal(older)}', "
        f"retain_last => {retain_last})"
    ).first()
    return {
        "step": "expire_snapshots", "table": fq, "retain_hours": retain_hours, "retain_last": retain_last,
        "before": before, "after": file_stats(spark, fq), "result": row.asDict() if row else {},
    }  # fmt: skip


def remove_orphans(spark: SparkSession, fq: str, older_than_hours: float = ORPHAN_OLDER_THAN_HOURS) -> dict[str, Any]:
    # S3FileIO has no Hadoop FileSystem behind it: the default listing fails with "No FileSystem for scheme s3",
    # so the procedure must list by prefix through the FileIO.
    older = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    rows = spark.sql(
        f"CALL lake.system.remove_orphan_files(table => '{short(fq)}', older_than => TIMESTAMP '{ts_literal(older)}', "
        "prefix_listing => true)"
    ).collect()
    return {
        "step": "remove_orphan_files",
        "table": fq,
        "older_than_hours": older_than_hours,
        "orphans_removed": len(rows),
    }


def retain_hours_for(fq: str) -> float:
    return BRONZE_RETAIN_HOURS if ".bronze." in fq else SILVER_RETAIN_HOURS


def all_tables(spark: SparkSession) -> list[str]:
    tables = []
    for ns in ("silver", "bronze"):
        tables += [f"lake.{ns}.{r['tableName']}" for r in spark.sql(f"SHOW TABLES IN lake.{ns}").collect()]
    return sorted(tables)
