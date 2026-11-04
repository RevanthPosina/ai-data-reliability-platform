"""Dagster code location (runs in the Spark image so jobs can use Iceberg procedures).

Jobs:
  skeleton_health      Qdrant point count (G0)
  iceberg_maintenance  compaction, manifest rewrite, snapshot expiry, orphan removal (G3)
  forget_logical       enqueue the logical phase of deletion requests (G8); config: request_ids only
  forget_physical      enqueue the physical phase of deletion requests (G8); config: request_ids only
NOTE: no `from __future__ import annotations` here: Dagster inspects the real annotation objects.
Streaming services are observed as assets and checks in later goals. Rule (inventory row 21): never log raw
entity or customer ids, only counts.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import OpExecutionContext


@dg.op
def qdrant_point_count(context: OpExecutionContext) -> int:
    from qdrant_client import QdrantClient

    client = QdrantClient(url=os.environ["QDRANT_URL"])
    if not client.collection_exists("text_chunks"):
        context.log.info("collection text_chunks not created yet")
        return 0
    count = client.count("text_chunks", exact=True).count
    context.log.info(f"text_chunks points={count}")
    return count


@dg.job
def skeleton_health() -> None:
    qdrant_point_count()


class MaintenanceConfig(dg.Config):  # type: ignore[misc]  # dagster is only installed in the Spark image
    tables: list[str] = []  # noqa: RUF012  # fully qualified (lake.silver.customers); empty = every silver and bronze table
    steps: list[str] = ["compact", "rewrite_manifests", "expire_snapshots", "remove_orphan_files"]  # noqa: RUF012
    retain_hours: float | None = None  # None = per-layer default (bronze longer than silver)
    retain_last: int = 5
    orphan_older_than_hours: float | None = None


@dg.op
def run_maintenance(context: OpExecutionContext, config: MaintenanceConfig) -> list[dict[str, Any]]:
    from adrp.spark import maintenance
    from adrp.spark.session import build_session

    spark = build_session("adrp-maintenance")
    spark.sparkContext.setLogLevel("ERROR")
    tables = config.tables or maintenance.all_tables(spark)
    results: list[dict[str, Any]] = []
    for fq in tables:
        for step in config.steps:
            if step == "compact":
                r = maintenance.compact(spark, fq)
            elif step == "rewrite_manifests":
                r = maintenance.rewrite_manifests(spark, fq)
            elif step == "expire_snapshots":
                hours = config.retain_hours if config.retain_hours is not None else maintenance.retain_hours_for(fq)
                r = maintenance.expire(spark, fq, hours, config.retain_last)
            elif step == "remove_orphan_files":
                hours = config.orphan_older_than_hours or maintenance.ORPHAN_OLDER_THAN_HOURS
                r = maintenance.remove_orphans(spark, fq, hours)
            else:
                raise dg.Failure(f"unknown maintenance step {step!r}")
            results.append(r)
            context.log.info(f"{step} {fq}")
    out_dir = Path(os.environ.get("EVENT_LOG_DIR", "/eventlog")) / "maintenance"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).isoformat()
    with open(out_dir / "runs.jsonl", "a") as fh:
        for r in results:
            fh.write(json.dumps({"run_at": stamp, **r}, default=str) + "\n")
    print("MAINTENANCE_RESULT " + json.dumps(results, default=str), flush=True)
    return results


@dg.job
def iceberg_maintenance() -> None:
    run_maintenance()


daily_maintenance = dg.ScheduleDefinition(
    job=iceberg_maintenance, cron_schedule="0 3 * * *", default_status=dg.DefaultScheduleStatus.STOPPED
)


class ForgetConfig(dg.Config):  # type: ignore[misc]
    request_ids: list[str]  # uuids of privacy.forget_requests rows; anything else is refused (never a customer id)


def _enqueue_forget(context: OpExecutionContext, config: ForgetConfig, phase: str) -> int:
    """HONEST LIMIT: this container has no Postgres client, no docker access and no pyarrow, so it cannot run the
    forget workflow. These jobs only enqueue: they write a queue file into the shared event-log mount, and the host
    executor (`scripts/forget.py run`) reads it and runs adrp.forget.workflow. Dagster therefore records that a
    phase was requested and by which request ids, never what was forgotten, and nothing here touches data."""
    from adrp.forget import queue

    ids = queue.clean_ids(config.request_ids)
    path = queue.write_entry(Path(os.environ.get("EVENT_LOG_DIR", "/eventlog")) / queue.QUEUE_SUBDIR, phase, ids)
    context.log.info(f"queued {phase} phase for {len(ids)} request(s): {path.name}")
    return len(ids)


@dg.op
def enqueue_forget_logical(context: OpExecutionContext, config: ForgetConfig) -> int:
    return _enqueue_forget(context, config, "logical")


@dg.op
def enqueue_forget_physical(context: OpExecutionContext, config: ForgetConfig) -> int:
    return _enqueue_forget(context, config, "physical")


@dg.job
def forget_logical() -> None:
    enqueue_forget_logical()


@dg.job
def forget_physical() -> None:
    enqueue_forget_physical()


defs = dg.Definitions(
    jobs=[skeleton_health, iceberg_maintenance, forget_logical, forget_physical], schedules=[daily_maintenance]
)
