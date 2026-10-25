"""Dagster code location. G0: one health job over the thin slice.

G3+ add maintenance jobs, streaming-service liveness sensors and LSN-lag asset checks.
Rule (inventory row 21): never log raw entity or customer ids, only counts.
"""

from __future__ import annotations

import os

import dagster as dg
from qdrant_client import QdrantClient


@dg.op
def qdrant_point_count(context: dg.OpExecutionContext) -> int:
    client = QdrantClient(url=os.environ["QDRANT_URL"])
    if not client.collection_exists("help_articles"):
        context.log.info("collection help_articles not created yet")
        return 0
    count = client.count("help_articles", exact=True).count
    context.log.info(f"help_articles points={count}")
    return count


@dg.job
def skeleton_health() -> None:
    qdrant_point_count()


defs = dg.Definitions(jobs=[skeleton_health])
