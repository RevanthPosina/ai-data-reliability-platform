"""Iceberg access for the embedding pipeline (pyiceberg only: no torch, so CLI checks start fast)."""

from __future__ import annotations

import os
from typing import Any

from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import In

from adrp.embed import logic

LOOKUP_BATCH = 500


def catalog() -> Any:
    env = os.environ
    return load_catalog(
        "lake",
        type="rest",
        uri=env["CATALOG_URI"],
        warehouse=env["WAREHOUSE"],
        **{
            "s3.endpoint": env["S3_ENDPOINT"],
            "s3.access-key-id": env["S3_ACCESS_KEY"],
            "s3.secret-access-key": env["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
            "s3.region": env.get("AWS_REGION", "us-east-1"),
        },
    )


def silver_lookup(cat: Any, table: str, ids: list[int]) -> dict[str, dict[str, Any]]:
    """Current silver rows for ids (pyiceberg applies the table's position deletes)."""
    t = cat.load_table(f"silver.{table}")
    cols = ("id", "lsn", "is_deleted", *logic.TEXT_TABLES[table])
    out: dict[str, dict[str, Any]] = {}
    for i in range(0, len(ids), LOOKUP_BATCH):
        for r in t.scan(row_filter=In("id", ids[i : i + LOOKUP_BATCH]), selected_fields=cols).to_arrow().to_pylist():
            out[logic.entity_id(table, int(r["id"]))] = r
    return out
