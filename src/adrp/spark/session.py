"""SparkSession wired to the Lakekeeper REST catalog and SeaweedFS (ADR-0005, ADR-0006)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_session(app_name: str) -> SparkSession:
    env = os.environ
    cat = "spark.sql.catalog.lake"
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "rest")
        .config(f"{cat}.uri", env["CATALOG_URI"])
        .config(f"{cat}.warehouse", env["WAREHOUSE"])
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.s3.endpoint", env["S3_ENDPOINT"])
        .config(f"{cat}.s3.path-style-access", "true")
        .config(f"{cat}.s3.access-key-id", env["S3_ACCESS_KEY"])
        .config(f"{cat}.s3.secret-access-key", env["S3_SECRET_KEY"])
        .config(f"{cat}.client.region", env.get("AWS_REGION", "us-east-1"))
        # Readers must see new snapshots without restarting.
        .config(f"{cat}.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
