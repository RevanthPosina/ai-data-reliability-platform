"""Fail if docs/design.md's version table disagrees with versions.env (matrix drift check)."""

from __future__ import annotations

import re
import sys

from _env import ROOT, read_env

# design.md row label -> versions.env key
ROWS = {
    "Postgres": "POSTGRES_IMAGE",
    "Debezium": "DEBEZIUM_VERSION",
    "Confluent Avro converter": "CONFLUENT_CONVERTER_VERSION",
    "Redpanda": "REDPANDA_IMAGE",
    "Spark": "SPARK_VERSION",
    "Iceberg": "ICEBERG_VERSION",
    "Iceberg format-version": "ICEBERG_FORMAT_VERSION",
    "Lakekeeper": "LAKEKEEPER_IMAGE",
    "Object store (SeaweedFS)": "SEAWEEDFS_IMAGE",
    "Qdrant": "QDRANT_IMAGE",
    "Trino": "TRINO_IMAGE",
    "dbt-core": "DBT_CORE_VERSION",
    "dbt-trino": "DBT_TRINO_VERSION",
    "Dagster": "DAGSTER_VERSION",
    "OpenLineage": "OPENLINEAGE_VERSION",
    "Marquez": "MARQUEZ_IMAGE",
    "pyiceberg": "PYICEBERG_VERSION",
    "Embedding model": "EMBED_MODEL",
}


def main() -> int:
    env = read_env(ROOT / "versions.env")
    text = (ROOT / "docs/design.md").read_text()
    block = re.search(r"<!-- versions:begin -->(.*?)<!-- versions:end -->", text, re.S)
    if not block:
        print("design.md: versions block missing")
        return 1
    table = dict(re.findall(r"^\| ([^|]+?) \| ([^|]+?) \|$", block.group(1), re.M))
    errors = []
    for label, key in ROWS.items():
        if table.get(label) != env.get(key):
            errors.append(f"{label}: design.md={table.get(label)!r} versions.env[{key}]={env.get(key)!r}")
    # Requirements pins must match too.
    reqs = (ROOT / "docker/app/requirements.txt").read_text()
    for pkg, key in {
        "dagster": "DAGSTER_VERSION",
        "pyiceberg[pyarrow]": "PYICEBERG_VERSION",
        "qdrant-client": "QDRANT_CLIENT_VERSION",
        "sentence-transformers": "SENTENCE_TRANSFORMERS_VERSION",
        "psycopg[binary]": "PSYCOPG_VERSION",
        "boto3": "BOTO3_VERSION",
    }.items():
        if f"{pkg}=={env[key]}" not in reqs:
            errors.append(f"docker/app/requirements.txt: {pkg} != {env[key]}")
    for e in errors:
        print("DRIFT", e)
    print(f"version matrix: {len(ROWS)} rows checked, {len(errors)} drift")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
