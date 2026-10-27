"""Spark property test (PLAN G3): the production MERGE path, on a local Iceberg table, agrees with the pure
model's oracle for random shuffled / duplicated / replayed deliveries. Runs inside the Spark image
(`make test-spark`); the host venv has no JVM.
"""

import itertools
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, given, settings

from adrp.common import canonical_json
from adrp.merge_model import Event, Row, oracle

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from test_merge_model import deliveries  # noqa: E402

from adrp.spark import silver  # noqa: E402

COLUMNS = [("id", "BIGINT", False), ("title", "STRING", True), ("amount", "DECIMAL(12,2)", True)]
COUNTER = itertools.count()


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("warehouse")
    cat = "spark.sql.catalog.lake"
    return (
        SparkSession.builder.master("local[2]")
        .appName("silver-merge-proptest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "hadoop")
        .config(f"{cat}.warehouse", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def changes_df(spark: SparkSession, batch: list[Event]):  # type: ignore[no-untyped-def]
    rows = []
    for e in batch:
        after = None
        if e.op != "d":
            assert e.values is not None
            after = canonical_json({"id": e.key, "title": f"t{e.values['v']}", "amount": f"{e.values['v']}.00"})
        rows.append((canonical_json({"id": e.key}), after, e.op, e.lsn, e.offset))
    return spark.createDataFrame(
        rows, "pk string, after string, op string, lsn bigint, kafka_offset bigint"
    ).selectExpr("pk", "after", "op", "lsn", "current_timestamp() AS source_ts", "kafka_offset")


def expected(state: dict[int, Row]) -> dict[int, tuple[int, bool, str | None, Decimal | None]]:
    out = {}
    for k, row in state.items():
        v = row.values
        out[k] = (row.lsn, row.is_deleted, f"t{v['v']}" if v else None, Decimal(f"{v['v']}.00") if v else None)
    return out


@settings(max_examples=50, deadline=None, database=None, suppress_health_check=list(HealthCheck))
@given(deliveries())
def test_spark_merge_matches_oracle(spark: SparkSession, data: tuple[list[Event], list[list[Event]]]) -> None:
    history, batches = data
    batches = [*batches[:5], [e for b in batches[5:] for e in b]] if len(batches) > 6 else batches
    name = f"t{next(COUNTER)}"
    table = silver.create_silver(spark, name, ["id"], COLUMNS)
    for b in batches:
        if b:
            silver.merge_changes(spark, table, changes_df(spark, b))
    got = {r["id"]: (r["lsn"], r["is_deleted"], r["title"], r["amount"]) for r in spark.table(table.name).collect()}
    assert got == expected(oracle(history))
    n = spark.table(table.name).count()
    assert n == len(got), "one row per key: no duplicates"
