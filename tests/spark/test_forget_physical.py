"""Physical removal on a local merge-on-read Iceberg table (PLAN G8): after DELETE, the removed row's bytes live on in
the old data file, in snapshots, in manifests (as DELETED-entry bounds) and in refs. The forget procedure
(rewrite_data_files threshold=1 -> drop refs -> expire retain_last=1 -> rewrite_manifests -> expire -> orphans)
leaves no file containing the canary; each mutation that skips a step leaves one, and the scanner finds it.

Runs inside the Spark image (`make test-spark`; mount ./src over /opt/app/adrp to test uncommitted code). The scanner
decodes Parquet only where pyarrow exists; the Spark image has none, so there Parquet is checked on raw bytes, which
sees the canary in the unencrypted pages and in the footer statistics of these small files.
"""

import itertools
import re
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from adrp.forget import ids, scan  # noqa: E402
from adrp.spark import forget  # noqa: E402
from adrp.spark.forget_sql import events_where, silver_where  # noqa: E402

COUNTER = itertools.count()
CANARY = ids.CANARY_PREFIX + "0123456789abcdef0123456789abcdef"
WHERE = "grp = 'a'"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("forget-warehouse")


@pytest.fixture(scope="module")
def spark(warehouse: Path) -> SparkSession:
    cat = "spark.sql.catalog.lake"
    return (
        SparkSession.builder.master("local[2]")
        .appName("forget-physical")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "hadoop")
        .config(f"{cat}.warehouse", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def make_table(spark: SparkSession) -> str:
    """Merge-on-read table, partitioned by grp. Canary row id 1 is updated once (an old version stays in the first
    file) and then deleted (a position delete file); row 2 shares its file and must survive."""
    fq = f"lake.fg.t{next(COUNTER)}"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.fg")
    spark.sql(
        f"""CREATE TABLE {fq} (id BIGINT, grp STRING, note STRING) USING iceberg PARTITIONED BY (grp)
        TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                       'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"""
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, 'a', '{CANARY} first'), (2, 'a', 'keep'), (3, 'b', 'keep b')")
    spark.sql(f"INSERT INTO {fq} VALUES (4, 'a', 'more')")
    spark.sql(f"UPDATE {fq} SET note = '{CANARY} second' WHERE id = 1")
    return fq


def delete_subject(spark: SparkSession, fq: str) -> None:
    spark.sql(f"DELETE FROM {fq} WHERE id = 1")
    assert spark.sql(f"SELECT count(*) FROM {fq} WHERE id = 1").first()[0] == 0


def survivors(spark: SparkSession, fq: str) -> list[tuple[int, str]]:
    return [(r["id"], r["note"]) for r in spark.sql(f"SELECT id, note FROM {fq} ORDER BY id").collect()]


def scan_table(warehouse: Path, fq: str) -> scan.ScanResult:
    """Every file under the table's directory (the other tests' tables hold the canary on purpose)."""
    m = ids.Matcher(canaries=scan.canary_prefixes([CANARY]))
    return scan.scan(scan.LocalStore(only_this_table(warehouse, fq)), m)


def assert_found(res: scan.ScanResult) -> None:
    assert res.errors == 0
    assert res.total_hits > 0 and res.matched_files > 0


def assert_clean(res: scan.ScanResult) -> None:
    assert res.clean(allow_undecoded=True), res.to_dict()
    assert res.files > 0


def only_this_table(warehouse: Path, fq: str) -> Path:
    return warehouse / fq.split(".", 1)[1].replace(".", "/")


def test_delete_leaves_the_canary_on_disk(spark: SparkSession, warehouse: Path) -> None:
    fq = make_table(spark)
    delete_subject(spark, fq)
    # non-vacuity: logically gone, physically still there (data file, delete file, snapshots)
    assert_found(scan_table(warehouse, fq))
    state = forget.state_table(spark, fq, {})
    assert state["delete_files"] >= 1 and state["snapshots"] >= 4


def test_forget_procedure_leaves_no_canary(spark: SparkSession, warehouse: Path) -> None:
    fq = make_table(spark)
    delete_subject(spark, fq)

    rw = forget.rewrite_table(spark, fq, WHERE)
    assert rw["failed_attempts"] == 0 and rw["result"]["rewritten_data_files_count"] >= 1
    assert rw["after"]["delete_files"] == 0, "remove-dangling-deletes drops the position delete file"
    assert_found(scan_table(warehouse, fq))  # old snapshots still reference the original files

    assert forget.drop_non_main_refs(spark, fq) == []
    res = forget.expire_table(spark, fq)
    assert res["orphans_removed"] is not None
    state = forget.state_table(spark, fq, {})
    assert state["snapshots"] == 1 and state["delete_files"] == 0 and state["non_main_refs"] == []
    assert_clean(scan_table(warehouse, fq))
    assert survivors(spark, fq) == [(2, "keep"), (3, "keep b"), (4, "more")]


def test_mutation_without_rewrite_leaves_the_canary(spark: SparkSession, warehouse: Path) -> None:
    """Everything but rewrite_data_files: the position delete file still masks a data file that holds the row."""
    fq = make_table(spark)
    delete_subject(spark, fq)
    forget.expire_table(spark, fq)
    assert forget.state_table(spark, fq, {})["delete_files"] >= 1
    assert_found(scan_table(warehouse, fq))


def test_mutation_without_manifest_rewrite_leaves_bounds(spark: SparkSession, warehouse: Path) -> None:
    """rewrite -> expire -> orphans, but no rewrite_manifests + second expire: a DELETED manifest entry still
    carries the canary as its lower/upper bound (truncated to 16 characters)."""
    fq = make_table(spark)
    delete_subject(spark, fq)
    forget.rewrite_table(spark, fq, WHERE)
    forget.expire_now(spark, fq)
    forget.remove_orphans_now(spark, fq)
    res = scan_table(warehouse, fq)
    assert_found(res)
    assert res.kinds["avro"].decoded.total > 0, "found in a decoded manifest, where raw bytes are blind"
    # the real procedure finishes the job
    forget.expire_table(spark, fq)
    assert_clean(scan_table(warehouse, fq))


def test_mutation_tag_pins_the_old_snapshot(spark: SparkSession, warehouse: Path) -> None:
    fq = make_table(spark)
    spark.sql(f"ALTER TABLE {fq} CREATE TAG before_forget")
    delete_subject(spark, fq)
    forget.rewrite_table(spark, fq, WHERE)
    forget.expire_table(spark, fq)  # expiry keeps the tagged snapshot
    assert [r["name"] for r in forget.list_refs(spark, fq)].count("before_forget") == 1
    assert_found(scan_table(warehouse, fq))
    dropped = forget.drop_non_main_refs(spark, fq)
    assert [r["name"] for r in dropped] == ["before_forget"]
    forget.expire_table(spark, fq)
    assert_clean(scan_table(warehouse, fq))


def test_rewrite_where_limits_the_files_touched(spark: SparkSession, warehouse: Path) -> None:
    fq = make_table(spark)
    delete_subject(spark, fq)
    before = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {fq}.files WHERE content = 0").collect()}
    forget.rewrite_table(spark, fq, WHERE)
    after = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {fq}.files WHERE content = 0").collect()}
    b_files = {p for p in before if "grp=b" in p}
    assert b_files and b_files <= after, "the grp=b file is outside the filter and keeps its path"


def test_session_timestamp_literal_is_session_zone(spark: SparkSession) -> None:
    from datetime import datetime, timezone

    literal = forget.session_ts_literal(spark, datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc))
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", literal)
    back = spark.sql(f"SELECT unix_timestamp(TIMESTAMP '{literal}') AS t").first()["t"]
    assert back == int(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp())


def test_delete_where_and_tombstone_purge_on_events_and_silver_shaped_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.fg")
    ev = f"lake.fg.ev{next(COUNTER)}"
    spark.sql(f"CREATE TABLE {ev} (source_table STRING, pk STRING, after STRING) USING iceberg")
    spark.sql(
        f"INSERT INTO {ev} VALUES ('public.t', '{{\"id\":1}}', 'x'), ('public.t', '{{\"id\":2}}', 'y'), "
        "('public.u', '{\"id\":1}', 'z')"
    )
    fp = {"public.t": ['{"id":1}']}
    res = forget.delete_where(spark, ev, events_where(fp) or "")
    assert (res["before"], res["after"], res["deleted"]) == (1, 0, True)
    assert spark.sql(f"SELECT count(*) FROM {ev}").first()[0] == 2

    sv = "lake.fg.t"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sv} (id BIGINT, name STRING, is_deleted BOOLEAN) USING iceberg")
    spark.sql(f"DELETE FROM {sv} WHERE true")
    spark.sql(f"INSERT INTO {sv} VALUES (1, NULL, true), (2, 'live', false), (3, 'live', false)")
    where = silver_where({"public.t": ['{"id":1}', '{"id":2}']}, "t") or ""
    p = forget.purge_tombstones_table(spark, sv, where)
    assert p["before"] == 1 and p["after"] == 0 and p["live_rows_with_footprint_keys"] == 1
    assert sorted(r["id"] for r in spark.sql(f"SELECT id FROM {sv}").collect()) == [2, 3]


def test_orphan_removal_with_zero_grace_removes_a_stray_file(spark: SparkSession, warehouse: Path) -> None:
    """The SQL procedure refuses older_than < 24 h; the Action API path used by expire_table does not."""
    import time

    fq = make_table(spark)
    stray = only_this_table(warehouse, fq) / "data" / "stray-orphan.parquet"
    stray.write_bytes(b"not referenced by any snapshot")
    time.sleep(0.2)
    assert forget.remove_orphans_now(spark, fq) == 1
    assert not stray.exists()
    assert forget.remove_orphans_now(spark, fq) == 0
