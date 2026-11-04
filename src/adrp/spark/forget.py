"""Physical removal of a forgotten subject from the Iceberg tables (G8, milestone A). One JVM per call; output lines
start with a tag, like probe.py. Run it the way the G3 check runs probe.py (spark-query container, tools profile).

A footprint is a JSON object {"public.support_tickets": ["{\\"id\\":1000000000123}", ...], ...}: the bronze
`source_table` and canonical `pk` strings of every row of the subject. Commands that need one read it from
`--footprint JSON` (an argument is limited to ~128 KB) or `--footprint-file PATH` (a path inside the container: mount
it with `compose run -v`). stdin does not reach the script under spark-submit.

footprint --customers 1,2 [--customers-file F]   ROW <source_table> <pk json>   one per footprint entry (raw ids!)
                                                 RESULT {counts: {source_table: n}, total}
presence [--footprint ..] [--canaries a,b] [--customers 1,2] [--tables ..]
                                                 RESULT {table: {footprint_rows, tombstones?, canary_rows, ...}}
delete --footprint ..  [--tables ..]             RESULT {table: {before, after, attempts, failed_attempts, ...}}
                                                 DELETE ... WHERE footprint on bronze + every quarantine_* table
rewrite --footprint .. [--tables ..]             RESULT {table: {rewritten_data_files_count, ..., before, after}}
refs [--list-only] [--tables ..]                 RESULT {table: {refs: [...], dropped: [...]}}
expire [--no-orphans] [--orphan-grace-s N]       RESULT {table: {expire1, rewrite_manifests, expire2, orphans_removed,
        [--tables ..]                                    before, after}}
state [--footprint ..] [--tables ..]             RESULT {table: {snapshots, current_snapshot_id, delete_files,
                                                         delete_files_affected, data_files, non_main_refs}}
purge-tombstones --footprint .. [--tables ..]    RESULT {table: {before, after, ...}}  hard delete of the subject's
                                                 silver tombstones (is_deleted); run rewrite + expire afterwards

`--tables` is a comma list of fully qualified names, or the words bronze / quarantine / silver. Default: bronze +
every quarantine_* + every silver table (the footprint selects the rows; tables without footprint rows are skipped).
No RESULT carries a customer id, a pk or a canary: failure messages are scrubbed of long digit runs.
"""

from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from adrp.spark import maintenance, silver
from adrp.spark.forget_sql import (
    BRONZE_TABLE,
    CUSTOMER_FK_TABLES,
    Footprint,
    events_where,
    footprint_size,
    json_customer_where,
    key_tuples,
    parse_footprint,
    pk_for_id,
    silver_where,
    sql_str,
)
from adrp.spark.probe import file_stats, out

REWRITE_OPTIONS = {
    "delete-file-threshold": "1",  # rewrite every file that has even one delete file applied
    "min-input-files": "1",  # ... even if it is the only file of its group
    "remove-dangling-deletes": "true",  # and drop delete files that no data file needs any more
    "partial-progress.enabled": "true",
}
LONG_DIGITS = re.compile(r"\d{9,}")
CANARY_OK = re.compile(r"^[A-Za-z0-9_-]+$")


def scrub(message: str) -> str:
    """Error text can quote the SQL it ran (pks, ids): cut long digit runs and keep only the first line."""
    first = message.splitlines()[0] if message else ""
    return LONG_DIGITS.sub("<n>", first)[:200]


# ---- table discovery ---------------------------------------------------------------------------------


def list_tables(spark: SparkSession, namespace: str) -> list[str]:
    try:
        return [f"lake.{namespace}.{r['tableName']}" for r in spark.sql(f"SHOW TABLES IN lake.{namespace}").collect()]
    except Exception:  # noqa: BLE001 - the namespace may not exist yet
        return []


def is_quarantine(fq: str) -> bool:
    return fq.split(".")[-1].startswith("quarantine_")


def is_event_table(fq: str) -> bool:
    return fq == BRONZE_TABLE or is_quarantine(fq)


def event_tables(spark: SparkSession) -> list[str]:
    """Bronze first, then every quarantine_* table in the silver and bronze namespaces."""
    bronze = [t for t in list_tables(spark, "bronze") if t == BRONZE_TABLE]
    quarantines = [t for ns in ("silver", "bronze") for t in list_tables(spark, ns) if is_quarantine(t)]
    return bronze + sorted(quarantines)


def silver_tables(spark: SparkSession) -> list[str]:
    return sorted(t for t in list_tables(spark, "silver") if not is_quarantine(t))


def resolve_tables(spark: SparkSession, spec: str) -> list[str]:
    if not spec:
        return event_tables(spark) + silver_tables(spark)
    tables: list[str] = []
    for tok in (t.strip() for t in spec.split(",") if t.strip()):
        if tok == "bronze":
            tables += [t for t in event_tables(spark) if t == BRONZE_TABLE]
        elif tok == "quarantine":
            tables += [t for t in event_tables(spark) if is_quarantine(t)]
        elif tok == "silver":
            tables += silver_tables(spark)
        else:
            tables.append(tok if tok.startswith("lake.") else f"lake.silver.{tok}")
    return list(dict.fromkeys(tables))


def table_where(fq: str, fp: Footprint) -> str | None:
    if is_event_table(fq):
        return events_where(fp)
    return silver_where(fp, fq.split(".")[-1])


def count_where(spark: SparkSession, fq: str, where: str) -> int:
    return int(spark.sql(f"SELECT count(*) AS n FROM {fq} WHERE {where}").first()["n"])


# ---- footprint ---------------------------------------------------------------------------------------


def compute_footprint(spark: SparkSession, customers: list[int]) -> Footprint:
    """Customer pks, plus the pk of every ticket / account / transaction whose before or after image (bronze,
    quarantine) or live silver row names one of the customers."""
    fp: dict[str, set[str]] = {"public.customers": {pk_for_id(c) for c in customers}}
    wanted = ", ".join(sql_str(f"public.{t}") for t in CUSTOMER_FK_TABLES)
    pred = json_customer_where(customers)
    for fq in event_tables(spark):
        rows = spark.sql(
            f"SELECT DISTINCT source_table, pk FROM {fq} WHERE source_table IN ({wanted}) AND pk IS NOT NULL AND {pred}"
        ).collect()
        for r in rows:
            fp.setdefault(r["source_table"], set()).add(r["pk"])
    ints = ", ".join(str(int(c)) for c in sorted(set(customers)))
    for fq in silver_tables(spark):
        name = fq.split(".")[-1]
        cols = spark.table(fq).columns
        if name in CUSTOMER_FK_TABLES and "customer_id" in cols and "id" in cols:
            for r in spark.sql(f"SELECT DISTINCT id FROM {fq} WHERE customer_id IN ({ints})").collect():
                fp.setdefault(f"public.{name}", set()).add(pk_for_id(r["id"]))
    return {t: sorted(p) for t, p in sorted(fp.items())}


def cmd_footprint(spark: SparkSession, customers: list[int]) -> None:
    fp = compute_footprint(spark, customers)
    for table, pks in fp.items():
        for pk in pks:
            print(f"ROW\t{table}\t{pk}", flush=True)
    out("RESULT", {"counts": {t: len(p) for t, p in fp.items()}, "total": footprint_size(fp)})


# ---- presence ----------------------------------------------------------------------------------------


def canary_expr_events(canary: str) -> str:
    c = sql_str(canary)
    return (
        f"instr(coalesce(before, ''), {c}) > 0 OR instr(coalesce(after, ''), {c}) > 0 "
        f"OR instr(coalesce(decode(value_bytes, 'ISO-8859-1'), ''), {c}) > 0"
    )


def presence_table(
    spark: SparkSession, fq: str, fp: Footprint, canaries: list[str], customers: list[int]
) -> dict[str, Any]:
    res: dict[str, Any] = {"rows": spark.table(fq).count()}
    where = table_where(fq, fp) if fp else None
    if where:
        res["footprint_rows"] = count_where(spark, fq, where)
        if not is_event_table(fq):
            res["footprint_tombstones"] = count_where(spark, fq, f"({where}) AND is_deleted")
    if canaries:
        if is_event_table(fq):
            expr = " OR ".join(f"({canary_expr_events(c)})" for c in canaries)
        else:
            expr = " OR ".join(f"instr(to_json(struct(*)), {sql_str(c)}) > 0" for c in canaries)
        res["canary_rows"] = count_where(spark, fq, expr)
    if customers and not is_event_table(fq) and "customer_id" in spark.table(fq).columns:
        ints = ", ".join(str(int(c)) for c in sorted(set(customers)))
        res["customer_id_rows"] = count_where(spark, fq, f"customer_id IN ({ints})")
    return res


def cmd_presence(
    spark: SparkSession, tables: list[str], fp: Footprint, canaries: list[str], customers: list[int]
) -> None:
    out("RESULT", {fq: presence_table(spark, fq, fp, canaries, customers) for fq in tables})


# ---- delete ------------------------------------------------------------------------------------------


def delete_where(spark: SparkSession, fq: str, where: str, attempts: int = 6) -> dict[str, Any]:
    """DELETE with retry: a concurrent append (the stream) can fail the commit validation."""
    before = count_where(spark, fq, where)
    failures: list[str] = []
    done = False
    for attempt in range(1, attempts + 1):
        try:
            spark.sql(f"DELETE FROM {fq} WHERE {where}")
            done = True
            break
        except Exception as e:  # noqa: BLE001 - validation conflicts surface as several Java exceptions
            failures.append(type(e).__name__ + ": " + scrub(str(e)))
            time.sleep(2 * attempt)
    return {
        "table": fq, "before": before, "after": count_where(spark, fq, where), "deleted": done,
        "attempts": len(failures) + (1 if done else 0), "failed_attempts": len(failures), "failures": failures,
    }  # fmt: skip


def cmd_delete(spark: SparkSession, tables: list[str], fp: Footprint, attempts: int) -> None:
    res: dict[str, Any] = {}
    for fq in tables:
        if not is_event_table(fq):
            res[fq] = {"skipped": "not a bronze/quarantine table (silver rows are tombstoned, not deleted)"}
            continue
        where = events_where(fp)
        res[fq] = delete_where(spark, fq, where, attempts) if where else {"skipped": "empty footprint"}
    out("RESULT", res)


# ---- rewrite -----------------------------------------------------------------------------------------


def rewrite_table(spark: SparkSession, fq: str, where: str, attempts: int = 3) -> dict[str, Any]:
    """rewrite_data_files restricted to `where`, rewriting every file that has a delete file applied."""
    res = maintenance.compact(spark, fq, attempts=attempts, options=REWRITE_OPTIONS, where=where)
    res["failures"] = [scrub(f) for f in res["failures"]]
    return res


def cmd_rewrite(spark: SparkSession, tables: list[str], fp: Footprint, attempts: int) -> None:
    res: dict[str, Any] = {}
    for fq in tables:
        where = table_where(fq, fp)
        res[fq] = rewrite_table(spark, fq, where, attempts) if where else {"skipped": "no footprint rows"}
    out("RESULT", res)


# ---- refs --------------------------------------------------------------------------------------------


def list_refs(spark: SparkSession, fq: str) -> list[dict[str, Any]]:
    rows = spark.sql(f"SELECT name, type, snapshot_id FROM {fq}.refs").collect()
    return [{"name": r["name"], "type": r["type"], "snapshot_id": r["snapshot_id"]} for r in rows]


def drop_non_main_refs(spark: SparkSession, fq: str) -> list[dict[str, Any]]:
    """Branches and tags other than main pin old snapshots (and their data files); drop them all."""
    dropped = []
    for ref in list_refs(spark, fq):
        if ref["name"] == "main":
            continue
        kind = "BRANCH" if str(ref["type"]).upper() == "BRANCH" else "TAG"
        spark.sql(f"ALTER TABLE {fq} DROP {kind} `{ref['name']}`")
        dropped.append(ref)
    return dropped


def cmd_refs(spark: SparkSession, tables: list[str], list_only: bool) -> None:
    res: dict[str, Any] = {}
    for fq in tables:
        refs = list_refs(spark, fq)
        entry: dict[str, Any] = {"refs": refs, "non_main": [r for r in refs if r["name"] != "main"]}
        if not list_only:
            entry["dropped"] = drop_non_main_refs(spark, fq)
            entry["refs_after"] = list_refs(spark, fq)
        res[fq] = entry
    out("RESULT", res)


# ---- expire ------------------------------------------------------------------------------------------


def session_ts_literal(spark: SparkSession, instant: datetime) -> str:
    """TIMESTAMP literals are read in the session time zone: render the instant there (UTC elsewhere)."""
    try:
        tz = ZoneInfo(spark.conf.get("spark.sql.session.timeZone"))
    except Exception:  # noqa: BLE001 - unknown zone: fall back to the UTC literal maintenance uses
        return maintenance.ts_literal(instant)
    return instant.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def expire_now(spark: SparkSession, fq: str) -> dict[str, Any]:
    """expire_snapshots older_than=now, retain_last=1. The literal is computed here (the CALL cannot do arithmetic),
    rounded up a second so a snapshot committed in the current second is expired too."""
    literal = session_ts_literal(spark, datetime.now(timezone.utc) + timedelta(seconds=1))
    row = spark.sql(
        f"CALL lake.system.expire_snapshots(table => '{maintenance.short(fq)}', "
        f"older_than => TIMESTAMP '{literal}', retain_last => 1)"
    ).first()
    return row.asDict() if row else {}


def remove_orphans_now(spark: SparkSession, fq: str, grace_s: float = 0) -> int:
    """Orphan removal through the Action API: the SQL procedure refuses older_than < 24 h. Lists by prefix because
    S3FileIO has no Hadoop FileSystem behind it. Returns the number of files removed."""
    jvm = spark._jvm
    jspark = spark._jsparkSession
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jspark, fq)
    cutoff_ms = int((time.time() - grace_s) * 1000)
    result = (
        jvm.org.apache.iceberg.spark.actions.SparkActions.get(jspark)
        .deleteOrphanFiles(table)
        .olderThan(cutoff_ms)
        .usePrefixListing(True)
        .execute()
    )
    it = result.orphanFileLocations().iterator()
    n = 0
    while it.hasNext():
        it.next()
        n += 1
    return n


def expire_table(spark: SparkSession, fq: str, orphans: bool = True, grace_s: float = 0) -> dict[str, Any]:
    """expire(retain_last=1) -> rewrite_manifests -> expire again -> orphan removal.

    The manifest rewrite and second expiry are not optional: after a data-file rewrite, the surviving manifest
    still holds a DELETED entry whose lower/upper bounds carry the removed values."""
    before = file_stats(spark, fq)
    expire1 = expire_now(spark, fq)
    manifests = maintenance.rewrite_manifests(spark, fq)["result"]
    expire2 = expire_now(spark, fq)
    removed = remove_orphans_now(spark, fq, grace_s) if orphans else None
    return {
        "table": fq, "before": before, "after": file_stats(spark, fq), "expire1": expire1,
        "rewrite_manifests": manifests, "expire2": expire2, "orphans_removed": removed,
    }  # fmt: skip


def cmd_expire(spark: SparkSession, tables: list[str], orphans: bool, grace_s: float) -> None:
    out("RESULT", {fq: expire_table(spark, fq, orphans, grace_s) for fq in tables})


# ---- state -------------------------------------------------------------------------------------------


def current_snapshot_id(spark: SparkSession, fq: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {fq}.refs WHERE name = 'main'").first()
    return int(row["snapshot_id"]) if row and row["snapshot_id"] is not None else None


def affected_buckets(spark: SparkSession, fq: str, fp: Footprint) -> tuple[set[str], set[int]]:
    """(source_tables, bucket numbers) the footprint touches in `fq`, for matching delete-file partitions."""
    if is_event_table(fq):
        values: list[tuple[Any]] = [(p,) for pks in fp.values() for p in pks]
        tables = set(fp)
        ddl = "k string"
    else:
        name = fq.split(".")[-1]
        values = [(int(next(iter(k.values()))),) for k in key_tuples(fp, name)]
        tables = set()
        ddl = "k bigint"
    if not values:
        return tables, set()
    spark.createDataFrame(values, ddl).createOrReplaceTempView("forget_state_keys")
    rows = spark.sql(f"SELECT DISTINCT lake.system.bucket({silver.BUCKETS}, k) AS b FROM forget_state_keys").collect()
    return tables, {int(r["b"]) for r in rows}


def delete_files_affected(spark: SparkSession, fq: str, fp: Footprint) -> int | None:
    """Delete files whose partition the footprint touches (None: the table has no footprint rows or the partition
    layout is unknown). An unpartitioned table's delete files all apply."""
    if table_where(fq, fp) is None:
        return None
    df = spark.table(f"{fq}.delete_files")
    pcols = df.select("partition.*").columns if "partition" in df.columns else []
    if not pcols:
        return df.count()
    tables, buckets = affected_buckets(spark, fq, fp)
    cond = F.lit(True)
    for c in pcols:
        if c == "source_table":
            cond = cond & F.col("partition.source_table").isin(sorted(tables))
        elif c.endswith("_bucket"):
            cond = cond & F.col(f"partition.{c}").isin(sorted(buckets))
    return df.where(cond).count()


def state_table(spark: SparkSession, fq: str, fp: Footprint) -> dict[str, Any]:
    stats = file_stats(spark, fq)
    return {
        "snapshots": stats["snapshots"], "current_snapshot_id": current_snapshot_id(spark, fq),
        "delete_files": stats["delete_files"], "delete_files_affected": delete_files_affected(spark, fq, fp),
        "data_files": stats["data_files"],
        "non_main_refs": [r["name"] for r in list_refs(spark, fq) if r["name"] != "main"],
    }  # fmt: skip


def cmd_state(spark: SparkSession, tables: list[str], fp: Footprint) -> None:
    out("RESULT", {fq: state_table(spark, fq, fp) for fq in tables})


# ---- silver tombstone purge --------------------------------------------------------------------------


def purge_tombstones_table(spark: SparkSession, fq: str, where: str, attempts: int = 6) -> dict[str, Any]:
    """Hard delete the footprint's tombstones (is_deleted) of one silver table. Live rows are never touched."""
    res = delete_where(spark, fq, f"({where}) AND is_deleted", attempts)
    res["live_rows_with_footprint_keys"] = count_where(spark, fq, f"({where}) AND NOT is_deleted")
    return res


def cmd_purge_tombstones(spark: SparkSession, tables: list[str], fp: Footprint, attempts: int) -> None:
    res: dict[str, Any] = {}
    for fq in tables:
        where = None if is_event_table(fq) else table_where(fq, fp)
        res[fq] = purge_tombstones_table(spark, fq, where, attempts) if where else {"skipped": "no footprint rows"}
    out("RESULT", res)


# ---- cli ---------------------------------------------------------------------------------------------


def read_ints(inline: str, path: str) -> list[int]:
    text = inline
    if path:
        text = open(path).read() + "," + inline
    return sorted({int(x) for x in re.split(r"[,\s]+", text) if x})


def read_footprint(inline: str, path: str) -> Footprint:
    if inline:
        return parse_footprint(inline)
    if path:
        with open(path) as fh:
            return parse_footprint(fh.read())
    return {}


COMMANDS = ["footprint", "presence", "delete", "rewrite", "refs", "expire", "state", "purge-tombstones"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=COMMANDS)
    ap.add_argument("--tables", default="")
    ap.add_argument("--customers", default="")
    ap.add_argument("--customers-file", default="")
    ap.add_argument("--footprint", default="")
    ap.add_argument("--footprint-file", default="")
    ap.add_argument("--canaries", default="")
    ap.add_argument("--attempts", type=int, default=6)
    ap.add_argument("--list-only", action="store_true")
    ap.add_argument("--no-orphans", action="store_true")
    ap.add_argument("--orphan-grace-s", type=float, default=0)
    a = ap.parse_args()
    canaries = [c for c in a.canaries.split(",") if c]
    bad = [c for c in canaries if not CANARY_OK.match(c)]
    if bad:
        ap.error("canaries may only contain letters, digits, '-' and '_'")
    customers = read_ints(a.customers, a.customers_file)
    fp = read_footprint(a.footprint, a.footprint_file)
    if a.cmd in ("delete", "rewrite", "purge-tombstones") and not fp:
        ap.error(f"{a.cmd} needs a non-empty --footprint / --footprint-file")
    if a.cmd == "footprint" and not customers:
        ap.error("footprint needs --customers / --customers-file")

    spark = build_session_for_cli()
    tables = resolve_tables(spark, a.tables)
    if a.cmd == "footprint":
        cmd_footprint(spark, customers)
    elif a.cmd == "presence":
        cmd_presence(spark, tables, fp, canaries, customers)
    elif a.cmd == "delete":
        cmd_delete(spark, [t for t in tables if is_event_table(t)], fp, a.attempts)
    elif a.cmd == "rewrite":
        cmd_rewrite(spark, tables, fp, min(a.attempts, 3))
    elif a.cmd == "refs":
        cmd_refs(spark, tables, a.list_only)
    elif a.cmd == "expire":
        cmd_expire(spark, tables, not a.no_orphans, a.orphan_grace_s)
    elif a.cmd == "state":
        cmd_state(spark, tables, fp)
    else:
        cmd_purge_tombstones(spark, [t for t in tables if not is_event_table(t)], fp, a.attempts)


def build_session_for_cli() -> SparkSession:
    from adrp.spark.session import build_session

    spark = build_session("adrp-forget")
    spark.sparkContext.setLogLevel("ERROR")
    return spark


if __name__ == "__main__":
    main()
