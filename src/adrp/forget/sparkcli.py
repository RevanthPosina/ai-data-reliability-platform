"""Host-side wrapper for the Spark forget CLIs (adrp/spark/forget.py and forget_check.py), one JVM per call.

    docker compose --env-file .run/compose.env --profile tools run --rm -T spark-query \
        /opt/app/adrp/spark/<script> <cmd> ...

A footprint travels inline as `--footprint '<json>'` (spark-submit does not forward stdin). It, and `--customers`,
are therefore visible in the process list of the host and in `docker inspect` for the life of the call: the
trade-off of the existing CLI. Nothing here prints them; failures are reported with every 4+ digit run masked.

The Spark image has no pyarrow: files.py and scan.py run on the host, never through this module.
The image must contain these scripts (`docker compose build`), as it does for probe.py.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from adrp.forget.rawscan import COMPOSE, run
from adrp.spark.forget_sql import BRONZE_TABLE, dump_footprint, pk_for_id

SPARK = [*COMPOSE, "--profile", "tools", "run", "--rm", "-T", "spark-query"]
FORGET_SCRIPT = "/opt/app/adrp/spark/forget.py"
CHECK_SCRIPT = "/opt/app/adrp/spark/forget_check.py"
_DIGITS = re.compile(r"\d{4,}")


class SparkError(RuntimeError):
    pass


def mask(text: str, limit: int = 600) -> str:
    return _DIGITS.sub("#", text)[-limit:]


def lsn_to_int(lsn: str) -> int:
    """'0/1200A208' -> int, the form bronze/silver store in their `lsn` column."""
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def parse_output(stdout: str) -> tuple[list[list[str]], dict[str, Any]]:
    """(ROW lines split on tabs without the tag, the last RESULT json)."""
    rows: list[list[str]] = []
    result: dict[str, Any] = {}
    for line in stdout.splitlines():
        if line.startswith("ROW\t"):
            rows.append(line.split("\t")[1:])
        elif line.startswith("RESULT "):
            result = json.loads(line[len("RESULT ") :])
    return rows, result


def call(script: str, cmd: str, *args: str, timeout: float = 3600) -> tuple[list[list[str]], dict[str, Any]]:
    r = run([*SPARK, script, cmd, *args], timeout=timeout)
    if r.rc != 0:
        raise SparkError(f"spark {cmd} exited {r.rc}: {mask(r.err_text)}")
    rows, result = parse_output(r.text)
    if not result:
        raise SparkError(f"spark {cmd} printed no RESULT: {mask(r.err_text)}")
    return rows, result


# ---- footprints ------------------------------------------------------------------------------------------


def to_footprint(ids_by_table: Mapping[str, Iterable[int]]) -> dict[str, list[str]]:
    """{'support_tickets': [7]} -> {'public.support_tickets': ['{"id":7}']}, the Spark CLI's footprint shape."""
    return {f"public.{t}": sorted({pk_for_id(i) for i in v}) for t, v in sorted(ids_by_table.items()) if list(v)}


def from_footprint(fp: Mapping[str, Iterable[str]]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for t, pks in fp.items():
        out[t.split(".", 1)[-1]] = sorted({int(json.loads(p)["id"]) for p in pks})
    return out


def merge_ids(*parts: Mapping[str, Iterable[int]]) -> dict[str, list[int]]:
    acc: dict[str, set[int]] = {}
    for p in parts:
        for t, v in p.items():
            acc.setdefault(t, set()).update(v)
    return {t: sorted(v) for t, v in sorted(acc.items()) if v}


def footprints(customers: Sequence[int]) -> dict[int, dict[str, list[int]]]:
    """Bronze/quarantine/silver history of each customer: {customer: {table: [pk ids]}} (raw ids, in memory)."""
    rows, _ = call(CHECK_SCRIPT, "footprints", "--customers", ",".join(str(int(c)) for c in customers))
    out: dict[int, dict[str, set[int]]] = {int(c): {} for c in customers}
    for cust, table, pk in rows:
        out[int(cust)].setdefault(table.split(".", 1)[-1], set()).add(int(json.loads(pk)["id"]))
    return {c: {t: sorted(v) for t, v in d.items()} for c, d in out.items()}


# ---- verification (forget_check.py) ----------------------------------------------------------------------


def logical(groups: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Per group label: bronze / quarantine footprint rows, bronze rows of the forget transaction, and per silver
    table live rows, tombstones, tombstones at or after the forget lsn, tombstones still holding values, rows that
    still carry the customer id. A group is {'footprint': ids_by_table, 'customers': [...], 'txid': int|None,
    'lsn': int|None}. Counts only come back."""
    spark_groups = {
        label: {
            "footprint": to_footprint(g["footprint"]),
            "customers": [int(c) for c in g.get("customers", [])],
            "txid": g.get("txid"),
            "lsn": g.get("lsn"),
        }
        for label, g in groups.items()
    }
    _, res = call(CHECK_SCRIPT, "logical", "--groups", json.dumps(spark_groups, separators=(",", ":")))
    return res


# ---- forget.py commands ----------------------------------------------------------------------------------


def _fp_args(ids_by_table: Mapping[str, Iterable[int]]) -> list[str]:
    return ["--footprint", dump_footprint(to_footprint(ids_by_table))]


def _tables_arg(tables: str | None) -> list[str]:
    return ["--tables", tables] if tables else []


def delete_events(ids_by_table: Mapping[str, Iterable[int]], attempts: int = 6) -> dict[str, Any]:
    """DELETE the footprint from bronze and every quarantine_* table (retries on commit conflicts)."""
    return call(FORGET_SCRIPT, "delete", *_fp_args(ids_by_table), "--attempts", str(attempts))[1]


def rewrite(ids_by_table: Mapping[str, Iterable[int]], tables: str | None = None) -> dict[str, Any]:
    return call(FORGET_SCRIPT, "rewrite", *_fp_args(ids_by_table), *_tables_arg(tables))[1]


def refs(tables: str | None = None, list_only: bool = False) -> dict[str, Any]:
    return call(FORGET_SCRIPT, "refs", *(["--list-only"] if list_only else []), *_tables_arg(tables))[1]


def expire(tables: str | None = None, orphans: bool = True) -> dict[str, Any]:
    return call(FORGET_SCRIPT, "expire", *([] if orphans else ["--no-orphans"]), *_tables_arg(tables))[1]


def state(ids_by_table: Mapping[str, Iterable[int]] | None = None, tables: str | None = None) -> dict[str, Any]:
    fp = _fp_args(ids_by_table) if ids_by_table else []
    return call(FORGET_SCRIPT, "state", *fp, *_tables_arg(tables))[1]


def purge_tombstones(ids_by_table: Mapping[str, Iterable[int]], attempts: int = 6) -> dict[str, Any]:
    return call(FORGET_SCRIPT, "purge-tombstones", *_fp_args(ids_by_table), "--attempts", str(attempts))[1]


def presence(
    ids_by_table: Mapping[str, Iterable[int]], canaries: Sequence[str] = (), customers: Sequence[int] = ()
) -> dict[str, Any]:
    args = _fp_args(ids_by_table) if ids_by_table else []
    if canaries:
        args += ["--canaries", ",".join(canaries)]
    if customers:
        args += ["--customers", ",".join(str(int(c)) for c in customers)]
    return call(FORGET_SCRIPT, "presence", *args)[1]


def bronze_snapshot_id() -> int | None:
    res = state(tables="bronze")
    return res.get(BRONZE_TABLE, {}).get("current_snapshot_id")


def total_residue(res: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    """Counts that must be zero after the Iceberg procedure, summed over tables, from a `state` result:
    delete files the footprint touches, non-main refs, snapshots beyond the one retained."""
    delete_files = sum(int(v.get("delete_files_affected") or 0) for v in res.values())
    refs_n = sum(len(v.get("non_main_refs", [])) for v in res.values())
    extra = sum(max(int(v.get("snapshots", 1)) - 1, 0) for v in res.values())
    return {"delete_files_affected": delete_files, "non_main_refs": refs_n, "extra_snapshots": extra}
