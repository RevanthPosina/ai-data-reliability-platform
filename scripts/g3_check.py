"""G3 done-condition check: Iceberg bronze and silver.

Precondition (the Makefile target `g3-check` arranges it): fresh volumes, core + apps up with
ADRP_FAULT_HOOKS=1, the Debezium connector running, nothing generated yet.

  P1 baseline    silver equals the generator's ground truth (no loss, no duplicates, no resurrection)
  P2 kill        SIGKILL the streaming container at three points inside a batch while changes flow;
                 silver still equals ground truth; bronze has no duplicate Kafka records and no gaps
  P3 rebuild     rebuild silver from bronze at batch-boundary LSNs; checksum equals the silver snapshot
  P4 maintenance Dagster job: file count falls, content checksum is unchanged; a run under concurrent writes
  P5 add column  ALTER TABLE ADD COLUMN reaches silver with no restart
  P6 quarantine  a column rename and a type narrowing land in quarantine_<table>; the stream keeps going
  P7 lineage     the OpenLineage listener wrote events
Writes a run manifest (kind g3-check).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import g2_check as g2
import pyarrow.parquet as pq
from _env import ROOT
from generate import run_once
from manifest import write_manifest

COMPOSE = g2.COMPOSE
TABLES = g2.TABLES
SEED = g2.SEED
AD_HOC = g2.AD_HOC
EVENT_LOG = ROOT / "runs/data/eventlog"
SPARK_IMAGE_REF = "adrp/spark:4.1.3-iceberg1.12.0"
RESULTS: list[dict[str, Any]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


def sh(*cmd: str, timeout: float = 600, check_rc: bool = True) -> str:
    r = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", timeout=timeout, check=False)
    if check_rc and r.returncode != 0:
        raise SystemExit(f"{' '.join(cmd[:6])}... failed ({r.returncode}):\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    return r.stdout


def compose(*args: str, timeout: float = 600, check_rc: bool = True) -> str:
    return sh(*COMPOSE, *args, timeout=timeout, check_rc=check_rc)


# ---- Spark probes -----------------------------------------------------------------------------


def probe(*args: str, timeout: float = 900) -> tuple[list[tuple[str, str]], Any]:
    out = compose(
        "--profile", "tools", "run", "--rm", "spark-query", "/opt/app/adrp/spark/probe.py", *args, timeout=timeout
    )
    rows, result = [], None
    for line in out.splitlines():
        if line.startswith("ROW\t"):
            _, table, js = line.split("\t", 2)
            rows.append((table, js))
        elif line.startswith("RESULT "):
            result = json.loads(line[len("RESULT ") :])
    return rows, result


def silver_rows(tables: list[str]) -> dict[str, list[dict[str, Any]]]:
    rows, _ = probe("dump", "--tables", ",".join(tables))
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table, js in rows:
        out[table].append(json.loads(js, parse_float=Decimal))
    return out


# ---- comparison with ground truth ----------------------------------------------------------------


def values_equal(expected: Any, actual: Any) -> bool:
    if isinstance(expected, datetime):
        return isinstance(actual, str) and datetime.fromisoformat(actual).astimezone(
            timezone.utc
        ) == expected.astimezone(timezone.utc)
    if isinstance(expected, date):
        return isinstance(actual, str) and date.fromisoformat(actual) == expected
    if isinstance(expected, Decimal):
        return actual is not None and Decimal(str(actual)) == expected
    return bool(expected == actual)


def compare_to_ground_truth(rows: dict[str, list[dict[str, Any]]], end_txn: int) -> dict[str, Any]:
    ops = g2.generator_ops(0, end_txn)
    final = g2.replay_state(ops)
    deleted: dict[str, set[int]] = defaultdict(set)
    for op in ops:
        if op.op == "D":
            deleted[op.table].add(op.pk)
    res = {"missing": 0, "differing": 0, "extra_live": 0, "resurrected": 0, "duplicates": 0, "tombstone_pii": 0}
    for t in TABLES:
        by_id: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in rows.get(t, []):
            if r["id"] < AD_HOC:
                by_id[r["id"]].append(r)
        res["duplicates"] += sum(1 for v in by_id.values() if len(v) > 1)
        for pk, expected in final[t].items():
            got = by_id.get(pk)
            if not got or got[0]["is_deleted"]:
                res["missing"] += 1
            elif any(not values_equal(v, got[0].get(c)) for c, v in expected.items()):
                res["differing"] += 1
        for pk, got in by_id.items():
            live = not got[0]["is_deleted"]
            if live and pk not in final[t]:
                res["resurrected" if pk in deleted[t] else "extra_live"] += 1
            if not live and any(
                v is not None for c, v in got[0].items() if c not in ("id", "lsn", "is_deleted", "op", "source_ts")
            ):
                res["tombstone_pii"] += 1
    return res


def gt_ok(res: dict[str, Any]) -> bool:
    return all(v == 0 for v in res.values())


# ---- waiting for the pipeline to drain ----------------------------------------------------------------


def read_event_log(columns: list[str]) -> list[dict[str, Any]]:
    """Rows of the silver batch event log. Reads finished part files one by one: Spark may be mid-write."""
    rows: list[dict[str, Any]] = []
    for f in sorted((EVENT_LOG / "silver_batches").glob("part-*.parquet")):
        try:
            rows += pq.read_table(f, columns=columns).to_pylist()
        except Exception:  # noqa: BLE001 - file still being written or renamed; the next poll sees it
            continue
    return rows


class Drain:
    """Knows the highest LSN in each topic and the highest LSN the silver event log has carried."""

    def __init__(self, c: Any) -> None:
        self.c = c
        self.offsets: dict[str, dict[int, int]] = {t: {} for t in TABLES}
        self.topic_lsn: dict[str, int] = dict.fromkeys(TABLES, 0)

    def refresh_topics(self) -> None:
        for t in TABLES:
            topic = f"cdc.public.{t}"
            end = g2.end_offsets(self.c, topic)
            for r in g2.read_range(self.c, topic, self.offsets[t], end):
                if not r["tombstone"] and r["lsn"]:
                    self.topic_lsn[t] = max(self.topic_lsn[t], r["lsn"])
            self.offsets[t] = end

    @staticmethod
    def event_log_lsn() -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for r in read_event_log(["table", "max_lsn"]):
            if r["max_lsn"]:
                out[r["table"]] = max(out[r["table"]], r["max_lsn"])
        return out

    def silver_caught_up(self) -> bool:
        self.refresh_topics()
        ev = self.event_log_lsn()
        return all(ev.get(t, 0) >= self.topic_lsn[t] for t in TABLES if self.topic_lsn[t])

    def wait_silver(self, timeout: float = 300) -> bool:
        return bool(g2.wait_for(self.silver_caught_up, timeout, 3.0))


def bronze_report(c: Any) -> dict[str, Any]:
    """Bronze vs Kafka: per topic, rows == distinct (partition, offset) == records in the topic."""
    _, stats = probe("bronze-stats")
    rep: dict[str, Any] = {}
    for t in TABLES:
        topic = f"cdc.public.{t}"
        total = sum(g2.end_offsets(c, topic).values())
        s = stats.get(topic, {"rows": 0, "distinct_offsets": 0})
        rep[t] = {"kafka_records": total, "bronze_rows": s["rows"], "bronze_distinct_offsets": s["distinct_offsets"]}
    return rep


def bronze_exact(rep: dict[str, Any]) -> bool:
    return all(v["bronze_rows"] == v["bronze_distinct_offsets"] == v["kafka_records"] for v in rep.values())


# ---- phases ------------------------------------------------------------------------------------------


def phase_baseline(c: Any, drain: Drain) -> int:
    print("P1 baseline")
    s = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=1500)
    end = s["txn_range"][1]
    ok = drain.wait_silver()
    check("baseline: silver caught up to the end of every topic", ok)
    res = compare_to_ground_truth(silver_rows(TABLES), end)
    check("baseline: silver equals ground truth (no loss, no duplicates, no resurrection)", gt_ok(res), json.dumps(res))
    return end


def log_text() -> str:
    r = subprocess.run(
        ["docker", "logs", "adrp-spark-streams-1"], capture_output=True, encoding="utf-8", errors="replace", check=False
    )
    return r.stdout + r.stderr


def clear_fault_file() -> None:
    sh("docker", "run", "--rm", "--pull", "never", "--entrypoint", "rm", "-v", "adrp_spark-checkpoints:/checkpoints",
       SPARK_IMAGE_REF, "-f", "/checkpoints/fault")  # fmt: skip


def phase_kill(c: Any, drain: Drain, start_txn: int) -> tuple[int, dict[str, Any]]:
    print("P2 kill and restart under load")
    gen = subprocess.Popen(
        [*COMPOSE, "run", "--rm", "generator", "adrp.gen.runner", "--seed", str(SEED), "--duration", "1e9",
         "--rate", "60", "--max-txns", "2500"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", errors="replace",
    )  # fmt: skip
    kills = []
    for point in ("before_merge", "mid_merge", "after_merge"):
        before = log_text().count(f"FAULT_HOOK point={point}")
        compose("exec", "-T", "spark-streams", "sh", "-c", f"echo {point} > /checkpoints/fault")
        hit = g2.wait_for(lambda p=point, b=before: log_text().count(f"FAULT_HOOK point={p}") > b, 150, 1.0)
        if not hit:
            check(f"kill: driver reached fault point {point}", False)
            continue
        sh("docker", "kill", "adrp-spark-streams-1")
        clear_fault_file()
        compose("--profile", "core", "--profile", "apps", "up", "-d", "spark-streams")
        kills.append(point)
        time.sleep(20)  # let it resume before the next hook
    check("kill: SIGKILLed the streaming container inside a batch at before_merge, mid_merge and after_merge",
          kills == ["before_merge", "mid_merge", "after_merge"], f"kills={kills}")  # fmt: skip
    summary_line = ""
    for line in (gen.communicate(timeout=900)[0] or "").splitlines():
        if line.startswith("SUMMARY "):
            summary_line = line
    end = json.loads(summary_line[len("SUMMARY ") :])["txn_range"][1] if summary_line else start_txn
    check(
        "kill: the generator finished its run while the container was being killed",
        bool(summary_line),
        f"txns to {end}",
    )
    ok = drain.wait_silver(420)
    check("kill: silver caught up after the restarts", ok)
    res = compare_to_ground_truth(silver_rows(TABLES), end)
    check("kill: silver equals ground truth (no loss, no duplicates, no resurrection)", gt_ok(res), json.dumps(res))
    rep = g2.wait_for(lambda: (r := bronze_report(c)) and bronze_exact(r) and r, 300, 5.0) or bronze_report(c)
    check("kill: bronze has no duplicate Kafka records and no gaps (rows == distinct offsets == topic records)",
          bronze_exact(rep), json.dumps({t: v["bronze_rows"] for t, v in rep.items()}))  # fmt: skip
    return end, {"kills": kills, "bronze": rep}


def phase_rebuild() -> dict[str, Any]:
    print("P3 rebuild from bronze")
    _, res = probe("rebuild-check", "--tables", ",".join(TABLES), "--pick", "3")
    n = bad = 0
    for t, info in res.items():
        for c in info["checks"]:
            n += 1
            if not (c["equal"] and c["snapshot_max_lsn"] == c["target_lsn"]):
                bad += 1
                print(f"        {t}: {c}")
    check("rebuild: rebuilt silver at each batch-boundary LSN equals the silver snapshot (count + checksum)",
          n > 0 and bad == 0, f"{n} boundaries over {len(res)} tables, {bad} differ")  # fmt: skip
    return res


def read_maintenance_log(since: int) -> list[dict[str, Any]]:
    p = EVENT_LOG / "maintenance/runs.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines()[since:]]


def run_dagster_maintenance(config: dict[str, Any]) -> None:
    cfg = json.dumps({"ops": {"run_maintenance": {"config": config}}})
    compose("exec", "-T", "dagster", "sh", "-c", f"cat > /tmp/maint.json <<'EOF'\n{cfg}\nEOF")
    compose("exec", "-T", "dagster", "dagster", "job", "execute", "-m", "adrp_orch.definitions", "-j", "iceberg_maintenance",
            "-c", "/tmp/maint.json", timeout=1800)  # fmt: skip


def phase_maintenance(c: Any, drain: Drain, end_txn: int) -> dict[str, Any]:
    print("P4 maintenance (Dagster job)")
    tables = [f"lake.silver.{t}" for t in TABLES] + ["lake.bronze.cdc_events"]
    _, before = probe("files", "--tables", ",".join(tables))
    _, sums_before = probe("checksum", "--tables", ",".join(TABLES))
    log_n = len(read_maintenance_log(0))
    run_dagster_maintenance({"tables": tables, "steps": ["compact", "rewrite_manifests", "expire_snapshots", "remove_orphan_files"],
                             "retain_hours": 0, "retain_last": 3})  # fmt: skip
    entries = read_maintenance_log(log_n)
    _, after = probe("files", "--tables", ",".join(tables))
    _, sums_after = probe("checksum", "--tables", ",".join(TABLES))

    def nfiles(d: dict[str, Any]) -> int:
        return sum(v["data_files"] + v["delete_files"] for v in d.values())

    check("maintenance: compaction + expiry reduce the total data+delete file count",
          nfiles(after) < nfiles(before), f"{nfiles(before)} -> {nfiles(after)} files")  # fmt: skip
    worse = [
        t
        for t in before
        if after[t]["data_files"] + after[t]["delete_files"] > before[t]["data_files"] + before[t]["delete_files"]
    ]
    check("maintenance: no table ends with more files than it started with", not worse, f"worse={worse}")
    check(
        "maintenance: table content is unchanged by maintenance (count + checksum per silver table)",
        sums_before == sums_after,
    )
    steps = {(e["step"], e["table"]) for e in entries}
    check("maintenance: every step ran on every table",
          all((s, t) in steps for t in tables for s in ("compact", "rewrite_manifests", "expire_snapshots", "remove_orphan_files")),
          f"{len(entries)} results")  # fmt: skip
    quiet_rate = [e["conflict_rate"] for e in entries if e["step"] == "compact"]

    # a compaction run while the generator is writing: record the conflict rate, then verify nothing was lost
    gen = subprocess.Popen(
        [*COMPOSE, "run", "--rm", "generator", "adrp.gen.runner", "--seed", str(SEED), "--duration", "1e9",
         "--rate", "40", "--max-txns", "1200"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", errors="replace",
    )  # fmt: skip
    time.sleep(15)
    log_n = len(read_maintenance_log(0))
    run_dagster_maintenance({"tables": tables, "steps": ["compact"]})
    loaded = [e for e in read_maintenance_log(log_n) if e["step"] == "compact"]
    out = gen.communicate(timeout=900)[0] or ""
    line = next((ln for ln in out.splitlines() if ln.startswith("SUMMARY ")), "")
    end = json.loads(line[len("SUMMARY ") :])["txn_range"][1] if line else end_txn
    attempts = sum(e["attempts"] for e in loaded)
    failed = sum(e["failed_attempts"] for e in loaded)
    check("maintenance: compaction under concurrent writes completed for every table", len(loaded) == len(tables),
          f"compaction attempts={attempts} failed={failed} conflict_rate={failed / attempts if attempts else None}")  # fmt: skip
    drain.wait_silver(420)
    res = compare_to_ground_truth(silver_rows(TABLES), end)
    check(
        "maintenance: silver still equals ground truth after compaction under concurrent writes",
        gt_ok(res),
        json.dumps(res),
    )
    return {
        "files_before": before, "files_after": after, "txn_end": end,
        "conflict_rate_quiet": quiet_rate,
        "under_load": {"attempts": attempts, "failed_attempts": failed},
    }  # fmt: skip


def container_state() -> dict[str, Any]:
    out = json.loads(sh("docker", "inspect", "adrp-spark-streams-1"))[0]
    return {
        "started_at": out["State"]["StartedAt"],
        "restart_count": out["RestartCount"],
        "running": out["State"]["Running"],
    }


def phase_add_column(drain: Drain) -> None:
    print("P5 add column")
    st0 = container_state()
    ids = [int(x) for x in g2.psql("SELECT id FROM help_articles WHERE id < 1000000000000 ORDER BY id LIMIT 3").split()]
    g2.psql("ALTER TABLE help_articles ADD COLUMN locale text")
    g2.psql(f"UPDATE help_articles SET locale = 'en' WHERE id IN ({','.join(map(str, ids))})")

    def landed() -> bool:
        rows = silver_rows(["help_articles"]).get("help_articles", [])
        got = {r["id"]: r.get("locale") for r in rows if r["id"] in ids}
        return len(got) == len(ids) and all(v == "en" for v in got.values())

    ok = g2.wait_for(landed, 240, 5.0)
    st1 = container_state()
    check("add column: the new column appears in silver and carries the values", bool(ok))
    check("add column: no restart was needed (same container start time, restart count unchanged)",
          st0["started_at"] == st1["started_at"] and st0["restart_count"] == st1["restart_count"], json.dumps(st1))  # fmt: skip
    _, cols = probe("columns", "--tables", "help_articles")
    check("add column: silver schema now lists `locale`", any(c.startswith("locale:") for c in cols["help_articles"]))
    _ = drain


def set_subject_compat(subject: str, level: str | None) -> None:
    import urllib.request

    req = urllib.request.Request(
        f"http://localhost:18081/config/{subject}",
        data=json.dumps({"compatibility": level}).encode() if level else None,
        method="PUT" if level else "DELETE",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    urllib.request.urlopen(req, timeout=15).read()


def phase_quarantine(drain: Drain) -> dict[str, Any]:
    print("P6 quarantine")
    subjects = ["cdc.public.help_articles-value", "cdc.public.refunds-value"]
    for s in subjects:
        set_subject_compat(s, "NONE")  # we test silver's handling, not the registry's gate
    aid = int(g2.psql("SELECT id FROM help_articles WHERE id < 1000000000000 ORDER BY id DESC LIMIT 1"))
    try:
        g2.psql("ALTER TABLE help_articles RENAME COLUMN locale TO locale_renamed")
        g2.psql(f"UPDATE help_articles SET view_count = view_count + 7 WHERE id = {aid}")
        g2.psql("ALTER TABLE refunds ALTER COLUMN amount TYPE numeric(8,2)")
        g2.psql("INSERT INTO refunds (transaction_id, amount, reason) SELECT t.id, 1.00, 'g3-narrowing' FROM transactions t "
                "WHERE t.id < 1000000000000 AND NOT EXISTS (SELECT 1 FROM refunds r WHERE r.transaction_id = t.id) LIMIT 1")  # fmt: skip

        def parked() -> dict[str, Any] | None:
            _, q = probe("quarantine")
            return q if q.get("quarantine_help_articles") and q.get("quarantine_refunds") else None

        q = g2.wait_for(parked, 300, 5.0)
        check("quarantine: a column rename (drop + add) lands in quarantine_help_articles with SCHEMA_INCOMPATIBLE",
              bool(q) and any("SCHEMA_INCOMPATIBLE" in r["reason"] and "rename" in r["reason"] for r in q["quarantine_help_articles"]),
              json.dumps((q or {}).get("quarantine_help_articles"))[:200])  # fmt: skip
        check("quarantine: a type narrowing lands in quarantine_refunds with SCHEMA_INCOMPATIBLE",
              bool(q) and any("SCHEMA_INCOMPATIBLE" in r["reason"] and "amount" in r["reason"] for r in q["quarantine_refunds"]),
              json.dumps((q or {}).get("quarantine_refunds"))[:200])  # fmt: skip
    finally:
        g2.psql("ALTER TABLE help_articles RENAME COLUMN locale_renamed TO locale")
        g2.psql("ALTER TABLE refunds ALTER COLUMN amount TYPE numeric(12,2)")
        for s in subjects:
            set_subject_compat(s, None)
    # the stream was not blocked: a later update applies, and silver catches up with Postgres
    g2.psql(f"UPDATE help_articles SET view_count = view_count + 1 WHERE id = {aid}")
    want = int(g2.psql(f"SELECT view_count FROM help_articles WHERE id = {aid}"))

    def recovered() -> bool:
        rows = silver_rows(["help_articles"]).get("help_articles", [])
        return any(r["id"] == aid and r["view_count"] == want and not r["is_deleted"] for r in rows)

    ok = g2.wait_for(recovered, 240, 5.0)
    check(
        "quarantine: the stream was not blocked; a later update to the same row applies and silver matches Postgres",
        bool(ok),
    )
    _ = drain
    return {"quarantined_article": True}


def phase_lineage() -> dict[str, Any]:
    print("P7 OpenLineage")
    p = EVENT_LOG / "openlineage/events.ndjson"
    events = []
    if p.exists():
        for line in p.read_text().splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    complete = [e for e in events if e.get("eventType") == "COMPLETE"]
    # Iceberg datasets are named by their warehouse path; the table name is in the `symlinks` facet.
    tables = {
        ident["name"]
        for e in complete
        for d in e.get("outputs", [])
        for ident in d.get("facets", {}).get("symlinks", {}).get("identifiers", [])
    }
    inputs = {d["name"] for e in complete for d in e.get("inputs", [])}
    check(
        "lineage: the OpenLineage Spark listener wrote COMPLETE events",
        len(complete) > 0,
        f"events={len(events)} complete={len(complete)}",
    )
    wanted = {"bronze.cdc_events", *(f"silver.{t}" for t in TABLES)}
    check("lineage: events name every bronze/silver Iceberg table as an output (symlinks facet)", wanted <= tables,
          f"missing={sorted(wanted - tables)} kafka inputs={sorted(i for i in inputs if i.startswith('cdc.'))[:3]}")  # fmt: skip
    outputs = tables
    return {"events": len(events), "complete": len(complete), "outputs_sample": sorted(outputs)[:10]}


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    c = g2.consumer()
    drain = Drain(c)
    details: dict[str, Any] = {}
    end = phase_baseline(c, drain)
    end, details["kill"] = phase_kill(c, drain, end)
    details["rebuild"] = phase_rebuild()
    details["maintenance"] = phase_maintenance(c, drain, end)
    phase_add_column(drain)
    details["quarantine"] = phase_quarantine(drain)
    details["lineage"] = phase_lineage()
    c.close()
    passed = all(r["ok"] for r in RESULTS)
    path = write_manifest(
        "g3-check", "core", SEED, {"started_at": started, "passed": passed, "checks": RESULTS, **details}
    )
    n_ok = sum(r["ok"] for r in RESULTS)
    print(f"{'PASSED' if passed else 'FAILED'} ({n_ok}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
