"""G6 done-condition check: the Qdrant index follows the text tables.

Precondition (Makefile target `g6-check`): fresh volumes, core + apps up, nothing generated yet.

  P1 bootstrap   generate rows; the index becomes consistent (stale = orphaned = duplicates = 0)
  P2 bursts      a generator burst, then a scripted burst of text updates, non-text updates and deletes judged
                 against Postgres: converges within the freshness SLO (design S3, 120 s)
  P3 pyiceberg   pyiceberg (what the worker uses) reads silver, which has MoR delete files, exactly as Spark does;
                 silver's layout matches the plan
  P4 rerun       restart the worker, then restart it with its state deleted: the index is byte-for-byte unchanged
  P5 bronze      compaction of bronze, then expiry of the worker's pointer snapshot: the worker still converges
Writes a run manifest (kind g6-check).
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import g2_check as g2
import g3_check as g3
from _env import ROOT
from generate import run_once
from manifest import write_manifest

sys.path.insert(0, str(ROOT / "src"))
from adrp.common import chunk_hash, chunk_text

SEED = g2.SEED
SLO_S = 120  # design S3: freshness to Qdrant
ZERO = ("stale_chunks", "orphaned_chunks", "duplicate_chunks", "wrong_model_chunks", "ahead_chunks", "missing_entities")
RESULTS: list[dict[str, Any]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


def embedder(*args: str) -> str:
    return g3.compose("exec", "-T", "embedder", "python", "-m", *args, timeout=300)


def tagged(out: str, tag: str) -> Any:
    line = next(ln for ln in out.splitlines() if ln.startswith(tag + " "))
    return json.loads(line[len(tag) + 1 :])


def index_checks() -> dict[str, int]:
    return tagged(embedder("adrp.embed.checks"), "CHECKS")


def consistent(r: dict[str, int]) -> bool:
    return r["points"] > 0 and all(r[k] == 0 for k in ZERO)


def wait_consistent(timeout: float) -> tuple[bool, float, dict[str, int]]:
    t0 = time.monotonic()
    last: dict[str, int] = {}
    while time.monotonic() - t0 < timeout:
        try:
            last = index_checks()
        except SystemExit:
            last = {}
        if last and consistent(last):
            return True, time.monotonic() - t0, last
        time.sleep(3)
    return False, time.monotonic() - t0, last


def qdrant_points(eid: str) -> list[dict[str, Any]]:
    body = {"filter": {"must": [{"key": "entity_id", "match": {"value": eid}}]}, "limit": 500, "with_payload": True}
    req = urllib.request.Request(
        "http://127.0.0.1:6333/collections/text_chunks/points/scroll",
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST",
    )  # fmt: skip
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)["result"]["points"]


def pg_json(sql: str) -> list[dict[str, Any]]:
    out = g2.psql(f"SELECT row_to_json(t) FROM ({sql}) t")
    return [json.loads(ln) for ln in out.splitlines() if ln.strip()]


def actions_since(n_lines: int) -> list[dict[str, Any]]:
    p = ROOT / "runs/data/eventlog/embedder/actions.jsonl"
    return [json.loads(ln) for ln in p.read_text().splitlines()[n_lines:]] if p.exists() else []


def n_actions() -> int:
    p = ROOT / "runs/data/eventlog/embedder/actions.jsonl"
    return len(p.read_text().splitlines()) if p.exists() else 0


def points_snapshot() -> dict[str, Any]:
    return tagged(embedder("adrp.embed.checks", "points"), "POINTS")


def drain_silver(drain: g3.Drain) -> None:
    drain.wait_silver(420)


# ---- phases ---------------------------------------------------------------------------------------------


def phase_bootstrap(drain: g3.Drain) -> int:
    print("P1 bootstrap")
    s = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=1500)
    drain_silver(drain)
    ok, _, r = wait_consistent(300)
    check(
        "bootstrap: the index is consistent (stale, orphaned, duplicate, wrong-model, missing all 0)", ok, json.dumps(r)
    )
    check(
        "bootstrap: both text tables are indexed",
        r.get("entities_in_index", 0) > 50,
        f"entities={r.get('entities_in_index')}",
    )
    return int(s["txn_range"][1])


def embedder_logs() -> str:
    r = g3.subprocess.run(
        ["docker", "logs", "adrp-embedder-1"], capture_output=True, encoding="utf-8", errors="replace", check=False
    )
    return r.stdout + r.stderr  # the worker logs to stderr


def vs_postgres(table: str, ids: list[int], cols: tuple[str, ...]) -> int:
    """Number of entities whose points differ from the current Postgres text (wrong chunks or more than one version)."""
    if not ids:
        return 0
    rows = pg_json(f"SELECT id, {', '.join(cols)} FROM {table} WHERE id IN ({','.join(map(str, ids))})")
    wrong = 0
    for row in rows:
        pts = qdrant_points(f"{table}:{row['id']}")
        want = sorted(chunk_hash(c) for c in chunk_text("\n\n".join(str(row[c]) for c in cols)))
        if (
            sorted(p["payload"]["chunk_hash"] for p in pts) != want
            or len({p["payload"]["source_version"] for p in pts}) > 1
        ):
            wrong += 1
    return wrong


def points_left(table: str, ids: list[int]) -> int:
    return sum(len(qdrant_points(f"{table}:{i}")) for i in ids)


def ids(xs: list[int]) -> str:
    return ",".join(map(str, xs))


def phase_burst(drain: g3.Drain) -> dict[str, Any]:
    print("P2 bursts")
    # (a) a generator burst of every kind of change; wait for silver, then for the index to agree with silver
    run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=600)
    drain_silver(drain)
    ok, _, r = wait_consistent(SLO_S + 60)
    check("burst A (generator, 600 transactions): the index agrees with silver", ok, json.dumps(r))
    # (b) a scripted burst, judged against Postgres directly so a lagging silver cannot make it look consistent
    arts = [r["id"] for r in pg_json("SELECT id FROM help_articles WHERE id < 1000000000000 ORDER BY id LIMIT 60")]
    tickets = [r["id"] for r in pg_json("SELECT id FROM support_tickets WHERE id < 1000000000000 ORDER BY id LIMIT 60")]
    assert len(arts) >= 17 and len(tickets) >= 45, f"corpus too small: {len(arts)} articles, {len(tickets)} tickets"
    text_upd, view_upd, art_del = arts[:6], arts[6:14], arts[14:17]
    tk_upd, tk_del = tickets[:30], tickets[30:45]
    n0 = n_actions()
    g2.psql(
        f"UPDATE help_articles SET body = body || E'\\n\\nAddendum from the G6 burst.' WHERE id IN ({ids(text_upd)})"
    )
    g2.psql(f"UPDATE help_articles SET view_count = view_count + 100 WHERE id IN ({ids(view_upd)})")
    g2.psql(f"UPDATE support_tickets SET body = body || E'\\n\\nG6 burst follow-up.' WHERE id IN ({ids(tk_upd)})")
    g2.psql(f"DELETE FROM support_tickets WHERE id IN ({ids(tk_del)})")
    g2.psql(f"DELETE FROM help_articles WHERE id IN ({ids(art_del)})")
    t_end = time.monotonic()

    def converged() -> bool:
        return (vs_postgres("help_articles", text_upd, ("title", "body")) == 0
                and vs_postgres("support_tickets", tk_upd, ("subject", "body")) == 0
                and points_left("help_articles", art_del) + points_left("support_tickets", tk_del) == 0)  # fmt: skip

    done = bool(g2.wait_for(converged, SLO_S + 120, 2.0))
    secs = time.monotonic() - t_end
    check(f"burst B (scripted): updated entities hold exactly their current Postgres text and deleted ones nothing, within {SLO_S} s",
          done and secs <= SLO_S, f"{secs:.0f} s after the last statement")  # fmt: skip
    ok, _, r = wait_consistent(60)
    check(
        "burst B: the index agrees with silver and has no stale, orphaned, duplicate or wrong-model chunks",
        ok,
        json.dumps(r),
    )
    check("burst B: updated articles' chunks are all at one version",
          vs_postgres("help_articles", text_upd, ("title", "body")) == 0, f"{len(text_upd)} articles, {len(tk_upd)} tickets")  # fmt: skip
    acts = actions_since(n0)
    view_set = {f"help_articles:{a}" for a in view_upd}
    reuse = [a for a in acts if a["entity"] in view_set and a["kind"] == "upsert"]
    check("burst B: non-text updates re-versioned the points without calling the model (embedded=0, reused>0)",
          {a["entity"] for a in reuse} == view_set and all(a["embedded"] == 0 and a["reused"] > 0 for a in reuse),
          f"{len(reuse)} upserts for {len(view_set)} entities")  # fmt: skip
    return {"seconds_to_converge_scripted_burst": secs, "final": r}


def phase_pyiceberg() -> dict[str, Any]:
    print("P3 pyiceberg reads silver with delete files; silver layout")
    py = tagged(embedder("adrp.embed.checks", "silver"), "SILVER")
    rows, _ = g3.probe("dump", "--tables", "help_articles,support_tickets")
    spark = {}
    for table, js in rows:
        r = json.loads(js)
        spark[f"{table}:{r['id']}"] = [r["lsn"], r["is_deleted"]]
    _, files = g3.probe("files", "--tables", "help_articles,support_tickets")
    deletes = sum(v["delete_files"] for v in files.values())
    check(
        "pyiceberg: silver has merge-on-read position-delete files (so the next check is not vacuous)",
        deletes > 0,
        f"delete files={deletes}",
    )
    check(
        "pyiceberg: pyiceberg and Spark read the same entities, versions and tombstones from silver",
        py == spark,
        f"{len(spark)} entities",
    )
    _, props = g3.probe(
        "props", "--tables", "help_articles,support_tickets,customers,accounts,transactions,refunds,merchants"
    )
    want = {"write.merge.mode": "merge-on-read", "write.update.mode": "merge-on-read", "write.delete.mode": "merge-on-read",
            "write.distribution-mode": "hash", "format-version": "2"}  # fmt: skip
    bad = {t: p for t, p in props.items() if any(p.get(k) != v for k, v in want.items())}
    check("silver layout matches the plan: format v2, merge-on-read, hash distribution", not bad, json.dumps(bad)[:200])
    return {"delete_files": deletes, "entities": len(spark), "props": props}


def stable_points() -> dict[str, Any]:
    prev = points_snapshot()
    for _ in range(10):
        time.sleep(6)
        cur = points_snapshot()
        if cur == prev:
            return cur
        prev = cur
    return prev


def phase_rerun() -> dict[str, Any]:
    print("P4 rerun is a no-op")
    before = stable_points()
    n0 = n_actions()
    g3.compose("restart", "embedder")
    time.sleep(45)
    mid = points_snapshot()
    check(
        "rerun: restarting the worker (state kept) leaves every point and payload unchanged",
        mid == before,
        f"{len(before)} points",
    )
    g3.compose("exec", "-T", "embedder", "rm", "-f", "/state/embedder.json")
    g3.compose("restart", "embedder")
    time.sleep(90)  # model load + full bronze rescan
    after = points_snapshot()
    acts = actions_since(n0)
    changed = [a for a in acts if a["kind"] == "upsert"]
    check("rerun: restarting with its state deleted (full bronze rescan) leaves every point and payload unchanged",
          after == before, f"{len(acts)} entities re-examined, {len(changed)} rewritten")  # fmt: skip
    check("rerun: the rescan embedded nothing", sum(a["embedded"] for a in acts) == 0)
    return {"points": len(before), "rescanned_entities": len(acts)}


def run_bronze_maintenance(steps: list[str], **cfg: Any) -> None:
    g3.run_dagster_maintenance({"tables": ["lake.bronze.cdc_events"], "steps": steps, **cfg})


def phase_bronze(drain: g3.Drain) -> None:
    print("P5 bronze compaction and expiry")
    run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=300)
    drain_silver(drain)
    wait_consistent(180)
    run_bronze_maintenance(["compact"])  # a `replace` snapshot: no new data
    run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=300)
    drain_silver(drain)
    ok, secs, r = wait_consistent(SLO_S + 60)
    check("bronze: after compaction (a replace snapshot) the worker still picks up new changes", ok, f"{secs:.0f} s")
    # stop the worker, let bronze move on, then expire the snapshot its pointer refers to
    g3.compose("stop", "embedder")
    run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=300)
    drain_silver(drain)
    run_bronze_maintenance(["compact", "expire_snapshots"], retain_hours=0, retain_last=1)
    g3.compose("--profile", "core", "--profile", "apps", "up", "-d", "embedder")
    ok, secs, r = wait_consistent(300)
    logs = embedder_logs()
    check("bronze: with the pointer's snapshot expired the worker falls back to a full scan and converges",
          ok and "pointer snapshot expired" in logs, f"{secs:.0f} s; {json.dumps(r)}")  # fmt: skip


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    os.environ.setdefault("ADRP_FAULT_HOOKS", "0")
    c = g2.consumer()
    drain = g3.Drain(c)
    details: dict[str, Any] = {}
    phase_bootstrap(drain)
    details["burst"] = phase_burst(drain)
    details["pyiceberg"] = phase_pyiceberg()
    details["rerun"] = phase_rerun()
    phase_bronze(drain)
    final = index_checks()
    check("final: the index is consistent", consistent(final), json.dumps(final))
    details["final"] = final
    c.close()
    passed = all(r["ok"] for r in RESULTS)
    path = write_manifest(
        "g6-check", "core", SEED, {"started_at": started, "passed": passed, "checks": RESULTS, **details}
    )
    print(
        f"{'PASSED' if passed else 'FAILED'} ({sum(r['ok'] for r in RESULTS)}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
