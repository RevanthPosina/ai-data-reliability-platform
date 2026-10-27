"""G2 done-condition check: CDC ingestion into Redpanda.

Precondition (the Makefile target `g2-check` arranges it): fresh volumes, infra up, NO connector yet.
Phases:
  P1 snapshot   generate rows, create the connector, verify every row arrives as an op='r' event
  P2 live       generate more changes, verify per-key op sequences, tombstones and final images
  P3 delete     explicit DELETE -> d event with a full before-image, then a tombstone
  P4 toast      update a non-text column of a ~10 KB article -> body intact in before and after
  P5 slot lag   exporter metrics; pause the connector -> lag grows, connector_up=0; resume -> recovers
Writes a run manifest (kind g2-check).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from _env import ROOT, all_env
from bootstrap import ensure_connector
from confluent_kafka import Consumer, KafkaException, TopicPartition
from generate import run_once
from manifest import write_manifest

os.environ.setdefault("SCHEMA_REGISTRY_URL", "http://localhost:18081")
sys.path.insert(0, str(ROOT / "src"))
from adrp.gen.model import Generator, stream_digest
from adrp.spark.decode import decode

COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".run/compose.env")]
TABLES = ["customers", "merchants", "accounts", "transactions", "refunds", "support_tickets", "help_articles"]
AD_HOC = 1_000_000_000_000  # ids >= this are not generator-owned (seed articles, smoke inserts)
SEED = 42
RESULTS: list[dict[str, Any]] = []
EPOCH = date(1970, 1, 1)


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


def psql(sql: str) -> str:
    r = subprocess.run(
        [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "shop", "-At", "-c", sql],
        capture_output=True, encoding="utf-8", errors="replace", check=False, timeout=120,
    )  # fmt: skip
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr}")
    return r.stdout.strip()


def wait_for(fn: Any, timeout: float, every: float = 2.0) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = fn()
        if got:
            return got
        time.sleep(every)
    return None


# ---- topic access ---------------------------------------------------------------------------


def consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": f"g2-check-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def end_offsets(c: Consumer, topic: str, attempts: int = 10) -> dict[int, int]:
    """Per-partition end offsets. Retries transient broker errors (e.g. leader election on a new topic)."""
    for attempt in range(attempts):
        try:
            meta = c.list_topics(topic, timeout=15)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                return {}
            return {
                p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1]
                for p in meta.topics[topic].partitions
            }
        except KafkaException:
            if attempt == attempts - 1:
                raise
            time.sleep(1.0)
    return {}


def read_range(c: Consumer, topic: str, start: dict[int, int], end: dict[int, int]) -> list[dict[str, Any]]:
    """All records in [start, end) per partition, in (partition, offset) order, decoded."""
    out: list[dict[str, Any]] = []
    for part, stop in sorted(end.items()):
        begin = start.get(part, 0)
        if stop <= begin:
            continue
        c.assign([TopicPartition(topic, part, begin)])
        while True:
            m = c.poll(5.0)
            if m is None:
                raise SystemExit(f"timed out reading {topic}[{part}] at {begin}..{stop}")
            if m.error():
                raise SystemExit(f"consume error: {m.error()}")
            offset = m.offset()
            assert offset is not None
            out.append(decode_record(topic, part, offset, m.key(), m.value()))
            if offset >= stop - 1:
                break
        c.unassign()
    return out


def decode_record(topic: str, part: int, offset: int, key: bytes | None, value: bytes | None) -> dict[str, Any]:
    pk = decode(key)[1] if key is not None else None
    if value is None:
        return {"topic": topic, "partition": part, "offset": offset, "pk": pk, "tombstone": True}
    schema_id, env = decode(value)
    return {
        "topic": topic, "partition": part, "offset": offset, "pk": pk, "tombstone": False,
        "schema_id": schema_id, "op": env["op"], "lsn": env["source"].get("lsn"),
        "txid": env["source"].get("txId"), "before": env.get("before"), "after": env.get("after"),
    }  # fmt: skip


def total_end(c: Consumer, topic: str) -> int:
    return sum(end_offsets(c, topic).values())


def read_all(c: Consumer, topic: str) -> list[dict[str, Any]]:
    end = end_offsets(c, topic)
    return read_range(c, topic, {}, end)


# ---- expected state from the (deterministic) generator ---------------------------------------


def generator_ops(start: int, count: int) -> list[Any]:
    gen = Generator(SEED)
    gen.skip(start)
    return [op for txn in gen.txns(count) for op in txn.ops]


def replay_state(ops: list[Any]) -> dict[str, dict[int, dict[str, Any]]]:
    state: dict[str, dict[int, dict[str, Any]]] = {t: {} for t in TABLES}
    for op in ops:
        if op.op == "D":
            state[op.table].pop(op.pk, None)
        else:
            state[op.table][op.pk] = op.row
    return state


def norm_expected(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc)
    if isinstance(v, date):
        return (v - EPOCH).days
    return v


def norm_actual(expected: Any, v: Any) -> Any:
    if isinstance(expected, datetime) and isinstance(v, str):
        return datetime.fromisoformat(v).astimezone(timezone.utc)
    if isinstance(v, (bytes, bytearray)):
        return v
    return v


def rows_equal(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    bad = []
    for col, ev in expected.items():
        av = actual.get(col)
        ev_n = norm_expected(ev)
        av_n = norm_actual(ev, av)
        if isinstance(ev_n, Decimal) and av_n is not None:
            av_n = Decimal(av_n)
        if ev_n != av_n:
            bad.append(col)
    return bad


def pk_of(rec: dict[str, Any]) -> int:
    return int(rec["pk"]["id"])


# ---- phases ---------------------------------------------------------------------------------


def phase_snapshot(c: Consumer, txns: int) -> tuple[dict[str, Any], int]:
    print("P1 snapshot")
    s1 = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=txns)
    start, end = s1["txn_range"]
    dry = stream_digest(SEED, start, end - start)
    check("generator digest on the host equals the in-container digest", dry[0] == s1["stream_sha256"],
          f"txns [{start},{end}) ops={dry[1]}")  # fmt: skip
    counts = {t: int(psql(f"SELECT count(*) FROM {t}")) for t in TABLES}
    ensure_connector(all_env())
    ok = wait_for(lambda: all(total_end(c, f"cdc.public.{t}") >= counts[t] for t in TABLES), 240)
    check("snapshot: every table's topic holds at least its row count", bool(ok), json.dumps(counts))
    expected = replay_state(generator_ops(0, end))
    snap_lsns: set[Any] = set()
    mism = 0
    missing = 0
    extra = 0
    non_r = 0
    for t in TABLES:
        recs = [r for r in read_all(c, f"cdc.public.{t}") if not r["tombstone"]]
        non_r += sum(1 for r in recs if r["op"] != "r")
        snap_lsns |= {r["lsn"] for r in recs}
        by_pk = {pk_of(r): r for r in recs if pk_of(r) < AD_HOC}
        missing += len(expected[t].keys() - by_pk.keys())
        extra += len(by_pk.keys() - expected[t].keys())
        for pk, row in expected[t].items():
            if pk in by_pk and rows_equal(row, by_pk[pk]["after"]):
                mism += 1
    check("snapshot: all events are op='r'", non_r == 0, f"non-r events={non_r}")
    check("snapshot: no missing, extra or differing rows vs ground truth", missing == extra == mism == 0,
          f"missing={missing} extra={extra} differing={mism}")  # fmt: skip
    check("snapshot events carry a source LSN (needed for LSN-conditional upsert)", None not in snap_lsns,
          f"distinct snapshot LSNs={len(snap_lsns)}")  # fmt: skip
    return {"counts_at_snapshot": counts, "snapshot_lsns": len(snap_lsns), "txn_range": [start, end]}, end


def phase_live(c: Consumer, start_txn: int, txns: int) -> int:
    print("P2 live changes")
    before_end = {t: sum(end_offsets(c, f"cdc.public.{t}").values()) for t in TABLES}
    s2 = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=txns)
    a, b = s2["txn_range"]
    ops = generator_ops(a, b - a)
    expected_events: dict[str, int] = defaultdict(int)
    for op in ops:
        expected_events[op.table] += 1 + (1 if op.op == "D" else 0)  # delete = d event + tombstone
    ok = wait_for(
        lambda: all(total_end(c, f"cdc.public.{t}") >= before_end[t] + expected_events[t] for t in TABLES), 240
    )
    check("live: every logical change produced its event(s)", bool(ok), json.dumps(dict(expected_events)))
    final = replay_state(generator_ops(0, b))
    gt_ops: dict[tuple[str, int], list[str]] = defaultdict(list)
    for op in ops:
        gt_ops[(op.table, op.pk)].append({"I": "c", "U": "u", "D": "d"}[op.op])
        if op.op == "D":
            gt_ops[(op.table, op.pk)].append("T")
    seq_bad = dup = img_bad = tomb_bad = 0
    for t in TABLES:
        per_key: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in read_all(c, f"cdc.public.{t}"):
            if r["pk"] is not None and pk_of(r) < AD_HOC:
                per_key[pk_of(r)].append(r)
        for pk, recs in per_key.items():
            recs.sort(key=lambda r: (r["partition"], r["offset"]))
            seq = ["T" if r["tombstone"] else r["op"] for r in recs]
            if seq and seq[0] == "r":
                seq = seq[1:]
            if seq != gt_ops.get((t, pk), []):
                seq_bad += 1
            if len({(r["partition"], r["offset"]) for r in recs}) != len(recs):
                dup += 1
            body = [r for r in recs if not r["tombstone"]]
            last = body[-1] if body else None
            if pk in final[t]:
                if last is None or last["op"] == "d" or rows_equal(final[t][pk], last["after"]):
                    img_bad += 1
            elif last is not None and last["op"] != "d":
                img_bad += 1
            if last is not None and last["op"] == "d" and not (recs[-1]["tombstone"]):
                tomb_bad += 1
    check("live: per-key op sequences equal ground truth (c/u/d, then tombstone after d)", seq_bad == 0,
          f"keys with a different sequence={seq_bad}")  # fmt: skip
    check("live: no duplicate records", dup == 0, f"keys with duplicates={dup}")
    check("live: last image of every key equals the final ground-truth row (deleted keys end in d)", img_bad == 0,
          f"mismatching keys={img_bad}")  # fmt: skip
    check("live: every delete is followed by a tombstone", tomb_bad == 0, f"missing tombstones={tomb_bad}")
    return b


def one_new_record(c: Consumer, topic: str, before: dict[int, int], timeout: float = 60) -> list[dict[str, Any]]:
    wait_for(lambda: sum(end_offsets(c, topic).values()) > sum(before.values()), timeout, 1.0)
    time.sleep(1.5)  # let a delete's tombstone land too
    return read_range(c, topic, before, end_offsets(c, topic))


def phase_delete(c: Consumer) -> dict[str, Any]:
    print("P3 delete")
    topic = "cdc.public.support_tickets"
    tid = int(psql("SELECT id FROM support_tickets WHERE id < 1000000000000 ORDER BY id DESC LIMIT 1"))
    row = psql(f"SELECT subject || '|' || body FROM support_tickets WHERE id = {tid}")
    before = end_offsets(c, topic)
    psql(f"DELETE FROM support_tickets WHERE id = {tid}")
    recs = [r for r in one_new_record(c, topic, before) if r["pk"] and pk_of(r) == tid]
    ops = ["T" if r["tombstone"] else r["op"] for r in recs]
    check("delete: Postgres DELETE produces a d event followed by a tombstone", ops == ["d", "T"], f"events={ops}")
    d = next((r for r in recs if not r["tombstone"]), None)
    full = bool(d and d["before"] and f"{d['before']['subject']}|{d['before']['body']}" == row and d["after"] is None)
    check("delete: the d event carries the full before-image (REPLICA IDENTITY FULL), after is null", full)
    return {"ticket_deleted": True}


def phase_toast(c: Consumer) -> dict[str, Any]:
    print("P4 TOAST")
    topic = "cdc.public.help_articles"
    aid = int(psql("SELECT id FROM help_articles WHERE id < 1000000000000 AND length(body) > 8000 ORDER BY id LIMIT 1"))
    body_len = int(psql(f"SELECT length(body) FROM help_articles WHERE id = {aid}"))
    body_md5 = psql(f"SELECT md5(body) FROM help_articles WHERE id = {aid}")
    before = end_offsets(c, topic)
    psql(f"UPDATE help_articles SET view_count = view_count + 1, updated_at = now() WHERE id = {aid}")
    recs = [r for r in one_new_record(c, topic, before) if r["pk"] and pk_of(r) == aid and not r["tombstone"]]
    r = recs[-1] if recs else None
    import hashlib

    def md5(s: str) -> str:
        return hashlib.md5(s.encode()).hexdigest()

    ok_after = bool(r and r["after"]["body"] != "__debezium_unavailable_value" and md5(r["after"]["body"]) == body_md5)
    ok_before = bool(r and r["before"] and md5(r["before"]["body"]) == body_md5)
    check("toast: update of a non-text column emits an 'u' event", bool(r and r["op"] == "u"), f"body chars={body_len}")
    check("toast: after-image body is intact (equals Postgres md5), not the unavailable placeholder", ok_after)
    check("toast: before-image body is intact", ok_before)
    return {"article_body_chars": body_len}


def metrics() -> dict[str, float]:
    with urllib.request.urlopen("http://localhost:9108/metrics", timeout=10) as resp:
        text = resp.read().decode()
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            k, v = line.rsplit(" ", 1)
            out[k] = float(v)
    return out


def connect(method: str, path: str) -> None:
    req = urllib.request.Request(f"http://localhost:8083{path}", method=method)
    urllib.request.urlopen(req, timeout=15).read()


def phase_slot_lag() -> dict[str, Any]:
    print("P5 slot lag and connector status")
    subprocess.run([*COMPOSE, "--profile", "apps", "up", "-d", "exporter"], check=True, capture_output=True)
    m = wait_for(lambda: _try(metrics), 60)
    slot = 'adrp_replication_slot_lag_bytes{slot="dbz_shop"}'
    up = 'adrp_connector_up{connector="pg-shop"}'
    retained = 'adrp_replication_slot_retained_wal_bytes{slot="dbz_shop"}'
    check(
        "metrics: slot lag, retained WAL and connector status are exported",
        bool(m) and slot in m and up in m and retained in m,
    )
    steady = wait_for(lambda: (mm := _try(metrics)) and mm[up] == 1.0 and mm, 60) or {}
    check("metrics: connector_up=1 and slot active while healthy", steady.get(up) == 1.0
          and steady.get('adrp_replication_slot_active{slot="dbz_shop"}') == 1.0)  # fmt: skip
    base_lag = steady.get(slot, -1.0)

    connect("PUT", "/connectors/pg-shop/pause")
    wait_for(lambda: (mm := _try(metrics)) and mm[up] == 0.0, 30, 1.0)
    run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=400)
    grown = (_try(metrics) or {}).get(slot, 0.0)
    check("metrics: while paused, slot lag grows as changes accumulate and connector_up=0",
          grown > max(base_lag, 0) + 10_000 and (_try(metrics) or {}).get(up) == 0.0,
          f"lag {base_lag:.0f} -> {grown:.0f} bytes")  # fmt: skip

    connect("PUT", "/connectors/pg-shop/resume")
    recovered = (
        wait_for(lambda: (mm := _try(metrics)) and mm[up] == 1.0 and mm[slot] < grown / 4 and mm, 120, 2.0) or {}
    )
    check("metrics: after resume, connector_up=1 and slot lag drops below a quarter of the paused peak",
          bool(recovered), f"lag now {recovered.get(slot, float('nan')):.0f} bytes")  # fmt: skip
    return {"lag_bytes_baseline": base_lag, "lag_bytes_paused": grown, "lag_bytes_recovered": recovered.get(slot)}


def _try(fn: Any) -> Any:
    try:
        return fn()
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    if _try(lambda: urllib.request.urlopen("http://localhost:8083/connectors/pg-shop", timeout=5)):
        raise SystemExit("a pg-shop connector already exists: run via `make g2-check` (fresh volumes, no connector)")
    c = consumer()
    details: dict[str, Any] = {}
    p1, nxt = phase_snapshot(c, txns=2000)
    details["snapshot"] = p1
    nxt = phase_live(c, nxt, txns=1500)
    details["delete"] = phase_delete(c)
    details["toast"] = phase_toast(c)
    details["slot_lag"] = phase_slot_lag()
    c.close()
    passed = all(r["ok"] for r in RESULTS)
    path = write_manifest(
        "g2-check", "core", SEED, {"started_at": started, "passed": passed, "checks": RESULTS, **details}
    )
    n_ok = sum(r["ok"] for r in RESULTS)
    print(f"{'PASSED' if passed else 'FAILED'} ({n_ok}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
