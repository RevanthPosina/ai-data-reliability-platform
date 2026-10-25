"""G0 thin slice: one Postgres insert -> topic -> bronze -> silver (Spark SQL) -> Qdrant.

Pass/fail per hop only. This is not a measurement run, so it records no latencies.
Writes a run manifest under runs/manifests/.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from typing import Any

from _env import ROOT
from manifest import write_manifest

COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".run/compose.env")]
TOPIC = "cdc.public.help_articles"


def sh(*args: str, timeout: float = 300) -> str:
    r = subprocess.run(
        [*COMPOSE, *args], capture_output=True, encoding="utf-8", errors="replace", timeout=timeout, check=False
    )
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:3])}... failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout


def hop(name: str, ok: bool, detail: str = "") -> dict[str, Any]:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    return {"hop": name, "ok": ok, "detail": detail}


def poll(fn: Any, timeout: float, every: float = 2.0) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = fn()
        if got:
            return got
        time.sleep(every)
    return None


def topic_contains(marker: str) -> bool:
    desc = sh("exec", "-T", "redpanda", "rpk", "topic", "describe", TOPIC, "-p")
    total = sum(int(line.split()[-1]) for line in desc.splitlines()[1:] if line.strip())
    if total == 0:
        return False
    out = sh("exec", "-T", "redpanda", "rpk", "topic", "consume", TOPIC, "-o", "start", "-n", str(total), "-f", "%v\n")
    return marker in out


def qdrant_points(entity_id: str, version: int) -> list[dict[str, Any]]:
    body = {
        "filter": {
            "must": [
                {"key": "entity_id", "match": {"value": entity_id}},
                {"key": "source_version", "match": {"value": version}},
            ]
        },
        "limit": 100,
        "with_payload": True,
    }
    req = urllib.request.Request(
        "http://localhost:6333/collections/help_articles/points/scroll",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r)["result"]["points"]
    except Exception:  # noqa: BLE001 - collection may not exist yet
        return []


def main() -> int:
    started = datetime.now(UTC).isoformat()
    marker = f"g0-smoke-{uuid.uuid4().hex[:12]}"
    hops: list[dict[str, Any]] = []
    print(f"G0 thin slice, marker={marker}")

    row = sh(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        "shop",
        "-At",
        "-c",
        f"INSERT INTO help_articles (title, body) VALUES ('Smoke test {marker}', "
        f"'Walking skeleton article {marker}. It should reach Qdrant.') RETURNING id;",
    )
    entity_id = row.strip().splitlines()[0]
    hops.append(hop("postgres insert", entity_id.isdigit(), f"id={entity_id}"))

    hops.append(hop("redpanda topic", bool(poll(lambda: topic_contains(marker), 60)), TOPIC))

    out = sh(
        "run", "--rm", "spark-query", "/opt/app/adrp/spark/query.py", "--id", entity_id, "--timeout", "180", timeout=420
    )
    result_line = next((ln for ln in out.splitlines() if ln.startswith("RESULT ")), "RESULT {}")
    res = json.loads(result_line[len("RESULT ") :])
    hops.append(hop("iceberg bronze (Spark SQL)", bool(res.get("found_bronze")), f"ops={res.get('bronze_ops')}"))
    silver = res.get("silver") or {}
    silver_ok = bool(silver) and marker in str(silver.get("title")) and silver.get("is_deleted") is False
    hops.append(hop("iceberg silver (Spark SQL)", silver_ok, f"lsn={silver.get('lsn')}"))

    points: list[dict[str, Any]] = []
    if silver_ok:
        points = poll(lambda: qdrant_points(entity_id, int(silver["lsn"])), 180) or []
    hops.append(hop("qdrant (filtered scroll, matching source_version)", bool(points), f"points={len(points)}"))

    passed = all(h["ok"] for h in hops)
    path = write_manifest(
        "g0-smoke",
        profile="core",
        seed=None,
        result={"started_at": started, "passed": passed, "hops": hops, "measurement": False},
    )
    print(f"{'PASSED' if passed else 'FAILED'}; manifest: {path.relative_to(ROOT)}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
