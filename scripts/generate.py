"""Host wrapper for the G1 generator. Every invocation writes a run manifest.

python3 scripts/generate.py run --seed 42 --duration 30 [--rate 500] [--max-txns N]
python3 scripts/generate.py determinism --seed 42 --txns 5000
python3 scripts/generate.py max-rate --seed 42 --reps 5 --duration 30 --warmup 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

from _env import ROOT
from manifest import write_manifest

COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".run/compose.env")]


def in_container(*module_args: str, timeout: float = 3600) -> str:
    r = subprocess.run(
        [*COMPOSE, "run", "--rm", "generator", *module_args],
        capture_output=True, encoding="utf-8", errors="replace", timeout=timeout, check=False,
    )  # fmt: skip
    if r.returncode != 0:
        raise SystemExit(f"generator failed:\n{r.stdout[-4000:]}\n{r.stderr[-4000:]}")
    return r.stdout


def tagged(out: str, tag: str) -> dict[str, Any]:
    line = next((ln for ln in out.splitlines() if ln.startswith(tag + " ")), None)
    if line is None:
        raise SystemExit(f"no {tag} line in output:\n{out[-2000:]}")
    return json.loads(line[len(tag) + 1 :])


def run_once(seed: int, duration: float, rate: float, warmup: float, max_txns: int) -> dict[str, Any]:
    args = ["adrp.gen.runner", "--seed", str(seed), "--duration", str(duration), "--rate", str(rate)]
    args += ["--warmup", str(warmup), "--max-txns", str(max_txns)]
    return tagged(in_container(*args), "SUMMARY")


def cmd_run(a: argparse.Namespace) -> int:
    started = datetime.now(UTC).isoformat()
    s = run_once(a.seed, a.duration, a.rate, a.warmup, a.max_txns)
    v = tagged(in_container("adrp.gen.verify"), "VERIFY")
    path = write_manifest("gen-run", "core", a.seed, {"started_at": started, "summary": s, "verify": v})
    print(json.dumps({"summary": s, "verify": v}, indent=2))
    print(f"manifest: {path.relative_to(ROOT)}")
    return 0 if v["ok"] else 1


def cmd_determinism(a: argparse.Namespace) -> int:
    """Same seed -> identical logical op sequence, checked three ways (PLAN G1 done condition)."""
    started = datetime.now(UTC).isoformat()
    s = run_once(a.seed, duration=1e9, rate=0, warmup=0, max_txns=a.txns)
    start, end = s["txn_range"]
    count = end - start
    dry1 = tagged(in_container("adrp.gen.digest", "stream", "--seed", str(a.seed), "--start", str(start),
                               "--count", str(count)), "DIGEST")  # fmt: skip
    dry2 = tagged(in_container("adrp.gen.digest", "stream", "--seed", str(a.seed), "--start", str(start),
                               "--count", str(count)), "DIGEST")  # fmt: skip
    other = tagged(in_container("adrp.gen.digest", "stream", "--seed", str(a.seed + 1), "--start", str(start),
                                "--count", str(count)), "DIGEST")  # fmt: skip
    data_dir = s["data_dir"] + "/ground_truth"
    parquet = tagged(in_container("adrp.gen.digest", "parquet", "--dir", data_dir), "DIGEST")
    verify = tagged(in_container("adrp.gen.verify"), "VERIFY")
    checks = {
        "dry_run_repeatable": dry1 == dry2,
        "db_run_equals_dry_run": s["stream_sha256"] == dry1["sha256"] and s["ops"] == dry1["ops"],
        "ground_truth_parquet_equals_dry_run": parquet == dry1,
        "different_seed_differs": other["sha256"] != dry1["sha256"],
        "run_log_commit_ts_complete": s["commit_ts_missing"] == 0,
        "db_matches_model": verify["ok"],
    }
    passed = all(checks.values())
    result = {"started_at": started, "passed": passed, "checks": checks, "summary": s, "dry_run": dry1,
              "parquet": parquet, "other_seed": other, "verify": verify}  # fmt: skip
    path = write_manifest("gen-determinism", "core", a.seed, result)
    for k, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {k}")
    print(f"txn_range={s['txn_range']} ops={s['ops']} sha256={s['stream_sha256'][:16]}…")
    print(f"{'PASSED' if passed else 'FAILED'}; manifest: {path.relative_to(ROOT)}")
    return 0 if passed else 1


def cmd_max_rate(a: argparse.Namespace) -> int:
    """Unthrottled fixed-duration reps; achieved row changes/s after warmup (median, min, max)."""
    started = datetime.now(UTC).isoformat()
    reps = []
    for i in range(a.reps):
        s = run_once(a.seed, a.duration, rate=0, warmup=a.warmup, max_txns=0)
        reps.append(s)
        print(f"  rep {i + 1}/{a.reps}: {s['achieved_changes_per_s']} changes/s, {s['achieved_txns_per_s']} txns/s")
    rates = [r["achieved_changes_per_s"] for r in reps]
    txn_rates = [r["achieved_txns_per_s"] for r in reps]
    verify = tagged(in_container("adrp.gen.verify"), "VERIFY")
    result = {
        "started_at": started,
        "definition": "generator -> Postgres, unthrottled, one connection, one logical txn per DB txn, "
        "core profile running (CDC + Spark + embedder consuming concurrently)",
        "reps": reps,
        "changes_per_s": {"median": statistics.median(rates), "min": min(rates), "max": max(rates)},
        "txns_per_s": {"median": statistics.median(txn_rates), "min": min(txn_rates), "max": max(txn_rates)},
        "verify": verify,
    }
    path = write_manifest("gen-max-rate", "core", a.seed, result)
    print(json.dumps({k: result[k] for k in ("changes_per_s", "txns_per_s")}, indent=2))
    print(f"db_matches_model={verify['ok']}; manifest: {path.relative_to(ROOT)}")
    return 0 if verify["ok"] else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--duration", type=float, default=30)
    r.add_argument("--rate", type=float, default=0)
    r.add_argument("--warmup", type=float, default=0)
    r.add_argument("--max-txns", type=int, default=0)
    d = sub.add_parser("determinism")
    d.add_argument("--seed", type=int, default=42)
    d.add_argument("--txns", type=int, default=5000)
    m = sub.add_parser("max-rate")
    m.add_argument("--seed", type=int, default=42)
    m.add_argument("--reps", type=int, default=5)
    m.add_argument("--duration", type=float, default=30)
    m.add_argument("--warmup", type=float, default=5)
    a = ap.parse_args()
    return {"run": cmd_run, "determinism": cmd_determinism, "max-rate": cmd_max_rate}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
