"""Host CLI for deletion propagation (G8). No raw customer id is ever printed; requests are named by their uuid.

  forget.py request --customer ID                        create a request; prints the request id
  forget.py status [--request-id UUID ...] [-v]          status, lags per phase (and per sink with -v)
  forget.py run [--phase logical|physical|all] [--request-id UUID ...] [--canary TOKEN ...] [--no-queue]
                                                         run the workflow for the named requests, or for every
                                                         pending one plus the Dagster queue (forget_queue/*.json)

`run` is the executor: Dagster's forget_logical / forget_physical jobs only enqueue request ids (the Dagster
container cannot reach Postgres or docker), this script performs the work. The physical phase stops spark-streams
and the embedder for a maintenance window and restarts them. Needs the stack up (`make up`), FORGET_HMAC_KEY, and
the Spark image built with adrp/spark/forget.py and forget_check.py.

`--customer ID` is the only place a raw id appears on a command line; scripts that must not leak it can insert the
privacy.forget_requests row themselves (workflow.register) and pass `--request-id` afterwards.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from _env import ROOT

sys.path.insert(0, str(ROOT / "src"))
from adrp.forget import audit, queue, workflow


def cmd_request(a: argparse.Namespace) -> int:
    rid = workflow.register(a.customer)
    print(rid)
    return 0


def cmd_status(a: argparse.Namespace) -> int:
    rows = [r for r in audit.list_requests() if not a.request_id or r.request_id in queue.clean_ids(a.request_id)]
    for r in rows:
        print(json.dumps(audit.public_summary(r)))
        if a.verbose:
            for sink, d in audit.request_lags(r.request_id).items():
                print("  " + json.dumps({"sink": sink, **d}))
    if not rows:
        print("no requests")
    return 0


def pending_ids(phase: str, use_queue: bool) -> list[str]:
    statuses = {
        "logical": ("requested", "failed"),
        "physical": ("logical_done",),
        "all": ("requested", "failed", "logical_done"),
    }
    ids = [r.request_id for r in audit.list_requests(statuses[phase])]
    if use_queue:
        ids += queue.pending_request_ids(
            workflow.Config().eventlog / queue.QUEUE_SUBDIR, "logical" if phase == "logical" else "physical"
        )
        if phase == "all":
            ids += queue.pending_request_ids(workflow.Config().eventlog / queue.QUEUE_SUBDIR, "logical")
    return queue.clean_ids(ids)


def summarize_outcomes(outcomes: dict[str, Any]) -> list[str]:
    return [f"    {n:<18} {o.verdict}" for n, o in sorted(outcomes.items())]


def cmd_run(a: argparse.Namespace) -> int:
    ids = queue.clean_ids(a.request_id) if a.request_id else pending_ids(a.phase, not a.no_queue)
    subjects = [s for rid in ids if (s := workflow.load_subject(rid, a.canary)) is not None]
    print(f"{len(ids)} request(s), {len(subjects)} still open; phase {a.phase}")
    if not subjects:
        return 0
    cfg = workflow.Config()
    ok = True
    if a.phase in ("logical", "all"):
        lrep = workflow.run_logical(subjects, cfg)
        ok &= lrep.ok
        print(f"logical: {'ok' if lrep.ok else 'FAILED'} in {lrep.seconds:.0f} s; skipped {len(lrep.skipped)}")
        for rid, why in lrep.failed.items():
            print(f"  {rid[:8]} {why}")
    if ok and a.phase in ("physical", "all"):
        prep = workflow.run_physical(subjects, cfg)
        ok &= prep.ok
        for name in ("window", "qdrant", "broker", "wal", "purge"):
            part = getattr(prep, name)
            if part is not None:
                print(f"physical {name}: {'ok' if part.ok else 'FAILED'}")
                print("\n".join(summarize_outcomes(part.outcomes)))
        if prep.purge is not None and prep.purge.deferred:
            print("  deferred: " + "; ".join(prep.purge.deferred))
    if not a.request_id and not a.no_queue:
        # queue files are consumed once their requests were looked at; a failed request stays visible in status
        for path, e in queue.read_entries(workflow.Config().eventlog / queue.QUEUE_SUBDIR):
            if all(audit.get_request(r) is not None for r in e["request_ids"]):
                queue.done(path)
    print(json.dumps(workflow.lag_report(ids)["sinks"]))
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("request")
    r.add_argument("--customer", type=int, required=True)
    s = sub.add_parser("status")
    s.add_argument("--request-id", action="append", default=[])
    s.add_argument("-v", "--verbose", action="store_true")
    n = sub.add_parser("run")
    n.add_argument("--phase", choices=["logical", "physical", "all"], default="all")
    n.add_argument("--request-id", action="append", default=[])
    n.add_argument("--canary", action="append", default=[])
    n.add_argument("--no-queue", action="store_true")
    a = ap.parse_args()
    return {"request": cmd_request, "status": cmd_status, "run": cmd_run}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
