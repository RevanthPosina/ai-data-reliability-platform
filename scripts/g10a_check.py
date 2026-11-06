"""G10a done-condition check: the six-fault subset of the pre-registered fault matrix (design section 5), each followed
by a reconciliation gate (scripts/reconcile.py), scored caught / self-healed / silent. This is a check on one machine
under the tested faults; it does not show that undetected faults are absent.

Precondition (Makefile target `g10a-check`): fresh volumes, core + apps up with ADRP_FAULT_HOOKS=1, NO connector yet
(`bootstrap.py --skip-connector`), nothing generated yet. `--topics compact` runs on the g8 topic settings.

  F4  connector crash mid-snapshot   first: it needs a connector that has never been created; writers stop for the retry
  F1  duplicate events               silver checkpoint rewound (its MERGE is LSN-guarded); bronze gets a batch append
  F2  out-of-order updates           older versions re-produced after newer ones, incl. an older insert after a logical delete
  F3  Spark kill mid-MERGE           one kill at the parked mid_merge hook, up to 3 in-flight kills after merge_start; continuous mode
  F5  Redpanda restart               graceful restart of the broker under load (dev-container broker: no hard-crash claim)
  F6a schema drift, add column       g3.phase_add_column under load
  F6b schema drift, incompatible     g3.phase_quarantine with the generator stopped; positive control, then recovery
  END orphan files counted and removed physically with writers stopped, final gate, fault table scored, manifest

Every fault runs through `run_fault`: preflight, injection, restoration, the reconciliation gate (10 checks), the
oracle (F4 to F6a) and scoring. A dirty gate fails the fault and the run whatever `classify` returns: classify says
CAUGHT as soon as any detection fired, so CAUGHT alone is never read as a pass.

Resume on the live stack (diagnosis only; a run of record is a full run):
    python scripts/g10a_check.py --topics default --only F3        state: .run/g10a-state.json
    python scripts/g10a_check.py --topics default --from F5
Render the fault table from one or two manifests:
    python scripts/g10a_check.py --render-table runs/manifests/<default>.json [runs/manifests/<compact>.json]

Not exercised against a live stack when this file was written: every phase below is reasoning from the code and the
specs (ADR-0020 to ADR-0023), marked "untested" where a first run is most likely to disagree.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import string
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any

import g2_check as g2
import g3_check as g3
import g6_check as g6
import reconcile as rc
from _env import ROOT, all_env
from bootstrap import ensure_connector, http
from confluent_kafka import Producer
from generate import run_once
from manifest import write_manifest

sys.path.insert(0, str(ROOT / "src"))
from adrp.forget import ids as fids
from adrp.forget import sparkcli
from adrp.forget.rawscan import run as run_proc
from adrp.forget.rawscan import service_running, wait_until
from adrp.forget.sparkcli import lsn_to_int
from adrp.recon import continuous
from adrp.recon.classify import Outcome, classify, wording_violations
from adrp.recon.cut import latest_committed, parse_offsets_file, uncovered

SEED = g2.SEED
TABLES = g2.TABLES
AD_HOC = g2.AD_HOC
RESULTS = g3.RESULTS  # g3's phases and the checks here share one list
check = g3.check
FAULTS = ("F4", "F1", "F2", "F3", "F5", "F6a", "F6b")  # run order on one shared stack (ADR-0020)
PHASES = (*FAULTS, "END")
TABLE_ORDER = ("F1", "F2", "F3", "F4", "F5", "F6a", "F6b")  # the matrix order of design section 5
DETECTION_LABELS = {
    "connector_status": "connector-status check (exporter sampler)",
    "quarantine_count": "quarantine count > 0",
    "reconciler_before_recovery": "reconciler mismatch before recovery",
}
STATE = ROOT / ".run/g10a-state.json"
TABLE_DOC = ROOT / "docs/results/g10a-fault-table.md"
CONNECT = "http://localhost:8083"
FAULT_SCRIPT = "/opt/app/adrp/spark/fault.py"
PROBE_SCRIPT = "/opt/app/adrp/spark/probe.py"
SERVICES = ("postgres", "redpanda", "connect", "seaweedfs", "lakekeeper", "qdrant", "spark-streams", "embedder")
ORACLE_FAULTS = ("F4", "F1", "F2", "F3", "F5", "F6a")  # F6b changes generator-owned view_count out of band: no oracle
SUBJECTS = ("cdc.public.help_articles-value", "cdc.public.refunds-value")
ADHOC_ARTICLE = AD_HOC + 700_001  # F2's scripted row: 13 digits, so it is never printed or stored
SCOPE_NOTE = "Zero mismatches under the 6 tested faults at this cut; undetected faults may exist."
DETAIL_WORDS = {
    "caught": "caught",
    "self_healed": "self-healed in this run",
    "silent": "silent",
    "gate_failed": "reconciliation gate not clean",
    "skipped": "skipped (precondition failed)",
    "not_run": "not run",
}

# Copied verbatim from docs/design.md section 5 (the rows are never edited; tests/test_g10a_pure.py compares them).
PREREGISTERED: dict[str, dict[str, str]] = {
    "F1": {
        "fault": "Duplicate events",
        "injection": "Reset the silver/bronze consumer to replay an offset range",
        "detection": "none needed",
        "outcome": "self-healed: silver unchanged (LSN guard); bronze holds duplicate rows by design",
    },
    "F2": {
        "fault": "Out-of-order updates",
        "injection": "Produce older-LSN versions of a key after newer ones",
        "detection": "none needed",
        "outcome": "self-healed: older LSN ignored by the MERGE condition",
    },
    "F3": {
        "fault": "Spark kill mid-MERGE",
        "injection": "`SIGKILL` the streaming container during foreachBatch",
        "detection": "reconciler (if any mismatch)",
        "outcome": "self-healed on restart: batch re-run is idempotent; orphan files left for G8 cleanup",
    },
    "F4": {
        "fault": "Connector crash mid-snapshot",
        "injection": "Kill the Connect worker during the initial snapshot",
        "detection": "connector-status alert",
        "outcome": "self-healed or caught: snapshot restarts; duplicates absorbed by the LSN guard",
    },
    "F5": {
        "fault": "Redpanda restart",
        "injection": "Restart the broker under load",
        "detection": "connector-status / slot-lag alert during downtime",
        "outcome": "self-healed: no loss after recovery",
    },
    "F6a": {
        "fault": "Schema drift: add column",
        "injection": "`ALTER TABLE … ADD COLUMN` on a live table",
        "detection": "none needed",
        "outcome": "self-healed: column appears in silver without a restart",
    },
    "F6b": {
        "fault": "Schema drift: incompatible change",
        "injection": "Type narrowing or rename",
        "detection": "registry compatibility check (CI) / quarantine count > 0",
        "outcome": "caught: rows land in `quarantine_<table>` with `SCHEMA_INCOMPATIBLE`",
    },
    "F13": {
        "fault": "TOAST placeholder leak",
        "injection": "Update a non-text column of a 10 KB help article",
        "detection": "`__debezium_unavailable_value` count = 0",
        "outcome": "must not occur (REPLICA IDENTITY FULL); caught if it does",
    },
}
# True: the pre-registered detection is an alert-like check whose firing is recorded (never required); None: no
# detection is expected, or it is conditional on a mismatch
EXPECTS_DETECTION: dict[str, bool | None] = {
    "F1": None, "F2": None, "F3": None, "F4": True, "F5": True, "F6a": None, "F6b": True,
}  # fmt: skip
INJECTION_AS_RUN = {
    "F1": "silver checkpoint rewound by 3 batches and restarted; a recorded offset range of customers and support_tickets appended to bronze by a batch read",
    "F2": "older versions of 9 seeded keys, and an older insert and update of one ad-hoc article after its logical delete, re-produced with identical bytes to the same partition",
    "F3": "one SIGKILL at the parked mid_merge hook (between two tables' MERGEs), then 3 SIGKILLs after the merge_start marker under load",
    "F4": "Connect SIGKILLed during the initial snapshot (test-only throttled snapshot query on transactions), writers stopped, Connect restarted",
    "F5": "graceful `compose restart redpanda` 30 s into a 60 changes/s load",
    "F6a": "ALTER TABLE help_articles ADD COLUMN locale under an 8 changes/s load",
    "F6b": "help_articles column rename and refunds.amount narrowing with the registry gate bypassed (compatibility NONE)",
}
RECOVERY = {
    "F1": "none: silver re-ran the range, bronze keeps the duplicate rows by design",
    "F2": "none",
    "F3": "fault file cleared, spark-streams restarted, load stopped after a 45 s settle",
    "F4": "load stopped, Connect restarted, snapshot re-emitted, unmodified pg-shop.json re-applied",
    "F5": "broker waited for, Connect task restarted if FAILED, spark-streams started if not running",
    "F6a": "none",
    "F6b": "no-op UPDATE of the parked refund after the schema was restored",
}
LIMITS = [
    "F4: the snapshot retry ran with no concurrent writers; a transaction committing across the retry was not tested",
    "F5: graceful restart of a dev-container broker; no hard-crash and no acknowledged-write claim",
    "F6b: registry gate bypassed (compatibility NONE); silver's gate tested; contracts-ci covers the registry separately",
    "F3: the parked mid_merge hook sits between two tables' MERGEs, not inside an Iceberg write; orphan files are counted at END and reported, not asserted",
    "Continuous mode catches wrong values; it does not detect lag or loss",
    "F2: the re-produced records keep stale images at new offsets in the topics; on a compacting topic a rebuild from the topic would not be safe, bronze is the rebuild source",
    "One machine, one seed, one run per topic setting; nothing here is extrapolated beyond this machine",
]
SNAPSHOT_OVERRIDES = {  # test-only: slows the snapshot of transactions so a SIGKILL lands inside it (untested property names on Debezium 3.7)
    "snapshot.fetch.size": "100",
    "snapshot.select.statement.overrides": "public.transactions",
    "snapshot.select.statement.overrides.public.transactions": "SELECT * FROM public.transactions WHERE pg_sleep(0.03) IS NOT NULL",
}
SNAPSHOT_KILL_MIN = 100  # r events on the transactions topic before the kill is allowed
CONNECT_UP = 'adrp_connector_up{connector="pg-shop"}'
CONNECT_SRC = 'adrp_exporter_source_up{source="connect"}'
SLOT_LAG = 'adrp_replication_slot_lag_bytes{slot="dbz_shop"}'


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mask(text: str, limit: int = 300) -> str:
    return sparkcli.mask(text, limit)


def cmp(*args: str, timeout: float = 600, check_rc: bool = True) -> str:
    """docker compose with both service profiles enabled (stop/restart/up by service name need them)."""
    return g3.compose("--profile", "core", "--profile", "apps", *args, timeout=timeout, check_rc=check_rc)


# ---- pure helpers (unit-tested in tests/test_g10a_pure.py) ------------------------------------------------------------


def connector_down(m: Mapping[str, float]) -> bool:
    """A connector-status check fires on adrp_connector_up == 0, or on adrp_exporter_source_up{connect} == 0: with Connect
    down the connector series is absent, so the second clause is required (ADR-0023)."""
    return m.get(CONNECT_UP) == 0.0 or m.get(CONNECT_SRC) == 0.0


@dataclass(frozen=True)
class Sample:
    t: float  # time.monotonic()
    answered: bool  # the exporter answered
    down: bool  # connector_down() on the sample
    lag: float | None  # slot lag in bytes


def window_summary(samples: Iterable[Sample], t0: float, t1: float) -> dict[str, Any]:
    """What a sampler saw in [t0, t1]: sample counts, whether the connector-status check fired, the slot-lag peak."""
    inside = [s for s in samples if t0 <= s.t <= t1]
    answered = [s for s in inside if s.answered]
    lags = [s.lag for s in answered if s.lag is not None]
    return {
        "samples": len(inside),
        "exporter_answered": len(answered),
        "connector_status_fired": any(s.down for s in answered),
        "peak_slot_lag_bytes": max(lags) if lags else None,
        "slot_lag_grew": bool(lags) and max(lags) > lags[0],
    }


# F5's window (injection to the first RUNNING read) is about 4 to 6 s and needs >= 3 answered samples
SAMPLE_EVERY_S = 0.5
SCRAPE_TIMEOUT_S = 1.0  # one exporter scrape takes 15 to 70 ms; a longer wait only costs samples


def parse_metrics(text: str) -> dict[str, float]:
    """Prometheus text exposition to {series: value} (the same reading as g2.metrics)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            k, v = line.rsplit(" ", 1)
            out[k] = float(v)
    return out


def scrape_exporter() -> dict[str, float]:
    with urllib.request.urlopen("http://localhost:9108/metrics", timeout=SCRAPE_TIMEOUT_S) as resp:
        return parse_metrics(resp.read().decode())


def next_wait(every_s: float, started: float, finished: float) -> float:
    """Seconds to sleep after a scrape that began at `started` and ended at `finished`: the poll stays on a fixed cadence
    (a 40 ms scrape sleeps 460 ms), and a scrape slower than the interval does not sleep, so scrapes never overlap."""
    return max(0.0, every_s - (finished - started))


class CdcSampler(threading.Thread):
    """Polls the exporter every 0.5 s (design S13 note: the Milestone A stand-in for the connector-status alert). One
    thread scrapes serially with a 1 s timeout: a scrape that blocks during an outage costs at most that second and
    nothing queues behind it."""

    def __init__(self, every_s: float = SAMPLE_EVERY_S) -> None:
        super().__init__(daemon=True)
        self.every_s = every_s
        self.samples: list[Sample] = []
        self.halt = threading.Event()

    def run(self) -> None:
        while not self.halt.is_set():
            started = time.monotonic()
            m = g2._try(scrape_exporter)
            t = time.monotonic()
            if isinstance(m, dict):
                self.samples.append(Sample(t, True, connector_down(m), m.get(SLOT_LAG)))
            else:
                self.samples.append(Sample(t, False, False, None))
            self.halt.wait(next_wait(self.every_s, started, t))

    def window(self, t0: float, t1: float) -> dict[str, Any]:
        return window_summary(list(self.samples), t0, t1)


def compaction_effect(topics: Mapping[str, Any], ends: Mapping[str, Mapping[int, int]]) -> dict[str, tuple[int, int]]:
    """{topic: (records Kafka still holds below the end offset, end offset total)}, counts only. `topics` is
    ReconReport.topics (OffsetResult per topic: `distinct` is bronze's count, so only `kafka_present` is read) and `ends`
    the cut's real end offsets {topic: {partition: end}}."""
    return {t: (int(r.kafka_present), sum(ends.get(t, {}).values())) for t, r in topics.items()}


def compaction_removed(effect: Mapping[str, tuple[int, int]]) -> bool:
    """True when Kafka holds fewer records than its end offsets on at least one topic: compaction removed records."""
    return any(present < end for present, end in effect.values())


def compaction_detail(effect: Mapping[str, tuple[int, int]]) -> str:
    if not effect:
        return "no END gate report"
    return "records present in Kafka of end offsets, per topic " + json.dumps(
        {t: f"{p} of {e}" for t, (p, e) in sorted(effect.items())}
    )


def expected_events(ops: Iterable[Any]) -> dict[str, int]:
    """Topic records a list of generator ops produces: one per op, and a tombstone after each delete (g2.phase_live)."""
    out: dict[str, int] = defaultdict(int)
    for op in ops:
        out[op.table] += 1 + (1 if op.op == "D" else 0)
    return dict(out)


_CKPT_FILE = re.compile(r"^\.?(\d+)(\.crc)?$")


def rewind_paths(
    root: str, commit_names: Sequence[str], offset_names: Sequence[str], keep_last: int
) -> tuple[int, int, list[str]] | None:
    """Plan for rewinding one streaming query's checkpoint by `keep_last` committed batches: (M, N, paths to remove).
    N is the newest committed batch, M = N - keep_last the newest one that stays. Every offsets/commits entry above M is
    removed, with its `.crc` sibling and including an uncommitted offsets/N+1 that a stop may have left. None when the
    query has fewer than keep_last + 1 committed batches."""
    n = latest_committed(commit_names)
    if n is None or n < keep_last:
        return None
    m = n - keep_last
    paths = [
        f"{root}/{d}/{name}"
        for d, names in (("offsets", offset_names), ("commits", commit_names))
        for name in names
        if (hit := _CKPT_FILE.match(name)) and int(hit.group(1)) > m
    ]
    return m, n, sorted(paths)


def newest_offsets_file(names: Sequence[str]) -> int | None:
    """Highest numeric `offsets/N` entry, whether or not it was committed."""
    nums = [int(n) for n in names if n.isascii() and n.isdigit()]
    return max(nums) if nums else None


def replayed_ranges(
    start: Mapping[str, Mapping[int, int]], old_end: Mapping[str, Mapping[int, int]]
) -> dict[str, dict[int, tuple[int, int]]]:
    """The offsets a rewound query reads again: per topic and partition [start, old end). A partition missing from
    `start` was not read yet, so it restarts at 0."""
    out: dict[str, dict[int, tuple[int, int]]] = {}
    for topic, parts in old_end.items():
        for p, e in parts.items():
            s = start.get(topic, {}).get(p, 0)
            if e > s:
                out.setdefault(topic, {})[p] = (s, e)
    return out


def range_size(ranges: Mapping[str, Mapping[int, tuple[int, int]]]) -> int:
    return sum(e - s for parts in ranges.values() for s, e in parts.values())


def last_n_ranges(ends: Mapping[int, int], n: int) -> dict[str, list[int]]:
    """The last `n` offsets of each non-empty partition as the {partition: [start, end)} JSON of fault.py replay-bronze.
    A compacting topic may hold fewer records than offsets: the count read, not this range, feeds the ledger."""
    return {str(p): [max(0, e - n), e] for p, e in sorted(ends.items()) if e > 0}


def entry_key(e: Mapping[str, Any]) -> tuple[int, str, str]:
    return int(e["batch_id"]), str(e["table"]), str(e["written_at"])


def rerun_batches(
    before: set[tuple[int, str, str]], entries: Iterable[Mapping[str, Any]], max_old_batch: int
) -> list[int]:
    """Batch ids at or below `max_old_batch` that the silver event log records again after `before`, carrying events."""
    return sorted(
        {
            int(e["batch_id"])
            for e in entries
            if entry_key(e) not in before and int(e["batch_id"]) <= max_old_batch and (e.get("n_events") or 0) > 0
        }
    )


def changes_seen(
    entries: Iterable[Mapping[str, Any]], before: set[tuple[int, str, str]], want: Mapping[tuple[str, int], int]
) -> int:
    """How many of the wanted (table, id) -> lsn pairs appear in the `changes` of event-log entries newer than `before`."""
    seen: set[tuple[str, int, int]] = set()
    for e in entries:
        if entry_key(e) in before:
            continue
        for ch in e.get("changes") or []:
            try:
                seen.add((str(e["table"]), int(json.loads(ch["pk"])["id"]), int(ch["lsn"])))
            except (KeyError, TypeError, ValueError):
                continue
    return sum(1 for (t, i), lsn in want.items() if (t, i, lsn) in seen)


@dataclass(frozen=True)
class Version:
    """One decoded record of a CDC topic."""

    table: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    pk: int | None
    lsn: int | None
    op: str | None  # None for a tombstone


def pick_older(versions: Sequence[Version], rng: random.Random, per_table: int) -> list[Version]:
    """For up to `per_table` seeded keys of each table that have at least two versions with different LSNs, the version
    with the lowest LSN: a record older than one the topic also holds."""
    by_key: dict[tuple[str, int, bytes], list[Version]] = defaultdict(list)
    for v in versions:
        if v.key is not None and v.value is not None and v.lsn is not None and v.op in ("c", "u", "r"):
            by_key[(v.table, v.partition, v.key)].append(v)
    out: list[Version] = []
    for table in sorted({k[0] for k in by_key}):
        cands = sorted(k for k, vs in by_key.items() if k[0] == table and len({x.lsn for x in vs}) >= 2)
        for k in sorted(rng.sample(cands, min(per_table, len(cands)))):
            out.append(min(by_key[k], key=lambda x: x.lsn or 0))
    return out


def dup_r_keys(records: Iterable[tuple[int, bytes | None, str | None]]) -> int:
    """Keys with two or more snapshot (`r`) events. Records are (partition, key bytes, op); a tombstone has op None."""
    seen: Counter[tuple[int, bytes]] = Counter()
    for p, key, op in records:
        if key is not None and op == "r":
            seen[(p, key)] += 1
    return sum(1 for n in seen.values() if n >= 2)


def counts_from(report: Any, oracle: Mapping[str, int] | None) -> dict[str, int | None]:
    """dupes / losses / resurrections for the fault record: from the reconciler's per-table results, else the oracle."""
    if report is not None:
        t = report.tables.values()
        return {
            "dupes": sum(r.duplicates for r in t),
            "losses": sum(r.missing for r in t),
            "resurrections": sum(r.resurrected for r in t),
        }
    if oracle is not None:
        return {"dupes": oracle["duplicates"], "losses": oracle["missing"], "resurrections": oracle["resurrected"]}
    return {"dupes": None, "losses": None, "resurrections": None}


def score_fault(fid: str, gate_ok: bool, fired: Sequence[str]) -> tuple[str, str, bool]:
    """(classified, outcome, passed). `classified` is classify()'s answer. `outcome` is that answer only when the
    reconciliation gate is clean, otherwise "gate_failed": classify says CAUGHT whenever a detection fired, even with a
    dirty reconciler. Passed needs a clean gate, never SILENT, and CAUGHT for F6b."""
    cls = classify(gate_ok, fired)
    outcome = cls.value if gate_ok else "gate_failed"
    passed = gate_ok and cls != Outcome.SILENT and (fid != "F6b" or cls == Outcome.CAUGHT)
    return cls.value, outcome, passed


def new_record(fid: str) -> dict[str, Any]:
    pre = PREREGISTERED[fid]
    return {
        "fault": pre["fault"], "injected_at": None, "recovered_s": None,
        "dupes": None, "losses": None, "resurrections": None,
        "outcome": "not_run", "classified": None, "gate_ok": None,
        "expected_detection": pre["detection"], "expected_detection_fired": None, "detections_fired": [],
        "expected_outcome": pre["outcome"], "injection": INJECTION_AS_RUN[fid], "recovery": RECOVERY[fid],
        "residual": "", "contaminated_by": None, "new_mismatches": None, "notes": {},
    }  # fmt: skip


def md_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_table(manifests: Sequence[tuple[str, dict[str, Any]]]) -> str:
    """Markdown fault table from manifests (name, parsed). Rendered, never typed by hand."""
    out = [
        "# G10a fault table",
        "",
        "Generated by `scripts/g10a_check.py --render-table`; do not edit by hand. Source manifests:"
        + "".join(f" `{name}`" for name, _ in manifests),
        "",
        f"Scope: {SCOPE_NOTE} One machine, one seed.",
        "",
    ]
    for name, mf in manifests:
        res = mf["result"]
        faults: dict[str, Any] = res.get("faults", {})
        n_silent = sum(1 for f in faults.values() if f.get("outcome") == "silent")
        out += [
            f"## Topic setting: {res.get('topic_mode', '?')} (manifest `{name}`, git {str(mf.get('git_sha'))[:10]}, "
            f"passed {res.get('passed')})",
            "",
            f"{n_silent} silent corruption detected by the reconciler across {len(faults)} fault rows (F6a and F6b are one fault).",
            "",
            "| ID | Fault | Pre-registered expectation | Detection fired | Recovery | Outcome | Residual effect |",
            "|---|---|---|---|---|---|---|",
        ]
        for fid in TABLE_ORDER:
            f = faults.get(fid)
            if not f:
                out.append(f"| {fid} | {md_cell(PREREGISTERED[fid]['fault'])} | | | | not run | |")
                continue
            exp = f"{f['expected_detection']} -> {f['expected_outcome']}"
            fired = ", ".join(DETECTION_LABELS.get(d, d) for d in f.get("detections_fired") or []) or "none"
            if f.get("expected_detection_fired") is False:
                fired += " (expected, did not fire)"
            out.append(
                f"| {fid} | {md_cell(f['fault'])} | {md_cell(exp)} | {md_cell(fired)} | {md_cell(f['recovery'])} | "
                f"{DETAIL_WORDS.get(f['outcome'], f['outcome'])} | {md_cell(f.get('residual') or 'none observed')} |"
            )
        gates = [c for c in res.get("checks", []) if c["check"].endswith("no __debezium_unavailable_value in silver")]
        pre = PREREGISTERED["F13"]
        out.append(
            f"| F13 | {md_cell(pre['fault'])} | {md_cell(pre['detection'] + ' -> ' + pre['outcome'])} | "
            f"checked at every gate, not injected | none | "
            f"{'clean' if gates and all(c['ok'] for c in gates) else 'see gates'} at {sum(1 for c in gates if c['ok'])}/{len(gates)} gates | none observed |"
        )
        out += ["", "Limits:", *[f"- {lim}" for lim in res.get("limits", [])], ""]
    return "\n".join(out) + "\n"


# ---- state ------------------------------------------------------------------------------------------------------------


@dataclass
class Ctx:
    consumer: Any
    mode: str
    sampler: CdcSampler
    started: str
    compacting: bool | None = None
    ledger: dict[str, int] = field(default_factory=dict)  # bronze records appended twice on purpose (F1)
    run_ids: list[str] = field(default_factory=list)
    loads: list[dict[str, Any]] = field(default_factory=list)
    faults: dict[str, dict[str, Any]] = field(default_factory=dict)
    gates: dict[str, dict[str, Any]] = field(default_factory=dict)
    seconds: dict[str, float] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    carried: set[str] = field(default_factory=set)  # mismatch keys of earlier failed gates (.run only: raw ids)
    first_dirty: str | None = None
    broken: bool = False


def save_state(ctx: Ctx) -> None:
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(
        json.dumps(
            {
                "mode": ctx.mode, "started": ctx.started, "compacting": ctx.compacting, "ledger": ctx.ledger,
                "run_ids": ctx.run_ids, "loads": ctx.loads, "faults": ctx.faults, "gates": ctx.gates,
                "seconds": ctx.seconds, "details": ctx.details, "carried": sorted(ctx.carried),
                "first_dirty": ctx.first_dirty, "broken": ctx.broken, "results": RESULTS,
            },
            default=str,
        )
    )  # fmt: skip


def load_state(ctx: Ctx) -> None:
    st = json.loads(STATE.read_text())
    if st["mode"] != ctx.mode:
        raise SystemExit(f"state was saved with --topics {st['mode']}; not {ctx.mode}")
    ctx.started, ctx.compacting, ctx.ledger = st["started"], st["compacting"], st["ledger"]
    ctx.run_ids, ctx.loads, ctx.faults, ctx.gates = st["run_ids"], st["loads"], st["faults"], st["gates"]
    ctx.seconds, ctx.details, ctx.carried = st["seconds"], st["details"], set(st["carried"])
    ctx.first_dirty, ctx.broken = st["first_dirty"], st["broken"]
    RESULTS[:] = st["results"]


# ---- stack helpers ------------------------------------------------------------------------------------------------------


def volume_run(*argv: str, rw: bool = False) -> subprocess.CompletedProcess[str]:
    """One command in a throwaway container with the Spark checkpoint volume mounted at /checkpoints."""
    return subprocess.run(
        ["docker", "run", "--rm", "--pull", "never", "-v", f"{rc.CHECKPOINT_VOLUME}:/checkpoints{'' if rw else ':ro'}",
         "--entrypoint", argv[0], g3.SPARK_IMAGE_REF, *argv[1:]],
        capture_output=True, encoding="utf-8", errors="replace", check=False, timeout=120,
    )  # fmt: skip


def fault_file_present() -> bool:
    return volume_run("test", "-e", "/checkpoints/fault").returncode == 0


def checkpoint_names(query: str, directory: str) -> list[str]:
    r = volume_run("ls", "-1a", f"/checkpoints/{query}/{directory}")
    if r.returncode != 0:
        raise RuntimeError(f"cannot list {query}/{directory}")
    return r.stdout.split()


def connector_state() -> tuple[str, str | None]:
    """(connector state, task 0 state): ABSENT when Connect answers 404, UNREACHABLE when it does not answer."""
    try:
        _, st = http("GET", f"{CONNECT}/connectors/pg-shop/status")
    except RuntimeError as e:
        return ("ABSENT", None) if "404" in str(e) else ("UNREACHABLE", None)
    except Exception:  # noqa: BLE001 - Connect down or restarting
        return "UNREACHABLE", None
    tasks = st.get("tasks", [])
    return str(st["connector"]["state"]), (str(tasks[0]["state"]) if tasks else None)


def rendered_connector_config() -> dict[str, str]:
    return dict(json.loads(string.Template((ROOT / "infra/connect/pg-shop.json").read_text()).substitute(all_env())))


def connector_config_matches() -> bool:
    try:
        _, live = http("GET", f"{CONNECT}/connectors/pg-shop/config")
    except Exception:  # noqa: BLE001
        return False
    return {k: v for k, v in live.items() if k != "name"} == rendered_connector_config()


def recover_connector(timeout: float = 300) -> tuple[bool, bool, float]:
    """Wait for the connector and its task to be RUNNING; restart a FAILED task once per sighting (a broker restart
    takes the registry down with it). Returns (running, restarted at least once, seconds)."""
    t0 = time.monotonic()
    restarted = False
    while time.monotonic() - t0 < timeout:
        state, task = connector_state()
        if (state, task) == ("RUNNING", "RUNNING"):
            return True, restarted, time.monotonic() - t0
        if task == "FAILED":
            restarted = True
            g2._try(lambda: g2.connect("POST", "/connectors/pg-shop/tasks/0/restart"))
        elif state == "FAILED":
            restarted = True
            g2._try(lambda: g2.connect("POST", "/connectors/pg-shop/restart?includeTasks=true"))
        time.sleep(3)
    return False, restarted, time.monotonic() - t0


def next_txn() -> int:
    """The oracle's end: gen.state.next_txn, not gen.end_txn."""
    return int(g2.psql("SELECT coalesce((SELECT next_txn FROM gen.state WHERE id = 1), 0)") or 0)


def topic_ends(c: Any) -> dict[str, dict[int, int]]:
    out: dict[str, dict[int, int]] = {}
    for t in TABLES:
        ends = g2.end_offsets(c, f"cdc.public.{t}")
        if ends:
            out[f"cdc.public.{t}"] = ends
    return out


def total_records(c: Any, table: str) -> int:
    return sum(g2.end_offsets(c, f"cdc.public.{table}").values())


def wait_caught_up(c: Any, queries: Sequence[str] = ("silver", "bronze"), timeout: float = 420) -> bool:
    """Spark's committed offsets (checkpoint files, never the topic's record count) cover the topics' end offsets."""

    def done() -> bool:
        ends = topic_ends(c)
        try:
            return all(not uncovered(rc.checkpoint_offsets(q), ends) for q in queries)
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError):
            return False

    return bool(g2.wait_for(done, timeout, 5.0))


def wait_stable(c: Any, quiet_s: float = 30, timeout: float = 300) -> bool:
    """Total records over the CDC topics unchanged for `quiet_s` seconds."""
    deadline = time.monotonic() + timeout
    last, since = -1, time.monotonic()
    while time.monotonic() < deadline:
        total = sum(sum(p.values()) for p in topic_ends(c).values())
        if total != last:
            last, since = total, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return True
        time.sleep(3)
    return False


def burst(ctx: Ctx, rate: float, txns: int) -> dict[str, Any]:
    s = run_once(SEED, duration=1e9, rate=rate, warmup=0, max_txns=txns)
    ctx.run_ids.append(s["run_id"])
    return s


def burst_and_wait(ctx: Ctx, rate: float, txns: int) -> bool:
    """A generator burst, then wait until every event is in Kafka (counted from the generator's own ops), Spark's silver and
    bronze committed offsets cover them, and the index is consistent."""
    c = ctx.consumer
    before = {t: total_records(c, t) for t in TABLES}
    s = burst(ctx, rate, txns)
    a, b = s["txn_range"]
    want = expected_events(g2.generator_ops(a, b - a))
    ok = g2.wait_for(lambda: all(total_records(c, t) >= before[t] + want.get(t, 0) for t in TABLES), 300, 2.0)
    return bool(ok) and wait_caught_up(c) and bool(g6.wait_consistent(300)[0])


class Load(g3.LoadGenerator):
    """g3.LoadGenerator that keeps each chunk's summary (run id, achieved rate) and the thread's error."""

    def __init__(self, ctx: Ctx, fid: str, rate: int = 60, chunk_txns: int = 300) -> None:
        super().__init__(rate, chunk_txns)
        self.ctx, self.fid = ctx, fid
        self.summaries: list[dict[str, Any]] = []
        self.error: str | None = None

    def _run(self) -> None:
        try:
            while not self.stop.is_set():
                s = run_once(SEED, duration=1e9, rate=self.rate, warmup=0, max_txns=self.chunk)
                self.end_txn = s["txn_range"][1]
                self.summaries.append(s)
                self.ctx.run_ids.append(s["run_id"])
        except BaseException as e:  # noqa: BLE001 - a thread must not die silently; the error is reported at exit
            self.error = mask(f"{type(e).__name__}: {e}", 200)

    def __enter__(self) -> Load:
        super().__enter__()
        return self

    def __exit__(self, *exc: object) -> None:
        super().__exit__(*exc)
        self.ctx.loads.append(
            {
                "fault": self.fid, "target_changes_per_s": self.rate, "chunks": len(self.summaries),
                "achieved_changes_per_s": [s.get("achieved_changes_per_s") for s in self.summaries],
                "fell_behind": any(bool(s.get("fell_behind")) for s in self.summaries),
            }
        )  # fmt: skip
        if self.error:
            check(f"{self.fid}: the load generator ran without error", False, self.error)


def prefix_since(n0: int, prefix: str) -> None:
    """Rename the checks a g3 phase appended since index n0."""
    for r in RESULTS[n0:]:
        r["check"] = prefix + r["check"]


def event_log() -> list[dict[str, Any]]:
    return g3.read_event_log(["batch_id", "table", "n_events", "written_at", "changes"])


def checksums() -> dict[str, Any]:
    _, res = g3.probe("checksum", "--tables", ",".join(TABLES))
    return dict(res)


def bronze_stats() -> dict[str, tuple[int, int]]:
    _, res = g3.probe("bronze-stats")
    return {t: (int(v["rows"]), int(v["distinct_offsets"])) for t, v in res.items()}


# ---- restoration, preflight, gate ---------------------------------------------------------------------------------------


def restore_schema() -> list[str]:
    """Backstop for g3's own try/finally: put back the two out-of-band schema changes if a phase died halfway."""
    notes = []
    if g2.psql(
        "SELECT 1 FROM information_schema.columns WHERE table_name = 'help_articles' AND column_name = 'locale_renamed'"
    ):
        g2.psql("ALTER TABLE help_articles RENAME COLUMN locale_renamed TO locale")
        notes.append("renamed help_articles.locale_renamed back")
    prec = g2.psql(
        "SELECT numeric_precision || '|' || numeric_scale FROM information_schema.columns WHERE table_name = 'refunds' AND column_name = 'amount'"
    )
    if prec and prec != "12|2":
        g2.psql("ALTER TABLE refunds ALTER COLUMN amount TYPE numeric(12,2)")
        notes.append("restored refunds.amount to numeric(12,2)")
    return notes


def restore_stack() -> list[str]:
    """The `finally` of every fault: fault file cleared, subject compatibility reset, schema restored, services up, the
    connector running with the unmodified config. Each step is independent; failures are noted, not raised."""
    notes: list[str] = []

    def step(name: str, fn: Callable[[], Any]) -> None:
        try:
            out = fn()
            if out:
                notes.append(f"{name}: {out}")
        except (Exception, SystemExit) as e:  # noqa: BLE001 - restoration is best effort
            notes.append(f"{name} failed: {mask(str(e), 120)}")

    step("fault file", g3.clear_fault_file)
    for s in SUBJECTS:
        step("subject compatibility", partial(g3.set_subject_compat, s, None))
    step("schema", lambda: "; ".join(restore_schema()))
    step("services", lambda: cmp("up", "-d") and None)  # starts what is stopped, leaves the rest alone
    g2.wait_for(lambda: connector_state()[0] != "UNREACHABLE", 120, 3.0)  # Connect may still be starting
    state = connector_state()[0]
    if state not in ("ABSENT", "UNREACHABLE"):
        step("connector", lambda: None if recover_connector(180)[0] else "not RUNNING")
        if not connector_config_matches():

            def reapply() -> str:
                ensure_connector(all_env())
                return "re-applied pg-shop.json"

            step("connector config", reapply)
    return notes


def preflight(fid: str) -> bool:
    problems = [f"{s} not running" for s in SERVICES if not service_running(s)]
    state = connector_state()
    if fid == "F4":
        if state[0] != "ABSENT":
            problems.append(f"a pg-shop connector already exists or Connect is unreachable ({state[0]})")
    elif state != ("RUNNING", "RUNNING"):
        problems.append(f"connector {state}")
    elif not connector_config_matches():
        problems.append("connector config differs from the rendered pg-shop.json")
    try:
        if fault_file_present():
            problems.append("fault file present")
    except subprocess.SubprocessError:
        problems.append("fault file unreadable")
    if rc.generator_running():
        problems.append("a generator container is running")
    return check(
        f"{fid}: preflight (services running, connector healthy, no fault file, no generator)",
        not problems,
        "; ".join(problems),
    )


def ensure_mode(ctx: Ctx) -> None:
    """Once: every CDC topic's cleanup.policy matches --topics (read from the broker, never inferred from data)."""
    if ctx.compacting is not None:
        return
    want = ctx.mode == "compact"
    got = rc.detect_compacting()
    check("topics: every CDC topic's cleanup.policy matches --topics", set(got.values()) == {want},
          f"--topics {ctx.mode}; compacting per table {json.dumps(got, sort_keys=True)}")  # fmt: skip
    ctx.compacting = want
    ctx.details["topics_observed"] = got


@dataclass
class GateOut:
    ok: bool
    report: Any | None
    seconds: float
    keys: set[str]
    error: str | None = None


def run_gate(ctx: Ctx, fid: str) -> GateOut:
    """The reconciliation gate: ten checks prefixed with the label, fed to check(). Raw ids stay in the report's diff
    (written by reconcile to .run/g10a-diff-<label>.json)."""
    ensure_mode(ctx)
    t0 = time.monotonic()
    try:
        report = rc.reconcile(ctx.consumer, fid, sum(ctx.ledger.values()), bool(ctx.compacting), deleted_ever=None)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - a gate that cannot run is a failed gate
        msg = mask(f"{type(e).__name__}: {e}", 300)
        print(mask(traceback.format_exc(), 3000), file=sys.stderr)
        check(f"{fid} reconcile: ran to completion", False, msg)
        return GateOut(False, None, time.monotonic() - t0, set(), msg)
    for name, ok, detail in report.gate_checks:
        check(name, ok, detail)
    keys = {f"{t}:{kind}:{i}" for t, kinds in report.diff().items() for kind, ids in kinds.items() for i in ids}
    ctx.gates[fid] = {k: v for k, v in report.to_dict().items() if k != "gate_checks"}
    return GateOut(bool(report.ok), report, time.monotonic() - t0, keys)


def oracle(ctx: Ctx, fid: str) -> dict[str, int] | None:
    try:
        res = g3.compare_to_ground_truth(g3.silver_rows(TABLES), next_txn())
    except (Exception, SystemExit) as e:  # noqa: BLE001
        check(f"{fid} oracle: silver equals ground truth", False, mask(str(e), 200))
        return None
    check(f"{fid} oracle: silver equals ground truth", g3.gt_ok(res), json.dumps(res))
    return dict(res)


Post = Callable[[GateOut], None]
FaultFn = Callable[[Ctx, dict[str, Any]], Post | None]


def run_fault(ctx: Ctx, fid: str, fn: FaultFn) -> None:
    print(f"\n{fid} {PREREGISTERED[fid]['fault']}", flush=True)
    fr = ctx.faults.setdefault(fid, new_record(fid))
    if ctx.broken:
        fr["outcome"] = "skipped"
        check(f"{fid}: skipped, an earlier precondition failed", False)
        return
    if not preflight(fid):
        ctx.broken = True
        fr["outcome"] = "skipped"
        return
    t0 = time.monotonic()
    fr["injected_at"] = now_iso()
    fr["detections_fired"] = []
    post: Post | None = None
    try:
        post = fn(ctx, fr)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - an injection that crashes is a finding; restore, then gate
        print(mask(traceback.format_exc(), 3000), file=sys.stderr)
        check(f"{fid}: injection ran to completion", False, mask(f"{type(e).__name__}: {e}", 300))
    fr["notes"]["restoration"] = restore_stack()
    gate = run_gate(ctx, fid)
    res = oracle(ctx, fid) if fid in ORACLE_FAULTS else None
    if post is not None:
        try:
            post(gate)
        except (Exception, SystemExit) as e:  # noqa: BLE001
            check(f"{fid}: post-gate checks ran to completion", False, mask(f"{type(e).__name__}: {e}", 300))
    fr["recovered_s"] = round(time.monotonic() - t0, 1)
    fr["gate_ok"] = gate.ok
    fr.update(counts_from(gate.report, res))
    new = gate.keys - ctx.carried
    fr["new_mismatches"] = len(new)
    if not gate.ok:
        if ctx.first_dirty and not new:
            fr["contaminated_by"] = ctx.first_dirty
        ctx.first_dirty = ctx.first_dirty or fid
        ctx.carried |= gate.keys
    score(fid, fr, gate)
    ctx.seconds[fid] = round(time.monotonic() - t0, 1)
    save_state(ctx)


def score(fid: str, fr: dict[str, Any], gate: GateOut) -> None:
    fired: list[str] = fr["detections_fired"]
    cls, outcome, passed = score_fault(fid, gate.ok, fired)
    fr["classified"], fr["outcome"] = cls, outcome
    expects = EXPECTS_DETECTION[fid]
    fr["expected_detection_fired"] = bool(fired) if expects else None
    said = DETAIL_WORDS[outcome]
    detail = f"outcome {said}; classify says {cls}; gate {'clean' if gate.ok else 'NOT clean'}; detections fired {fired or 'none'}"
    if expects and not fired:
        detail += "; the expected detection did not fire (recorded, not required)"
    if not gate.ok and fired:
        detail += "; a detection fired but the reconciler is not clean, so this fails"
    check(
        f"{fid} scoring: scored against the pre-registered matrix (clean gate required, SILENT fails{', CAUGHT required' if fid == 'F6b' else ''})",
        passed,
        detail,
    )


# ---- F4: connector crash mid-snapshot ------------------------------------------------------------------------------------


def fault_f4(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    c = ctx.consumer
    env = all_env()
    burst(ctx, 0, 1500)
    rows = {t: int(g2.psql(f"SELECT count(*) FROM {t}")) for t in TABLES}
    window_hi = max(rows["transactions"] // 2, SNAPSHOT_KILL_MIN + 1)
    at_kill: int | None = None
    t_kill = 0.0
    with Load(ctx, "F4", 8, 100) as load:
        fr["injected_at"] = now_iso()
        ensure_connector(env, overrides=SNAPSHOT_OVERRIDES)
        deadline = time.monotonic() + 300
        seen = 0
        while time.monotonic() < deadline:
            seen = total_records(c, "transactions")
            if seen >= SNAPSHOT_KILL_MIN:
                if seen < window_hi:
                    g3.sh("docker", "kill", "adrp-connect-1")
                    at_kill, t_kill = seen, time.monotonic()
                break
            time.sleep(0.1)
        load_end = load.end_txn
    # the load has stopped: the writers are gone before Connect comes back (ADR-0020, retried-snapshot limit)
    time.sleep(2)
    ends_kill = {t: total_records(c, t) for t in TABLES}
    counts2 = {t: int(g2.psql(f"SELECT count(*) FROM {t}")) for t in TABLES}
    check("F4: Connect was SIGKILLed inside the snapshot (0 < r events < table rows)",
          at_kill is not None and 0 < at_kill < rows["transactions"],
          f"r events on transactions at the kill {at_kill if at_kill is not None else f'none, window missed at {seen}'} of {rows['transactions']} rows")  # fmt: skip
    cmp("up", "-d", "--wait", "connect", timeout=600)
    t_back = time.monotonic()
    running, restarted, _ = recover_connector(300)
    target = {t: ends_kill[t] + counts2[t] for t in TABLES}
    reemitted = g2.wait_for(lambda: all(total_records(c, t) >= target[t] for t in TABLES), 900, 3.0)
    stable = wait_stable(c)
    ensure_connector(env)  # PUT replaces the whole config: the throttling overrides are gone
    dup_keys: dict[str, int] = {}
    for t in TABLES:
        topic = f"cdc.public.{t}"
        recs = rc.read_raw_range(topic, {}, g2.end_offsets(c, topic))
        dup_keys[t] = dup_r_keys(
            (
                r.partition,
                r.key,
                g2.decode_record(topic, r.partition, r.offset, r.key, r.value)["op"] if r.value else None,
            )
            for r in recs
        )
    check("F4: the snapshot restarted and re-emitted rows (keys with 2+ r events > 0)", sum(dup_keys.values()) > 0 and bool(reemitted),
          f"keys with 2+ r events {json.dumps(dup_keys, sort_keys=True)}; topics reached the re-emission target {bool(reemitted)}; connector RUNNING {running}; topics stable {stable}")  # fmt: skip
    win = ctx.sampler.window(t_kill, t_back) if at_kill is not None else {}
    fired = bool(win.get("connector_status_fired"))
    check("F4: exporter sampler covered the Connect outage (connector-status check fired: recorded, not required)",
          bool(win.get("exporter_answered")), json.dumps({**win, "fired": fired}))  # fmt: skip
    if fired:
        fr["detections_fired"].append("connector_status")
    matches = connector_config_matches()
    check(
        "F4: connector config restored to pg-shop.json",
        matches,
        "live config equals the rendered file" if matches else "live config differs",
    )
    fr["notes"]["f4"] = {
        "rows_at_start": rows,
        "r_events_at_kill": at_kill,
        "keys_with_2plus_r_events": dup_keys,
        "task_restarted": restarted,
        "txns_before_kill": load_end,
    }
    fr["residual"] = (
        f"{sum(dup_keys.values())} keys hold 2+ snapshot events in the topics (bronze keeps them by design); silver holds one row per key"
    )
    return None


# ---- F1: duplicate events ---------------------------------------------------------------------------------------------------


def rewind_silver(keep_last: int) -> tuple[dict[str, dict[int, tuple[int, int]]], int, int, int]:
    """With spark-streams stopped: remove the newest `keep_last` committed batches (and anything newer) from silver's
    checkpoint. Returns (replayed ranges, M, N, removed files). Only silver is touched: bronze's checkpoint stays (the
    Iceberg sink skips epochs it committed, ADR-0020)."""
    commits = checkpoint_names("silver", "commits")
    offsets = checkpoint_names("silver", "offsets")
    plan = rewind_paths("/checkpoints/silver", commits, offsets, keep_last)
    if plan is None:
        raise SystemExit(f"silver has fewer than {keep_last + 1} committed batches: nothing to replay")
    m, n, paths = plan
    newest = newest_offsets_file(offsets)
    assert newest is not None
    start = parse_offsets_file(volume_run("cat", f"/checkpoints/silver/offsets/{m}").stdout)
    old_end = parse_offsets_file(volume_run("cat", f"/checkpoints/silver/offsets/{newest}").stdout)
    assert all(p.startswith("/checkpoints/silver/") for p in paths)
    r = volume_run("rm", "-f", *paths, rw=True)
    if r.returncode != 0:
        raise SystemExit(f"checkpoint rewind failed: {mask(r.stderr, 200)}")
    return replayed_ranges(start, old_end), m, n, len(paths)


def fault_f1(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    c = ctx.consumer
    in_sync = burst_and_wait(ctx, 60, 300)
    sums0, pts0, bs0 = checksums(), g6.stable_points(), bronze_stats()
    ev0 = {entry_key(e) for e in event_log()}
    fr["injected_at"] = now_iso()
    cmp("stop", "-t", "60", "spark-streams")
    silver_ranges, m, n, removed = rewind_silver(3)
    ends = {t: g2.end_offsets(c, f"cdc.public.{t}") for t in ("customers", "support_tickets")}
    bronze_ranges = {f"cdc.public.{t}": last_n_ranges(e, 200) for t, e in ends.items()}
    _, res = sparkcli.call(
        FAULT_SCRIPT, "replay-bronze", "--bootstrap", "redpanda:9092", "--ranges", json.dumps(bronze_ranges)
    )
    read = int(res["read"])  # the count actually read, which can be below the range (compaction)
    ctx.ledger["F1"] = read
    cmp("up", "-d", "spark-streams")
    caught_up = wait_caught_up(c)
    entries = event_log()
    reruns = rerun_batches(ev0, entries, n)
    size = range_size(silver_ranges)
    check("F1: the silver consumer re-ran a non-empty offset range (event log shows the re-run batch with n_events > 0)",
          bool(reruns) and size > 0 and caught_up,
          f"replayed {size} offsets of silver over batches {m + 1}..{n} ({removed} checkpoint files removed); batches re-run in the event log {len(reruns)}; caught up {caught_up}")  # fmt: skip
    sums1 = checksums()
    check("F1: silver content unchanged by the replay (count + checksum, 7 tables)", sums0 == sums1 and in_sync,
          f"tables compared {len(sums1)}; index in sync before {in_sync}")  # fmt: skip
    bs1 = bronze_stats()
    d_rows = sum(bs1[t][0] - bs0[t][0] for t in bs1 if t in bs0)
    d_distinct = sum(bs1[t][1] - bs0[t][1] for t in bs1 if t in bs0)
    check("F1: bronze duplicate rows equal the replayed records exactly (bronze holds duplicates by design)",
          d_rows == read and d_distinct == 0 and read > 0,
          f"records appended again {read}; bronze rows +{d_rows}, distinct offsets +{d_distinct}")  # fmt: skip
    g6.wait_consistent(300)
    pts1 = g6.stable_points()
    check("F1: Qdrant points and payloads unchanged", pts0 == pts1, f"points {len(pts1)}")
    fr["notes"]["f1"] = {
        "silver_replayed_offsets": size,
        "batches_removed": [m + 1, n],
        "bronze_records_appended": read,
        "bronze_ranges_requested": sum(e - s for t in bronze_ranges.values() for s, e in t.values()),
    }
    fr["residual"] = f"bronze holds {read} duplicate rows by design (replay ledger); silver unchanged"
    return None


# ---- F2: out-of-order updates ------------------------------------------------------------------------------------------------


def decode_versions(table: str, recs: Sequence[rc.RawRecord]) -> list[Version]:
    out = []
    topic = f"cdc.public.{table}"
    for r in recs:
        if r.value is None:
            out.append(Version(table, r.partition, r.offset, r.key, None, None, None, None))
            continue
        d = g2.decode_record(topic, r.partition, r.offset, r.key, r.value)
        out.append(
            Version(table, r.partition, r.offset, r.key, r.value, int((d["pk"] or {}).get("id", -1)), d["lsn"], d["op"])
        )
    return out


def produce_raw(items: Sequence[Version]) -> tuple[int, list[str]]:
    """Produce each version's key and value bytes, unchanged, to the same topic and partition. (delivered, errors)."""
    delivered: list[int] = []
    errors: list[str] = []

    def cb(err: Any, msg: Any) -> None:
        if err is not None:
            errors.append(str(err)[:100])
        else:
            delivered.append(msg.offset())

    p = Producer({"bootstrap.servers": rc.KAFKA_BOOTSTRAP})
    for v in items:
        p.produce(f"cdc.public.{v.table}", key=v.key, value=v.value, partition=v.partition, on_delivery=cb)
    p.flush(60)
    return len(delivered), errors


def fault_f2(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    c = ctx.consumer
    rng = random.Random(SEED)
    pre = {t: g2.end_offsets(c, f"cdc.public.{t}") for t in ("accounts", "customers", "support_tickets")}
    in_sync = burst_and_wait(ctx, 60, 300)
    # a scripted ad-hoc article: insert, update, logical delete (the resurrection test)
    g2.psql(f"DELETE FROM help_articles WHERE id = {ADHOC_ARTICLE}")  # only matters on a resumed stack
    ha0 = g2.end_offsets(c, "cdc.public.help_articles")
    g2.psql(
        f"INSERT INTO help_articles (id, title, body, category, view_count) VALUES ({ADHOC_ARTICLE}, 'g10a f2', 'g10a f2 body', 'g10a', 0)"
    )
    g2.psql(f"UPDATE help_articles SET view_count = 1, updated_at = now() WHERE id = {ADHOC_ARTICLE}")
    g2.psql(f"DELETE FROM help_articles WHERE id = {ADHOC_ARTICLE}")
    g2.wait_for(lambda: total_records(c, "help_articles") >= sum(ha0.values()) + 4, 120, 2.0)  # c, u, d, tombstone
    in_sync = wait_caught_up(c) and bool(g6.wait_consistent(300)[0]) and in_sync
    sums0, pts0 = checksums(), g6.stable_points()
    ev0 = {entry_key(e) for e in event_log()}
    silver_tomb = [r for r in g3.silver_rows(["help_articles"]).get("help_articles", []) if r["id"] == ADHOC_ARTICLE]
    was_tomb = bool(silver_tomb) and all(r["is_deleted"] for r in silver_tomb)

    versions: list[Version] = []
    for t, start in pre.items():
        versions += decode_versions(
            t, rc.read_raw_range(f"cdc.public.{t}", start, g2.end_offsets(c, f"cdc.public.{t}"))
        )
    older = pick_older(versions, rng, 3)
    ad_hoc = [
        v
        for v in decode_versions(
            "help_articles",
            rc.read_raw_range("cdc.public.help_articles", ha0, g2.end_offsets(c, "cdc.public.help_articles")),
        )
        if v.pk == ADHOC_ARTICLE and v.value is not None and v.op in ("c", "u")
    ]
    ad_hoc.sort(key=lambda v: v.lsn or 0)
    items = [*older, *ad_hoc]
    hwm0 = sum(total_records(c, t) for t in TABLES)
    fr["injected_at"] = now_iso()
    delivered, errors = produce_raw(items)
    hwm1 = sum(total_records(c, t) for t in TABLES)
    check("F2: N older-LSN records were produced after newer ones (high watermarks rose by exactly N)",
          len(items) > len(ad_hoc) and delivered == len(items) and hwm1 - hwm0 == len(items) and not errors,
          f"N={len(items)}: produced {delivered} of {len(items)}, of which {len(ad_hoc)} for the ad-hoc article; high watermarks +{hwm1 - hwm0}; errors {errors[:2]}")  # fmt: skip
    caught_up = wait_caught_up(c)
    want: dict[tuple[str, int], int] = {}
    for v in items:
        if v.pk is not None and v.lsn is not None:
            key = (v.table, v.pk)
            want[key] = max(want.get(key, -1), v.lsn)
    seen = changes_seen(event_log(), ev0, want)
    check("F2: the MERGE received the older LSNs (event log changes contain them, non-vacuous)", seen == len(want) and bool(want),
          f"older (table, key, lsn) triples in the event log {seen} of {len(want)}; silver caught up {caught_up}")  # fmt: skip
    sums1 = checksums()
    check(
        "F2: silver content unchanged (count + checksum, 7 tables)",
        sums0 == sums1 and in_sync,
        f"tables compared {len(sums1)}; index in sync before {in_sync}",
    )
    after = [r for r in g3.silver_rows(["help_articles"]).get("help_articles", []) if r["id"] == ADHOC_ARTICLE]
    check("F2: an older insert after a logical delete did not resurrect the row",
          was_tomb and bool(after) and all(r["is_deleted"] for r in after),
          f"silver tombstone before {was_tomb}; after {bool(after) and all(r['is_deleted'] for r in after)}; older records re-produced for the article {len(ad_hoc)}")  # fmt: skip
    g6.wait_consistent(300)
    pts1 = g6.stable_points()
    check("F2: Qdrant points and payloads unchanged", pts0 == pts1, f"points {len(pts1)}")
    fr["notes"]["f2"] = {
        "older_records_produced": len(items),
        "ad_hoc_records": len(ad_hoc),
        "tables": sorted({v.table for v in older}),
    }
    fr["residual"] = (
        f"{len(items)} stale images re-produced at new offsets in the topics (inventory row 34); silver unchanged"
    )
    return None


# ---- F3: Spark kill mid-MERGE --------------------------------------------------------------------------------------------------


def marker_seen() -> bool:
    r = subprocess.run(
        ["docker", "logs", "--since", "3s", "adrp-spark-streams-1"],
        capture_output=True, encoding="utf-8", errors="replace", check=False, timeout=30,
    )  # fmt: skip
    return "FAULT_HOOK point=merge_start" in r.stdout + r.stderr


def kill_after_marker(rng: random.Random) -> bool:
    """Arm the non-parking merge_start hook, wait for the host-side poll to see it print, sleep a seeded 0.2 to 1.5 s so
    the kill lands inside the write, SIGKILL, clear the fault file, restart. g3.kill_at_point only handles parked hooks."""
    for _ in range(12):  # the container may still be starting after the previous kill
        try:
            g3.compose("exec", "-T", "spark-streams", "sh", "-c", "echo merge_start > /checkpoints/fault")
            break
        except SystemExit:
            time.sleep(5)
    else:
        return False
    hit = g2.wait_for(marker_seen, 240, 0.25)
    if hit:
        time.sleep(rng.uniform(0.2, 1.5))
        g3.sh("docker", "kill", "adrp-spark-streams-1")
    g3.clear_fault_file()
    cmp("up", "-d", "spark-streams")
    return bool(hit)


def bronze_txids() -> dict[str, dict[int, int]]:
    """table -> {lsn: txid} of the bronze change behind each current silver row. Snapshot rows (op r, no txid) are left out."""
    p = run_proc([*sparkcli.SPARK, PROBE_SCRIPT, "bronze-txids", "--tables", ",".join(TABLES)], timeout=1800)
    if p.rc != 0:
        raise SystemExit(f"probe bronze-txids exited {p.rc}: {mask(p.err_text, 200)}")
    out: dict[str, dict[int, int]] = defaultdict(dict)
    for line in p.text.splitlines():
        if line.startswith("TXID\t"):
            _, table, js = line.split("\t", 2)
            d = json.loads(js)
            if d.get("op") != "r" and d.get("txid") is not None:
                out[table][int(d["lsn"])] = int(d["txid"])
    return out


def xid_to_txn(run_ids: Sequence[str]) -> dict[int, int]:
    """xid32 -> txn_no from the run logs of this check's own generator chunks. Only finished chunks have a run log."""
    import pyarrow.parquet as pq

    out: dict[int, int] = {}
    for rid in run_ids:
        for f in sorted((ROOT / "runs/data" / rid / "run_log").glob("*.parquet")):
            try:
                for r in pq.read_table(f, columns=["txn_no", "xid32"]).to_pylist():
                    out[int(r["xid32"])] = int(r["txn_no"])
            except Exception:  # noqa: BLE001 - a file still being written: the chunk is not finished
                continue
    return out


def continuous_mode(ctx: Ctx) -> dict[str, int]:
    """Judge each silver row against the generator's version of its key in the transaction that wrote it (per key, never
    against live Postgres). Wrong values are caught; lag and loss are not."""
    ops = g2.generator_ops(0, next_txn())
    xids = xid_to_txn(ctx.run_ids)
    txids = bronze_txids()
    silver = g3.silver_rows(TABLES)
    total = {"judged": 0, "unjudged": 0, "mismatches": 0}
    for t in TABLES:
        sample = next((op.row for op in ops if op.table == t and op.row is not None), None)
        if sample is None:
            continue
        pg_cols = rc.parse_columns(
            g2.psql(
                f"SELECT column_name || '|' || data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{t}' ORDER BY ordinal_position"
            )
        )
        cols = [(n, ty) for n, ty in pg_cols if n in sample]  # generator-controlled columns only
        r = continuous.judge_at_lsn(silver.get(t, []), txids.get(t, {}), xids, ops, cols, table=t)
        for k in total:
            total[k] += r[k]
    return total


def fault_f3(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    rng = random.Random(SEED)
    start = next_txn()
    inflight = 0
    parked = False
    cont: dict[str, int] = {}
    with Load(ctx, "F3", 60, 300) as load:
        fr["injected_at"] = now_iso()
        parked = g3.kill_at_point("mid_merge")
        if parked:
            time.sleep(45)
        for _ in range(3):
            if kill_after_marker(rng):
                inflight += 1
            time.sleep(45)
        g3.clear_fault_file()
        cmp("up", "-d", "spark-streams")
        time.sleep(45)
        try:
            cont = continuous_mode(ctx)
        except (Exception, SystemExit) as e:  # noqa: BLE001
            check("F3 continuous: ran to completion", False, mask(str(e), 200))
        end = load.end_txn or start
    check("F3: SIGKILLed spark-streams at mid_merge (between two tables' MERGEs in one batch)", parked)
    check(
        f"F3: SIGKILLed spark-streams after merge_start under load (in-flight attempts={inflight})",
        inflight == 3,
        f"{inflight} of 3 kills landed after the marker",
    )
    check("F3: changes kept flowing during the kills", end > start, f"txns {start} -> {end}")
    if cont:
        check("F3 continuous: silver rows match the ground-truth version at their own LSN (judged>0, mismatches=0)",
              cont["judged"] > 0 and cont["mismatches"] == 0, json.dumps(cont))  # fmt: skip
    fr["notes"]["f3"] = {"parked_kill": parked, "in_flight_kills": inflight, "continuous": cont}
    fr["residual"] = "orphan files from the killed writers are counted and removed physically at END"
    return None


# ---- F5: Redpanda restart ----------------------------------------------------------------------------------------------------


def wait_flush_advanced(before: str | None, timeout: float) -> tuple[bool, str | None, float]:
    """Poll the slot until confirmed_flush_lsn is above `before`. (reached, last value read, seconds). Debezium flushes
    it from its offset commit (every 60 s here), so a connector that is alive and has changes to forward moves it at
    least once per commit; RUNNING in the Connect status says nothing about that."""
    base = lsn_to_int(before) if before else -1
    last: list[str | None] = [before]

    def moved() -> bool:
        last[0] = rc.confirmed_flush_text()
        return last[0] is not None and lsn_to_int(last[0]) > base

    ok, waited = wait_until(moved, timeout, 3.0)
    return ok, last[0], waited


def fault_f5(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    manual_up = False
    flush0: str | None = None
    advanced, flush1, flush_wait = False, None, 0.0
    with Load(ctx, "F5", 60, 300):
        time.sleep(30)
        n0 = next_txn()
        t_inj = time.monotonic()
        fr["injected_at"] = now_iso()
        cmp("restart", "-t", "30", "redpanda", timeout=300)
        cmp("up", "-d", "--wait", "redpanda", timeout=600)
        n1 = next_txn()
        running, restarted, _ = recover_connector(300)
        t_conn = time.monotonic()
        try:
            st = g3.container_state()
        except (Exception, SystemExit):  # noqa: BLE001
            st = {"running": False, "restart_count": None}
        if not st.get("running"):
            manual_up = True
            cmp("up", "-d", "spark-streams")
            time.sleep(20)
            st = g3.container_state()
        flush0 = rc.confirmed_flush_text()
        time.sleep(60)  # load continues after the broker is healthy
        advanced, flush1, flush_wait = wait_flush_advanced(flush0, 150.0)  # two offset-commit intervals more at most
    win = ctx.sampler.window(t_inj, t_conn)
    check(
        "F5: Redpanda restarted while the generator kept writing",
        n1 > n0,
        f"transactions committed during the outage {n1 - n0}",
    )
    check("F5: connector RUNNING after recovery", running, f"task restarted by this check: {restarted}")
    check(
        "F5: the connector forwarded changes after recovery (RUNNING is not proof: confirmed_flush_lsn must advance while the generator writes)",
        advanced,
        f"confirmed_flush_lsn {flush0} -> {flush1} after {60 + flush_wait:.0f} s of load past recovery",
    )
    check(
        "F5: spark-streams running after recovery",
        bool(st.get("running")),
        f"restart count {st.get('restart_count')}; started by this check: {manual_up}",
    )
    check("F5: slot lag and connector status were sampled through the outage", win["exporter_answered"] >= 3,
          json.dumps({**win, "slot_lag_threshold_bytes": 2**30, "breach_claimed": False}))  # fmt: skip
    if win["connector_status_fired"]:
        fr["detections_fired"].append("connector_status")
    fr["notes"]["f5"] = {
        **win, "task_restarted": restarted, "spark_started_by_check": manual_up, "flush_advanced_after_recovery": advanced,
    }  # fmt: skip
    fr["residual"] = (
        "connector not RUNNING after recovery"
        if not running
        else "none observed after recovery"
        if advanced
        else "connector RUNNING but not forwarding changes after recovery (confirmed_flush_lsn frozen)"
    )
    return None


# ---- F6a / F6b: schema drift ----------------------------------------------------------------------------------------------------


def fault_f6a(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    n0 = len(RESULTS)
    drain = g3.Drain(ctx.consumer)  # phase_add_column only does `_ = drain`: g3.Drain's LSN wait is never called here
    try:
        with Load(ctx, "F6a", 8, 100):
            fr["injected_at"] = now_iso()
            g3.phase_add_column(drain)
    finally:
        prefix_since(n0, "F6a: ")
    fr["residual"] = "silver gained the column `locale`"
    return None


def fault_f6b(ctx: Ctx, fr: dict[str, Any]) -> Post | None:
    ensure_mode(ctx)
    n0 = len(RESULTS)
    drain = g3.Drain(ctx.consumer)  # phase_quarantine only does `_ = drain`
    fr["injected_at"] = now_iso()
    try:
        g3.phase_quarantine(drain)
    finally:
        prefix_since(n0, "F6b: ")
    parked_ok = all(r["ok"] for r in RESULTS[n0:])
    if parked_ok:
        fr["detections_fired"].append("quarantine_count")
    # positive control: while the refund is still parked the reconciler must fail on it (proves it can fail)
    try:
        ctrl = rc.reconcile(
            ctx.consumer, "F6b-control", sum(ctx.ledger.values()), bool(ctx.compacting), check_qdrant=False
        )
        missing = ctrl.tables["refunds"].missing
        detail = f"reconcile ok {ctrl.ok}; refunds missing {missing}"
        control = missing >= 1 and not ctrl.tables["refunds"].ok
    except (Exception, SystemExit) as e:  # noqa: BLE001
        control, detail = False, mask(str(e), 200)
    check("F6b: before recovery the reconciler reports the parked row as a mismatch (non-vacuous)", control, detail)
    if control:
        fr["detections_fired"].append("reconciler_before_recovery")
    # recovery: a no-op update emits a CDC update carrying the restored schema
    g2.psql("UPDATE refunds SET reason = reason WHERE reason = 'g3-narrowing'")

    def applied() -> bool:
        rows = g3.silver_rows(["refunds"]).get("refunds", [])
        return any(r.get("reason") == "g3-narrowing" and not r["is_deleted"] for r in rows)

    recovered = bool(g2.wait_for(applied, 240, 5.0))

    def post(gate: GateOut) -> None:
        unrec = sum(r.unreconciled_quarantine for r in gate.report.tables.values()) if gate.report is not None else None
        check("F6b: every quarantined key has a later applied silver version after recovery", recovered and unrec == 0,
              f"parked refund applied {recovered}; quarantined keys without a later silver version {unrec}")  # fmt: skip

    fr["residual"] = (
        "quarantined rows remain in quarantine_<table> as the record of the incompatible events; the parked refund was repaired by a no-op update"
    )
    fr["notes"]["f6b"] = {
        "registry_gate": "bypassed (compatibility NONE); silver's gate tested; contracts-ci covers the registry separately"
    }
    return post


# ---- END, manifest ---------------------------------------------------------------------------------------------------------------


def scan_text(ctx: Ctx, rendered: str) -> tuple[list[str], list[str]]:
    """(wording violations, 13-digit identifiers) over every check, the fault records, the notes and the table."""
    texts = [f"{r['check']} {r['detail']}" for r in RESULTS]
    texts += [
        json.dumps(ctx.faults, default=str),
        json.dumps(LIMITS),
        json.dumps(ctx.details, default=str),
        json.dumps(ctx.gates, default=str),
        rendered,
        SCOPE_NOTE,
    ]
    texts += [" ".join(v.values()) for v in PREREGISTERED.values()]
    viol = [v for t in texts for v in wording_violations(t)]
    ids = [m.group(0) for t in texts for m in fids.ID13.finditer(t)]
    return viol, ids


def manifest_result(ctx: Ctx, passed: bool) -> dict[str, Any]:
    silent = sum(1 for f in ctx.faults.values() if f["outcome"] == "silent")
    return {
        "started_at": ctx.started, "passed": passed, "scope_note": SCOPE_NOTE, "topic_mode": ctx.mode,
        "topics_observed": ctx.details.get("topics_observed"),
        "resumed": bool(ctx.details.get("resumed")),
        "summary": f"{len(FAULTS)} faults tested on one machine, 1 run; {silent} silent corruption detected by the reconciler",
        "run_ids": ctx.run_ids, "loads": ctx.loads, "faults": ctx.faults, "gates": ctx.gates,
        "bronze_duplicate_ledger": {**ctx.ledger, "total": sum(ctx.ledger.values())},
        "orphans": ctx.details.get("orphans"), "phase_seconds": ctx.seconds, "limits": LIMITS,
        "preregistered": PREREGISTERED, "recovered_s_definition": "seconds from the start of the injection to the end of the gate and oracle",
        "checks": RESULTS,
    }  # fmt: skip


def phase_end(ctx: Ctx, write: bool) -> bool:
    print("\nEND orphan files, final gate, table, manifest", flush=True)
    t0 = time.monotonic()
    if ctx.broken:
        check("END: skipped the orphan removal and the final gate, an earlier precondition failed", False)
    else:
        cmp("stop", "-t", "60", "spark-streams")
        res: dict[str, Any] = {}
        try:
            _, res = sparkcli.call(FAULT_SCRIPT, "orphans", "--grace-s", "0")
        except (Exception, SystemExit) as e:  # noqa: BLE001
            res = {"total": 0, "orphans_removed": {}, "errors": {"call": mask(str(e), 120)}}
        finally:
            cmp("up", "-d", "spark-streams")
        per = {k: v for k, v in (res.get("orphans_removed") or {}).items() if v}
        total = int(res.get("total", 0))
        ctx.details["orphans"] = {
            "removed_total": total,
            "removed_by_table": per,
            "errors": res.get("errors", {}),
            "grace_s": 0,
        }
        check("END: physical, Iceberg orphan files from the killed writers counted and removed with writers stopped", not res.get("errors"),
              f"{total} orphan files removed physically ({json.dumps(per, sort_keys=True) if per else 'none observed'}); errors {res.get('errors') or 'none'}")  # fmt: skip
        gate = run_gate(ctx, "END")
        if not gate.ok and ctx.first_dirty is None:
            ctx.first_dirty = "END"
        if ctx.mode == "compact":
            effect = compaction_effect(gate.report.topics, gate.report.cut.end) if gate.report is not None else {}
            check("END: compaction physically removed records from Kafka (the offset-gap path was exercised)",
                  compaction_removed(effect),
                  compaction_detail(effect))  # fmt: skip
    ran = [f for f in FAULTS if ctx.faults.get(f, {}).get("outcome") not in (None, "not_run", "skipped")]
    table = render_table([("(this run)", {"result": manifest_result(ctx, False), "git_sha": None})])
    scored = len(ran) == len(FAULTS) and not wording_violations(table)
    check(
        "END: fault table scored against design section 5",
        scored,
        f"faults scored {len(ran)} of {len(FAULTS)}; table wording clean {not wording_violations(table)}",
    )
    viol, ids = scan_text(ctx, table)
    check(
        "manifest: wording and identifier scan clean",
        not viol and not ids,
        f"wording violations {viol[:5]}; 13-digit identifiers {len(ids)}",
    )
    ctx.seconds["END"] = round(time.monotonic() - t0, 1)
    passed = all(r["ok"] for r in RESULTS) and len(ran) == len(FAULTS)
    if write:
        path = write_manifest("g10a-check", "core", SEED, manifest_result(ctx, passed))
        n_ok = sum(r["ok"] for r in RESULTS)
        print(f"{'PASSED' if passed else 'FAILED'} ({n_ok}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}")
    save_state(ctx)
    return passed


FAULT_FNS: dict[str, FaultFn] = {
    "F4": fault_f4, "F1": fault_f1, "F2": fault_f2, "F3": fault_f3, "F5": fault_f5, "F6a": fault_f6a, "F6b": fault_f6b,
}  # fmt: skip


def render_main(paths: Sequence[str]) -> int:
    manifests = [(Path(p).name, json.loads(Path(p).read_text())) for p in paths]
    text = render_table(manifests)
    viol = wording_violations(text)
    if viol:
        print("wording violations in the rendered table:", *viol, sep="\n  ", file=sys.stderr)
        return 1
    TABLE_DOC.parent.mkdir(parents=True, exist_ok=True)
    TABLE_DOC.write_text(text)
    print(f"wrote {TABLE_DOC.relative_to(ROOT)} from {', '.join(n for n, _ in manifests)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="G10a: six faults, each followed by a reconciliation gate.")
    ap.add_argument(
        "--topics",
        choices=["default", "compact"],
        default="default",
        help="the topic setting this stack was bootstrapped with",
    )
    ap.add_argument(
        "--only", choices=PHASES, help="run just this phase on the live stack (state in .run/g10a-state.json)"
    )
    ap.add_argument("--from", dest="from_phase", choices=PHASES, help="run this phase and the ones after it")
    ap.add_argument("--no-manifest", action="store_true")
    ap.add_argument(
        "--render-table",
        nargs="+",
        metavar="MANIFEST",
        help="write docs/results/g10a-fault-table.md from manifests and exit",
    )
    a = ap.parse_args()
    if a.render_table:
        return render_main(a.render_table)
    os.environ["ADRP_FAULT_HOOKS"] = "1"  # every `compose up` here must keep the fault hooks on
    sampler = CdcSampler()
    sampler.start()
    c = g2.consumer()
    ctx = Ctx(c, a.topics, sampler, now_iso())
    resumed = bool(a.only or a.from_phase)
    if resumed and STATE.exists():
        load_state(ctx)
    ctx.details["resumed"] = ctx.details.get("resumed", False) or resumed  # a run of record is a full run
    todo = [a.only] if a.only else list(PHASES[PHASES.index(a.from_phase) :]) if a.from_phase else list(PHASES)
    passed = False
    try:
        for ph in todo:
            if ph == "END":
                passed = phase_end(ctx, write=not a.no_manifest)
            else:
                run_fault(ctx, ph, FAULT_FNS[ph])
    finally:
        sampler.halt.set()
        c.close()
    if "END" not in todo:
        n_ok = sum(r["ok"] for r in RESULTS)
        print(
            f"{'ok' if all(r['ok'] for r in RESULTS) else 'FAILED'} so far ({n_ok}/{len(RESULTS)}); phases run: {','.join(todo)}"
        )
        return 0 if all(r["ok"] for r in RESULTS) else 1
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
