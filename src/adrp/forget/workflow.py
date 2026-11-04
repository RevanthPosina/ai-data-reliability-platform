"""The forget workflow (G8): phase L (logical, under load), phase P (physical, in a maintenance window), checked by
adrp.forget.verify and recorded in privacy.* (adrp.forget.audit). Host-side: it needs docker compose, psql in the
postgres container, the Spark CLI image, pyarrow, and the broker/Qdrant/SeaweedFS endpoints on localhost.

Public entry points (all take ForgetSubject lists; none returns or logs a raw id):

  register(customer_id)                       -> request_id           (privacy.forget_requests, status requested)
  load_subject(request_id, canaries=())       -> ForgetSubject | None (None once the raw id was NULLed: finished)
  presence(subjects)                          -> PresenceResult       (non-vacuity; call BEFORE any forget)
  run_logical(subjects)                       -> LogicalReport        (requested -> logical_done)
  run_physical_window(subjects)               -> WindowReport         (stops spark-streams and the embedder)
  run_qdrant_physical / run_broker_physical / run_postgres_wal (subjects) -> PhysicalReport
  run_resweep_and_purge(subjects)             -> PurgeReport          (logical_done -> physical_done)
  run_physical(subjects) / run_all(subjects)  -> AllReport            (window, qdrant, broker, wal, purge)
  lag_report(request_ids)                     -> per-sink logical/physical lag summaries (manifest-ready)

State machine (privacy.forget_requests.status): requested -> logical_done -> physical_done; any phase may move a
request to failed, which keeps the error and resumes from the start of the phase it was in. Re-running a request
that is physical_done (or whose raw id is already NULL) does nothing.

Ordering rules the code enforces, from ADR-0017/0018/0019:
  * footprint_ids BEFORE the forget transaction; Qdrant delete only AFTER silver shows the tombstone PII-free;
  * 'delivered' means Debezium consumed the whole transaction (slot_confirmed(end_lsn)) AND bronze holds its rows;
  * the physical window runs only for requests that are logical_done, with spark-streams stopped and the embedder
    drained and stopped; services are restarted even when a step fails;
  * the silver tombstone is hard-deleted only after the broker is verified free of the subject; until then the raw
    key is still in silver and the report says so;
  * the raw id (and the scratch footprint) is NULLed only when every physical sink passed, then the privacy tables
    are VACUUM FULLed.
"""

from __future__ import annotations

import contextlib
import json
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from adrp.forget import audit, postgres, sparkcli, verify
from adrp.forget import ids as fids
from adrp.forget.rawscan import COMPOSE, ROOT, Subject, run, service_running, wait_until

EMBEDDER = "embedder"
STREAMS = "spark-streams"
LOGICAL_SINKS_EXCLUDED = ("raw_id_store",)  # only meaningful once the raw id is NULLed (end of phase P)
WINDOW_SINKS = (
    "postgres", "bronze", "silver", "quarantine", "seaweedfs", "spark_checkpoints", "spark_spill",
    "event_logs", "embedder_files", "ground_truth",
)  # fmt: skip
FINAL_SINKS = ("container_logs", "dagster_storage", "metrics", "manifests")
EMBEDDER_SCRIPT = r"""
import json, os, sys
req = json.load(sys.stdin)
mapping = req.get("mapping", {})
path = "/state/embedder.json"
if not os.path.exists(path):
    print(json.dumps({"exists": False, "changed": 0, "snapshot": None, "pending": 0, "pending_footprint": 0}))
    sys.exit(0)
state = json.load(open(path))
n = [0]
def rename(o):
    if isinstance(o, dict):
        r = {}
        for k, v in o.items():
            if k in mapping:
                n[0] += 1
                k = mapping[k]
            r[k] = rename(v)
        return r
    if isinstance(o, list):
        return [rename(v) for v in o]
    return o
new = rename(state)
pend = state.get("pending", {})
out = {"exists": True, "changed": n[0], "snapshot": state.get("snapshot"), "pending": len(pend),
       "pending_footprint": sum(1 for k in pend if k in mapping)}
if req["op"] == "rewrite":
    if req.get("snapshot") is not None:
        new["snapshot"] = req["snapshot"]
        out["snapshot"] = req["snapshot"]
    if n[0] or req.get("snapshot") is not None:
        tmp = path[:-5] + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(json.dumps(new))
        os.replace(tmp, path)
print(json.dumps(out))
"""


class WorkflowError(RuntimeError):
    """A phase could not run or did not finish. The message never contains an id."""


# ---- configuration and logging -----------------------------------------------------------------------------


@dataclass
class Config:
    logical_deadline_s: float = verify.LOGICAL_DEADLINE_S  # design S6
    delivery_deadline_s: float = 900.0  # CDC delivery of the forget transaction to bronze and silver
    drain_deadline_s: float = 300.0  # embedder pointer == bronze snapshot
    settle_deadline_s: float = 120.0  # polling of physical sinks after their action ran
    qdrant_deadline_s: float = 180.0
    broker_deadline_s: float = 1800.0  # nudge + wait_absent; the bound is recorded when it passes
    wal_max_iterations: int = 40
    stats_timeout_s: float = 600.0  # pg_statistic: wait for the slot's catalog_xmin
    poll_s: float = verify.POLL_S
    qdrant_collection: str = "text_chunks"
    eventlog: Path = ROOT / "runs/data/eventlog"
    runs_data: Path = ROOT / "runs/data"
    require_presence: bool = False
    log: Callable[[str], None] = field(default=lambda m: print(f"[forget] {m}", file=sys.stderr, flush=True))


# ---- subjects ----------------------------------------------------------------------------------------------


@dataclass
class ForgetSubject:
    """One request in memory. Holds raw ids: never print it (repr hides them), never put it in a manifest."""

    request_id: str
    customer_id: int = field(repr=False)
    canaries: tuple[str, ...] = field(default=(), repr=False)  # g8-check planted tokens
    pii: tuple[str, ...] = field(default=(), repr=False)  # captured before the forget (email), searched as text
    requested_at: float = 0.0  # Postgres epoch seconds
    ids: dict[str, list[int]] = field(default_factory=dict, repr=False)  # footprint: table -> pks
    txn: postgres.ForgetTxn | None = field(default=None, repr=False)

    @property
    def tokens(self) -> tuple[str, ...]:
        return tuple(sorted({*self.canaries, *self.pii}))

    def subject(self) -> Subject:
        return Subject({t: list(v) for t, v in self.ids.items()}, self.tokens)


def union_subject(subjects: Sequence[ForgetSubject]) -> Subject:
    merged = sparkcli.merge_ids(*[s.ids for s in subjects])
    return Subject(merged, tuple(sorted({t for s in subjects for t in s.tokens})))


def register(customer_id: int) -> str:
    """Create a request. The raw id is stored in privacy.forget_requests only."""
    return audit.new_request(customer_id)


def load_subject(request_id: str, canaries: Sequence[str] = ()) -> ForgetSubject | None:
    """Rebuild a subject from privacy.forget_requests (raw id, scratch footprint). None when the request is
    finished (raw id NULLed): there is nothing left to do and nothing to look for."""
    row = audit.get_request(request_id)
    if row is None:
        raise WorkflowError("unknown request")
    cid = postgres.raw_customer_id(request_id)
    if cid is None:
        return None
    scratch = audit.load_scratch(request_id) or {}
    return ForgetSubject(
        request_id,
        cid,
        tuple(canaries),
        tuple(scratch.get("pii", ())),
        row.requested_at,
        {t: [int(i) for i in v] for t, v in scratch.get("ids", {}).items()},
    )


def next_phase(status: str) -> str | None:
    """The phase a request in `status` needs next (pure). failed resumes the logical phase, whose steps are
    idempotent (the forget transaction is skipped when its audit row exists)."""
    return {"requested": "logical", "failed": "logical", "logical_done": "physical", "physical_done": None}[status]


def _active(subjects: Sequence[ForgetSubject], statuses: Sequence[str]) -> list[ForgetSubject]:
    out = []
    for s in subjects:
        row = audit.get_request(s.request_id)
        if row is not None and row.status in statuses:
            s.requested_at = row.requested_at
            out.append(s)
    return out


# ---- capture -----------------------------------------------------------------------------------------------


def capture(subjects: Sequence[ForgetSubject], cfg: Config) -> None:
    """Footprint (Postgres now, plus bronze/quarantine/silver history from one Spark call) and PII tokens, saved to
    the request's scratch. Idempotent: a subject that already has ids is left alone. Must run BEFORE the forget
    transaction: afterwards the tickets are gone from Postgres."""
    todo = [s for s in subjects if not s.ids]
    if not todo:
        return
    for s in todo:
        pg_ids = postgres.footprint_ids(s.customer_id)
        out = postgres.psql(f"SELECT email FROM customers WHERE id = {int(s.customer_id)};")
        s.pii = tuple(t for t in {*s.pii, out} if len(t) >= 6)
        s.ids = {t: list(v) for t, v in pg_ids.items()}
    hist = sparkcli.footprints([s.customer_id for s in todo])
    for s in todo:
        s.ids = sparkcli.merge_ids(s.ids, hist.get(s.customer_id, {}), {"customers": [s.customer_id]})
        audit.save_scratch(s.request_id, {"ids": s.ids, "pii": list(s.pii)})
        cfg.log(f"{s.request_id[:8]} footprint " + ", ".join(f"{t}={len(v)}" for t, v in sorted(s.ids.items())))


def _spark_group(s: ForgetSubject) -> dict[str, Any]:
    t = s.txn
    return {
        "footprint": s.ids,
        "customers": [s.customer_id],
        "txid": t.xid32 if t and t.found else None,
        "lsn": sparkcli.lsn_to_int(t.begin_lsn) if t and t.found and t.begin_lsn else None,
    }


# ---- file rewrites -----------------------------------------------------------------------------------------


def entity_ids(ids: Mapping[str, Sequence[int]]) -> list[str]:
    return [f"{t}:{i}" for t, v in sorted(ids.items()) for i in sorted(v)]


def entity_maps(key: str, ids: Mapping[str, Sequence[int]]) -> tuple[dict[str, str], dict[str, int]]:
    ents = entity_ids(ids)
    return {e: fids.pseudonym(key, e) for e in ents}, {e: fids.pseudonym_int(key, e) for e in ents}


def _embedder_call(op: str, mapping: Mapping[str, str], snapshot: int | None = None) -> dict[str, Any]:
    """embedder.json lives in a docker volume: run a throwaway container of the embedder service (works while the
    service is stopped). The mapping carries raw entity ids and goes in on STDIN."""
    payload = json.dumps({"op": op, "mapping": dict(mapping), "snapshot": snapshot}).encode()
    r = run(
        [*COMPOSE, "--profile", "apps", "run", "--rm", "-T", "--no-deps", EMBEDDER, "python", "-c", EMBEDDER_SCRIPT],
        stdin=payload,
        timeout=300,
    )
    lines = [ln for ln in r.text.splitlines() if ln.startswith("{")]
    if r.rc != 0 or not lines:
        raise WorkflowError(f"embedder state call failed (rc {r.rc}): {sparkcli.mask(r.err_text, 300)}")
    res: dict[str, Any] = json.loads(lines[-1])
    return res


def rewrite_files(
    key: str, ids: Mapping[str, Sequence[int]], cfg: Config, snapshot: int | None = None
) -> dict[str, int]:
    """Atomic rewrites (files.py) of silver_batches, every ground_truth dir, embedder actions.jsonl, and
    embedder.json (through a throwaway container). Returns items changed per file group."""
    from adrp.forget import files

    smap, imap = entity_maps(key, ids)
    sb = files.rewrite_silver_batches(cfg.eventlog / "silver_batches", smap)
    gt = files.Stats()
    for d in files.ground_truth_dirs(cfg.runs_data):
        gt.add(files.rewrite_ground_truth(d, imap))
    act = files.rewrite_jsonl(cfg.eventlog / "embedder" / "actions.jsonl", smap)
    st = _embedder_call("rewrite", smap, snapshot)
    return {
        "silver_batches": sb.items_changed, "ground_truth": gt.items_changed,
        "embedder_actions": act.items_changed, "embedder_state": int(st["changed"]),
    }  # fmt: skip


def file_residual(key: str, ids: Mapping[str, Sequence[int]], cfg: Config) -> dict[str, Callable[[], int]]:
    """Read-only counters for verify.physical_sinks: how many items of each file group still name the subject
    (0 = clean). They never write, so they are safe to call before the forget as the presence check."""
    smap, imap = entity_maps(key, ids)

    def silver_batches() -> int:
        import pyarrow.parquet as pq

        from adrp.forget import files

        n = 0
        d = cfg.eventlog / "silver_batches"
        for p in sorted(d.glob("*.parquet")) if d.is_dir() else []:
            t = pq.read_table(p, columns=["table", "changes"])
            for table, changes in zip(t["table"].to_pylist(), t["changes"].to_pylist(), strict=True):
                n += sum(1 for c in changes or [] if files.silver_entity(table, c["pk"]) in smap)
        return n

    def ground_truth() -> int:
        import pyarrow.parquet as pq

        from adrp.forget import files

        n = 0
        for d in files.ground_truth_dirs(cfg.runs_data):
            for p in sorted(d.glob("*.parquet")):
                t = pq.read_table(p, columns=["table", "pk"])
                n += sum(
                    1
                    for tb, pk in zip(t["table"].to_pylist(), t["pk"].to_pylist(), strict=True)
                    if f"{tb}:{pk}" in imap
                )
        return n

    def embedder() -> int:
        n = 0
        actions = cfg.eventlog / "embedder" / "actions.jsonl"
        if actions.exists():
            for line in actions.read_text().splitlines():
                try:
                    if json.loads(line).get("entity") in smap:
                        n += 1
                except ValueError:
                    continue
        return n + int(_embedder_call("count", smap)["changed"])

    return {"silver_batches": silver_batches, "ground_truth": ground_truth, "embedder": embedder}


# ---- sinks for a batch -------------------------------------------------------------------------------------


def iceberg_scan_fn(subject: Subject, with_ids: bool = True) -> Callable[[], dict[str, Any]]:
    """Whole-bucket scan of s3://lake (listing everything, decoding Parquet and Avro, gunzipping metadata). Customer
    ids below 10**12 are not matched as numbers (they would hit unrelated integers); a subject made of such ids is
    found through its PII tokens, and through the Spark entity counts. Other entity pks are never searched as
    numbers (see Subject.needles). `with_ids=False` looks for canaries and PII text only: the silver tombstone
    keeps the raw customer id (its key) until the broker is verified clean and the tombstone is purged (ADR-0017),
    so the window before that purge cannot ask for the id."""

    def go() -> dict[str, Any]:
        from adrp.forget import broker
        from adrp.forget import scan as fscan

        big = sorted({i for i in subject.customer_ids if i >= 10**12}) if with_ids else []
        canaries = fscan.canary_prefixes([c for c in subject.canaries if c.startswith(fids.CANARY_PREFIX)])
        exact = [c for c in subject.canaries if not c.startswith(fids.CANARY_PREFIX)]
        schema_ids = sorted({sid for t in broker.SUBJECT_TABLES for sid in broker.key_schema_ids(t)}) if big else []
        m = fids.Matcher(subject_ids=big, canaries=[*canaries, *exact], schema_ids=schema_ids)
        res = fscan.scan(fscan.s3_store(audit.repo_env()), m)
        d = res.to_dict()
        return {k: d[k] for k in ("files", "total_hits", "matched_files", "errors", "undecoded_parquet", "kinds")}

    return go


@dataclass
class Batch:
    """Everything the physical sinks of one batch share."""

    subjects: list[ForgetSubject]
    cfg: Config
    key: str
    memo: verify.Memo = field(default_factory=verify.Memo)
    wal_iterations: int = 0
    postgres_bound: str | None = None
    scan_fn: Callable[[], Mapping[str, Any]] | None = None
    scan_ids: bool = True  # False: the silver tombstones still hold the raw customer key (before the purge)

    @property
    def request_ids(self) -> list[str]:
        return [s.request_id for s in self.subjects]

    @property
    def subject(self) -> Subject:
        return union_subject(self.subjects)

    def sinks(self, names: Sequence[str] | None = None) -> list[verify.Sink]:
        subj = self.subject
        ids = sparkcli.merge_ids(*[s.ids for s in self.subjects])
        scan_fn = self.scan_fn or iceberg_scan_fn(subj, self.scan_ids)
        all_sinks = verify.physical_sinks(
            subj,
            iceberg=scan_fn,
            file_residual=file_residual(self.key, ids, self.cfg),
            memo=self.memo,
            wal_iterations=lambda: self.wal_iterations,
            postgres_bound=lambda: self.postgres_bound,
            eventlog=self.cfg.eventlog,
            qdrant_collection=self.cfg.qdrant_collection,
        )
        return [s for s in all_sinks if names is None or s.name in names]

    def verifier(self) -> verify.Verifier:
        return verify.Verifier(self.request_ids)


# ---- presence ----------------------------------------------------------------------------------------------


@dataclass
class PresenceResult:
    logical: dict[str, verify.PresenceReport]
    physical: verify.PresenceReport

    @property
    def ok(self) -> bool:
        return self.physical.ok and all(r.ok for r in self.logical.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "logical": {k[:8]: v.to_dict() for k, v in self.logical.items()},
            "physical": self.physical.to_dict(),
        }


def presence(
    subjects: Sequence[ForgetSubject],
    cfg: Config | None = None,
    allow_missing: Sequence[str] = ("quarantine",),
) -> PresenceResult:
    """Non-vacuity: every sink must find the subject before any forget, or its later 'absent' proves nothing.
    Captures the footprint (read-only against the data stores), then asks each sink for its current answer;
    `allow_missing` names sinks that may legitimately not hold this subject. Nothing is deleted or rewritten."""
    cfg = cfg or Config()
    capture(subjects, cfg)
    spark = verify.SparkLogical({s.request_id: _spark_group(s) for s in subjects})
    logical = {
        s.request_id: verify.presence_pass(
            verify.logical_sinks(s.request_id, s.subject(), spark, qdrant_collection=cfg.qdrant_collection),
            allow_missing,
        )
        for s in subjects
    }
    batch = Batch(list(subjects), cfg, audit.hmac_key())
    physical = verify.presence_pass(batch.sinks(), allow_missing)
    return PresenceResult(logical, physical)


# ---- phase L -----------------------------------------------------------------------------------------------


@dataclass
class LogicalReport:
    request_ids: list[str]
    skipped: list[str] = field(default_factory=list)  # already past the logical phase
    failed: dict[str, str] = field(default_factory=dict)  # request -> reason (sink names, never ids)
    outcomes: dict[str, dict[str, verify.SinkOutcome]] = field(default_factory=dict)
    qdrant_points_deleted: dict[str, int] = field(default_factory=dict)
    delivery_s: dict[str, float] = field(
        default_factory=dict
    )  # request -> seconds from forget txn to delivery+tombstone
    bronze_delete: dict[str, Any] = field(default_factory=dict)
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.failed


def _txn_from_audit(request_id: str) -> postgres.ForgetTxn | None:
    a = audit.audit_row(request_id)
    if a is None or a["xid32"] is None or a["forget_lsn"] is None:
        return None
    counts = {t: int(n) for t, n in a["footprint"].items()}
    end = postgres.psql("SELECT pg_current_wal_lsn();")  # >= the real end_lsn
    return postgres.ForgetTxn(True, a["xid32"], a["forget_lsn"], end, None, counts)


def _expected_min_rows(s: ForgetSubject) -> int:
    """Lower bound of the rows the forget transaction put on the change stream: every row it touched, plus the
    placeholder customer insert."""
    assert s.txn is not None
    return sum(s.txn.counts.values()) + 1


def run_logical(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> LogicalReport:
    """Phase L for every subject whose request is requested/failed; others are skipped (idempotent)."""
    cfg = cfg or Config()
    audit.hmac_key()  # fail early if the key is missing: nothing may run without a subject handle
    t0 = time.monotonic()
    rep = LogicalReport([s.request_id for s in subjects])
    todo = _active(subjects, ("requested", "failed"))
    rep.skipped = [s.request_id for s in subjects if s not in todo]
    if not todo:
        return rep
    if cfg.require_presence:
        pr = presence(todo, cfg)
        if not pr.ok:
            raise WorkflowError("presence pass failed: a sink does not find the subject (vacuous check)")
    capture(todo, cfg)

    # 1. the Postgres transaction (skipped when this request already has its audit row)
    for s in todo:
        txn = _txn_from_audit(s.request_id)
        if txn is None:
            txn = postgres.forget_customer(s.customer_id)
            if not txn.found:
                audit.fail(s.request_id, "customer not found in Postgres")
                rep.failed[s.request_id] = "customer not found"
                continue
            audit.record_audit_row(s.request_id, s.customer_id, txn, s.ids)
            cfg.log(f"{s.request_id[:8]} forget transaction committed ({sum(txn.counts.values())} rows)")
        else:
            cfg.log(f"{s.request_id[:8]} forget transaction already committed; resuming")
        s.txn = txn
    todo = [s for s in todo if s.request_id not in rep.failed]
    if not todo:
        return rep

    # 2. wait for the change stream: slot consumed the transaction, bronze holds it, silver tombstone is PII-free
    spark = verify.SparkLogical({s.request_id: _spark_group(s) for s in todo}, ttl_s=1.0)
    t_txn = time.monotonic()
    pending = {s.request_id: s for s in todo}
    ready: dict[str, ForgetSubject] = {}

    def poll_delivery() -> bool:
        spark.fresh()
        for rid, s in list(pending.items()):
            assert s.txn is not None and s.txn.end_lsn is not None
            res = spark.get(rid)
            silver = res.get("silver", {})
            if any(int(t.get("tombstones_with_values") or 0) > 0 for t in silver.values()):
                raise WorkflowError("a silver tombstone still holds values (tombstone not PII-free)")
            delivered, _ = verify.delivery_judgement(res, _expected_min_rows(s))
            if delivered and postgres.slot_confirmed(s.txn.end_lsn) and verify.silver_judgement(res)[0]:
                ready[rid] = pending.pop(rid)
                rep.delivery_s[rid] = time.monotonic() - t_txn
        return not pending

    try:
        wait_until(poll_delivery, cfg.delivery_deadline_s, cfg.poll_s)
    except (WorkflowError, sparkcli.SparkError) as e:
        for rid in pending:
            audit.fail(rid, f"logical: {e}")
            rep.failed[rid] = "tombstone not PII-free or spark failed"
        pending.clear()
    for rid in pending:
        audit.fail(rid, "logical: CDC delivery or silver tombstone not seen before the deadline")
        rep.failed[rid] = "delivery/tombstone deadline"
    batch = [s for s in todo if s.request_id in ready]
    if not batch:
        rep.seconds = time.monotonic() - t0
        return rep

    # 3. Qdrant, only now that silver shows the tombstones
    from adrp.forget import qdrant

    for s in batch:
        ents = s.subject().entity_strings(["support_tickets"])
        rep.qdrant_points_deleted[s.request_id] = qdrant.delete_entities(ents, cfg.qdrant_collection) if ents else 0

    # 4. bronze and quarantine: one Spark call for the whole batch
    rep.bronze_delete = sparkcli.delete_events(sparkcli.merge_ids(*[s.ids for s in batch]))
    spark.ttl_s = 5.0
    spark.fresh()

    # 5. verify per sink, all requests polled together; first-seen-absent is recorded the moment it happens
    sinks: dict[str, list[verify.Sink]] = {
        s.request_id: [
            x
            for x in verify.logical_sinks(s.request_id, s.subject(), spark, qdrant_collection=cfg.qdrant_collection)
            if x.name not in LOGICAL_SINKS_EXCLUDED
        ]
        for s in batch
    }
    rep.outcomes = verify.Verifier(()).run_scoped(sinks, "logical", cfg.logical_deadline_s, cfg.poll_s)
    for s in batch:
        bad = verify.failures(rep.outcomes[s.request_id])
        if bad:
            audit.fail(s.request_id, "logical: not absent: " + ",".join(bad))
            rep.failed[s.request_id] = "not absent: " + ",".join(bad)
        else:
            audit.move(s.request_id, "logical_done")
    rep.seconds = time.monotonic() - t0
    return rep


# ---- maintenance window ------------------------------------------------------------------------------------


@dataclass
class Window:
    stopped: list[str] = field(default_factory=list)
    seconds: float = 0.0
    _t0: float = 0.0
    cfg: Config = field(default_factory=Config)

    def stop(self, service: str) -> None:
        if service in self.stopped or not service_running(service):
            return
        r = run([*COMPOSE, "stop", service], timeout=180)
        if r.rc != 0:
            raise WorkflowError(f"could not stop {service}: {sparkcli.mask(r.err_text, 200)}")
        self.stopped.append(service)
        self.cfg.log(f"stopped {service}")


@contextlib.contextmanager
def maintenance_window(cfg: Config, services: Sequence[str] = (STREAMS,)) -> Iterator[Window]:
    """Stops `services` (more may be added with Window.stop), yields, and restarts everything the window stopped,
    also when the body raises. Window.seconds is the wall time from the first stop to the services running again."""
    w = Window(cfg=cfg)
    w._t0 = time.monotonic()
    try:
        for svc in services:
            w.stop(svc)
        yield w
    finally:
        for svc in reversed(w.stopped):
            run([*COMPOSE, "start", svc], timeout=180)
        wait_until(lambda: all(service_running(s) for s in w.stopped), 120, 3.0)
        w.seconds = time.monotonic() - w._t0
        cfg.log(f"window closed after {w.seconds:.0f} s")


def drain_embedder(ids: Mapping[str, Sequence[int]], cfg: Config, w: Window) -> dict[str, Any]:
    """Wait until the embedder's pointer equals bronze's current snapshot and its pending set holds no footprint
    entity, then stop it (ADR-0018 step 3). With the streams stopped bronze no longer moves. An embedder that is
    not running, or has no state yet, counts as drained."""
    if not service_running(EMBEDDER):
        return {"embedder_was_running": 0}
    smap, _ = entity_maps(audit.hmac_key(), ids)
    snap = sparkcli.bronze_snapshot_id()
    last: dict[str, Any] = {}

    def drained() -> bool:
        last.update(_embedder_call("count", smap))
        return (not last["exists"]) or (last["snapshot"] == snap and last["pending_footprint"] == 0)

    ok, waited = wait_until(drained, cfg.drain_deadline_s, cfg.poll_s)
    w.stop(EMBEDDER)
    if not ok:
        raise WorkflowError("embedder did not drain before the deadline")
    return {"embedder_was_running": 1, "drain_s": round(waited, 1), "pending": last.get("pending", 0)}


def iceberg_pass(ids: Mapping[str, Sequence[int]], cfg: Config, w: Window) -> dict[str, Any]:
    """drain -> rewrite -> drop refs -> expire (+ rewrite_manifests + expire + orphans) -> state, for bronze,
    quarantine and silver (ADR-0018). Call with the streams stopped. Returns counts."""
    out: dict[str, Any] = {"drain": drain_embedder(ids, cfg, w)}
    rw = sparkcli.rewrite(ids)
    out["rewritten_files"] = sum(
        int((v.get("result") or {}).get("rewritten_data_files_count") or 0) for v in rw.values() if isinstance(v, dict)
    )
    out["delete_files_removed"] = sum(
        int((v.get("result") or {}).get("removed_delete_files_count") or 0) for v in rw.values() if isinstance(v, dict)
    )
    refs = sparkcli.refs()
    out["refs_dropped"] = sum(len(v.get("dropped", [])) for v in refs.values())
    ex = sparkcli.expire()
    out["orphans_removed"] = sum(int(v.get("orphans_removed") or 0) for v in ex.values() if isinstance(v, dict))
    st = sparkcli.state(ids)
    out["residue"] = sparkcli.total_residue(st)
    out["bronze_snapshot"] = st.get("lake.bronze.cdc_events", {}).get("current_snapshot_id")
    return out


def seaweed_vacuum() -> dict[str, Any]:
    from adrp.forget import storage

    r = storage.vacuum()
    return {
        "ok": int(r.ok), "garbage_permille_before": int(r.before.ratio * 1000),
        "garbage_permille_after": int(r.after.ratio * 1000), "seconds": round(r.elapsed_s, 1),
    }  # fmt: skip


@dataclass
class WindowReport:
    request_ids: list[str]
    skipped: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    seconds: float = 0.0  # window length: services stopped -> running again
    steps: dict[str, Any] = field(default_factory=dict)  # counts only
    outcomes: dict[str, verify.SinkOutcome] = field(default_factory=dict)
    verified_at: float = 0.0  # Postgres epoch: physical lag of a request = verified_at - its requested_at

    @property
    def ok(self) -> bool:
        return not self.failed and all(o.ok for o in self.outcomes.values())


def _need_logical_done(subjects: Sequence[ForgetSubject]) -> tuple[list[ForgetSubject], list[str]]:
    batch, skipped = [], []
    for s in subjects:
        row = audit.get_request(s.request_id)
        if row is None:
            raise WorkflowError("unknown request")
        if row.status == "physical_done":
            skipped.append(s.request_id)
        elif row.status == "logical_done":
            s.requested_at = row.requested_at
            batch.append(s)
        else:
            raise WorkflowError(f"request {s.request_id[:8]} is {row.status}: run the logical phase first")
    return batch, skipped


def run_physical_window(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> WindowReport:
    """Phase P in one maintenance window for all `subjects` that are logical_done (batching: each request's
    physical lag is the window's verified-at minus its own request time). Stops spark-streams, drains and stops the
    embedder, runs the Iceberg procedure, the whole-bucket scan, the SeaweedFS vacuum, the file rewrites and the
    Postgres physical steps, verifies, and restarts the services."""
    cfg = cfg or Config()
    key = audit.hmac_key()
    batch_subjects, skipped = _need_logical_done(subjects)
    rep = WindowReport([s.request_id for s in subjects], skipped)
    if not batch_subjects:
        return rep
    batch = Batch(batch_subjects, cfg, key, scan_ids=False)  # tombstones keep the raw key until the purge
    ids = sparkcli.merge_ids(*[s.ids for s in batch_subjects])
    if not ids:
        raise WorkflowError("subjects carry no footprint (load them with load_subject)")
    try:
        with maintenance_window(cfg) as w:
            rep.steps["iceberg"] = iceberg_pass(ids, cfg, w)
            rep.steps["seaweedfs"] = seaweed_vacuum()
            rep.steps["files"] = rewrite_files(key, ids, cfg, rep.steps["iceberg"].get("bronze_snapshot"))
            rep.steps["postgres"] = postgres_physical(batch, cfg)
            batch.memo.drop()
            rep.outcomes = batch.verifier().run(
                batch.sinks(WINDOW_SINKS), "physical", cfg.settle_deadline_s, cfg.poll_s
            )
            rep.steps["bucket_scan"] = rep.outcomes["bronze"].evidence
        rep.seconds = w.seconds
    except (WorkflowError, sparkcli.SparkError, postgres.PsqlError) as e:
        for s in batch_subjects:
            audit.fail(s.request_id, f"physical window: {type(e).__name__}")
            rep.failed[s.request_id] = type(e).__name__
        raise
    rep.verified_at = audit.db_now()
    for s in batch_subjects:
        audit.record_sink(
            s.request_id, "window_verified", physical_absent=True, evidence={"window_seconds": round(rep.seconds, 1)}
        )
    return rep


def postgres_physical(batch: Batch, cfg: Config) -> dict[str, Any]:
    """VACUUM FULL (shop + privacy tables), pg_statistic refresh, CHECKPOINT. Maintenance window only: ACCESS
    EXCLUSIVE locks. A residue the procedure could not remove becomes the sink's bound."""
    v = postgres.vacuum_full()
    stats = postgres.analyze_and_vacuum_statistic(timeout_s=cfg.stats_timeout_s)
    postgres.checkpoint()
    notes = []
    if v.blocked_dead_rows:
        notes.append(
            f"VACUUM FULL could not remove {v.blocked_dead_rows} dead row versions that an open transaction still needs"
        )
    if not stats.ok:
        notes.append(
            f"pg_statistic copies stay until {stats.slots_blocking} replication slot(s) advance catalog_xmin "
            "(the slot holds catalog vacuum back); retry later"
        )
    batch.postgres_bound = "postgres: " + "; ".join(notes) + ". Bounded, not removed." if notes else None
    return {
        "vacuum_seconds": round(v.seconds, 1), "blocked_dead_rows": v.blocked_dead_rows,
        "statistic_ok": int(stats.ok), "statistic_wait_s": round(stats.waited_s, 1),
    }  # fmt: skip


# ---- physical checks outside the window ---------------------------------------------------------------------


@dataclass
class PhysicalReport:
    request_ids: list[str]
    outcomes: dict[str, verify.SinkOutcome] = field(default_factory=dict)
    steps: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(o.ok for o in self.outcomes.values())


def _logical_done_batch(subjects: Sequence[ForgetSubject], cfg: Config) -> Batch | None:
    batch, _ = _need_logical_done(subjects)
    return Batch(batch, cfg, audit.hmac_key()) if batch else None


def run_qdrant_physical(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> PhysicalReport:
    """Aggressive vacuum settings, observe the segment rewrite, grep the storage volume; qdrant (segments) and
    qdrant_wal (the bound: the WAL rolls out after about 2 x wal_capacity_mb of later writes)."""
    cfg = cfg or Config()
    b = _logical_done_batch(subjects, cfg)
    if b is None:
        return PhysicalReport([s.request_id for s in subjects])
    from adrp.forget import qdrant

    raw, bounds = qdrant.physical_pass(b.subject, cfg.qdrant_collection, cfg.qdrant_deadline_s)
    rep = PhysicalReport(
        b.request_ids, steps={"segment_files": raw.segment_files, "wal_files": raw.wal_files, "bounds": len(bounds)}
    )
    rep.outcomes = b.verifier().run(b.sinks(("qdrant", "qdrant_wal")), "physical", cfg.settle_deadline_s, cfg.poll_s)
    return rep


def run_broker_physical(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> PhysicalReport:
    """Nudge writes (a no-op UPDATE of other rows per topic, repeated), wait for the raw bytes to leave the
    segments; on timeout the sink carries the bound derived from the live topic config. ADR-0019."""
    cfg = cfg or Config()
    b = _logical_done_batch(subjects, cfg)
    if b is None:
        return PhysicalReport([s.request_id for s in subjects])
    from adrp.forget import broker

    wr = broker.wait_absent(b.subject, cfg.broker_deadline_s, every_s=10.0, nudge_every_s=60.0)
    rep = PhysicalReport(
        b.request_ids,
        steps={"absent": int(wr.absent), "elapsed_s": round(wr.elapsed_s, 1), "polls": wr.polls, "nudges": wr.nudges},
    )
    rep.outcomes = b.verifier().run(b.sinks(("broker",)), "physical", 60.0, cfg.poll_s)
    return rep


def run_postgres_wal(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> PhysicalReport:
    """Push the WAL forward (insert + pg_switch_wal + CHECKPOINT rounds) until no pg_wal file holds the subject, or
    the iteration cap; the sink carries the bound statement when it does not get there. The slot must already
    have consumed the forget transaction (run_logical waits for that)."""
    cfg = cfg or Config()
    b = _logical_done_batch(subjects, cfg)
    if b is None:
        return PhysicalReport([s.request_id for s in subjects])
    needles = b.subject.needles()
    if not needles:
        return PhysicalReport(b.request_ids, steps={"searchable": 0})
    wr = postgres.wal_loop(needles, cfg.wal_max_iterations)
    b.wal_iterations = wr.iterations
    rep = PhysicalReport(
        b.request_ids,
        steps={
            "absent": int(wr.absent), "iterations": wr.iterations, "seconds": round(wr.seconds, 1),
            "files_before": wr.files_before, "files_after": wr.files_after,
        },
    )  # fmt: skip
    rep.outcomes = b.verifier().run(b.sinks(("pg_wal",)), "physical", 60.0, cfg.poll_s)
    return rep


# ---- re-sweep and purge --------------------------------------------------------------------------------------


@dataclass
class PurgeReport:
    request_ids: list[str]
    skipped: list[str] = field(default_factory=list)
    broker_clean: bool = False
    tombstones_purged: bool = False
    deferred: list[str] = field(default_factory=list)  # what was not done and why (names only)
    failed: dict[str, str] = field(default_factory=dict)
    steps: dict[str, Any] = field(default_factory=dict)
    outcomes: dict[str, verify.SinkOutcome] = field(default_factory=dict)
    window_seconds: float = 0.0
    finished: list[str] = field(default_factory=list)  # requests now physical_done

    @property
    def ok(self) -> bool:
        return not self.failed and all(o.ok for o in self.outcomes.values())


def _broker_clean(request_ids: Sequence[str]) -> bool:
    for rid in request_ids:
        rows = {r.sink: r for r in audit.sink_rows(rid)}
        if "broker" not in rows or rows["broker"].physical_absent_at is None:
            return False
    return True


def run_resweep_and_purge(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> PurgeReport:
    """After the broker check: a second short window (streams stopped) that repeats the bronze/quarantine DELETE
    (the broker may have replayed rows into bronze), hard-deletes the silver tombstones when the broker is verified
    clean, and runs the Iceberg procedure and the bucket scan again. Then the remaining physical sinks are
    verified, the raw id and scratch are NULLed, the privacy tables VACUUM FULLed, and the requests become
    physical_done. Tombstones are NOT purged while the broker still holds the key (ADR-0013/0017): the report says
    `deferred` and the request stays logical_done."""
    cfg = cfg or Config()
    key = audit.hmac_key()
    batch_subjects, skipped = _need_logical_done(subjects)
    rep = PurgeReport([s.request_id for s in subjects], skipped)
    if not batch_subjects:
        return rep
    batch = Batch(batch_subjects, cfg, key)
    ids = sparkcli.merge_ids(*[s.ids for s in batch_subjects])
    rep.broker_clean = _broker_clean(batch.request_ids)
    try:
        with maintenance_window(cfg) as w:
            rep.steps["bronze_delete"] = {
                t: {"after": v.get("after")}
                for t, v in sparkcli.delete_events(ids).items()
                if isinstance(v, dict) and "after" in v
            }
            if rep.broker_clean:
                pt = sparkcli.purge_tombstones(ids)
                rep.steps["tombstones_purged"] = sum(
                    int(v.get("before", 0)) - int(v.get("after", 0))
                    for v in pt.values()
                    if isinstance(v, dict) and "before" in v
                )
                rep.tombstones_purged = True
            else:
                rep.deferred.append(
                    "silver_tombstone_purge: the broker still holds the subject (key is kept in silver)"
                )
            rep.steps["iceberg"] = iceberg_pass(ids, cfg, w)
            batch.scan_fn = iceberg_scan_fn(batch.subject, rep.tombstones_purged)
            rep.steps["seaweedfs"] = seaweed_vacuum()
            rep.steps["files"] = rewrite_files(key, ids, cfg, rep.steps["iceberg"].get("bronze_snapshot"))
            batch.memo.drop()
            rep.outcomes = batch.verifier().run(
                batch.sinks(WINDOW_SINKS), "physical", cfg.settle_deadline_s, cfg.poll_s
            )
        rep.window_seconds = w.seconds
    except (WorkflowError, sparkcli.SparkError, postgres.PsqlError) as e:
        for s in batch_subjects:
            audit.fail(s.request_id, f"resweep: {type(e).__name__}")
            rep.failed[s.request_id] = type(e).__name__
        raise
    if rep.tombstones_purged:
        for s in batch_subjects:
            audit.record_sink(s.request_id, "silver_tombstone_purge", physical_absent=True, evidence={"purged": 1})
    # final verification of everything that can reappear or was not yet checked
    rep.outcomes.update(batch.verifier().run(batch.sinks(FINAL_SINKS), "physical", cfg.settle_deadline_s, cfg.poll_s))
    bad = verify.failures(rep.outcomes)
    if bad or rep.deferred:
        if bad:
            for s in batch_subjects:
                audit.fail(s.request_id, "physical: not absent and not bounded: " + ",".join(bad))
                rep.failed[s.request_id] = "physical: " + ",".join(bad)
        return rep
    # the raw id and scratch go last, then the dead tuples
    for s in batch_subjects:
        audit.null_subject(s.request_id)
    vf = postgres.vacuum_full(postgres.PRIVACY_TABLES)
    postgres.checkpoint()
    rep.steps["privacy_vacuum"] = {"seconds": round(vf.seconds, 1), "blocked_dead_rows": vf.blocked_dead_rows}
    ro = batch.verifier().run([verify.raw_id_physical_sink(batch.subject)], "physical", 60.0, cfg.poll_s)
    rep.outcomes["raw_id_store"] = ro["raw_id_store"]
    if not verify.failures(ro):
        for s in batch_subjects:
            audit.move(s.request_id, "physical_done")
            rep.finished.append(s.request_id)
    else:
        for s in batch_subjects:
            audit.fail(s.request_id, "physical: raw id still in Postgres files")
            rep.failed[s.request_id] = "raw_id_store"
    return rep


# ---- everything ---------------------------------------------------------------------------------------------


@dataclass
class AllReport:
    logical: LogicalReport | None = None
    window: WindowReport | None = None
    qdrant: PhysicalReport | None = None
    broker: PhysicalReport | None = None
    wal: PhysicalReport | None = None
    purge: PurgeReport | None = None

    @property
    def ok(self) -> bool:
        parts: list[Any] = [self.logical, self.window, self.qdrant, self.broker, self.wal, self.purge]
        return all(p is None or p.ok for p in parts)


def run_physical(
    subjects: Sequence[ForgetSubject], cfg: Config | None = None, rep: AllReport | None = None
) -> AllReport:
    """Phase P end to end: window, Qdrant, broker, WAL, re-sweep and purge, in that order, stopping at the first
    step that fails. A request that is not logical_done raises WorkflowError; one that is physical_done is skipped."""
    cfg = cfg or Config()
    rep = rep or AllReport()
    rep.window = run_physical_window(subjects, cfg)
    if not rep.window.ok:
        return rep
    rep.qdrant = run_qdrant_physical(subjects, cfg)
    rep.broker = run_broker_physical(subjects, cfg)
    rep.wal = run_postgres_wal(subjects, cfg)
    rep.purge = run_resweep_and_purge(subjects, cfg)
    return rep


def run_all(subjects: Sequence[ForgetSubject], cfg: Config | None = None) -> AllReport:
    """Logical, then physical. Idempotent: a finished request is skipped by every phase."""
    cfg = cfg or Config()
    logical = run_logical(subjects, cfg)
    rep = AllReport(logical=logical)
    if not logical.ok:
        return rep
    return run_physical(subjects, cfg, rep)


def lag_report(request_ids: Sequence[str]) -> dict[str, Any]:
    """Manifest-ready: per sink logical/physical lag median/min/max over the requests, plus how many carry a bound,
    and the per-request status. Counts and seconds only."""
    return {
        "sinks": audit.lag_table(request_ids),
        "requests": [audit.public_summary(r) for rid in request_ids if (r := audit.get_request(rid)) is not None],
    }
