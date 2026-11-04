"""Verifier for the forget workflow (G8): sinks, the polling loop, the bound rule, and the non-vacuity pass.

A Sink answers two questions about one place that may hold a subject:
  logical_absent()  by query: True gone, False still there, None the sink has no logical state (broker, files)
  physical_absent() (True | False | None, evidence, bound): by looking at bytes. `evidence` is counts only.
                    `bound` is the statement of why the bytes may stay (WAL, spill, SeaweedFS needles, ...).

Rules (design S6-S8, ADR-0017):
  * the lag of a sink is the time of the FIRST observation of absence minus the request time; polling every 5 s
    (a sink may ask for a longer interval: a Spark JVM per probe, a container-wide grep);
  * a sink that is not absent at its deadline MUST carry a non-empty bound, else it counts as a failure; a logical
    check has no bound: not absent in time is a failure;
  * `presence_pass` runs before any forget: a sink that does not find the subject would make "absent" vacuous.

Nothing here puts a raw id, pk or canary in a result: evidence is validated by audit.safe_evidence when recorded.
Sink adapters at the bottom map to the inventory rows of docs/design.md section 3.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from adrp.forget import audit
from adrp.forget.rawscan import ROOT, Needles, Subject

POLL_S = 5.0
LOGICAL_DEADLINE_S = 15 * 60  # design S6
PHYSICAL_DEADLINE_S = 24 * 3600  # design S7
SEAWEED_SETTLE_RATIO = 0.001

PhysicalResult = tuple[bool | None, Mapping[str, Any], str | None]


class Sink(Protocol):
    name: str
    inventory_row: str  # row(s) of docs/design.md section 3
    presence_expected: bool  # False: a post-condition only (garbage, offsets), not something found before a forget
    min_interval_s: float

    def logical_absent(self) -> bool | None: ...

    def physical_absent(self) -> PhysicalResult: ...


@dataclass
class FnSink:
    """A Sink built from two callables. A missing callable means 'not applicable' (None)."""

    name: str
    inventory_row: str
    logical: Callable[[], bool | None] | None = None
    physical: Callable[[], PhysicalResult] | None = None
    presence_expected: bool = True
    min_interval_s: float = POLL_S

    def logical_absent(self) -> bool | None:
        return self.logical() if self.logical else None

    def physical_absent(self) -> PhysicalResult:
        return self.physical() if self.physical else (None, {}, None)


# ---- pure rules ------------------------------------------------------------------------------------------


def logical_ok(absent: bool | None) -> bool:
    """Logical pass: absent, or not applicable. Not absent in time is a failure; there is no bound for it."""
    return absent is None or absent is True


def physical_ok(absent: bool | None, bound: str | None) -> bool:
    """Physical pass: absent, not applicable, or not absent WITH a non-empty bound."""
    return absent is None or absent is True or bool(bound and bound.strip())


def presence_class(logical: bool | None, physical: bool | None) -> str:
    """'present' (the sink finds the subject), 'vacuous' (it finds nothing: its later 'absent' proves nothing) or
    'na' (it can see neither)."""
    seen = [x for x in (logical, physical) if x is not None]
    if not seen:
        return "na"
    return "present" if any(x is False for x in seen) else "vacuous"


@dataclass
class PresenceReport:
    present: list[str] = field(default_factory=list)
    vacuous: list[str] = field(default_factory=list)
    na: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)  # post-condition sinks (presence_expected False)
    errors: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.vacuous and not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in ("present", "vacuous", "na", "skipped", "errors")} | {"ok": self.ok}


def presence_pass(sinks: Sequence[Sink], allow_missing: Sequence[str] = ()) -> PresenceReport:
    """Every sink must find the subject before any forget. `allow_missing` names sinks that legitimately may not
    hold this subject (for example quarantine when nothing was forced there); they are reported under `na`."""
    rep = PresenceReport()
    for s in sinks:
        if not s.presence_expected:
            rep.skipped.append(s.name)
            continue
        try:
            cls = presence_class(s.logical_absent(), s.physical_absent()[0])
        except Exception as e:  # noqa: BLE001 - a sink that cannot be read is a finding, not a crash
            rep.errors[s.name] = type(e).__name__
            continue
        if cls == "vacuous" and s.name in allow_missing:
            rep.na.append(s.name)
        else:
            getattr(rep, cls).append(s.name)
    return rep


# ---- polling ---------------------------------------------------------------------------------------------


@dataclass
class SinkOutcome:
    sink: str
    inventory_row: str
    phase: str  # 'logical' | 'physical'
    absent: bool | None  # last observation (None: not applicable)
    first_absent_s: float | None  # seconds from the start of this poll to the first observation of absence
    bound: str | None
    evidence: dict[str, Any]
    polls: int
    reappeared: bool = False
    errors: int = 0

    @property
    def _reappeared_unbounded(self) -> bool:
        """A sink seen absent and then present again fails, unless it is a physical sink that states a bound: the
        subject's bytes can come back through our own bookkeeping (a Postgres checkpoint writing a page image of
        the request row that still holds the raw id), which is exactly what the bound describes."""
        return self.reappeared and not (self.phase == "physical" and bool(self.bound and self.bound.strip()))

    @property
    def ok(self) -> bool:
        if self._reappeared_unbounded:
            return False
        if self.phase == "logical":
            return logical_ok(self.absent)
        return physical_ok(self.absent, self.bound)

    @property
    def verdict(self) -> str:
        """absent | bounded | n/a | FAIL: the wording used in reports. Never 'deleted' alone."""
        if self._reappeared_unbounded:
            return "FAIL"
        if self.absent is None:
            return "n/a"
        if self.absent:
            return "absent"
        return "bounded" if self.ok else "FAIL"


def poll(
    sinks: Sequence[Sink],
    phase: str,
    deadline_s: float,
    every_s: float = POLL_S,
    on_first_absent: Callable[[Sink, dict[str, Any]], None] | None = None,
    on_final: Callable[[Sink, SinkOutcome], None] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    recheck: bool = True,
) -> dict[str, SinkOutcome]:
    """Evaluate `phase` ('logical' | 'physical') of every sink until all are absent (or not applicable), or the
    deadline passes. Each sink is evaluated at least once. `on_first_absent` fires the moment a sink is first seen
    absent (the caller stamps the time); `on_final` fires once per sink at the end (bound and evidence).

    After the loop, sinks that were absent are evaluated once more: a point that comes back (the embedder
    re-creating a Qdrant point, the stream re-appending) sets `reappeared`, which fails the sink."""
    if phase not in ("logical", "physical"):
        raise ValueError(phase)
    t0 = clock()
    out: dict[str, SinkOutcome] = {
        s.name: SinkOutcome(s.name, s.inventory_row, phase, False, None, None, {}, 0) for s in sinks
    }
    last_eval: dict[str, float] = {}
    done: set[str] = set()

    def evaluate(s: Sink) -> tuple[bool | None, Mapping[str, Any], str | None]:
        if phase == "logical":
            return s.logical_absent(), {}, None
        return s.physical_absent()

    while True:
        for s in sinks:
            if s.name in done:
                continue
            o = out[s.name]
            if s.name in last_eval and clock() - last_eval[s.name] < s.min_interval_s:
                continue
            last_eval[s.name] = clock()
            try:
                absent, evidence, bound = evaluate(s)
            except Exception as e:  # noqa: BLE001 - keep polling; the count is reported
                o.errors += 1
                o.evidence = {"error": type(e).__name__, "errors": o.errors}
                o.absent = False
                o.polls += 1
                continue
            o.polls += 1
            o.absent, o.evidence, o.bound = absent, dict(evidence), bound
            if absent is None:
                done.add(s.name)
            elif absent:
                o.first_absent_s = clock() - t0
                done.add(s.name)
                if on_first_absent:
                    on_first_absent(s, o.evidence)
        if len(done) == len(sinks) or clock() - t0 >= deadline_s:
            break
        sleep(min(every_s, max(0.0, deadline_s - (clock() - t0))))

    if recheck:
        for s in sinks:
            o = out[s.name]
            if o.absent is True:
                try:
                    again, evidence, bound = evaluate(s)
                except Exception:  # noqa: BLE001
                    o.errors += 1
                    continue
                if again is False:
                    o.reappeared, o.absent, o.evidence, o.bound = True, False, dict(evidence), bound
    for s in sinks:
        if on_final:
            on_final(s, out[s.name])
    return out


@dataclass
class Verifier:
    """Binds a poll to the privacy.forget_sink_status rows of one or more requests. One batch observation applies to
    every request of the batch; each request's lag is then its own (first-seen time minus its requested_at)."""

    request_ids: Sequence[str]
    recorder: Callable[..., None] = audit.record_sink

    def _record(self, sink: Sink, phase: str, absent: bool, bound: str | None, evidence: Mapping[str, Any]) -> None:
        for rid in self.request_ids:
            self._record_one(rid, sink, phase, absent, bound, evidence)

    def run(
        self,
        sinks: Sequence[Sink],
        phase: str,
        deadline_s: float | None = None,
        every_s: float = POLL_S,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, SinkOutcome]:
        deadline = (
            deadline_s if deadline_s is not None else LOGICAL_DEADLINE_S if phase == "logical" else PHYSICAL_DEADLINE_S
        )

        def first(s: Sink, evidence: dict[str, Any]) -> None:
            self._record(s, phase, True, None, evidence)

        def final(s: Sink, o: SinkOutcome) -> None:
            if o.absent is not True and o.absent is not None:  # keep the bound / last evidence of a sink not absent
                self._record(s, phase, False, o.bound if phase == "physical" else None, o.evidence)

        return poll(sinks, phase, deadline, every_s, first, final, clock, sleep)

    def run_scoped(
        self,
        per_request: Mapping[str, Sequence[Sink]],
        phase: str,
        deadline_s: float | None = None,
        every_s: float = POLL_S,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, dict[str, SinkOutcome]]:
        """Poll the sinks of several requests together (one deadline, one loop) and record each observation only
        against its own request. Returns {request_id: {sink: outcome}}."""
        deadline = (
            deadline_s if deadline_s is not None else LOGICAL_DEADLINE_S if phase == "logical" else PHYSICAL_DEADLINE_S
        )
        scoped: list[Sink] = [_Scoped(rid, s) for rid, sinks in per_request.items() for s in sinks]

        def first(s: Sink, evidence: dict[str, Any]) -> None:
            assert isinstance(s, _Scoped)
            self._record_one(s.request_id, s.inner, phase, True, None, evidence)

        def final(s: Sink, o: SinkOutcome) -> None:
            assert isinstance(s, _Scoped)
            if o.absent is not True and o.absent is not None:
                self._record_one(
                    s.request_id, s.inner, phase, False, o.bound if phase == "physical" else None, o.evidence
                )

        raw = poll(scoped, phase, deadline, every_s, first, final, clock, sleep)
        out: dict[str, dict[str, SinkOutcome]] = {rid: {} for rid in per_request}
        for sc in scoped:
            assert isinstance(sc, _Scoped)
            o = raw[sc.name]
            o.sink = sc.inner.name
            out[sc.request_id][sc.inner.name] = o
        return out

    def _record_one(
        self, rid: str, sink: Sink, phase: str, absent: bool, bound: str | None, evidence: Mapping[str, Any]
    ) -> None:
        self.recorder(
            rid,
            sink.name,
            logical_absent=absent and phase == "logical",
            physical_absent=absent and phase == "physical",
            bound=bound,
            evidence=evidence,
        )


class _Scoped:
    """A sink of one request inside a multi-request poll: the poll keys outcomes by name, so the name carries the
    request id; observations are recorded against `request_id` and the inner sink's own name."""

    def __init__(self, request_id: str, inner: Sink) -> None:
        self.request_id = request_id
        self.inner = inner
        self.name = f"{request_id}|{inner.name}"
        self.inventory_row = inner.inventory_row
        self.presence_expected = inner.presence_expected
        self.min_interval_s = inner.min_interval_s

    def logical_absent(self) -> bool | None:
        return self.inner.logical_absent()

    def physical_absent(self) -> PhysicalResult:
        return self.inner.physical_absent()


def failures(outcomes: Mapping[str, SinkOutcome]) -> list[str]:
    return sorted(n for n, o in outcomes.items() if not o.ok)


def outcome_dict(o: SinkOutcome) -> dict[str, Any]:
    return {
        "sink": o.sink, "inventory_row": o.inventory_row, "phase": o.phase, "verdict": o.verdict,
        "first_absent_s": o.first_absent_s, "bounded": bool(o.bound), "polls": o.polls, "errors": o.errors,
        "reappeared": o.reappeared, "evidence": o.evidence,
    }  # fmt: skip


# ---- memo ------------------------------------------------------------------------------------------------


class Memo:
    """Short-lived cache so sinks that read the same expensive source (one Spark probe, one bucket scan, one
    Qdrant grep) do not each run it."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._v: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, fn: Callable[[], Any], ttl_s: float) -> Any:
        hit = self._v.get(key)
        if hit is not None and self._clock() - hit[0] < ttl_s:
            return hit[1]
        v = fn()
        self._v[key] = (self._clock(), v)
        return v

    def put(self, key: str, value: Any) -> None:
        self._v[key] = (self._clock(), value)

    def drop(self, key: str | None = None) -> None:
        if key is None:
            self._v.clear()
        else:
            self._v.pop(key, None)


# ---- pure judgements over probe results -------------------------------------------------------------------

TOMBSTONE_TABLES = ("customers", "support_tickets")


def silver_judgement(res: Mapping[str, Any], expect_lsn: bool = True) -> tuple[bool, dict[str, Any]]:
    """Logical absence in silver from one group of sparkcli.logical(): tombstone tables have no live row and no
    tombstone holding a value; the customer tombstone carries lsn >= the forget lsn; no table has a row that still
    names the customer. Returns (absent, counts-only evidence)."""
    silver = res.get("silver", {})
    ev: dict[str, Any] = {"live_rows": 0, "tombstones_with_values": 0, "customer_id_rows": 0, "tombstones_below_lsn": 0}
    for name, t in silver.items():
        if name in TOMBSTONE_TABLES:
            ev["live_rows"] += int(t.get("live_rows") or 0)
            ev["tombstones_with_values"] += int(t.get("tombstones_with_values") or 0)
        ev["customer_id_rows"] += int(t.get("customer_id_rows") or 0)
        if name == "customers" and expect_lsn and t.get("tombstones_at_lsn") is not None:
            ev["tombstones_below_lsn"] += int(t.get("footprint_rows") or 0) - int(t["tombstones_at_lsn"])
    ev["tables"] = len(silver)
    absent = (
        ev["live_rows"] == 0
        and ev["tombstones_with_values"] == 0
        and ev["customer_id_rows"] == 0
        and ev["tombstones_below_lsn"] <= 0
    )
    if "customers" not in silver:
        # the subject has no silver customer row at all: absent, but only meaningful if it never was there
        ev["no_customer_row"] = 1
    return absent, ev


def delivery_judgement(res: Mapping[str, Any], expected_min_rows: int) -> tuple[bool, dict[str, Any]]:
    """Has the forget transaction's change stream reached bronze? res['bronze_txn_rows'] counts bronze rows with
    the transaction's xid32 and lsn >= its begin lsn; the transaction touched at least expected_min_rows rows."""
    n = res.get("bronze_txn_rows")
    return (n is not None and int(n) >= expected_min_rows), {"bronze_txn_rows": n, "expected_min": expected_min_rows}


# ---- Spark-backed logical state ---------------------------------------------------------------------------


@dataclass
class SparkLogical:
    """One sparkcli.logical() call serves every silver/bronze/quarantine sink of every request in the batch."""

    groups: dict[str, dict[str, Any]]  # label -> {footprint, customers, txid, lsn}
    memo: Memo = field(default_factory=Memo)
    ttl_s: float = 20.0
    fetch: Callable[[Mapping[str, Mapping[str, Any]]], dict[str, dict[str, Any]]] | None = None

    def get(self, label: str) -> dict[str, Any]:
        def go() -> dict[str, dict[str, Any]]:
            fn = self.fetch
            if fn is None:
                from adrp.forget import sparkcli

                fn = sparkcli.logical
            return fn(self.groups)

        res: dict[str, dict[str, Any]] = self.memo.get("spark-logical", go, self.ttl_s)
        return res[label]

    def fresh(self) -> None:
        self.memo.drop("spark-logical")


# ---- sink adapters ----------------------------------------------------------------------------------------


def _searchable(subject: Subject) -> Needles:
    return subject.needles()


def _na_unsearchable() -> PhysicalResult:
    return None, {"searchable_needles": 0}, None


def logical_sinks(
    label: str, subject: Subject, spark: SparkLogical, *, qdrant_collection: str = "text_chunks", db: str = "shop"
) -> list[Sink]:
    """Logical-absence sinks of ONE request (S6). `label` is the request id: it keys the request in `spark.groups`
    and names the privacy.forget_requests row the raw_id_store sink reads."""
    from adrp.forget import postgres, qdrant

    cust = list(subject.customer_ids)
    tickets = sorted(subject.entities.get("support_tickets", ()))

    def pg_rows() -> bool:
        def arr(xs: Sequence[int]) -> str:
            return "ARRAY[" + ",".join(str(int(x)) for x in xs) + "]::bigint[]" if xs else "ARRAY[]::bigint[]"

        out = postgres.psql(
            "SELECT (SELECT count(*) FROM customers WHERE id = ANY(" + arr(cust) + ")) + "
            "(SELECT count(*) FROM support_tickets WHERE customer_id = ANY("
            + arr(cust)
            + ") OR id = ANY("
            + arr(tickets)
            + ")) + "
            "(SELECT count(*) FROM accounts WHERE customer_id = ANY(" + arr(cust) + ")) + "
            "(SELECT count(*) FROM transactions WHERE customer_id = ANY(" + arr(cust) + "));",
            db,
        )
        return int(out) == 0

    def silver() -> bool:
        return silver_judgement(spark.get(label))[0]

    def bronze() -> bool:
        return int(spark.get(label).get("bronze_rows", 0)) == 0

    def quarantine() -> bool:
        return int(spark.get(label).get("quarantine_rows", 0)) == 0

    def qd() -> bool:
        return qdrant.count_entities(subject.entity_strings(["support_tickets"]), qdrant_collection) == 0

    def raw_id_gone() -> bool:
        row = audit.get_request(label)
        return row is not None and not row.raw_present

    spark_interval = max(POLL_S, spark.ttl_s)
    return [
        FnSink("postgres", "1", logical=pg_rows),
        FnSink("silver", "9", logical=silver, min_interval_s=spark_interval),
        FnSink("bronze", "8", logical=bronze, min_interval_s=spark_interval),
        FnSink("quarantine", "10", logical=quarantine, min_interval_s=spark_interval),
        FnSink("qdrant", "14", logical=qd),
        FnSink("raw_id_store", "32", logical=raw_id_gone),
    ]


def physical_sinks(
    subject: Subject,
    *,
    iceberg: Callable[[], Mapping[str, Any]],
    file_residual: Mapping[str, Callable[[], int]],
    memo: Memo | None = None,
    wal_iterations: Callable[[], int] = lambda: 0,
    postgres_bound: Callable[[], str | None] = lambda: None,
    eventlog: Path = ROOT / "runs/data/eventlog",
    qdrant_collection: str = "text_chunks",
) -> list[Sink]:
    """Physical-absence sinks for a BATCH (`subject` is the union of the batch's subjects). `iceberg()` returns
    the whole-bucket scan summary (scan.ScanResult.to_dict()); `file_residual[name]()` re-applies a file rewrite
    and returns how many items it still had to change (0 = already clean); names: silver_batches, embedder,
    ground_truth."""
    memo = memo or Memo()
    needles = _searchable(subject)

    def iceberg_sink(name: str, row: str) -> FnSink:
        def phys() -> PhysicalResult:
            r = memo.get("iceberg", lambda: dict(iceberg()), 25.0)
            clean = r.get("total_hits", 1) == 0 and r.get("errors", 1) == 0 and r.get("undecoded_parquet", 1) == 0
            ev = {k: r.get(k) for k in ("files", "total_hits", "matched_files", "errors", "undecoded_parquet")}
            return bool(clean), ev, None

        return FnSink(name, row, physical=phys, min_interval_s=30.0)

    def postgres_phys() -> PhysicalResult:
        from adrp.forget import postgres
        from adrp.forget.rawscan import grep_files

        if not needles:
            return _na_unsearchable()
        patterns = [*needles.text(), *needles.pg_int8()]
        g = grep_files("postgres", [f"{postgres.PGDATA}/base", f"{postgres.PGDATA}/global"], patterns)
        kept, excluded = exclude_privacy(g.paths, privacy_stems(), postgres.PGDATA)
        bound = postgres_bound()
        ev = {"files_with_hits": len(kept), "privacy_files_excluded": len(excluded), "errors": g.errors}
        return len(kept) == 0 and g.errors == 0, ev, bound

    def pg_wal_phys() -> PhysicalResult:
        from adrp.forget import postgres

        if not needles:
            return _na_unsearchable()
        wal = postgres.pg_wal_files_with(needles)
        slots = postgres.raw_grep_total(needles, ("pg_replslot", "pg_logical"))
        absent = wal.files == 0 and slots == 0 and wal.errors == 0
        ev = {"wal_files_with_hits": wal.files, "slot_dir_files_with_hits": slots, "errors": wal.errors}
        bound = None
        if not absent:
            mn, seg, lag = postgres.wal_settings()
            bound = postgres.wal_bound_statement(mn, seg, lag, wal_iterations(), False)
        return absent, ev, bound

    def broker_phys() -> PhysicalResult:
        from adrp.forget import broker

        if not needles:
            return _na_unsearchable()
        raw = broker.raw_grep(subject)
        ev = {"canary_files": raw.canary_files, "key_files": raw.key_files, "errors": raw.errors}
        bound = (
            None
            if raw.absent
            else broker.broker_bound(broker.topic_config("customers"), broker.cluster_segment_min_ms())
        )
        return raw.absent, ev, bound

    def qdrant_raw() -> Any:
        from adrp.forget import qdrant

        return memo.get("qdrant-raw", lambda: qdrant.raw_grep(qdrant.subject_patterns(subject)), 20.0)

    def qdrant_phys() -> PhysicalResult:
        from adrp.forget import qdrant

        if not qdrant.subject_patterns(subject):
            return _na_unsearchable()
        raw = qdrant_raw()
        absent = raw.segment_files + raw.other_files == 0 and raw.errors == 0
        ev = {"segment_files": raw.segment_files, "other_files": raw.other_files, "errors": raw.errors}
        bound = None
        if not absent:
            bound = " ".join(b for b in qdrant.bounds(raw, qdrant_collection) if not b.startswith("qdrant WAL")) or None
        return absent, ev, bound

    def qdrant_wal_phys() -> PhysicalResult:
        from adrp.forget import qdrant

        if not qdrant.subject_patterns(subject):
            return _na_unsearchable()
        raw = qdrant_raw()
        ev = {"wal_files": raw.wal_files, "snapshots_empty": int(qdrant.snapshots_empty())}
        bound = None
        if raw.wal_files:
            bound = " ".join(b for b in qdrant.bounds(raw, qdrant_collection) if b.startswith("qdrant WAL")) or None
        return raw.wal_files == 0, ev, bound

    def seaweed_phys() -> PhysicalResult:
        from adrp.forget import storage

        g = storage.garbage_ratio()
        raw = storage.seaweed_raw_grep(subject)
        absent = g.ratio <= SEAWEED_SETTLE_RATIO and raw.files == 0
        ev = {
            "garbage_ratio_permille": int(g.ratio * 1000), "volumes": g.volumes, "deleted_bytes": g.deleted_bytes,
            "size_bytes": g.size, "raw_grep_files": raw.files,
        }  # fmt: skip
        bound = None if absent else (
            "seaweedfs: deleted needles keep their bytes in the volume .dat file until volume.vacuum rewrites it; "
            "compressible objects are stored gzip'd, so a grep can miss them and the garbage ratio is the evidence. "
            "Bounded until the next vacuum, not removed."
        )  # fmt: skip
        return absent, ev, bound

    def checkpoints_phys() -> PhysicalResult:
        from adrp.forget import storage

        if not needles:
            return _na_unsearchable()
        g = storage.spark_checkpoint_grep(subject)
        return g.files == 0 and g.errors == 0, {"files_with_hits": g.files, "errors": g.errors}, None

    def spill_phys() -> PhysicalResult:
        from adrp.forget import storage
        from adrp.forget.rawscan import service_running

        if not service_running("spark-streams"):
            return False, {"container_running": 0}, storage.SPILL_BOUND
        g = storage.spark_spill_grep(subject)
        return (
            g.files == 0 and g.errors == 0,
            {"files_with_hits": g.files, "errors": g.errors},
            (None if g.files == 0 and g.errors == 0 else storage.SPILL_BOUND),
        )

    def logs_phys() -> PhysicalResult:
        from adrp.forget import storage

        if not needles:
            return _na_unsearchable()
        hits = storage.container_logs_scan(subject)
        n = sum(hits.values())
        ev = {"services_with_hits": sum(1 for v in hits.values() if v), "hits": n}
        return n == 0, ev, None if n == 0 else storage.CONTAINER_LOG_BOUND

    def dagster_phys() -> PhysicalResult:
        from adrp.forget import storage

        if not needles:
            return _na_unsearchable()
        g = storage.dagster_storage_grep(subject)
        return g.files == 0 and g.errors == 0, {"files_with_hits": g.files, "errors": g.errors}, None

    def metrics_phys() -> PhysicalResult:
        from adrp.forget import storage

        n = storage.metrics_scan(subject)
        if n < 0:
            return None, {"reachable": 0}, None
        return n == 0, {"hits": n}, None

    def manifests_phys() -> PhysicalResult:
        from adrp.forget import storage

        r = storage.manifests_scan(subject)
        return r["hits"] == 0, {"files": r["files"], "hits": r["hits"]}, None

    def eventlog_phys() -> PhysicalResult:
        from adrp.forget import scan as fscan
        from adrp.forget.ids import Matcher

        residual = file_residual["silver_batches"]()
        big = [i for i in subject.customer_ids if i >= 10**12]
        res = fscan.scan(fscan.LocalStore(eventlog), Matcher(subject_ids=big, canaries=subject.canaries))
        ev = {
            "files_scanned": res.files, "hits": res.total_hits, "errors": res.errors, "residual_items": residual,
            "undecoded_parquet": res.undecoded_parquet,
        }  # fmt: skip
        return residual == 0 and res.clean(), ev, None

    def residual_sink(name: str, row: str, key: str) -> FnSink:
        def phys() -> PhysicalResult:
            n = file_residual[key]()
            return n == 0, {"residual_items": n}, None

        return FnSink(name, row, physical=phys, min_interval_s=10.0)

    return [
        FnSink("postgres", "1,26", physical=postgres_phys, min_interval_s=30.0),
        iceberg_sink("bronze", "8,11"),
        iceberg_sink("silver", "9"),
        iceberg_sink("quarantine", "10"),
        FnSink("qdrant", "14", physical=qdrant_phys, min_interval_s=10.0),
        FnSink("qdrant_wal", "15", physical=qdrant_wal_phys, min_interval_s=10.0),
        FnSink("broker", "4,5,29", physical=broker_phys, min_interval_s=10.0),
        FnSink("pg_wal", "2", physical=pg_wal_phys, min_interval_s=15.0),
        FnSink("seaweedfs", "28", physical=seaweed_phys, presence_expected=False, min_interval_s=15.0),
        FnSink("spark_checkpoints", "18", physical=checkpoints_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("spark_spill", "19", physical=spill_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("container_logs", "27", physical=logs_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("dagster_storage", "21", physical=dagster_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("metrics", "22", physical=metrics_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("manifests", "30", physical=manifests_phys, presence_expected=False, min_interval_s=30.0),
        FnSink("event_logs", "17,21", physical=eventlog_phys, min_interval_s=30.0),
        residual_sink("embedder_files", "20,25", "embedder"),
        residual_sink("ground_truth", "16", "ground_truth"),
        raw_id_physical_sink(subject),
    ]


def exclude_privacy(paths: Iterable[str], stems: Iterable[str], pgdata: str) -> tuple[list[str], list[str]]:
    """Split grep hits into (kept, excluded). privacy.forget_requests deliberately holds the raw id and the scratch
    footprint until the request is finished, so its own files (heap, forks `_fsm`/`_vm`, segments `.1`, TOAST, indexes)
    are not evidence about the shop tables; they are checked by the raw_id_store sink after the id is NULLed.
    `stems` are relative file paths such as 'base/16384/24601' as pg_relation_filepath() prints them."""
    absolute = [f"{pgdata.rstrip('/')}/{st}" for st in stems]
    kept: list[str] = []
    excluded: list[str] = []
    for p in sorted(paths):
        hit = any(p == st or p.startswith((st + "_", st + ".")) for st in absolute)
        (excluded if hit else kept).append(p)
    return kept, excluded


def privacy_stems(db: str = "shop") -> list[str]:
    """pg_relation_filepath of every relation in schema privacy, their TOAST tables and TOAST indexes."""
    from adrp.forget import postgres

    out = postgres.psql(
        "SELECT pg_relation_filepath(c.oid) FROM pg_class c WHERE c.relnamespace = 'privacy'::regnamespace "
        "AND c.relkind IN ('r', 'i') "
        "UNION SELECT pg_relation_filepath(t.oid) FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid "
        "WHERE c.relnamespace = 'privacy'::regnamespace "
        "UNION SELECT pg_relation_filepath(i.indexrelid) FROM pg_index i "
        "JOIN pg_class c ON c.reltoastrelid = i.indrelid "
        "WHERE c.relnamespace = 'privacy'::regnamespace;",
        db,
    )
    return [ln for ln in out.splitlines() if ln.strip()]


def raw_id_physical_sink(subject: Subject) -> FnSink:
    """Row 32, physical half: after the raw id is NULLed and the privacy tables VACUUM FULLed, the customer id must
    not be in the Postgres data files. Only ids >= 10**12 can be searched (a short id would match anything)."""

    def phys() -> PhysicalResult:
        from adrp.forget import postgres

        ids = Needles(ids=tuple(i for i in subject.customer_ids if i >= 10**12))
        if not ids.text():
            return None, {"searchable_needles": 0}, None
        files = postgres.raw_grep_total(ids, ("base", "global"))
        return files == 0, {"files_with_hits": files}, None

    return FnSink("raw_id_store", "32", physical=phys, presence_expected=False, min_interval_s=30.0)


def to_json(outcomes: Mapping[str, SinkOutcome]) -> str:
    return json.dumps({n: outcome_dict(o) for n, o in sorted(outcomes.items())}, default=str)
