"""Reconciler adapters and CLI (G10a, design S10): read the live stack at a consistent cut and compare its stores.

It never injects anything. The pure logic is in src/adrp/recon; this module talks to Docker, Postgres, Kafka, Spark.

`reconcile(...)` returns a `ReconReport`:
  .ok, .label            True when every gate check is true
  .gate_checks           list[(name, ok, detail)], names "<label> cut: ..." / "<label> reconcile: ...", ready for
                         `check(name, ok, detail)`; detail strings hold counts only
  .cut                   the `Cut` (below)
  .tables                {table: TableResult}   Postgres vs silver (counts, buckets, hex checksums)
  .topics                {topic: OffsetResult}  Kafka vs bronze offsets
  .rebuild, .qdrant      silver-vs-rebuild-from-bronze and Qdrant results (counts and hex checksums)
  .to_dict()             JSON-safe summary: counts, bucket numbers and hex64 checksums only, never row ids
  .diff()                raw ids per table and category, for .run/g10a-diff-<label>.json only (`write_diff`)

`consistent_cut(...)` returns a `Cut`:
  .lsn, .lsn_int         pg_current_wal_lsn() at the cut          .end  {topic: {partition: end offset}}
  .pause_start/.pause_end  generator container state, gen.state.next_txn and write counters of the 7 tables
  .paused                True when the generator was down and nothing wrote between the two samples
  .flush_reached         confirmed_flush_lsn on slot dbz_shop reached the cut
  .checkpoints_reached   silver's and bronze's committed Spark offsets cover `end` (.committed, .lag)
  .ok                    all three; .verify_paused() takes the closing sample; .to_dict() is manifest-safe

Offsets are never assumed contiguous: a compacting topic drops records. g2.read_range, g3.Drain, g8 SafeDrain and
g3.bronze_report are not used here. Wording: nothing here claims exactly-once; deletions are "logical" or "physical".
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import g2_check as g2
import g3_check as g3
import g6_check as g6
from _env import ROOT
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition
from manifest import write_manifest

sys.path.insert(0, str(ROOT / "src"))
from adrp.forget import broker
from adrp.forget.rawscan import wait_until
from adrp.forget.sparkcli import lsn_to_int
from adrp.recon.canonical import PK, hex64
from adrp.recon.classify import wording_violations
from adrp.recon.compare import TableResult, compare_table
from adrp.recon.cut import Offsets, latest_committed, parse_offsets_file, uncovered
from adrp.recon.offsets import OffsetResult, compare_offsets

TABLES = g2.TABLES
SEED = g2.SEED
SLOT = "dbz_shop"
QUERIES = ("silver", "bronze")  # the two Spark streaming queries; their checkpoints live in /checkpoints/<query>
COMPOSE_PROJECT = "adrp"
CHECKPOINT_VOLUME = f"{COMPOSE_PROJECT}_spark-checkpoints"
KAFKA_BOOTSTRAP = "localhost:19092"
RUN_DIR = ROOT / ".run"  # gitignored
SCOPE_NOTE = (
    "A clean reconcile means no mismatch was found between the compared stores at this cut. "
    "It does not show that undetected faults are absent."
)
WRITE_COUNTERS_SQL = (
    "SELECT relname || '|' || (n_tup_ins + n_tup_upd + n_tup_del) FROM pg_stat_user_tables "
    f"WHERE schemaname = 'public' AND relname IN ({', '.join(repr(t) for t in TABLES)}) ORDER BY relname"
)  # the heartbeat table is not listed: Debezium writes it while the generator is paused


# ---- pause state (the cut is only consistent when no writer runs) -----------------------------------------------


@dataclass
class PauseState:
    generator_running: bool
    next_txn: int
    writes: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {"generator_running": self.generator_running, "next_txn": self.next_txn, "writes": dict(self.writes)}


def parse_write_counters(text: str) -> dict[str, int]:
    """`table|n` lines of WRITE_COUNTERS_SQL."""
    out: dict[str, int] = {}
    for line in text.splitlines():
        if line.strip():
            name, _, n = line.partition("|")
            out[name.strip()] = int(n)
    return out


def pause_stable(a: PauseState, b: PauseState) -> bool:
    """No generator in either sample, and neither gen.state.next_txn nor any table's write counter moved."""
    return not a.generator_running and not b.generator_running and a.next_txn == b.next_txn and a.writes == b.writes


def generator_running() -> bool:
    out = g3.sh(
        "docker", "ps", "-q",
        "--filter", f"label=com.docker.compose.project={COMPOSE_PROJECT}",
        "--filter", "label=com.docker.compose.service=generator",
    )  # fmt: skip
    return bool(out.strip())


def pause_state() -> PauseState:
    running = generator_running()
    next_txn = int(g2.psql("SELECT coalesce((SELECT next_txn FROM gen.state WHERE id = 1), 0)") or 0)
    return PauseState(running, next_txn, parse_write_counters(g2.psql(WRITE_COUNTERS_SQL)))


def settle_pause_state(max_wait_s: float = 30.0, every_s: float = 2.0) -> tuple[PauseState, bool]:
    """Sample until two consecutive samples are stable (stats of a just-stopped writer flush within about a second).
    Returns (last sample, True when stable and no generator ran)."""
    deadline = time.monotonic() + max_wait_s
    prev = pause_state()
    while True:
        time.sleep(every_s)
        cur = pause_state()
        if pause_stable(prev, cur):
            return cur, True
        prev = cur
        if time.monotonic() >= deadline:
            return cur, False


# ---- Spark checkpoints ---------------------------------------------------------------------------------------------


def _volume_run(*argv: str) -> subprocess.CompletedProcess[str]:
    """Run a command in a throwaway container with the checkpoint volume mounted read-only."""
    return subprocess.run(
        ["docker", "run", "--rm", "--pull", "never", "-v", f"{CHECKPOINT_VOLUME}:/checkpoints:ro",
         "--entrypoint", argv[0], g3.SPARK_IMAGE_REF, *argv[1:]],
        capture_output=True, encoding="utf-8", errors="replace", check=False, timeout=120,
    )  # fmt: skip


def checkpoint_offsets(query: str = "silver") -> Offsets:
    """The Kafka offsets (next offset to read, the same meaning as a high watermark) of the newest COMMITTED batch
    of a streaming query: the last line of `offsets/N`, N the highest `commits/N`. {} when nothing is committed."""
    if query not in QUERIES:
        raise ValueError(f"unknown streaming query {query!r}")
    listing = _volume_run("ls", "-1", f"/checkpoints/{query}/commits")
    if listing.returncode != 0:
        return {}
    n = latest_committed(listing.stdout.split())
    if n is None:
        return {}
    got = _volume_run("cat", f"/checkpoints/{query}/offsets/{n}")
    if got.returncode != 0:
        raise RuntimeError(f"cannot read {query} offsets/{n}: {got.stderr.strip()[:200]}")
    return parse_offsets_file(got.stdout)


# ---- Kafka ---------------------------------------------------------------------------------------------------------


class RangeTracker:
    """Which partitions of a [start, end) read are still open. A partition closes at its partition EOF or when the
    offset `end - 1` or beyond is seen, whichever comes first, so a missing `end - 1` (compacted away) cannot hang
    the read (g2.read_range waits for it)."""

    def __init__(self, start: Mapping[int, int], end: Mapping[int, int]) -> None:
        self.start = dict(start)
        self.end = dict(end)
        self.open = {p for p, e in self.end.items() if e > self.start.get(p, 0)}

    def see(self, partition: int, offset: int) -> bool:
        """Record a read record. True when it lies inside [start, end) of its partition."""
        inside = self.start.get(partition, 0) <= offset < self.end.get(partition, 0)
        if offset >= self.end.get(partition, 0) - 1:
            self.open.discard(partition)
        return inside

    def eof(self, partition: int) -> None:
        self.open.discard(partition)

    @property
    def done(self) -> bool:
        return not self.open


def _scan(topic: str, start: Mapping[int, int], end: Mapping[int, int], timeout_s: float = 180.0) -> Iterator[Any]:
    """Yield every record in [start, end) per partition that exists in the broker now."""
    tracker = RangeTracker(start, end)
    if tracker.done:
        return
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"reconcile-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    deadline = time.monotonic() + timeout_s
    try:
        c.assign([TopicPartition(topic, p, start.get(p, 0) or OFFSET_BEGINNING) for p in sorted(tracker.open)])
        while not tracker.done:
            m = c.poll(1.0)
            if m is None:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"timed out scanning {topic}; open partitions {sorted(tracker.open)}")
                continue
            partition = m.partition()
            assert partition is not None
            err = m.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    tracker.eof(partition)
                    continue
                raise KafkaException(err)
            offset = m.offset()
            assert offset is not None
            if tracker.see(partition, offset):
                yield m
    finally:
        c.close()


def kafka_present_offsets(topic: str, end: Mapping[int, int]) -> dict[int, set[int]]:
    """The offsets below `end` that exist in the topic now, per partition. Ends at EOF; never waits for a specific
    offset, so a compacting topic (which drops records) is read the same way as any other."""
    present: dict[int, set[int]] = {p: set() for p in end}
    for m in _scan(topic, {}, end):
        present[m.partition()].add(m.offset())
    return present


@dataclass(frozen=True)
class RawRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None
    headers: tuple[tuple[str, bytes | None], ...] = ()


def read_raw_range(topic: str, start: Mapping[int, int], end: Mapping[int, int]) -> list[RawRecord]:
    """The records in [start, end) per partition that exist now, undecoded, sorted by (partition, offset). Offsets
    that compaction removed are simply absent."""
    out = []
    for m in _scan(topic, start, end):
        ts_type, ts = m.timestamp()
        out.append(
            RawRecord(
                topic, m.partition(), m.offset(), m.key(), m.value(), ts if ts_type != 0 else None,
                tuple((str(k), v) for k, v in (m.headers() or [])),
            )
        )  # fmt: skip
    return sorted(out, key=lambda r: (r.partition, r.offset))


def detect_compacting() -> dict[str, bool | None]:
    """Per table: does its topic compact? None when the topic configuration could not be read."""
    out: dict[str, bool | None] = {}
    for t in TABLES:
        cfg = broker.topic_config(t)
        out[t] = broker.compaction_enabled(cfg) if "cleanup.policy" in cfg else None
    return out


# ---- Postgres ------------------------------------------------------------------------------------------------------


def parse_columns(text: str) -> list[tuple[str, str]]:
    """`name|data_type` lines of information_schema.columns."""
    cols = []
    for line in text.splitlines():
        if line.strip():
            name, _, typ = line.partition("|")
            cols.append((name.strip(), typ.strip()))
    return cols


def parse_pg_json_lines(text: str) -> list[dict[str, Any]]:
    """`SELECT row_to_json(t)` lines. Floats parse as Decimal (numerics must not pass through binary floats). A NULL
    column is absent from to_json output in some paths; consumers use row.get(column)."""
    return [json.loads(line, parse_float=Decimal) for line in text.splitlines() if line.strip()]


def pg_rows(table: str) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """All rows of a public table and its (column, information_schema data_type) pairs, read now."""
    if table not in TABLES:
        raise ValueError(f"not a reconciled table: {table!r}")
    cols = parse_columns(
        g2.psql(
            "SELECT column_name || '|' || data_type FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table}' ORDER BY ordinal_position"
        )
    )
    return parse_pg_json_lines(g2.psql(f"SELECT row_to_json(t) FROM {table} t")), cols


# ---- the consistent cut --------------------------------------------------------------------------------------------


@dataclass
class Cut:
    lsn: str
    lsn_int: int
    pause_start: PauseState
    pause_settled: bool  # two consecutive samples before the cut were stable with no generator
    end: Offsets = field(default_factory=dict)
    missing_topics: list[str] = field(default_factory=list)  # no topic yet: no record was ever produced for it
    flush_lsn: str | None = None
    flush_reached: bool = False
    flush_wait_s: float = 0.0
    committed: dict[str, Offsets] = field(default_factory=dict)
    lag: dict[str, dict[str, dict[int, int]]] = field(default_factory=dict)  # query -> topic -> partition -> lag
    checkpoints_reached: bool = False
    checkpoint_wait_s: float = 0.0
    pause_end: PauseState | None = None
    notes: list[str] = field(default_factory=list)

    def verify_paused(self) -> bool:
        """Take the closing sample (call after the last read). True when the generator stayed down and nothing wrote."""
        self.pause_end = pause_state()
        return self.paused

    @property
    def paused(self) -> bool:
        return self.pause_settled and self.pause_end is not None and pause_stable(self.pause_start, self.pause_end)

    @property
    def ok(self) -> bool:
        return self.paused and self.flush_reached and self.checkpoints_reached

    def lagging(self) -> dict[str, int]:
        """Partitions behind the end offsets, per streaming query."""
        return {q: sum(len(parts) for parts in lag.values()) for q, lag in self.lag.items()}

    def to_dict(self) -> dict[str, Any]:
        return {
            "lsn": self.lsn,
            "confirmed_flush_lsn": self.flush_lsn,
            "paused": self.paused,
            "pause_start": self.pause_start.to_dict(),
            "pause_end": self.pause_end.to_dict() if self.pause_end else None,
            "flush_reached": self.flush_reached,
            "flush_wait_s": round(self.flush_wait_s, 1),
            "checkpoints_reached": self.checkpoints_reached,
            "checkpoint_wait_s": round(self.checkpoint_wait_s, 1),
            "end_offsets": self.end,
            "committed_offsets": self.committed,
            "lagging_partitions": self.lagging(),
            "lag": self.lag,
            "missing_topics": self.missing_topics,
            "notes": self.notes,
        }


def confirmed_flush_text() -> str | None:
    out = g2.psql(
        f"SELECT coalesce(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    )
    return out or None


def consistent_cut(consumer: Any, timeout: float = 300.0, checkpoint_timeout: float = 420.0) -> Cut:
    """Take a cut while the (already paused) generator is down: the WAL position now, wait until Debezium has
    flushed it (slot confirmed_flush_lsn), read the topics' end offsets, then wait until the silver and bronze
    streaming queries have committed batches that cover those offsets. The waits only report: nothing here injects
    or changes state. Run `Cut.verify_paused()` after the last read to complete the pause check."""
    pause, settled = settle_pause_state()
    lsn = g2.psql("SELECT pg_current_wal_lsn()")
    cut = Cut(lsn=lsn, lsn_int=lsn_to_int(lsn), pause_start=pause, pause_settled=settled)
    if not settled:
        cut.notes.append("the generator was running or a table was still being written: waits skipped")
        return cut

    def flushed() -> bool:
        cut.flush_lsn = confirmed_flush_text()
        return cut.flush_lsn is not None and lsn_to_int(cut.flush_lsn) >= cut.lsn_int

    cut.flush_reached, cut.flush_wait_s = wait_until(flushed, timeout, 3.0)
    if not cut.flush_reached:
        cut.notes.append(f"confirmed_flush_lsn did not reach the cut within {timeout:.0f} s")
        return cut

    for t in TABLES:
        topic = broker.topic_for(t)
        ends = g2.end_offsets(consumer, topic)
        if ends:
            cut.end[topic] = ends
        else:
            cut.missing_topics.append(topic)

    def covered() -> bool:
        try:
            for q in QUERIES:
                cut.committed[q] = checkpoint_offsets(q)
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as e:
            cut.notes.append(f"checkpoint read failed: {type(e).__name__}")
            return False
        cut.lag = {
            q: {topic: dict(parts) for topic, parts in uncovered(cut.committed[q], cut.end).items()} for q in QUERIES
        }
        return not any(cut.lag.values())

    cut.checkpoints_reached, cut.checkpoint_wait_s = wait_until(covered, checkpoint_timeout, 5.0)
    if not cut.checkpoints_reached:
        cut.notes.append(f"streaming checkpoints did not cover the end offsets within {checkpoint_timeout:.0f} s")
    return cut


# ---- pure parsing of the Spark probe's result ----------------------------------------------------------------------


def expand_ranges(ranges: Iterable[Sequence[int]]) -> set[int]:
    """Inclusive [lo, hi] islands to the set of offsets."""
    out: set[int] = set()
    for lo, hi in ranges:
        if hi < lo:
            raise ValueError(f"bad range [{lo}, {hi}]")
        out.update(range(lo, hi + 1))
    return out


def bronze_inputs(topic_entry: Mapping[str, Mapping[str, Any]]) -> tuple[dict[int, int], dict[int, set[int]]]:
    """One topic's `bronze_offsets` entry of probe recon: ({partition: rows}, {partition: distinct offsets}). Raises
    when a partition's distinct count disagrees with its ranges."""
    rows: dict[int, int] = {}
    distinct: dict[int, set[int]] = {}
    for part, info in topic_entry.items():
        offsets = expand_ranges(info["ranges"])
        if len(offsets) != int(info["distinct"]):
            raise ValueError(f"partition {part}: {len(offsets)} offsets in ranges, distinct={info['distinct']}")
        rows[int(part)] = int(info["rows"])
        distinct[int(part)] = offsets
    return rows, distinct


def parse_quarantine(pairs: Iterable[Sequence[Any]]) -> dict[int, int]:
    """probe recon `quarantine[table]` ([pk json string, lsn] pairs) to {id: highest quarantined LSN}."""
    out: dict[int, int] = {}
    for pk, lsn in pairs:
        key = int(json.loads(pk)[PK]) if isinstance(pk, str) else int(pk[PK])
        out[key] = max(out.get(key, -1), int(lsn))
    return out


def rebuild_summary(entry: Mapping[str, Any] | None) -> dict[str, Any]:
    """probe recon `rebuild[table]` with checksums as hex64 (a plain 16-digit hex could trip the 13-digit id scan)."""
    if not entry:
        return {"equal": False, "error": "missing"}
    out: dict[str, Any] = {"rows": entry.get("rows"), "equal": bool(entry.get("equal")) and "error" not in entry}
    for src, dst in (("checksum", "checksum"), ("rebuilt_checksum", "rebuilt_checksum")):
        if entry.get(src) is not None:
            out[dst] = hex64(int(entry[src]))
    if entry.get("rebuilt_rows") is not None:
        out["rebuilt_rows"] = entry["rebuilt_rows"]
    if "error" in entry:
        out["error"] = entry["error"]
    return out


def dup_rows(rows: Mapping[int, int], distinct: Mapping[int, Collection[int]]) -> int:
    return sum(rows.values()) - sum(len(s) for s in distinct.values())


# ---- the report ----------------------------------------------------------------------------------------------------

COUNT_FIELDS = (
    "missing",
    "extra_live",
    "resurrected",
    "differing",
    "duplicates",
    "silver_only_columns",
    "unreconciled_quarantine",
)
# every TableResult.FAILING count is judged by exactly one gate check (tests/test_reconcile_adapters.py)
JUDGED_ELSEWHERE = ("bucket_mismatches", "tombstone_nonnull", "unavailable_values")


def _nonzero(results: Mapping[str, TableResult], names: Sequence[str]) -> str:
    bad = {t: {n: getattr(r, n) for n in names if getattr(r, n)} for t, r in results.items()}
    bad = {t: v for t, v in bad.items() if v}
    return json.dumps(bad, sort_keys=True) if bad else "all 0"


@dataclass
class ReconReport:
    label: str
    cut: Cut
    tables: dict[str, TableResult]
    topics: dict[str, OffsetResult]
    ledger_total: bool  # expected_bronze_dups was one total over all topics (otherwise per table)
    dup_rows_total: int
    expected_dups_total: int
    rebuild: dict[str, dict[str, Any]]
    qdrant: dict[str, Any]
    compacting: dict[str, bool]  # effective, per table
    compacting_unverified: list[
        str
    ]  # tables whose declared setting differs from, or could not be read against, the broker
    notes: list[str] = field(default_factory=list)
    durations_s: dict[str, float] = field(default_factory=dict)
    diff_file: str | None = None

    @property
    def ledger_equal(self) -> bool:
        return (
            self.dup_rows_total == self.expected_dups_total
            if self.ledger_total
            else all(r.dups_equal for r in self.topics.values())
        )

    @property
    def gate_checks(self) -> list[tuple[str, bool, str]]:
        L = self.label
        c = self.cut
        t = self.tables
        pe = c.pause_end
        out: list[tuple[str, bool, str]] = [
            (
                f"{L} cut: the generator stayed paused during the reconcile",
                c.paused,
                f"generator_running {c.pause_start.generator_running}->{pe.generator_running if pe else None}; "
                f"next_txn {c.pause_start.next_txn}->{pe.next_txn if pe else None}; settled={c.pause_settled}",
            ),
            (
                f"{L} cut: confirmed_flush_lsn reached the cut",
                c.flush_reached,
                f"cut={c.lsn} flushed={c.flush_lsn} waited {c.flush_wait_s:.0f} s",
            ),
            (
                f"{L} cut: silver and bronze committed offsets reached the topic end offsets",
                c.checkpoints_reached,
                f"lagging partitions {json.dumps(c.lagging(), sort_keys=True)} waited {c.checkpoint_wait_s:.0f} s",
            ),
            (
                f"{L} reconcile: per-table counts equal Postgres",
                bool(t)
                and all(
                    r.pg_count == r.silver_live and all(getattr(r, n) == 0 for n in COUNT_FIELDS) for r in t.values()
                ),
                _nonzero(t, COUNT_FIELDS)
                + f"; postgres rows {sum(r.pg_count for r in t.values())}, silver live {sum(r.silver_live for r in t.values())}",
            ),
            (
                f"{L} reconcile: per-table and per-bucket checksums equal Postgres",
                bool(t) and all(r.digest_equal and r.bucket_mismatches == 0 for r in t.values()),
                json.dumps({n: r.mismatch_buckets for n, r in t.items() if r.mismatch_buckets}, sort_keys=True) or "{}",
            ),
            (
                f"{L} reconcile: silver tombstones have no live Postgres row and no non-key values (logical delete)",
                bool(t) and all(r.tombstone_nonnull == 0 for r in t.values()),
                f"tombstones {sum(r.silver_tombstones for r in t.values())}; " + _nonzero(t, ("tombstone_nonnull",)),
            ),
            (
                f"{L} reconcile: bronze holds every record present in Kafka; duplicate rows equal the replay ledger",
                bool(self.topics)
                and all(r.ok for r in self.topics.values())
                and self.ledger_equal
                and not self.compacting_unverified,
                f"duplicate rows {self.dup_rows_total} expected {self.expected_dups_total}; "
                f"missing in bronze {sum(r.missing_in_bronze for r in self.topics.values())}; "
                f"beyond end {sum(r.beyond_end for r in self.topics.values())}; "
                f"gaps {sum(r.gaps for r in self.topics.values())} (allowed only on compacting topics); "
                f"compacting {sorted(k for k, v in self.compacting.items() if v)}; unverified {self.compacting_unverified}",
            ),
            (
                f"{L} reconcile: silver equals its rebuild from bronze (count + checksum)",
                bool(self.rebuild) and all(e.get("equal") for e in self.rebuild.values()),
                json.dumps(
                    {n: e.get("error", "differs") for n, e in self.rebuild.items() if not e.get("equal")},
                    sort_keys=True,
                ),
            ),
            (
                f"{L} reconcile: Qdrant has 0 stale, orphaned, duplicate, wrong-model and missing, and equals Postgres",
                bool(self.qdrant.get("consistent", {}).get("ok"))
                and bool(self.qdrant.get("matches_postgres", {}).get("ok")),
                json.dumps(self.qdrant, sort_keys=True, default=str)[:400],
            ),
            (
                f"{L} reconcile: no __debezium_unavailable_value in silver",
                bool(t) and all(r.unavailable_values == 0 for r in t.values()),
                _nonzero(t, ("unavailable_values",)),
            ),
        ]
        return out

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.gate_checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "ok": self.ok,
            "scope_note": SCOPE_NOTE,
            "gate_checks": [{"check": n, "ok": ok, "detail": d} for n, ok, d in self.gate_checks],
            "cut": self.cut.to_dict(),
            "tables": {n: r.to_dict() for n, r in self.tables.items()},
            "topics": {n: _offset_dict(r, self.ledger_total) for n, r in self.topics.items()},
            "bronze_duplicates": {
                "scope": "total over all topics" if self.ledger_total else "per table",
                "duplicate_rows": self.dup_rows_total,
                "expected": self.expected_dups_total,
                "equal": self.ledger_equal,
            },
            "compacting": self.compacting,
            "compacting_unverified": self.compacting_unverified,
            "rebuild": self.rebuild,
            "qdrant": self.qdrant,
            "notes": self.notes,
            "durations_s": {k: round(v, 1) for k, v in self.durations_s.items()},
            "diff_file": self.diff_file,
        }

    def diff(self) -> dict[str, dict[str, list[int]]]:
        """Raw ids per table and category. For the gitignored diff file only: never put this in a manifest."""
        return {n: r.diff_keys for n, r in self.tables.items() if r.diff_keys}


def _offset_dict(r: OffsetResult, ledger_total: bool) -> dict[str, Any]:
    d = r.to_dict()
    if ledger_total:  # the ledger is judged over all topics, so a per-topic equal/expected would mislead
        for k in ("expected_dups", "dups_equal", "ok"):
            d.pop(k)
        d["ok_ignoring_ledger"] = r.ok  # passed expected_dups = its own duplicate rows, so ok judges everything else
    return d


def write_diff(report: ReconReport, directory: Path = RUN_DIR) -> Path:
    """Write the raw ids of every difference to .run/g10a-diff-<label>.json (gitignored). Keys only, no payloads.
    Always written, so a stale file from an earlier run with the same label cannot be mistaken for this one."""
    directory.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", report.label).strip("-") or "unlabelled"
    path = directory / f"g10a-diff-{safe}.json"
    body = {"label": report.label, "written_at": datetime.now(timezone.utc).isoformat(), "differences": report.diff()}
    path.write_text(json.dumps(body, indent=1, sort_keys=True) + "\n")
    return path


# ---- the reconcile ---------------------------------------------------------------------------------------------------


def generator_deleted_ever(next_txn: int) -> dict[str, set[int]]:
    """Keys the generator deleted at some point, from its deterministic stream up to gen.state.next_txn."""
    out: dict[str, set[int]] = defaultdict(set)
    for op in g2.generator_ops(0, next_txn):
        if op.op == "D":
            out[op.table].add(op.pk)
    return out


def _qdrant_checks() -> dict[str, Any]:
    out: dict[str, Any] = {}
    ok, waited, counts = g6.wait_consistent(300)
    out["consistent"] = {"ok": bool(ok), "waited_s": round(waited, 1), "counts": counts}
    try:
        ok2, waited2, counts2 = g6.wait_matches_postgres(240)
    except SystemExit as e:  # the embedder container could not be reached
        ok2, waited2, counts2 = False, 0.0, {"error": str(e)[:120]}  # type: ignore[dict-item]
    out["matches_postgres"] = {"ok": bool(ok2), "waited_s": round(waited2, 1), "counts": counts2}
    return out


def reconcile(
    consumer: Any,
    label: str,
    expected_bronze_dups: int | Mapping[str, int],
    compacting: bool,
    deleted_ever: Mapping[str, Iterable[int]] | None = None,
    *,
    check_qdrant: bool = True,
) -> ReconReport:
    """Reconcile the stores at a consistent cut. The caller has paused the generator; this verifies it (and that no
    table was written) at the start and again at the end.

    `expected_bronze_dups`: bronze rows minus distinct (partition, offset) the replay ledger accounts for, either one
    total over all topics (int) or {table: n}. `compacting`: what the caller expects of the topics; it is checked
    against the broker's topic configs (broker.compaction_enabled), and the offsets gate fails if they differ,
    because a topic that does not compact must have no offset gap. `deleted_ever`: {table: ids}; default, derived from
    the generator stream. Qdrant is skipped (and its gate check fails) when `check_qdrant` is False or the cut was not
    reached."""
    t0 = time.monotonic()
    dur: dict[str, float] = {}
    cut = consistent_cut(consumer)
    dur["cut"] = time.monotonic() - t0
    notes: list[str] = list(cut.notes)

    detected = detect_compacting()
    effective = {t: bool(detected[t]) for t in TABLES}  # unreadable config is treated as not compacting: strict
    unverified = [t for t in TABLES if detected[t] is None or detected[t] != compacting]
    if unverified:
        notes.append(f"declared compacting={compacting} but the broker reports {detected}")

    t1 = time.monotonic()
    pg = {t: pg_rows(t) for t in TABLES}
    dur["postgres"] = time.monotonic() - t1

    t1 = time.monotonic()
    probe_rows, result = g3.probe("recon", "--tables", ",".join(TABLES))
    if not isinstance(result, dict):
        raise RuntimeError("probe recon returned no RESULT line")
    silver: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table, js in probe_rows:
        silver[table].append(json.loads(js, parse_float=Decimal))
    dur["spark_probe"] = time.monotonic() - t1

    if deleted_ever is None:
        deleted: Mapping[str, Iterable[int]] = generator_deleted_ever(cut.pause_start.next_txn)
    else:
        deleted = deleted_ever
    tables: dict[str, TableResult] = {}
    for t in TABLES:
        rows, cols = pg[t]
        quarantined = parse_quarantine(result.get("quarantine", {}).get(t, []))
        tables[t] = compare_table(t, rows, silver.get(t, []), cols, deleted.get(t, ()), quarantined)

    t1 = time.monotonic()
    ledger_total = isinstance(expected_bronze_dups, int)
    topics: dict[str, OffsetResult] = {}
    dup_total = 0
    for t in TABLES:
        topic = broker.topic_for(t)
        end = cut.end.get(topic, {})
        rows_by_p, distinct = bronze_inputs(result.get("bronze_offsets", {}).get(topic, {}))
        present = kafka_present_offsets(topic, end) if end else {}
        dups = dup_rows(rows_by_p, distinct)
        dup_total += dups
        want = dups if isinstance(expected_bronze_dups, int) else int(expected_bronze_dups.get(t, 0))
        topics[topic] = compare_offsets(present, rows_by_p, distinct, end, effective[t], want)
    dur["kafka"] = time.monotonic() - t1
    expected_total = (
        expected_bronze_dups if isinstance(expected_bronze_dups, int) else sum(expected_bronze_dups.values())
    )

    rebuild = {t: rebuild_summary(result.get("rebuild", {}).get(t)) for t in TABLES}

    t1 = time.monotonic()
    if check_qdrant and cut.ok:
        qdrant = _qdrant_checks()
    else:
        qdrant = {"skipped": "check_qdrant is False" if not check_qdrant else "the cut was not reached"}
    dur["qdrant"] = time.monotonic() - t1

    cut.verify_paused()
    dur["total"] = time.monotonic() - t0
    report = ReconReport(
        label=label, cut=cut, tables=tables, topics=topics, ledger_total=ledger_total, dup_rows_total=dup_total,
        expected_dups_total=expected_total, rebuild=rebuild, qdrant=qdrant, compacting=effective,
        compacting_unverified=unverified, notes=notes, durations_s=dur,
    )  # fmt: skip
    report.diff_file = str(write_diff(report).relative_to(ROOT))
    return report


# ---- CLI -------------------------------------------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile the stores at a consistent cut. Pause the generator first.")
    ap.add_argument("--label", default="manual")
    ap.add_argument(
        "--expected-bronze-dups", type=int, default=0, help="bronze duplicate rows the replay ledger explains"
    )
    ap.add_argument("--compacting", choices=["auto", "yes", "no"], default="auto", help="what the topics should do")
    ap.add_argument(
        "--no-qdrant", action="store_true", help="skip the Qdrant checks (the Qdrant gate check then fails)"
    )
    ap.add_argument("--no-manifest", action="store_true")
    a = ap.parse_args()
    started = datetime.now(timezone.utc).isoformat()
    if a.compacting == "auto":
        kinds = set(detect_compacting().values())
        if len(kinds) != 1 or None in kinds:
            raise SystemExit(f"topics are mixed or unreadable ({kinds}); pass --compacting yes|no")
        compacting = bool(kinds.pop())
    else:
        compacting = a.compacting == "yes"
    c = g2.consumer()
    try:
        report = reconcile(c, a.label, a.expected_bronze_dups, compacting, check_qdrant=not a.no_qdrant)
    finally:
        c.close()
    body = report.to_dict()
    violations = wording_violations(json.dumps(body, default=str))
    body["wording_violations"] = violations
    print(json.dumps(body, indent=2, default=str))
    passed = report.ok and not violations
    if not a.no_manifest:
        path = write_manifest(
            "reconcile", "core", SEED,
            {"started_at": started, "passed": passed, "checks": [
                {"check": n, "ok": ok, "detail": d} for n, ok, d in report.gate_checks
            ], **{k: v for k, v in body.items() if k != "gate_checks"}},
        )  # fmt: skip
        print(f"manifest: {path.relative_to(ROOT)}", file=sys.stderr)
    print(f"{'CLEAN' if passed else 'NOT CLEAN'} ({sum(ok for _, ok, _ in report.gate_checks)}/{len(report.gate_checks)}); "
          f"differences, if any: {report.diff_file}", file=sys.stderr)  # fmt: skip
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
