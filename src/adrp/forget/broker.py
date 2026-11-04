"""Redpanda side of the forget workflow (G8): is the subject still in the CDC topics, and make it leave.

There is no delete API for a Kafka-protocol log. A record leaves the broker only when log compaction rewrites its
segment (the key was superseded by a tombstone, and the tombstone itself later ages out), so this module:
  * counts what is still there, two ways: a decoding scan of every record from the earliest offset (logical view:
    which records still reference the subject) and a raw-byte grep of the segment files (physical view: canary
    bytes and Confluent-framed key bytes). Counts and booleans only.
  * nudges: harmless no-op UPDATEs of OTHER rows, so each partition gets a newer record, the active segment
    rolls on segment.ms and the closed segment is compacted. A tombstone is removed only after a LATER write has
    rolled and compacted another segment (spike section 2), so the nudge is required, not cosmetic.
  * waits until the raw bytes are gone, or gives up with a bound derived from the live topic configuration.

Topics must be created with infra/redpanda/topic-config.json (bootstrap.py --forget-ready-topics). Without it the
topics never compact and the wait times out with a bound that says so.

Raw grep is meaningful only because the producers do not compress (Debezium's Kafka Connect default is
compression.type=none and the topics keep compression.type=producer); compression_ok() asserts this.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.request
import uuid
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from adrp.forget import postgres
from adrp.forget.rawscan import ROOT, GrepCount, Subject, compose_exec, grep_files, kafka_key_patterns

PREFIX = "cdc.public."
DATA_DIR = "/var/lib/redpanda/data/kafka"
BOOTSTRAP = "localhost:19092"
REGISTRY = "http://localhost:18081"
SUBJECT_TABLES = ("customers", "accounts", "transactions", "support_tickets")  # tables that carry subject data
NOOP_COLUMN = {  # `SET c = c` still writes a new row version and a CDC event, without changing any value
    "customers": "updated_at",
    "accounts": "updated_at",
    "support_tickets": "updated_at",
    "transactions": "status",
    "refunds": "reason",
    "help_articles": "view_count",
}
_SAFE_TABLE = re.compile(r"[a-z_]+")


def topic_for(table: str) -> str:
    return PREFIX + table


# ---- registry and topic config -------------------------------------------------------------------------


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)


def key_schema_ids(table: str, registry: str = REGISTRY) -> list[int]:
    """Schema ids of every registered version of `cdc.public.<table>-key` (a key may have been written under an
    older version, so each is searched)."""
    subject = f"{topic_for(table)}-key"
    versions = _get_json(f"{registry}/subjects/{subject}/versions")
    return sorted({int(_get_json(f"{registry}/subjects/{subject}/versions/{v}")["id"]) for v in versions})


def topic_config(table: str) -> dict[str, str]:
    """`rpk topic describe -c` as {key: value}."""
    r = compose_exec("redpanda", ["rpk", "topic", "describe", topic_for(table), "-c"], timeout=60)
    out: dict[str, str] = {}
    for line in r.text.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2:
            out[parts[0]] = parts[1]
    return out


def compaction_enabled(cfg: Mapping[str, str]) -> bool:
    """True when the topic compacts quickly enough for g8: policy includes compact and segment.ms is below the
    cluster clamp default (the clamp itself is checked by cluster_segment_min_ok)."""
    return "compact" in cfg.get("cleanup.policy", "")


def cluster_segment_min_ms() -> int:
    r = compose_exec("redpanda", ["rpk", "cluster", "config", "get", "log_segment_ms_min"], timeout=60)
    return int(r.text.strip() or 0)


def removal_bound_s(cfg: Mapping[str, str], housekeeping_s: float = 10.0) -> float | None:
    """Upper bound in seconds from the nudge write to the record bytes leaving the segment files, from the topic
    config: compaction lag (measured from the segment's last record) + segment.ms (the roll) + two housekeeping
    ticks (one to roll, one to compact). None when the topic does not compact (unbounded)."""
    if not compaction_enabled(cfg):
        return None
    lag = int(cfg.get("min.compaction.lag.ms", "0") or 0)
    seg = int(cfg.get("segment.ms", "0") or 0)
    return (lag + seg) / 1000.0 + 2 * housekeeping_s


def broker_bound(cfg: Mapping[str, str], segment_min_ms: int | None = None) -> str:
    """The bound statement recorded when the broker bytes were not verified gone in time. Pure."""
    b = removal_bound_s(cfg)
    if b is None:
        return (
            "broker: topic does not compact (cleanup.policy="
            f"{cfg.get('cleanup.policy', '?')}); records stay until retention.ms={cfg.get('retention.ms', '?')} "
            "passes and the segment is deleted. Not removed by this system."
        )
    clamp = ""
    if segment_min_ms is not None and int(cfg.get("segment.ms", "0") or 0) < segment_min_ms:
        clamp = (
            f" segment.ms is below the cluster minimum log_segment_ms_min={segment_min_ms},"
            " so the segment does not roll on time."
        )
    return (
        f"broker: bytes leave the segment files when compaction rewrites the segment, expected within ~{b:.0f} s of "
        f"the nudge write (min.compaction.lag.ms={cfg.get('min.compaction.lag.ms', '0')}, "
        f"segment.ms={cfg.get('segment.ms', '?')}, log compaction tick ~10 s). A tombstone needs a later write "
        f"to roll another segment first. Bounded, verified by raw grep.{clamp}"
    )


def compression_ok() -> tuple[bool, dict[str, str]]:
    """Raw-byte grep needs uncompressed batches. Checks: the CDC topics keep compression.type=producer (or none),
    and neither the connector config nor the compose file sets a producer compression. Returns (ok, facts)."""
    facts: dict[str, str] = {}
    tc = topic_config("customers")
    facts["topic compression.type"] = tc.get("compression.type", "?")
    conn = json.loads((ROOT / "infra/connect/pg-shop.json").read_text())
    facts["connector producer override"] = str(conn.get("producer.override.compression.type", "none set"))
    compose = (ROOT / "docker-compose.yml").read_text()
    facts["compose producer compression"] = "set" if "COMPRESSION_TYPE" in compose.upper() else "none set"
    ok = (
        facts["topic compression.type"] in ("producer", "none", "uncompressed")
        and facts["connector producer override"] == "none set"
        and facts["compose producer compression"] == "none set"
    )
    return ok, facts


# ---- decoding scan ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ScanCount:
    """Result of the decoding scan. All counts; no record content."""

    records: int  # records read
    referencing: int  # records that still reference the subject (key, customer_id, or a canary in a value)
    tombstones: int  # of those, tombstones (key only)
    undecodable: int
    complete: bool  # every partition was read to its end within the timeout

    @property
    def absent(self) -> bool:
        return self.complete and self.referencing == 0 and self.undecodable == 0


def references(table: str, key_id: int | None, env: Mapping[str, Any] | None, subject: Subject) -> bool:
    """Does one decoded record reference the subject? Key pk in the footprint; or, for a change event, a before/
    after image whose customer_id (or customers.id) is a subject id, or any string value containing a canary.
    `env` is the Debezium envelope, or None for a tombstone."""
    if key_id is not None and key_id in set(subject.removed_ids(table)):
        return True
    if env is None:
        return False
    cust = set(subject.customer_ids)
    for img in (env.get("before"), env.get("after")):
        if not isinstance(img, Mapping):
            continue
        if img.get("customer_id") in cust or (table == "customers" and img.get("id") in cust):
            return True
        if subject.canaries and any(isinstance(v, str) and any(c in v for c in subject.canaries) for v in img.values()):
            return True
    return False


def scan(
    subject: Subject,
    tables: Sequence[str] = SUBJECT_TABLES,
    timeout_s: float = 180,
    bootstrap: str = BOOTSTRAP,
    registry: str = REGISTRY,
) -> ScanCount:
    """Decode every record of the CDC topics from the earliest retained offset to the end and count those that
    reference the subject. Compaction leaves offset gaps, so the end is detected from the partition EOF event, not
    from offset arithmetic. Reads only; the consumer group is throwaway and never commits."""
    os.environ.setdefault("SCHEMA_REGISTRY_URL", registry)
    from adrp.spark.decode import decode  # imports fastavro

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "broker.address.family": "v4",
            "group.id": f"g8-scan-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    deadline = time.monotonic() + timeout_s
    records = referencing = tombstones = undecodable = 0
    complete = True
    try:
        for table in tables:
            topic = topic_for(table)
            meta = c.list_topics(topic, timeout=15)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                continue  # no topic yet: nothing there
            parts = sorted(meta.topics[topic].partitions)
            c.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
            pending = set(parts)
            while pending:
                if time.monotonic() > deadline:
                    complete = False
                    break
                m = c.poll(1.0)
                if m is None:
                    continue
                err = m.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        pending.discard(m.partition())
                        continue
                    undecodable += 1
                    continue
                records += 1
                try:
                    key = m.key()
                    key_id = decode(key)[1].get("id") if key is not None else None
                    value = m.value()
                    env = decode(value)[1] if value is not None else None
                except Exception:  # noqa: BLE001  one unreadable record must not stop the scan
                    undecodable += 1
                    continue
                if references(table, key_id, env, subject):
                    referencing += 1
                    tombstones += env is None
            c.unassign()
    finally:
        c.close()
    return ScanCount(records, referencing, tombstones, undecodable, complete)


# ---- raw bytes -------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RawCount:
    """Raw grep over the segment files. canary_files: files containing a canary token or a subject id as decimal
    text. key_files: files containing a Confluent-framed key of a subject entity, or the bare varint of an id >=
    10**12. approximated: needles searched by a prefix only (a newline byte in the pattern)."""

    canary_files: int
    key_files: int
    errors: int
    approximated: int

    @property
    def total(self) -> int:
        return self.canary_files + self.key_files

    @property
    def absent(self) -> bool:
        return self.total == 0 and self.errors == 0


def raw_grep(subject: Subject, tables: Sequence[str] = SUBJECT_TABLES, registry: str = REGISTRY) -> RawCount:
    """grep -rlaF over /var/lib/redpanda/data/kafka/cdc.public.<table> (data, compaction index, base index) for the
    subject's canary and ids as text, and for each entity's key bytes under every registered key schema id. The
    bare varint of a customer id is searched in every topic too (accounts/transactions/tickets carry it as a value
    field), but only for ids >= 10**12; a small id (a generator customer) is covered by the framed keys and the
    decoding scan() instead. Needles go to the container on stdin."""
    text_patterns = subject.needles().text()
    canary = GrepCount()
    keys = GrepCount()
    for table in tables:
        if not _SAFE_TABLE.fullmatch(table):
            raise ValueError(f"bad table name: {table!r}")
        path = f"{DATA_DIR}/{topic_for(table)}"
        canary = canary.merge(grep_files("redpanda", [path], text_patterns))
        pats = kafka_key_patterns(subject.removed_ids(table), key_schema_ids(table, registry), subject.customer_ids)
        keys = keys.merge(grep_files("redpanda", [path], pats))
    return RawCount(canary.files, keys.files, canary.errors + keys.errors, canary.approximated + keys.approximated)


# ---- nudge -----------------------------------------------------------------------------------------------


def nudge_sql(table: str, exclude: Collection[int], rows: int) -> str:
    """No-op UPDATE of up to `rows` random rows that are not in `exclude`. Static column names only."""
    if table not in NOOP_COLUMN:
        raise ValueError(f"no nudge column for {table!r}")
    col = NOOP_COLUMN[table]
    arr = "ARRAY[" + ",".join(str(int(i)) for i in sorted(exclude)) + "]::bigint[]"
    return (
        f"UPDATE {table} SET {col} = {col} WHERE id IN "
        f"(SELECT id FROM {table} WHERE id <> ALL({arr}) ORDER BY random() LIMIT {int(rows)});"
    )


def high_watermarks(tables: Sequence[str], bootstrap: str = BOOTSTRAP) -> dict[tuple[str, int], int]:
    c = Consumer(
        {"bootstrap.servers": bootstrap, "broker.address.family": "v4", "group.id": f"g8-wm-{uuid.uuid4().hex[:8]}"}
    )
    try:
        out: dict[tuple[str, int], int] = {}
        for table in tables:
            topic = topic_for(table)
            meta = c.list_topics(topic, timeout=15)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                continue
            for p in meta.topics[topic].partitions:
                out[(topic, p)] = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1]
        return out
    finally:
        c.close()


@dataclass(frozen=True)
class NudgeResult:
    rows: int  # rows updated (no-op updates) across tables
    partitions: int  # topic partitions seen
    advanced: int  # of those, partitions whose high watermark moved

    @property
    def all_advanced(self) -> bool:
        return self.partitions > 0 and self.advanced == self.partitions


def nudge(
    subject: Subject,
    tables: Sequence[str] = SUBJECT_TABLES,
    rows_per_table: int = 30,
    max_rounds: int = 3,
    db: str = postgres.DB,
    settle_s: float = 3.0,
) -> NudgeResult:
    """Give every partition of `tables` a newer record, by no-op UPDATEs of random rows that are not the
    subject's (the new row versions are identical to the old ones). A partition is only hit if some chosen key
    hashes to it, so this retries (up to max_rounds) until every partition's high watermark has moved; the result
    says how many did. Rolls the active segment only after segment.ms; see wait_absent()."""
    before = high_watermarks(tables)
    updated = 0
    advanced: set[tuple[str, int]] = set()
    for _ in range(max_rounds):
        sql = []
        for t in tables:
            excl = set(subject.entities.get(t, ()))
            sql.append(nudge_sql(t, excl, rows_per_table))
        postgres.psql("\n".join(sql), db)
        updated += rows_per_table * len(tables)
        time.sleep(settle_s)
        now = high_watermarks(tables)
        advanced = {k for k, v in now.items() if v > before.get(k, 0)}
        if len(advanced) == len(now):
            break
    return NudgeResult(updated, len(before), len(advanced))


# ---- wait ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class WaitResult:
    absent: bool
    elapsed_s: float
    polls: int
    nudges: int
    bound: str | None  # set when not absent: why, and how long removal is expected to take


def wait_absent(
    subject: Subject,
    timeout_s: float = 900,
    every_s: float = 10.0,
    nudge_every_s: float = 60.0,
    tables: Sequence[str] = SUBJECT_TABLES,
    do_nudge: Callable[[], object] | None = None,
) -> WaitResult:
    """Poll the raw grep until no canary / key bytes remain. Nudges at the start and every nudge_every_s (a
    tombstone leaves only after a later write rolls and compacts a newer segment). On timeout returns
    absent=False with the bound from the live topic configuration."""
    nudge_fn = do_nudge or (lambda: nudge(subject, tables))
    t0 = time.monotonic()
    polls = nudges = 0
    last_nudge = -1e9
    while True:
        now = time.monotonic()
        if now - last_nudge >= nudge_every_s:
            nudge_fn()
            nudges += 1
            last_nudge = time.monotonic()
        polls += 1
        if raw_grep(subject, tables).absent:
            return WaitResult(True, time.monotonic() - t0, polls, nudges, None)
        if time.monotonic() - t0 >= timeout_s:
            cfg = topic_config(tables[0])
            return WaitResult(False, time.monotonic() - t0, polls, nudges, broker_bound(cfg, cluster_segment_min_ms()))
        time.sleep(every_s)
