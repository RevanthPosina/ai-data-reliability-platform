"""Postgres side of the forget workflow (G8): the forget transaction, privacy.* bookkeeping, physical removal.

All SQL runs as `psql` inside the postgres container (`docker compose exec -T postgres psql`), script on STDIN.
A raw customer id therefore appears only in the stdin script, never on a command line, in a log or in a returned
string. Functions return counts, booleans and dataclasses of counts.

Locking, read before calling:
  * forget_customer() takes a row lock on the customer, so concurrent inserts that reference it (FK key-share
    lock) wait and then fail; run it with the generator stopped, or accept the failed insert.
  * vacuum_full() takes ACCESS EXCLUSIVE locks and rewrites each table: writers and readers block for its whole
    duration, and it needs free disk for a second copy. Maintenance window only.
  * checkpoint() and wal_loop() are cheap but write WAL.

Decision 1 (re-key, then delete): accounts and transactions keep their rows, pointing at a placeholder customer
whose PII is a fixed redacted constant; support tickets and the customer row are deleted, in one transaction.
Foreign keys here are plain NO ACTION (schema.sql), so the order is: placeholder insert, re-key, delete children,
delete the customer.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from adrp.forget.rawscan import GrepCount, Needles, compose_exec, grep_counts_by_path, grep_files

DB = "shop"
PGDATA = "/var/lib/postgresql/data"
RAW_DIRS = ("base", "global", "pg_wal", "pg_replslot", "pg_logical")
SHOP_TABLES = ("public.customers", "public.support_tickets", "public.accounts", "public.transactions")
PRIVACY_TABLES = ("privacy.forget_requests", "privacy.forget_audit", "privacy.forget_sink_status")
WAL_SCRATCH = "privacy.wal_scratch"  # outside schema public, so outside the dbz_pub publication
STATUSES = ("requested", "logical_done", "physical_done", "failed")

REDACTED = {
    "full_name": "[redacted]",
    "email": "redacted@invalid.example",
    "phone": "[redacted]",
    "address": "[redacted]",
    "date_of_birth": "1900-01-01",
}

_IDENT = re.compile(r"[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?")


class PsqlError(RuntimeError):
    """psql failed. The message carries psql's stderr, which never includes the stdin script's literals except in
    a syntax error context line; callers must not print it for statements that embed a raw id."""


def lit(value: str) -> str:
    """SQL string literal (standard_conforming_strings is on)."""
    return "'" + value.replace("'", "''") + "'"


def ident(name: str) -> str:
    """A validated, lower-case, optionally schema-qualified table name."""
    if not _IDENT.fullmatch(name):
        raise ValueError(f"bad table name: {name!r}")
    return name


def psql(script: str, db: str = DB, timeout: float = 900) -> str:
    """Run a psql script (statements and meta-commands) from stdin; returns stdout. Raises PsqlError on failure."""
    return psql_full(script, db, timeout)[0]


def psql_full(script: str, db: str = DB, timeout: float = 900) -> tuple[str, str]:
    """(stdout, stderr) of a psql script. `-X -q -At`: unaligned tuples only, `|` between columns."""
    r = compose_exec(
        "postgres",
        ["psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", ident_db(db), "-f", "-"],
        stdin=script.encode(),
        timeout=timeout,
    )
    if r.rc != 0:
        raise PsqlError(f"psql exited {r.rc}: {scrub(r.err_text)}")
    return r.text.strip(), r.err_text


def scrub(text: str, limit: int = 400) -> str:
    """psql error text with every run of 4+ digits replaced by '#': constraint errors quote the offending key."""
    return re.sub(r"\d{4,}", "#", text.strip())[:limit]


def ident_db(db: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", db):
        raise ValueError(f"bad database name: {db!r}")
    return db


# ---- the forget transaction ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ForgetTxn:
    """What the forget transaction did. No ids of the subject. LSNs are text such as '0/1200A208'.

    begin_lsn / end_lsn bracket the transaction: every change event Debezium emits for it carries a source lsn
    inside [begin_lsn, end_lsn]. begin_lsn is pg_current_wal_insert_lsn() read inside the transaction before any
    change; end_lsn is pg_current_wal_lsn() right after COMMIT (>= the commit record). A silver tombstone with
    lsn >= begin_lsn is therefore this transaction's (or a later one); the slot has consumed the whole transaction
    once confirmed_flush_lsn >= end_lsn (see slot_confirmed()).
    """

    found: bool
    xid32: int | None = None
    begin_lsn: str | None = None
    end_lsn: str | None = None
    placeholder_id: int | None = None
    counts: Mapping[str, int] = field(default_factory=dict)  # rows touched per table


def forget_script(customer_id: int) -> str:
    """The psql script of the forget transaction (decision 1). Static SQL; the id is set once with `\\set`."""
    cid = int(customer_id)
    ph = ", ".join(lit(v) for v in REDACTED.values())
    cols = ", ".join(REDACTED)
    return f"""\\set cid {cid}
BEGIN;
SELECT count(*) = 1 AS found FROM (SELECT id FROM customers WHERE id = :cid FOR UPDATE) c \\gset
\\if :found
SELECT 'xid32', txid_current() % 4294967296;
SELECT 'begin_lsn', pg_current_wal_insert_lsn();
INSERT INTO customers ({cols}) VALUES ({ph}) RETURNING id AS ph_id \\gset
SELECT 'placeholder', :ph_id;
WITH u AS (UPDATE accounts SET customer_id = :ph_id, updated_at = now() WHERE customer_id = :cid RETURNING 1)
  SELECT 'accounts', count(*) FROM u;
WITH u AS (UPDATE transactions SET customer_id = :ph_id WHERE customer_id = :cid RETURNING 1)
  SELECT 'transactions', count(*) FROM u;
WITH d AS (DELETE FROM support_tickets WHERE customer_id = :cid RETURNING 1)
  SELECT 'support_tickets', count(*) FROM d;
WITH d AS (DELETE FROM customers WHERE id = :cid RETURNING 1)
  SELECT 'customers', count(*) FROM d;
COMMIT;
SELECT 'end_lsn', pg_current_wal_lsn();
\\else
ROLLBACK;
SELECT 'not_found', 1;
\\endif
"""


def parse_forget_output(out: str) -> ForgetTxn:
    """Parse the `key|value` lines printed by forget_script()."""
    kv: dict[str, str] = {}
    for line in out.splitlines():
        k, _, v = line.partition("|")
        kv[k.strip()] = v.strip()
    if "not_found" in kv or "end_lsn" not in kv:
        return ForgetTxn(found=False)
    tables = ("accounts", "transactions", "support_tickets", "customers")
    return ForgetTxn(
        found=True,
        xid32=int(kv["xid32"]),
        begin_lsn=kv["begin_lsn"],
        end_lsn=kv["end_lsn"],
        placeholder_id=int(kv["placeholder"]),
        counts={t: int(kv[t]) for t in tables if t in kv},
    )


def forget_customer(customer_id: int, db: str = DB) -> ForgetTxn:
    """Run the forget transaction. found=False (nothing changed) when the customer row does not exist, so a
    re-run is a no-op. Record the footprint with footprint_ids() BEFORE calling: afterwards the tickets are gone."""
    return parse_forget_output(psql(forget_script(customer_id), db))


def footprint_ids(customer_id: int, db: str = DB) -> dict[str, list[int]]:
    """Entity pks the subject owns in Postgres right now: customers, support_tickets, accounts, transactions.
    Returns raw ids (for the in-process workflow); never print them."""
    cid = int(customer_id)
    out = psql(
        f"""SELECT 'customers', id FROM customers WHERE id = {cid}
UNION ALL SELECT 'support_tickets', id FROM support_tickets WHERE customer_id = {cid}
UNION ALL SELECT 'accounts', id FROM accounts WHERE customer_id = {cid}
UNION ALL SELECT 'transactions', id FROM transactions WHERE customer_id = {cid}
ORDER BY 1, 2;""",
        db,
    )
    res: dict[str, list[int]] = {"customers": [], "support_tickets": [], "accounts": [], "transactions": []}
    for line in out.splitlines():
        t, _, i = line.partition("|")
        res[t].append(int(i))
    return res


def slot_confirmed(lsn: str, slot: str = "dbz_shop", db: str = DB) -> bool:
    """True once the replication slot has confirmed flushing everything up to `lsn` (Debezium consumed it)."""
    out = psql(
        f"SELECT coalesce(bool_or(confirmed_flush_lsn >= {lit(lsn)}::pg_lsn), false) "
        f"FROM pg_replication_slots WHERE slot_name = {lit(slot)};",
        db,
    )
    return out == "t"


# ---- privacy.* bookkeeping ----------------------------------------------------------------------------


def create_request(customer_id: int, db: str = DB) -> str:
    """Insert a privacy.forget_requests row (status 'requested') and return its request_id (uuid text)."""
    return psql(
        f"INSERT INTO privacy.forget_requests (raw_customer_id) VALUES ({int(customer_id)}) RETURNING request_id;", db
    ).splitlines()[0]


def raw_customer_id(request_id: str, db: str = DB) -> int | None:
    """The raw id of a request, or None once null_raw_customer_id() has run. In-process use only."""
    out = psql(f"SELECT raw_customer_id FROM privacy.forget_requests WHERE request_id = '{_uuid(request_id)}';", db)
    return int(out) if out else None


def set_status(request_id: str, status: str, error: str | None = None, db: str = DB) -> None:
    """Set the request status; 'logical_done' / 'physical_done' also stamp the matching *_done_at with now()."""
    if status not in STATUSES:
        raise ValueError(f"unknown status {status!r}")
    stamp = {"logical_done": ", logical_done_at = now()", "physical_done": ", physical_done_at = now()"}.get(status, "")
    err = lit(error) if error is not None else "NULL"
    psql(
        f"UPDATE privacy.forget_requests SET status = {lit(status)}{stamp}, error = {err} "
        f"WHERE request_id = '{_uuid(request_id)}';",
        db,
    )


def null_raw_customer_id(request_id: str, db: str = DB) -> None:
    """Drop the only stored copy of the raw id. The dead tuple stays in the heap until the physical procedure
    (vacuum_full on privacy.forget_requests) runs."""
    psql(f"UPDATE privacy.forget_requests SET raw_customer_id = NULL WHERE request_id = '{_uuid(request_id)}';", db)


def record_audit(
    subject_hmac: str, request_id: str, txn: ForgetTxn, footprint_counts: Mapping[str, int], db: str = DB
) -> None:
    """Insert the privacy.forget_audit row: HMAC handle, xid32, begin LSN (forget_lsn), footprint counts."""
    if not txn.found or txn.xid32 is None or txn.begin_lsn is None:
        raise ValueError("no forget transaction to audit")
    psql(
        "INSERT INTO privacy.forget_audit "
        "(subject_hmac, request_id, requested_at, forget_xid32, forget_lsn, footprint) "
        f"SELECT {lit(subject_hmac)}, request_id, requested_at, {int(txn.xid32)}, {lit(txn.begin_lsn)}::pg_lsn, "
        f"{lit(json.dumps(dict(footprint_counts)))}::jsonb "
        f"FROM privacy.forget_requests WHERE request_id = '{_uuid(request_id)}' "
        "ON CONFLICT (subject_hmac, request_id) DO UPDATE SET forget_xid32 = EXCLUDED.forget_xid32, "
        "forget_lsn = EXCLUDED.forget_lsn, footprint = EXCLUDED.footprint;",
        db,
    )


def set_sink_status(
    request_id: str,
    sink: str,
    *,
    logical_absent: bool = False,
    physical_absent: bool = False,
    bound: str | None = None,
    evidence: Mapping[str, Any] | None = None,
    db: str = DB,
) -> None:
    """Upsert privacy.forget_sink_status. logical_absent / physical_absent stamp now() the first time they are
    set (an earlier timestamp is kept); bound and evidence replace the stored value when given. Evidence is JSON
    of counts and sizes only."""
    la = "now()" if logical_absent else "NULL"
    pa = "now()" if physical_absent else "NULL"
    ev = f"{lit(json.dumps(dict(evidence)))}::jsonb" if evidence is not None else "NULL"
    psql(
        "INSERT INTO privacy.forget_sink_status "
        "(request_id, sink, logical_absent_at, physical_absent_at, bound, evidence) "
        f"VALUES ('{_uuid(request_id)}', {lit(sink)}, {la}, {pa}, {lit(bound) if bound is not None else 'NULL'}, {ev}) "
        "ON CONFLICT (request_id, sink) DO UPDATE SET "
        "logical_absent_at = coalesce(privacy.forget_sink_status.logical_absent_at, EXCLUDED.logical_absent_at), "
        "physical_absent_at = coalesce(privacy.forget_sink_status.physical_absent_at, EXCLUDED.physical_absent_at), "
        "bound = coalesce(EXCLUDED.bound, privacy.forget_sink_status.bound), "
        "evidence = coalesce(EXCLUDED.evidence, privacy.forget_sink_status.evidence);",
        db,
    )


def _uuid(request_id: str) -> str:
    return str(uuid.UUID(request_id))  # validates; also normalises the spelling


# ---- physical removal ---------------------------------------------------------------------------------


def toast_table(table: str, db: str = DB) -> str | None:
    """Schema-qualified TOAST relation of `table`, or None when it has none."""
    out = psql(
        f"SELECT CASE WHEN reltoastrelid = 0 THEN '' ELSE reltoastrelid::regclass::text END "
        f"FROM pg_class WHERE oid = {lit(ident(table))}::regclass;",
        db,
    )
    return out or None


def vacuum_script(tables: Iterable[str], toast: Mapping[str, str | None]) -> str:
    """VACUUM (FULL, VERBOSE) for each table, then its TOAST relation. One statement per psql statement so none
    runs inside a transaction block (VACUUM cannot)."""
    lines = []
    for t in tables:
        lines.append(f"VACUUM (FULL, VERBOSE) {ident(t)};")
        tt = toast.get(t)
        if tt:
            lines.append(f"VACUUM (FULL, VERBOSE) {tt};")
    return "\n".join(lines) + "\n"


_NOT_REMOVABLE = re.compile(r"(\d+) (?:dead row versions|are dead but not yet removable)", re.I)
_CANNOT_REMOVE = re.compile(r"(\d+) dead row versions cannot be removed yet", re.I)


def parse_vacuum_blocked(stderr: str) -> int:
    """Total dead row versions VACUUM reported it could not remove yet (an old snapshot or slot holds them)."""
    return sum(int(m.group(1)) for m in _CANNOT_REMOVE.finditer(stderr))


@dataclass(frozen=True)
class VacuumResult:
    tables: int
    toast_tables: int
    blocked_dead_rows: int  # > 0: something (an open transaction) still needed those versions; rerun later
    seconds: float


def vacuum_full(tables: Sequence[str] = (*SHOP_TABLES, *PRIVACY_TABLES), db: str = DB) -> VacuumResult:
    """VACUUM FULL each table and its TOAST relation: rewrites the heap into new files, so deleted and pre-update
    tuple bytes leave the old relfilenode, which is unlinked at the next checkpoint (call checkpoint()).

    ACCESS EXCLUSIVE lock per table for the whole rewrite; maintenance window only. Plain VACUUM does not scrub
    dead tuple bytes (spike section 6), which is why this is FULL."""
    t0 = time.monotonic()
    toast = {t: toast_table(t, db) for t in tables}
    _, err = psql_full(vacuum_script(tables, toast), db)
    return VacuumResult(
        len(tables), sum(1 for v in toast.values() if v), parse_vacuum_blocked(err), time.monotonic() - t0
    )


def checkpoint(db: str = DB) -> None:
    """CHECKPOINT: flushes dirty pages, unlinks the relfilenodes VACUUM FULL replaced, recycles old WAL."""
    psql("CHECKPOINT;", db)


def xid_after(catalog_xmin: int, marker: int) -> bool:
    """True when 32-bit xid `catalog_xmin` is at or after `marker` in xid circular order (a wraparound-safe >=)."""
    return ((catalog_xmin - marker) & 0xFFFFFFFF) < 0x80000000


def slot_catalog_xmins(db: str = DB) -> list[int]:
    out = psql("SELECT catalog_xmin::text FROM pg_replication_slots WHERE catalog_xmin IS NOT NULL;", db)
    return [int(x) for x in out.split()] if out else []


def current_xid32(db: str = DB) -> int:
    return int(psql("SELECT txid_current() % 4294967296;", db))


@dataclass(frozen=True)
class StatsResult:
    ok: bool  # pg_statistic was vacuumed (full) after every slot's catalog_xmin had passed the ANALYZE
    waited_s: float
    slots_blocking: int  # slots whose catalog_xmin was still behind when we gave up (0 when ok)


def analyze_and_vacuum_statistic(
    tables: Sequence[str] = SHOP_TABLES, db: str = DB, timeout_s: float = 600, poll_s: float = 5.0
) -> StatsResult:
    """Delete the tables' pg_statistic rows and ANALYZE again (the old rows hold deleted values as most-common-values
    and histogram bounds; ANALYZE alone keeps them for a table that is now empty), wait until every replication
    slot's catalog_xmin is past that transaction, then VACUUM FULL pg_statistic. The slot's catalog_xmin holds
    catalog vacuum back (spike: 2-3 minutes with 10 s heartbeats), so VACUUM before then removes nothing. On
    timeout pg_statistic is still vacuumed (harmless) and ok is False.

    VACUUM FULL pg_statistic takes an ACCESS EXCLUSIVE lock on the catalog table, briefly blocking the planner."""
    t0 = time.monotonic()
    # ANALYZE leaves the old statistics of a table untouched when it has no rows to sample (a table whose rows were
    # all deleted keeps its old most-common-values), so drop the table's pg_statistic rows first (superuser DML).
    regs = ", ".join(lit(ident(t)) + "::regclass::oid" for t in tables)
    psql(f"DELETE FROM pg_catalog.pg_statistic WHERE starelid IN ({regs});\n", db)
    psql("".join(f"ANALYZE {ident(t)};\n" for t in tables), db)
    marker = current_xid32(db)  # a later transaction than the ANALYZE, so its xid is above the ANALYZE's

    def passed() -> bool:
        return all(xid_after(x, marker) for x in slot_catalog_xmins(db))

    ok = False
    while True:
        # Advance the xid counter so a slot with no other traffic can move its catalog_xmin past the marker.
        psql("SELECT txid_current();", db)
        ok = passed()
        if ok or time.monotonic() - t0 >= timeout_s:
            break
        time.sleep(poll_s)
    psql("VACUUM (FULL) pg_catalog.pg_statistic;\n", db)
    blocking = 0 if ok else sum(1 for x in slot_catalog_xmins(db) if not xid_after(x, marker))
    return StatsResult(ok, time.monotonic() - t0, blocking)


def raw_grep(needles: Needles, dirs: Sequence[str] = RAW_DIRS) -> dict[str, int]:
    """Raw-byte grep over the data directory: {dir: number of files containing a needle}. The needles are the
    canary and id text plus the id as 8-byte little-endian int8 (heap/index pages). A file hit does not say which
    needle; -1 marks a directory grep could not read. Counts files, never prints matches. WAL, replication slot
    spill and logical-decoding snapshots are inside this scan: a hit there is the 'bounded' case (see wal_loop)."""
    patterns = [*needles.text(), *needles.pg_int8()]
    return grep_counts_by_path("postgres", [f"{PGDATA}/{d}" for d in dirs], patterns)


def raw_grep_total(needles: Needles, dirs: Sequence[str] = RAW_DIRS) -> int:
    return sum(max(v, 0) for v in raw_grep(needles, dirs).values())


def pg_wal_files_with(needles: Needles) -> GrepCount:
    return grep_files("postgres", [f"{PGDATA}/pg_wal"], [*needles.text(), *needles.pg_int8()])


@dataclass(frozen=True)
class WalResult:
    absent: bool  # no pg_wal file contains a needle
    iterations: int  # insert + pg_switch_wal + CHECKPOINT rounds run
    seconds: float
    files_before: int  # pg_wal files holding a needle at the start
    files_after: int
    slot_lag_bytes: int | None  # pg_current_wal_lsn - min(slot restart_lsn): WAL kept for the slot
    bound: str


def wal_bound_statement(
    min_wal_mb: int, segment_mb: int, slot_lag_bytes: int | None, iterations: int, absent: bool
) -> str:
    """Human-readable bound for the WAL sink. Pure; see wal_loop()."""
    lag = "unknown" if slot_lag_bytes is None else f"{slot_lag_bytes / 2**20:.1f} MiB"
    head = (
        f"absent from pg_wal after {iterations} switch+checkpoint rounds"
        if absent
        else f"still present in pg_wal after {iterations} switch+checkpoint rounds (not removed)"
    )
    return (
        f"WAL: {head}. Segments are recycled (renamed), not zeroed, so the bytes live until the recycled file is "
        f"overwritten. Bound: min_wal_size={min_wal_mb} MiB of recycled segments ({min_wal_mb // segment_mb} x "
        f"{segment_mb} MiB) plus the WAL held for the replication slot (now {lag}); each round overwrites at most "
        f"one segment. 'Bounded, not removed' unless the grep above is clean."
    )


def wal_settings(db: str = DB) -> tuple[int, int, int | None]:
    """(min_wal_size MiB, wal segment MiB, slot lag bytes or None)."""
    out = psql(
        "SELECT pg_size_bytes(current_setting('min_wal_size')) / 1048576, "
        "pg_size_bytes(current_setting('wal_segment_size')) / 1048576, "
        "(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), min(restart_lsn))::bigint FROM pg_replication_slots);",
        db,
    )
    a, b, c = out.split("|")
    return int(a), int(b), (int(c) if c else None)


def wal_scratch_round(pad_kb: int = 256, db: str = DB) -> None:
    """One WAL-forcing round: write pad_kb of data to the non-captured scratch table, switch the WAL segment,
    CHECKPOINT. pg_switch_wal() does nothing unless WAL was written since the last switch, hence the insert."""
    psql(
        f"CREATE TABLE IF NOT EXISTS {WAL_SCRATCH} (id bigserial PRIMARY KEY, pad text NOT NULL);\n"
        f"INSERT INTO {WAL_SCRATCH} (pad) SELECT md5(random()::text) || repeat('x', {int(pad_kb) * 1024});\n"
        "DELETE FROM " + WAL_SCRATCH + " WHERE id < (SELECT max(id) FROM " + WAL_SCRATCH + ");\n"
        "SELECT pg_switch_wal();\nCHECKPOINT;\n",
        db,
    )


def wal_loop(needles: Needles, max_iterations: int = 40, pad_kb: int = 256, db: str = DB) -> WalResult:
    """Push the WAL forward until no pg_wal file contains a needle, or max_iterations rounds. Measures the number
    of rounds and seconds, and states the bound.

    Why this and not a delete: Postgres recycles a finished WAL segment by renaming it for future use without
    zeroing it, so the old bytes survive until WAL writing reaches and overwrites that file (spike: 5 + 8 rounds).
    The replication slot must also have moved past the segment (restart_lsn); until Debezium confirms it, no
    number of rounds helps, and the result says so through slot_lag_bytes and `absent=False`.

    Writes `pad_kb` of WAL per round to privacy.wal_scratch (created on demand, not in dbz_pub, so never captured
    by CDC). No lock on shop tables."""
    t0 = time.monotonic()
    before = pg_wal_files_with(needles).files
    after, i = before, 0
    while after > 0 and i < max_iterations:
        wal_scratch_round(pad_kb, db)
        i += 1
        after = pg_wal_files_with(needles).files
    min_wal, seg, lag = wal_settings(db)
    absent = after == 0
    return WalResult(
        absent, i, time.monotonic() - t0, before, after, lag, wal_bound_statement(min_wal, seg, lag, i, absent)
    )
