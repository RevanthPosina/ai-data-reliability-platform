"""G4 done-condition check: the gold layer (dbt on Trino over the Iceberg lake), on the live stack.

Precondition (the Makefile target `g4-check` arranges it): fresh volumes, core infra, bootstrap (connector running),
spark-streams and trino up, nothing generated yet. No embedder, dagster or exporter. This script does not start or stop
the stack. It is the run of record for ADR-0030 (the pre-registered g4-check list).

  P0 ready       Trino answers; lake.bronze.cdc_events and the lake.silver tables exist
  P1 build 1     generator 1,000 txns; scripted merchant M, customers A and B and money moves dated 2026-01-02 and
                 2026-01-03; drain; dbt build #1 (strict reconcile); ledger, balances, SCD2 vs ground truth
  P2 build 2     generator 500 more txns; scripted A/B/M change sequence (several changes between runs) and a re-key;
                 drain; dbt build #2; the same checks, the dbt snapshot's lost versions, the re-key flow
  P3 idempotent  dbt build #3 with no new data: every gold checksum and the ledger entry_id set unchanged
  P4 backfill    one-day backfill of 2026-01-01 (build #4): checksums identical, non-vacuous snapshot evidence
  P5 rebuild     `--target rebuild` into gold_rebuild (build #5): equals the incremental result; then dropped
  P6 hygiene     no canary or raw id in dbt_gold/target or dbt_gold/logs
  P7 guard       host-sleep guard (always run): the run is invalid if the Mac slept
Writes a run manifest (kind g4-check). Checks name scripted rows by label (A, B, M) only: no raw customer or merchant
id and no canary email is printed or stored (ids of ad-hoc rows have 13 digits; a manifest scan flags them).

`--from Pn` runs Pn..P6 and `--only Pn` runs just Pn (P0 and P7 always run). Such a run is marked `partial` in the
manifest and is not a run of record. P2 needs P1's in-memory scripted state, so `--from P2`/`--only P2` is refused.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import g2_check as g2
import pyarrow.parquet as pq
import trino
import xxhash
from _env import ROOT
from g3_check import Drain
from generate import run_once
from hostsleep import SleepGuard
from manifest import write_manifest

sys.path.insert(0, str(ROOT / "src"))
from adrp.gold import oracle
from adrp.recon.canonical import canonical_row, digest, hex64, row_hash

SEED = g2.SEED
AD_HOC = g2.AD_HOC
GEN_1 = 1000
GEN_2 = 500
BACKFILL_DAY = "2026-01-01"
PROFILE = "core+transform (spark-streams, no embedder/dagster/exporter)"
RESULTS: list[dict[str, Any]] = []
BUILDS: list[dict[str, Any]] = []

# A deterministic canary: the volumes are wiped before every run, so it never collides with an earlier run.
CANARY = "g4canary-" + hashlib.sha256(b"g4-check:A").hexdigest()[:16] + "@example.invalid"
_BIGNUM = re.compile(r"\d{13,}")

CUSTOMER_TRACKED = ("full_name", "email", "phone", "address", "date_of_birth")
MERCHANT_TRACKED = ("name", "category", "country")
GOLD_TABLES = ("dim_customer", "dim_merchant", "dim_account", "dim_date", "fct_transactions", "fct_ledger_entries")
KEYS = {
    "dim_customer": "customer_sk",
    "dim_merchant": "merchant_sk",
    "dim_account": "account_id",
    "dim_date": "date_key",
    "fct_transactions": "transaction_id",
    "fct_ledger_entries": "entry_id",
}
PHASE_NAMES = ("P1", "P2", "P3", "P4", "P5", "P6")


class Abort(Exception):
    """A phase cannot continue (a build failed, the stack is not ready): later phases would only produce noise."""


def scrub(text: str) -> str:
    """Remove the canary and anything that looks like an ad-hoc id (13+ digits) from text that is printed or stored."""
    return _BIGNUM.sub("<n>", text.replace(CANARY, "<canary>"))


def check(name: str, ok: bool, detail: str = "") -> bool:
    detail = scrub(detail)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


# ---- Trino ------------------------------------------------------------------------------------------


class Trino:
    """Thin wrapper over the `trino` client (the version dbt-trino pins). Autocommit, catalog lake."""

    def __init__(self) -> None:
        self.host = os.environ.get("TRINO_HOST", "localhost")
        self.port = int(os.environ.get("TRINO_PORT", "8080"))
        connect: Any = trino.dbapi.connect
        self.conn = connect(host=self.host, port=self.port, user="g4-check", catalog="lake", schema="gold")

    def run(self, sql: str) -> tuple[list[tuple[str, str]], list[tuple[Any, ...]]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        desc = [(str(d[0]), str(d[1])) for d in (cur.description or [])]
        return desc, [tuple(r) for r in rows]

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        return self.run(sql)[1]

    def one(self, sql: str) -> Any:
        rows = self.rows(sql)
        return rows[0][0] if rows else None

    def dicts(self, sql: str) -> list[dict[str, Any]]:
        desc, rows = self.run(sql)
        names = [n for n, _ in desc]
        return [dict(zip(names, r, strict=True)) for r in rows]

    def exec(self, sql: str) -> None:
        self.run(sql)

    def ready(self) -> bool:
        with urllib.request.urlopen(f"http://{self.host}:{self.port}/v1/info", timeout=5) as resp:
            return not json.load(resp).get("starting", True)


_TR: Trino | None = None


def tr() -> Trino:
    global _TR
    if _TR is None:
        _TR = Trino()
    return _TR


def pg_type(trino_type: str) -> str:
    """Postgres type name (for adrp.recon.canonical) of a Trino column type."""
    t = trino_type.lower()
    if t.startswith("timestamp") and "with time zone" in t:
        return "timestamp with time zone"
    if t.startswith("decimal"):
        return "numeric"
    if t.startswith(("varchar", "char")):
        return "text"
    if t in ("bigint", "integer", "smallint", "date", "boolean"):
        return t
    raise ValueError(f"no canonical mapping for Trino type {trino_type!r}")


@dataclass
class TableState:
    count: int
    checksum: int
    rows: dict[str, int] | None  # key -> row hash, only when asked for

    def tag(self) -> str:
        return f"{self.count}:{hex64(self.checksum)}"


def table_state(fq: str, key: str, where: str = "", *, with_rows: bool = False) -> TableState:
    """Count and order-independent checksum (adrp.recon.canonical.digest) of every row of a Trino table."""
    desc, rows = tr().run(f"SELECT * FROM {fq}" + (f" WHERE {where}" if where else ""))
    cols = [(n, pg_type(t)) for n, t in desc]
    dicts = [dict(zip((n for n, _ in desc), r, strict=True)) for r in rows]
    for d in dicts:  # `digest` wants an int `id` to bucket on; any stable int of the key will do
        d["id"] = int(xxhash.xxh64_intdigest(str(d[key]).encode("utf-8")))
    dg = digest(dicts, cols)
    per_row = {str(d[key]): row_hash(canonical_row(d, cols)) for d in dicts} if with_rows else None
    return TableState(dg.count, dg.checksum, per_row)


def gold_states(schema: str = "gold", **kw: Any) -> dict[str, TableState]:
    return {t: table_state(f"lake.{schema}.{t}", KEYS[t], **kw) for t in GOLD_TABLES}


# ---- Postgres, scripted rows --------------------------------------------------------------------------


def scripted(stmts: list[str]) -> tuple[int, int]:
    """Run statements as one Postgres transaction. Returns (xid32, commit time in epoch ms): the ground truth of the
    scripted change (xid32 is Debezium's txId, the commit time is the CDC source_ts)."""
    out = g2.psql("; ".join(stmts) + "; SELECT 'XID=' || txid_current()")
    m = re.search(r"XID=(\d+)", out)
    if m is None:
        raise RuntimeError("txid_current() missing from the psql output")
    xid = int(m.group(1))
    ms = g2.psql(
        f"SELECT floor(extract(epoch from pg_xact_commit_timestamp(({xid} % 4294967296)::text::xid)) * 1000)::bigint"
    )
    if not ms.isdigit():
        raise RuntimeError("pg_xact_commit_timestamp returned nothing (track_commit_timestamp off?)")
    return xid % 4294967296, int(ms)


@dataclass
class Script:
    """The scripted changes as oracle events. LSNs here are synthetic (a counter): only their order matters."""

    counter: int = 0
    events: dict[str, list[oracle.ChangeEvent]] = field(default_factory=dict)
    current: dict[str, dict[str, Any]] = field(default_factory=dict)

    def record(self, label: str, pk: int, op: str, xid32: int, commit_ms: int, attrs: dict[str, Any] | None) -> None:
        self.counter += 100
        ev = oracle.ChangeEvent(pk, self.counter, xid32, op, commit_ms, commit_ms, attrs)
        self.events.setdefault(label, []).append(ev)


@dataclass
class State:
    A: int = 0
    B: int = 0
    M: int = 0
    A_acct: int = 0
    M_acct: int = 0
    R: int = 0  # A's payment that the re-key moves to B
    run_ids: list[str] = field(default_factory=list)
    end_txn: int = 0
    script: Script = field(default_factory=Script)
    have_script: bool = False
    cut1: int = 0  # synthetic scripted-LSN counter at build 1 and build 2
    cut2: int = 0
    gen_cut1: int | None = None  # highest generator op seq covered by build 1
    gen_cut2: int | None = None
    ledger_base: dict[str, int] | None = None
    r_before: dict[str, Any] | None = None
    r_entries_before: dict[str, int] | None = None
    details: dict[str, Any] = field(default_factory=dict)


ST = State()


def pg_id(sql: str) -> int:
    return int(g2.psql(sql))


def ensure_ids() -> None:
    """Ids of the scripted rows, looked up by name (they never leave this process)."""
    if ST.A:
        return
    ST.A = pg_id("SELECT id FROM customers WHERE full_name = 'g4-A'")
    ST.B = pg_id("SELECT id FROM customers WHERE full_name = 'g4-B'")
    ST.M = pg_id(f"SELECT id FROM merchants WHERE id >= {AD_HOC} ORDER BY id LIMIT 1")
    ST.A_acct = pg_id(f"SELECT id FROM accounts WHERE customer_id = {ST.A}")
    ST.M_acct = pg_id(f"SELECT id FROM accounts WHERE merchant_id = {ST.M}")
    ST.R = pg_id("SELECT id FROM transactions WHERE description = 'g4-T2'")


def insert_merchant_m(name: str, category: str, country: str, ts: str) -> None:
    xid32, ms = scripted(
        [
            f"INSERT INTO merchants (name, category, country, created_at, updated_at) "
            f"VALUES ('{name}', '{category}', '{country}', '{ts}', '{ts}')",
            f"INSERT INTO accounts (owner_type, merchant_id, currency, balance, created_at, updated_at) "
            f"SELECT 'merchant', id, 'USD', 0.00, '{ts}', '{ts}' FROM merchants WHERE name = '{name}'",
        ]
    )
    ST.M = pg_id(f"SELECT id FROM merchants WHERE name = '{name}'")
    attrs = {"name": name, "category": category, "country": country}
    ST.script.current["M"] = dict(attrs)
    ST.script.record("M", ST.M, "c", xid32, ms, dict(attrs))


def insert_customer(label: str, full_name: str, email: str, phone: str, address: str, ts: str, opening: str) -> int:
    stmts = [
        f"INSERT INTO customers (full_name, email, phone, address, date_of_birth, created_at, updated_at) "
        f"VALUES ('{full_name}', '{email}', '{phone}', '{address}', '1990-01-01', '{ts}', '{ts}')"
    ]
    if opening:
        stmts.append(
            f"INSERT INTO accounts (owner_type, customer_id, currency, balance, created_at, updated_at) "
            f"SELECT 'customer', id, 'USD', {opening}, '{ts}', '{ts}' FROM customers WHERE full_name = '{full_name}'"
        )
    xid32, ms = scripted(stmts)
    pk = pg_id(f"SELECT id FROM customers WHERE full_name = '{full_name}'")
    attrs = {"full_name": full_name, "email": email, "phone": phone, "address": address, "date_of_birth": "1990-01-01"}
    ST.script.current[label] = dict(attrs)
    ST.script.record(label, pk, "c", xid32, ms, dict(attrs))
    return pk


def scripted_update(table: str, label: str, pk: int, changes: list[dict[str, str]]) -> None:
    """One Postgres transaction with one UPDATE per entry of `changes` (an empty dict touches only updated_at)."""
    stmts = []
    for ch in changes:
        sets = [f"{c} = '{v}'" for c, v in ch.items()] + ["updated_at = now()"]
        stmts.append(f"UPDATE {table} SET {', '.join(sets)} WHERE id = {pk}")
    xid32, ms = scripted(stmts)
    for ch in changes:
        ST.script.current[label].update(ch)
        ST.script.record(label, pk, "u", xid32, ms, dict(ST.script.current[label]))


def money(stmts: list[str]) -> None:
    g2.psql("; ".join(stmts))


def scripted_money() -> None:
    """Topup, payments and refunds dated 2026-01-02 and 2026-01-03, one Postgres transaction each, balances updated in
    the same transaction. Nothing here is dated 2026-01-01, so the backfill of that day must leave all of it alone."""
    a, m, aa, ma = ST.A, ST.M, ST.A_acct, ST.M_acct

    def txn(desc: str, kind: str, amount: str, ts: str) -> str:
        cols = "txn_type, customer_id, merchant_id, payer_account_id, payee_account_id, amount, currency, status"
        if kind == "topup":
            vals = f"'topup', {a}, NULL, NULL, {aa}, {amount}, 'USD', 'settled'"
        else:
            vals = f"'payment', {a}, {m}, {aa}, {ma}, {amount}, 'USD', 'settled'"
        return f"INSERT INTO transactions ({cols}, description, created_at) VALUES ({vals}, '{desc}', '{ts}')"

    def bal(account: int, delta: str) -> str:
        return f"UPDATE accounts SET balance = balance + ({delta}), updated_at = now() WHERE id = {account}"

    def refund(desc: str, amount: str, ts: str) -> str:
        return (
            "INSERT INTO refunds (transaction_id, amount, reason, created_at) "
            f"SELECT id, {amount}, 'g4', '{ts}' FROM transactions WHERE description = '{desc}'"
        )

    money([txn("g4-T1", "topup", "20.00", "2026-01-02 10:00:00+00"), bal(aa, "20.00")])
    money([txn("g4-T2", "payment", "12.50", "2026-01-03 10:00:00+00"), bal(aa, "-12.50"), bal(ma, "12.50")])
    money([refund("g4-T2", "12.50", "2026-01-03 11:00:00+00"), bal(ma, "-12.50"), bal(aa, "12.50")])  # full refund
    money([txn("g4-T4", "payment", "8.00", "2026-01-03 12:00:00+00"), bal(aa, "-8.00"), bal(ma, "8.00")])
    money([refund("g4-T4", "4.00", "2026-01-03 13:00:00+00"), bal(ma, "-4.00"), bal(aa, "4.00")])  # half refund
    ST.R = pg_id("SELECT id FROM transactions WHERE description = 'g4-T2'")


# ---- waiting -----------------------------------------------------------------------------------------


SILVER_T = ("customers", "merchants", "accounts", "transactions", "refunds")


def wait_trino_ready(timeout: float = 420) -> bool:
    def ready() -> bool:
        try:
            if not tr().ready():
                return False
            # Spark creates each silver table on its first rows, so on fresh volumes only bronze exists here;
            # silver visibility is waited for after the first drain (wait_caught_up).
            bronze = {r[0] for r in tr().rows("SHOW TABLES FROM lake.bronze")}
            return "cdc_events" in bronze
        except Exception:  # noqa: BLE001 - Trino or the catalog is still starting; poll again
            return False

    return bool(g2.wait_for(ready, timeout, 3.0))


def wait_caught_up(drain: Drain, timeout: float = 300) -> bool:
    """Silver's batch log has reached every topic, and Trino sees bronze and silver at the topic's last LSN."""
    if not drain.wait_silver(timeout):
        return False

    def visible() -> bool:
        silver = {r[0] for r in tr().rows("SHOW TABLES FROM lake.silver")}
        if not set(SILVER_T) <= silver:
            return False
        for t in SILVER_T:
            need = drain.topic_lsn[t]
            if not need:
                continue
            b = tr().one(
                "SELECT coalesce(max(lsn), 0) FROM lake.bronze.cdc_events "
                f"WHERE source_table = 'public.{t}' AND op IS NOT NULL"
            )
            s = tr().one(f"SELECT coalesce(max(lsn), 0) FROM lake.silver.{t}")
            if b < need or s < need:
                return False
        return True

    return bool(g2.wait_for(visible, timeout, 3.0))


# ---- dbt ----------------------------------------------------------------------------------------------


def dbt_bin() -> str:
    cand = Path(sys.executable).parent / "dbt"
    return str(cand) if cand.exists() else (shutil.which("dbt") or "dbt")


def dbt_build(k: int, *, target: str = "dev", variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """`dbt build` (models, tests, snapshot; unit tests excluded) as in `make gold-build`, at a quiescent cut, so the
    balance reconciliation test runs as an error (strict_reconcile). Counts come from run_results.json."""
    vars_ = {"strict_reconcile": True, **(variables or {})}
    cmd = [dbt_bin(), "build", "--project-dir", "dbt_gold", "--profiles-dir", "dbt_gold", "--target", target]
    cmd += ["--exclude", "test_type:unit", "--vars", json.dumps(vars_)]
    t0 = time.monotonic()
    r = subprocess.run(
        cmd, cwd=ROOT, capture_output=True, encoding="utf-8", errors="replace", timeout=1800, check=False
    )
    secs = round(time.monotonic() - t0, 1)
    counts: dict[str, int] = {"model": 0, "test": 0, "snapshot": 0, "other": 0}
    bad: list[str] = []
    try:
        results = json.loads((ROOT / "dbt_gold/target/run_results.json").read_text())["results"]
    except (OSError, ValueError, KeyError):
        results = []
    for res in results:
        kind = str(res["unique_id"]).split(".")[0]
        counts[kind if kind in counts else "other"] += 1
        if res["status"] not in ("success", "pass"):
            bad.append(f"{res['unique_id']}={res['status']}")
    ok = r.returncode == 0 and not bad and counts["model"] > 0 and counts["test"] > 0 and counts["snapshot"] > 0
    detail = (
        f"models={counts['model']} tests={counts['test']} snapshots={counts['snapshot']} nonpass={len(bad)} {secs}s"
    )
    check(f"dbt: build #{k} passed (models, tests, snapshot)", ok, detail)
    BUILDS.append(
        {
            "build": k,
            "target": target,
            "vars": vars_,
            "returncode": r.returncode,
            "counts": counts,
            "nonpass": bad,
            "seconds": secs,
        }
    )
    if not ok:
        for line in (r.stdout + r.stderr).splitlines()[-40:]:
            print("        " + scrub(line))
        raise Abort(f"dbt build #{k} failed")
    return {"counts": counts, "seconds": secs}


# ---- checks ----------------------------------------------------------------------------------------------


def ledger_nets(k: int) -> None:
    led = "lake.gold.fct_ledger_entries"
    by_journal = tr().one(
        f"SELECT count(*) FROM (SELECT journal_id FROM {led} GROUP BY journal_id HAVING sum(amount) <> 0)"
    )
    by_txn = tr().one(
        f"SELECT count(*) FROM (SELECT transaction_id FROM {led} WHERE transaction_id IS NOT NULL "
        "GROUP BY transaction_id HAVING sum(amount) <> 0)"
    )
    n_j = tr().one(f"SELECT count(DISTINCT journal_id) FROM {led}")
    n_t = tr().one(f"SELECT count(DISTINCT transaction_id) FROM {led}")
    check(
        f"ledger: every journal nets to zero (group by journal_id having sum(amount) <> 0 returns 0 rows) (build #{k})",
        by_journal == 0 and n_j > 0,
        f"{n_j} journals, {by_journal} not netting",
    )
    check(
        "ledger: every transaction nets to zero, a payment plus its refund reversal (group by transaction_id) "
        f"(build #{k})",
        by_txn == 0 and n_t > 0,
        f"{n_t} transactions, {by_txn} not netting",
    )


def reconcile_balances(k: int) -> None:
    pg: dict[int, Decimal] = {}
    for line in g2.psql("SELECT id, balance FROM accounts").splitlines():
        i, b = line.split("|")
        pg[int(i)] = Decimal(b)
    led = {
        int(a): Decimal(str(s))
        for a, s in tr().rows(
            "SELECT account_id, sum(amount) FROM lake.gold.fct_ledger_entries WHERE account_id > 0 GROUP BY account_id"
        )
    }
    mismatches = sum(1 for a, b in pg.items() if led.get(a, Decimal(0)) != b)
    ledger_only = sum(1 for a in led if a not in pg)
    no_entries = sum(1 for a in pg if a not in led)
    check(
        f"balances: every Postgres account's ledger sum equals accounts.balance at a quiescent cut (build #{k})",
        len(pg) > 0 and mismatches == 0 and ledger_only == 0,
        f"n accounts={len(pg)} mismatches={mismatches} ledger-only accounts={ledger_only} "
        f"accounts with no entries={no_entries}",
    )
    ST.details.setdefault("balances", {})[f"build_{k}"] = {
        "accounts": len(pg),
        "mismatches": mismatches,
        "ledger_only": ledger_only,
        "accounts_without_entries": no_entries,
    }


def load_run_log() -> dict[int, tuple[int, int]]:
    """txn_no -> (xid32, commit time epoch ms) from this check's own generator runs."""
    out: dict[int, tuple[int, int]] = {}
    for rid in ST.run_ids:
        for f in sorted((ROOT / "runs/data" / rid / "run_log").glob("*.parquet")):
            for r in pq.read_table(f, columns=["txn_no", "xid32", "commit_ts"]).to_pylist():
                if r["commit_ts"] is not None:
                    out[int(r["txn_no"])] = (int(r["xid32"]), oracle.epoch_ms(r["commit_ts"]))
    return out


_OPS: dict[int, list[Any]] = {}


def generator_ops(n: int) -> list[Any]:
    if n not in _OPS:
        _OPS[n] = g2.generator_ops(0, n)
    return _OPS[n]


def norm_attr(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def version_diffs(expected: Sequence[oracle.Version], rows: Sequence[dict[str, Any]], tracked: Sequence[str]) -> int:
    """Number of expected versions that the dimension does not hold exactly (attributes, txid, valid_from, valid_to,
    is_current, closed_by_delete), plus the difference in count. 0 means equal."""
    bad = abs(len(expected) - len(rows))
    for v, r in zip(expected, rows, strict=False):
        same = (
            tuple(norm_attr(a) for a in v.attrs) == tuple(norm_attr(r[c]) for c in tracked)
            and r["txid"] == v.txid
            and oracle.epoch_ms(r["valid_from"]) == v.valid_from_ms
            and (None if r["valid_to"] is None else oracle.epoch_ms(r["valid_to"])) == v.valid_to_ms
            and bool(r["is_current"]) == v.is_current
            and bool(r["closed_by_delete"]) == v.closed_by_delete
        )
        bad += 0 if same else 1
    return bad


def dim_rows(table: str, key: str, tracked: Sequence[str], where: str) -> dict[int, list[dict[str, Any]]]:
    cols = ", ".join(
        [
            key,
            *tracked,
            "valid_from",
            "valid_to",
            "valid_from_lsn",
            "valid_to_lsn",
            "txid",
            "is_current",
            "closed_by_delete",
        ]
    )
    out: dict[int, list[dict[str, Any]]] = {}
    for r in tr().dicts(f"SELECT {cols} FROM lake.gold.{table} WHERE {where} ORDER BY {key}, valid_from_lsn"):
        out.setdefault(int(r[key]), []).append(r)
    return out


def generator_versions(end_txn: int) -> tuple[dict[int, list[oracle.Version]], list[oracle.Version], int]:
    """Expected dim_customer versions of the generator's customers (ground truth: the generator's own ops, with each
    transaction's xid32 and commit time from the run log). Returns (by customer, all, highest op seq)."""
    runlog = load_run_log()
    ops = [op for op in generator_ops(end_txn) if op.txn_no < end_txn]
    events = []
    for op in ops:
        if op.table != "customers":
            continue
        xid32, ms = runlog[op.txn_no]
        events.append(oracle.ChangeEvent(op.pk, op.seq, xid32, {"I": "c", "U": "u", "D": "d"}[op.op], ms, ms, op.row))
    versions = oracle.scd2_versions(events, CUSTOMER_TRACKED)
    by_pk: dict[int, list[oracle.Version]] = {}
    for v in versions:
        by_pk.setdefault(v.pk, []).append(v)
    return by_pk, versions, max(op.seq for op in ops)


def scd2_generator(k: int) -> None:
    by_pk, versions, max_seq = generator_versions(ST.end_txn)
    if k == 1:
        ST.gen_cut1 = max_seq
    else:
        ST.gen_cut2 = max_seq
    rows = dim_rows("dim_customer", "customer_id", CUSTOMER_TRACKED, f"customer_id < {AD_HOC}")
    bad = sum(version_diffs(by_pk.get(pk, []), rows.get(pk, []), CUSTOMER_TRACKED) for pk in by_pk.keys() | rows.keys())
    check(
        "scd2: generator customers: dim_customer versions equal the generator ops in LSN order; valid_from equals the "
        f"run-log commit time (ms) and txid equals xid32 (build #{k})",
        bad == 0 and len(versions) > 0,
        f"{len(by_pk)} customers, {len(versions)} versions, {bad} differing",
    )


def scripted_versions(label: str, tracked: Sequence[str]) -> list[oracle.Version]:
    return oracle.scd2_versions(ST.script.events.get(label, []), tracked)


def scd2_scripted(k: int) -> None:
    if not ST.have_script:
        return
    parts: dict[str, str] = {}
    bad_total = 0
    for label, table, key, tracked, pk in (
        ("A", "dim_customer", "customer_id", CUSTOMER_TRACKED, ST.A),
        ("B", "dim_customer", "customer_id", CUSTOMER_TRACKED, ST.B),
        ("M", "dim_merchant", "merchant_id", MERCHANT_TRACKED, ST.M),
    ):
        exp = scripted_versions(label, tracked)
        got = dim_rows(table, key, tracked, f"{key} = {pk}").get(pk, [])
        d = version_diffs(exp, got, tracked)
        bad_total += d
        parts[label] = f"{len(exp)} expected/{len(got)} held/{d} differing"
    check(
        "scd2: scripted A, B and M versions equal the oracle over the scripted list (txid and commit time captured per "
        f"scripted transaction) (build #{k})",
        bad_total == 0,
        "; ".join(f"{a}: {b}" for a, b in parts.items()),
    )


def scd2_structure(k: int) -> None:
    out: dict[str, dict[str, int]] = {}
    for table, key in (("dim_customer", "customer_id"), ("dim_merchant", "merchant_id")):
        t = f"lake.gold.{table}"
        multi = tr().one(
            f"SELECT count(*) FROM (SELECT {key} FROM {t} GROUP BY {key} HAVING count_if(is_current) > 1 "
            f"OR (count_if(is_current) = 0 AND NOT max_by(closed_by_delete, valid_from_lsn)))"
        )
        gaps = tr().one(
            f"SELECT count(*) FROM (SELECT valid_from_lsn, "
            f"lag(valid_to_lsn) OVER (PARTITION BY {key} ORDER BY valid_from_lsn) AS prev_to, "
            f"lag(valid_from_lsn) OVER (PARTITION BY {key} ORDER BY valid_from_lsn) AS prev_from FROM {t}) "
            "WHERE prev_from IS NOT NULL AND (prev_to IS NULL OR prev_to <> valid_from_lsn)"
        )
        zero = tr().one(f"SELECT count(*) FROM {t} WHERE valid_to_lsn IS NOT NULL AND valid_to_lsn <= valid_from_lsn")
        rows = tr().one(f"SELECT count(*) FROM {t}")
        out[table] = {"rows": rows, "keys_not_one_current": multi, "non_contiguous": gaps, "zero_length": zero}
    check(
        f"scd2: one current row per key, contiguous LSN intervals, no zero-length interval (build #{k})",
        all(
            v["rows"] > 0 and v["keys_not_one_current"] == v["non_contiguous"] == v["zero_length"] == 0
            for v in out.values()
        ),
        json.dumps(out),
    )


def ledger_unchanged(k: int) -> None:
    cur = table_state("lake.gold.fct_ledger_entries", "entry_id", with_rows=True).rows or {}
    if ST.ledger_base is None:
        ST.ledger_base = cur
        return
    base = ST.ledger_base
    changed = sum(1 for e, h in base.items() if cur.get(e) != h)
    check(
        f"ledger: append-only by construction; entries present after the first build are unchanged after build #{k}",
        changed == 0 and len(base) > 0,
        f"{len(base)} entries from the first build, {changed} changed or missing, {len(cur) - len(base)} added since",
    )


def after_build(k: int) -> None:
    ledger_nets(k)
    reconcile_balances(k)
    scd2_generator(k)
    scd2_scripted(k)
    scd2_structure(k)
    ledger_unchanged(k)


# ---- phases -------------------------------------------------------------------------------------------------


def phase_p0() -> None:
    print("P0 readiness")
    ok = wait_trino_ready()
    check("P0: Trino answers, and lake.bronze.cdc_events exists (silver tables appear with their first rows)", ok)
    if not ok:
        raise Abort("Trino or lake.bronze.cdc_events is not ready")


def phase_p1(drain: Drain) -> None:
    print("P1 generator, scripted rows, build #1")
    s = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=GEN_1)
    ST.run_ids.append(s["run_id"])
    ST.end_txn = s["txn_range"][1]
    check(
        "P1: the generator ran from transaction 0 (fresh volumes), as the ground truth assumes",
        s["txn_range"] == [0, GEN_1] and s["commit_ts_missing"] == 0,
        f"txn_range={s['txn_range']}",
    )
    insert_merchant_m("g4-M v1", "grocery", "US", "2026-01-02 08:00:00+00")
    ST.A = insert_customer("A", "g4-A", CANARY, "555-0100", "1 Test Street", "2026-01-02 09:00:00+00", "50.00")
    ST.B = insert_customer(
        "B", "g4-B", "g4-b@example.invalid", "555-0200", "2 Test Street", "2026-01-02 09:30:00+00", ""
    )
    ST.A_acct = pg_id(f"SELECT id FROM accounts WHERE customer_id = {ST.A}")
    ST.M_acct = pg_id(f"SELECT id FROM accounts WHERE merchant_id = {ST.M}")
    ST.have_script = True
    scripted_money()
    if not check("P1: silver and bronze (as Trino sees them) caught up with every topic", wait_caught_up(drain)):
        raise Abort("pipeline did not drain")
    ST.cut1 = ST.script.counter
    dbt_build(1)
    after_build(1)
    ST.r_before = tr().dicts(f"SELECT * FROM lake.gold.fct_transactions WHERE transaction_id = {ST.R}")[0]
    ST.r_entries_before = table_state(
        "lake.gold.fct_ledger_entries", "entry_id", f"transaction_id = {ST.R}", with_rows=True
    ).rows


def phase_p2(drain: Drain) -> None:
    print("P2 more changes, scripted change sequence, re-key, build #2")
    if not ST.have_script or ST.r_before is None:
        raise Abort("P2 needs P1's scripted state in this process (use --from P3 or later, or run from P1)")
    s = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=GEN_2)
    ST.run_ids.append(s["run_id"])
    ST.end_txn = s["txn_range"][1]
    check(
        "P2: the generator resumed at transaction 1,000 and ran 500 more",
        s["txn_range"] == [GEN_1, GEN_1 + GEN_2] and s["commit_ts_missing"] == 0,
        f"txn_range={s['txn_range']}",
    )
    # A: two updates in separate transactions, then one transaction with two updates to the same key (collapses)
    scripted_update("customers", "A", ST.A, [{"phone": "555-0101"}])
    scripted_update("customers", "A", ST.A, [{"address": "1 Test Street, Apt 2"}])
    scripted_update("customers", "A", ST.A, [{"email": "g4-a2@example.invalid"}, {"phone": "555-0103"}])
    # B: one update. M: a change, a no-op for the tracked columns (updated_at only), then two updates in one transaction
    scripted_update("customers", "B", ST.B, [{"phone": "555-0201"}])
    scripted_update("merchants", "M", ST.M, [{"category": "dining"}])
    scripted_update("merchants", "M", ST.M, [{}])
    scripted_update("merchants", "M", ST.M, [{"name": "g4-M v2"}, {"country": "CA"}])
    # re-key: A's payment (with a full refund) moves to the placeholder customer B, as a forget does
    g2.psql(f"UPDATE transactions SET customer_id = {ST.B} WHERE id = {ST.R}")
    if not check("P2: silver and bronze (as Trino sees them) caught up with every topic", wait_caught_up(drain)):
        raise Abort("pipeline did not drain")
    ST.cut2 = ST.script.counter
    dbt_build(2)
    after_build(2)
    snapshot_checks()
    rekey_checks()


def snapshot_checks() -> None:
    """The dbt snapshot (check strategy, run at builds #1 and #2) against the oracle's prediction."""
    snap = "lake.gold_snapshots.snap_customer_attrs"
    h = (
        "to_hex(xxhash64(to_utf8(coalesce(cast(email as varchar), '') || '|' || coalesce(cast(phone as varchar), '') "
        "|| '|' || coalesce(cast(address as varchar), ''))))"
    )
    summary: dict[str, Any] = {}
    ok = True
    for label, pk in (("A", ST.A), ("B", ST.B)):
        versions = scripted_versions(label, CUSTOMER_TRACKED)
        cuts = [ST.cut1, ST.cut2]
        captured = oracle.snapshot_captures(versions, cuts)
        lost = oracle.lost_intermediate(versions, cuts)
        dim_hash = [
            r[1]
            for r in tr().rows(
                f"SELECT valid_from_lsn, {h} FROM lake.gold.dim_customer "
                f"WHERE customer_id = {pk} ORDER BY valid_from_lsn"
            )
        ]
        want = sorted(dim_hash[versions.index(v)] for v in captured) if len(dim_hash) == len(versions) else []
        have = sorted(r[0] for r in tr().rows(f"SELECT attr_hash FROM {snap} WHERE customer_id = {pk}"))
        summary[label] = {"versions": len(versions), "captured": len(have), "lost": len(versions) - len(have)}
        ok = ok and want == have and lost == len(versions) - len(have) and len(dim_hash) == len(versions)
        if label == "A":
            ok = ok and len(versions) == 4 and len(have) == 2 and lost == 2
    check(
        "snapshot: scripted A has 4 SCD2 versions and the dbt snapshot captured exactly 2 of them, so exactly 2 were "
        "lost (asserted); B (one change between runs) is predicted by the oracle too",
        ok,
        json.dumps(summary),
    )
    ST.details["snapshot_scripted"] = summary
    # generator customers: reported, not asserted (it depends on the schedule)
    n_versions = tr().one(f"SELECT count(*) FROM lake.gold.dim_customer WHERE customer_id < {AD_HOC}")
    n_captured = tr().one(f"SELECT count(*) FROM {snap} WHERE customer_id < {AD_HOC}")
    _, all_versions, _ = generator_versions(ST.end_txn)
    predicted = oracle.lost_intermediate(all_versions, [c for c in (ST.gen_cut1, ST.gen_cut2) if c is not None])
    lost_n = n_versions - n_captured
    check(
        f"snapshot: {lost_n} of {n_versions} customer versions were not captured by the snapshot at 2 dbt runs in this "
        "run (reported, not asserted: it depends on the schedule)",
        n_versions > 0 and n_captured > 0,
        f"oracle predicts {predicted} not captured",
    )
    ST.details["snapshot_generator"] = {
        "versions": n_versions,
        "captured": n_captured,
        "not_captured": lost_n,
        "oracle_predicts_not_captured": predicted,
    }


def rekey_checks() -> None:
    assert ST.r_before is not None
    before = ST.r_before
    after = tr().dicts(f"SELECT * FROM lake.gold.fct_transactions WHERE transaction_id = {ST.R}")[0]
    moved_fields = {"customer_id", "src_lsn", "src_source_ts"}
    others_same = all(before[c] == after[c] for c in before if c not in moved_fields)
    entries_after = table_state(
        "lake.gold.fct_ledger_entries", "entry_id", f"transaction_id = {ST.R}", with_rows=True
    ).rows
    check(
        "re-key: the re-keyed transaction shows the placeholder customer in fct_transactions after build #2 and the "
        "ledger entries of that transaction are unchanged",
        before["customer_id"] == ST.A
        and after["customer_id"] == ST.B
        and others_same
        and after["src_lsn"] > before["src_lsn"]
        and entries_after == ST.r_entries_before
        and bool(entries_after),
        f"customer_id moved A->B={before['customer_id'] == ST.A and after['customer_id'] == ST.B} "
        f"other columns (incl. customer_sk, first_lsn) unchanged={others_same} "
        f"src_lsn advanced={after['src_lsn'] > before['src_lsn']} "
        f"ledger entries of the transaction: {len(entries_after or {})}",
    )


def phase_p3(drain: Drain) -> None:
    print("P3 build #3, no new data (idempotent rerun)")
    ensure_ids()
    wait_caught_up(drain)
    before = gold_states(with_rows=False)
    ids_before = table_state("lake.gold.fct_ledger_entries", "entry_id", with_rows=True).rows or {}
    snap_before = tr().one("SELECT count(*) FROM lake.gold_snapshots.snap_customer_attrs")
    dbt_build(3)
    after = gold_states(with_rows=False)
    ids_after = table_state("lake.gold.fct_ledger_entries", "entry_id", with_rows=True).rows or {}
    snap_after = tr().one("SELECT count(*) FROM lake.gold_snapshots.snap_customer_attrs")
    same = {t: before[t].tag() == after[t].tag() for t in GOLD_TABLES}
    check(
        "idempotent: rerunning dbt build on unchanged inputs (build #3) left every gold checksum unchanged and the "
        "ledger entry_id set unchanged in this run",
        all(same.values()) and set(ids_before) == set(ids_after) and snap_before == snap_after,
        f"checksums unchanged: {sum(same.values())}/{len(same)} tables; "
        f"entry_id set {len(ids_before)} -> {len(ids_after)}; "
        f"snapshot rows {snap_before} -> {snap_after}",
    )
    ST.details["idempotent"] = {t: after[t].tag() for t in GOLD_TABLES}
    ledger_unchanged(3)


def snapshot_ids(table: str) -> set[int]:
    return {int(r[0]) for r in tr().rows(f'SELECT snapshot_id FROM lake.gold."{table}$snapshots"')}


def snapshot_evidence(table: str, before: set[int]) -> dict[str, int]:
    """Summed Iceberg summary of the snapshots a build added. Trino's DELETE on an unpartitioned table writes position
    delete files (added-position-deletes) rather than dropping whole data files (deleted-records), so both count."""
    rows = tr().rows(f'SELECT snapshot_id, summary FROM lake.gold."{table}$snapshots"')
    new = [dict(r[1]) for r in rows if int(r[0]) not in before]
    tot = {"new_snapshots": len(new), "deleted_records": 0, "position_deletes": 0, "added_records": 0}
    for s in new:
        tot["deleted_records"] += int(s.get("deleted-records", 0))
        tot["position_deletes"] += int(s.get("added-position-deletes", 0)) + int(s.get("added-equality-deletes", 0))
        tot["added_records"] += int(s.get("added-records", 0))
    return tot


def phase_p4(drain: Drain) -> None:
    print(f"P4 one-day backfill of {BACKFILL_DAY} (build #4)")
    ensure_ids()
    wait_caught_up(drain)
    ft, led = "lake.gold.fct_transactions", "lake.gold.fct_ledger_entries"
    day = f"business_date = DATE '{BACKFILL_DAY}'"
    rest = f"business_date <> DATE '{BACKFILL_DAY}'"
    before = {
        "ft_all": table_state(ft, "transaction_id"),
        "led_all": table_state(led, "entry_id"),
        "ft_rest": table_state(ft, "transaction_id", rest),
        "led_rest": table_state(led, "entry_id", rest),
        "ft_day": table_state(ft, "transaction_id", day),
        "led_day": table_state(led, "entry_id", day),
    }
    snaps = {
        "fct_transactions": snapshot_ids("fct_transactions"),
        "fct_ledger_entries": snapshot_ids("fct_ledger_entries"),
    }
    dbt_build(4, variables={"backfill_day": BACKFILL_DAY})
    after = {
        "ft_all": table_state(ft, "transaction_id"),
        "led_all": table_state(led, "entry_id"),
        "ft_rest": table_state(ft, "transaction_id", rest),
        "led_rest": table_state(led, "entry_id", rest),
    }
    same = {k: before[k].tag() == after[k].tag() for k in after}
    check(
        f"backfill: reprocessing {BACKFILL_DAY} (build #4) left the checksums of fct_transactions and "
        "fct_ledger_entries "
        "identical, and the rows dated 2026-01-02 and 2026-01-03 untouched",
        all(same.values())
        and before["ft_day"].count > 0
        and before["led_day"].count > 0
        and before["led_rest"].count > 0,
        f"{BACKFILL_DAY}: {before['ft_day'].count} transactions, {before['led_day'].count} ledger entries; "
        f"other days: {before['ft_rest'].count} transactions, {before['led_rest'].count} entries; "
        f"unchanged={sum(same.values())}/{len(same)}",
    )
    ev_led = snapshot_evidence("fct_ledger_entries", snaps["fct_ledger_entries"])
    ev_ft = snapshot_evidence("fct_transactions", snaps["fct_transactions"])
    check(
        "backfill: not vacuous: the Iceberg snapshots of fct_ledger_entries show rows deleted and added "
        "(deleted-records "
        "or position deletes > 0, added-records > 0)",
        (ev_led["deleted_records"] + ev_led["position_deletes"]) > 0 and ev_led["added_records"] > 0,
        f"fct_ledger_entries {json.dumps(ev_led)}; fct_transactions {json.dumps(ev_ft)}",
    )
    ST.details["backfill"] = {"day": BACKFILL_DAY, "ledger": ev_led, "transactions": ev_ft}
    ledger_unchanged(4)


def row_diff_columns(a: list[dict[str, Any]], b: list[dict[str, Any]], key: str) -> set[str]:
    """Columns in which two row sets (matched on `key`) differ."""
    bm = {r[key]: r for r in b}
    cols: set[str] = set()
    for r in a:
        o = bm.get(r[key])
        if o is None:
            cols.add("<missing>")
            continue
        cols |= {c for c in r if r[c] != o[c]}
    return cols | ({"<count>"} if len(a) != len(b) else set())


def drop_rebuild() -> int:
    dropped = 0
    for sch in ("gold_rebuild", "gold_rebuild_snapshots"):
        try:
            names = [r[0] for r in tr().rows(f"SHOW TABLES FROM lake.{sch}")]
        except Exception:  # noqa: BLE001 - the schema does not exist
            continue
        for t in names:
            tr().exec(f'DROP TABLE IF EXISTS lake.{sch}."{t}"')
            dropped += 1
        tr().exec(f"DROP SCHEMA IF EXISTS lake.{sch}")
    return dropped


def phase_p5(drain: Drain, keep: bool) -> None:
    print("P5 from-scratch rebuild into gold_rebuild (build #5)")
    ensure_ids()
    wait_caught_up(drain)
    try:
        dbt_build(5, target="rebuild")
        r = ST.R
        excl = {
            "fct_transactions": f"transaction_id <> {r}",
            "fct_ledger_entries": f"(transaction_id IS NULL OR transaction_id <> {r})",
        }
        diffs: dict[str, str] = {}
        for t in GOLD_TABLES:
            where = excl.get(t, "")
            a = table_state(f"lake.gold.{t}", KEYS[t], where)
            b = table_state(f"lake.gold_rebuild.{t}", KEYS[t], where)
            diffs[t] = f"{a.count}={b.count}" if a.tag() == b.tag() else f"DIFFERENT {a.count} vs {b.count}"
        check(
            "rebuild: the incremental gold tables equal a from-scratch rebuild (checksums equal; the one re-keyed "
            "transaction is compared separately)",
            all(not v.startswith("DIFFERENT") for v in diffs.values()),
            json.dumps(diffs),
        )
        ft_inc = tr().dicts(f"SELECT * FROM lake.gold.fct_transactions WHERE transaction_id = {r}")
        ft_new = tr().dicts(f"SELECT * FROM lake.gold_rebuild.fct_transactions WHERE transaction_id = {r}")
        le_inc = tr().dicts(f"SELECT * FROM lake.gold.fct_ledger_entries WHERE transaction_id = {r}")
        le_new = tr().dicts(f"SELECT * FROM lake.gold_rebuild.fct_ledger_entries WHERE transaction_id = {r}")
        d_ft = row_diff_columns(ft_inc, ft_new, "transaction_id")
        d_le = row_diff_columns(le_inc, le_new, "entry_id")
        check(
            "rebuild: the re-keyed transaction differs from-scratch only where the incremental run carries values "
            "forward (customer_sk, merchant_sk and first_lsn in fct_transactions; source_lsn and src_source_ts of its "
            "payment legs in the ledger)",
            d_ft <= {"customer_sk", "merchant_sk", "first_lsn"} and d_le <= {"source_lsn", "src_source_ts"},
            f"fct_transactions differing columns={sorted(d_ft)} ledger differing columns={sorted(d_le)}",
        )
        ST.details["rebuild"] = {
            "tables": diffs,
            "rekeyed_fct_columns": sorted(d_ft),
            "rekeyed_ledger_columns": sorted(d_le),
        }
    finally:
        if keep:
            print("  (--keep-rebuild: gold_rebuild and gold_rebuild_snapshots are left in place)")
        else:
            n = drop_rebuild()
            left = tr().rows("SHOW SCHEMAS FROM lake LIKE 'gold_rebuild%'")
            check("rebuild: gold_rebuild and gold_rebuild_snapshots dropped", not left, f"{n} tables dropped")


def phase_p6() -> None:
    print("P6 artifact hygiene")
    try:
        ensure_ids()
    except ValueError:  # scripted rows absent (an earlier phase aborted): the canary is still searched for
        pass
    ids = (ST.A, ST.B, ST.M, ST.A_acct, ST.M_acct, ST.R)
    needles = [n.encode() for n in (CANARY, *(str(i) for i in ids if i))]
    scanned = hits = 0
    for sub in ("dbt_gold/target", "dbt_gold/logs"):
        for f in (ROOT / sub).rglob("*"):
            if f.is_file():
                scanned += 1
                data = f.read_bytes()
                hits += any(n in data for n in needles)
    check(
        "hygiene: no canary email and no scripted raw id in dbt_gold/target or dbt_gold/logs",
        scanned > 0 and hits == 0,
        f"{scanned} files scanned, {hits} with a hit",
    )


# ---- main --------------------------------------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sel = ap.add_mutually_exclusive_group()
    sel.add_argument(
        "--from", dest="from_phase", choices=PHASE_NAMES, help="run Pn..P6 (P0 and P7 always run); partial"
    )
    sel.add_argument("--only", choices=PHASE_NAMES, help="run just Pn (P0 and P7 always run); partial")
    ap.add_argument("--keep-rebuild", action="store_true", help="do not drop gold_rebuild after P5 (for inspection)")
    args = ap.parse_args()
    if args.only is not None:
        selected = [args.only]
    elif args.from_phase is not None:
        selected = list(PHASE_NAMES[PHASE_NAMES.index(args.from_phase) :])
    else:
        selected = list(PHASE_NAMES)
    if "P2" in selected and "P1" not in selected:
        raise SystemExit("P2 needs P1's in-memory scripted state: use --from P1, --from P3 or later, or no option")
    partial = selected != list(PHASE_NAMES)

    guard = SleepGuard()
    guard.start()
    started = datetime.now(timezone.utc).isoformat()
    c = g2.consumer()
    drain = Drain(c)
    seconds: dict[str, float] = {}
    runners = {
        "P1": lambda: phase_p1(drain),
        "P2": lambda: phase_p2(drain),
        "P3": lambda: phase_p3(drain),
        "P4": lambda: phase_p4(drain),
        "P5": lambda: phase_p5(drain, args.keep_rebuild),
        "P6": phase_p6,
    }
    failed = False
    for name in ["P0", *selected]:
        if failed and name != "P6":
            continue
        t0 = time.monotonic()
        try:
            if name == "P0":
                phase_p0()
            else:
                runners[name]()
        except (Abort, Exception, SystemExit) as exc:  # noqa: BLE001 - recorded as a failed check; later phases are skipped
            check(f"{name}: phase aborted ({type(exc).__name__})", False, str(exc)[:300])
            failed = True
        seconds[name] = round(time.monotonic() - t0, 1)
    c.close()

    sleep = guard.stop()
    check(
        "host sleep guard: no Sleep, Wake or DarkWake event in the run window and wall-clock drift within 2 s",
        not sleep["sleep_detected"],
        (
            f"drift_s={sleep['drift_s']} events={len(sleep['events'])} pmset_error={sleep['pmset_error']}"
            if sleep["checked"]
            else f"not checked on platform {sleep['platform']}"
        ),
    )
    details: dict[str, Any] = {"phases": selected, "phase_seconds": seconds, "builds": BUILDS, **ST.details}
    blob = json.dumps([RESULTS, details, sleep], default=str)
    check(
        "hygiene: the result being written holds no canary email and no 13-digit number",
        CANARY not in blob and not _BIGNUM.search(blob),
    )
    passed = all(r["ok"] for r in RESULTS) and not partial
    path = write_manifest(
        "g4-check",
        PROFILE,
        SEED,
        {
            "started_at": started,
            "passed": passed,
            "partial": partial,
            "checks": RESULTS,
            "host_sleep": sleep,
            **details,
        },
    )
    n_ok = sum(r["ok"] for r in RESULTS)
    verdict = "PASSED" if passed else ("PARTIAL" if partial and n_ok == len(RESULTS) else "FAILED")
    print(f"{verdict} ({n_ok}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}")
    return 0 if all(r["ok"] for r in RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
