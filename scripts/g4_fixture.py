"""G4 fixture check (FX1-FX8): the gold dbt project against a throwaway Trino, with scripted data and no Docker stack.

Run by `make dbt-fixture` after the throwaway Trino container (catalog `fx`, file-metastore Iceberg) is up:

    TRINO_PORT=28080 python scripts/g4_fixture.py [--keep] [--only FX3]

What it does. It creates `fx.src_bronze.cdc_events` and `fx.src_silver.{customers,merchants,accounts,transactions,
refunds}` with the column types of the real Spark tables (src/adrp/spark/streams.py, silver.py; Spark TIMESTAMP is
`timestamp(6) with time zone` in Trino). It loads `events_from_generator(42, 300)` plus scripted events, runs
`dbt build --target fx` by subprocess between steps, and compares the gold tables with the pure oracle
(adrp.gold.oracle) and with an independent in-script book of the scripted money movements.

Timeline (one linear history; each stage is one dbt build; `--only FXn` runs the stages up to the one that owns FXn
and prints only that scenario's checks, plus the build checks):

  B1  baseline: scripted A, B, P (placeholder), merchant M, two accounts, topups, payments, a full and a partial
      refund. Full build with the dbt unit tests, build_log.                       FX1 FX2 FX4 FX8
  B2  between runs: A changes 3 times (two separate transactions and one transaction with two updates), a no-op
      update and a delete of B, a change of M, a payment whose LSN falls between A's changes, a re-key of one
      transaction to P. Bronze rows arrive duplicated and out of order.                 FX1 FX2 FX4 FX6
  B3  late arrivals: one inside the lookback (picked up), one beyond it (missed; the completeness test must fail).
                                                                                        FX3
  B4  backfill of the missed row's day recovers it; everything is clean again.         FX3
  B5  backfill of a complete day: checksums of fct_transactions and fct_ledger_entries identical, with evidence
      from the Iceberg snapshots that rows were deleted and re-added (not vacuous).    FX5
  B6  rerun with no new data: every gold checksum and the ledger entry-id set unchanged. FX8
  B7  contract break (dim_date.date_key as varchar) fails the build and leaves the table intact. FX7

Environment: TRINO_PORT (the Makefile's name; default 28080) or TRINO_FX_PORT, TRINO_HOST (default localhost).
The script exports TRINO_FX_PORT and TRINO_PORT, both set to the same port, to every dbt subprocess, because the
fx target of dbt_gold/profiles.yml reads TRINO_FX_PORT while the Makefile sets TRINO_PORT.

Prints PASS/FAIL lines like scripts/g3_check.check, exits non-zero on any failure, and drops the fx schemas at the end
(unless --keep). It writes no manifest. Not run against a live Trino when this file was written.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import trino
import xxhash

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adrp.common import canonical_json
from adrp.gold import oracle as orc
from adrp.gold.oracle import _debezium_row as debezium_row  # the same JSON the real decoder produces

ROOT = Path(__file__).resolve().parents[1]

CATALOG = "fx"
BRONZE_SCHEMA = "src_bronze"
SILVER_SCHEMA = "src_silver"
GOLD_SCHEMA = "g4fx_gold"
SNAP_SCHEMA = "g4fx_gold_snapshots"  # dbt_project.yml: snapshots +schema: snapshots -> <target schema>_snapshots
FX_SCHEMAS = (GOLD_SCHEMA, SNAP_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA)
BRONZE_FQ = f"{CATALOG}.{BRONZE_SCHEMA}.cdc_events"
GOLD_FQ = f"{CATALOG}.{GOLD_SCHEMA}"
DBT_DIR = ROOT / "dbt_gold"
SEED = 42
GEN_TXNS = 300
LOOKBACK_MIN = 60  # dbt_project.yml var gold_lookback_minutes
SILVER_TABLES = ("customers", "merchants", "accounts", "transactions", "refunds")
CUST_TRACKED = ("full_name", "email", "phone", "address", "date_of_birth")
MERCH_TRACKED = ("name", "category", "country")
OK_STATUSES = frozenset({"success", "pass"})
SCENARIOS = tuple(f"FX{i}" for i in range(1, 9))

# ids of the scripted entities (all far from the generator's 1..n and from the identity default 1e12)
CUST_P, CUST_A, CUST_B = 9_000_000, 9_000_001, 9_000_002
MERCH_M = 9_100_001
ACCT_A, ACCT_M = 9_200_001, 9_200_002
T1, T2, T3, T4, T5, T6 = (9_300_000 + i for i in range(1, 7))
R1, R2 = 9_400_001, 9_400_002
OFFSET_BASE = 1_000_000  # scripted kafka offsets sit above the generator's

RESULTS: list[dict[str, Any]] = []
ONLY: str | None = None


def check(tag: str, name: str, ok: bool, detail: str = "") -> bool:
    """Same output shape as scripts/g3_check.check, with the scenario tag. Tag ALL is always shown."""
    if ONLY is not None and tag not in (ONLY, "ALL"):
        return True
    print(f"  {'PASS' if ok else 'FAIL'}  [{tag}] {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": f"[{tag}] {name}", "ok": bool(ok), "detail": detail})
    return bool(ok)


# ---- pure helpers (unit-tested by --selftest) ---------------------------------------------------


def at(day: int, hh: int, mm: int = 0) -> datetime:
    """2026-01-<day> hh:mm UTC."""
    return datetime(2026, 1, day, hh, mm, tzinfo=timezone.utc)


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _ts_lit(v: Any) -> str:
    dt = orc.UNIX_EPOCH + timedelta(milliseconds=v) if isinstance(v, int) else v
    u = dt.astimezone(timezone.utc)
    return f"CAST(TIMESTAMP '{u:%Y-%m-%d %H:%M:%S}.{u.microsecond:06d} UTC' AS timestamp(6) with time zone)"


def lit(value: Any, trino_type: str) -> str:
    """A Trino literal of the column type. NULL is always typed, so VALUES lists never see an unknown type."""
    if value is None:
        return f"CAST(NULL AS {trino_type})"
    if trino_type in ("bigint", "integer"):
        return str(int(value))
    if trino_type == "boolean":
        return "TRUE" if value else "FALSE"
    if trino_type == "varchar":
        return sql_str(str(value))
    if trino_type == "date":
        d = value.date() if isinstance(value, datetime) else value
        return f"DATE '{d if isinstance(d, str) else d.isoformat()}'"
    if trino_type.startswith("decimal"):
        return f"CAST('{format(Decimal(str(value)), 'f')}' AS {trino_type})"
    if trino_type.startswith("timestamp"):
        return _ts_lit(value)
    raise ValueError(f"no literal for type {trino_type}")


def _canon(v: Any) -> Any:
    if isinstance(v, Decimal):
        return "0" if v == 0 else format(v.normalize(), "f")
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(v, date):
        return v.isoformat()
    return v


def rows_digest(rows: Iterable[Mapping[str, Any]]) -> tuple[int, int]:
    """(row count, sum of xxh64(canonical row) mod 2**64): order-independent, like adrp.recon.canonical.digest but
    for rows without an `id` column. Decimals, timestamps and dates are normalized first."""
    n = total = 0
    for r in rows:
        h = xxhash.xxh64_intdigest(canonical_json({k: _canon(v) for k, v in r.items()}).encode("utf-8"))
        n += 1
        total = (total + h) % 2**64
    return n, total


def diff_dicts(actual: Mapping[Any, Any], expected: Mapping[Any, Any]) -> tuple[int, str]:
    """(number of keys that are missing, extra or different, a short description of the first)."""
    bad = [k for k in expected if k not in actual or actual[k] != expected[k]] + [
        k for k in actual if k not in expected
    ]
    if not bad:
        return 0, ""
    k = bad[0]
    return len(bad), f"first {k!r}: actual={actual.get(k, '<missing>')!r} expected={expected.get(k, '<missing>')!r}"


def nets(rows: Iterable[Mapping[str, Any]], key: str) -> tuple[int, int]:
    """(groups, groups whose amounts do not sum to zero), skipping rows whose key is NULL."""
    sums: dict[Any, Decimal] = {}
    for r in rows:
        if r[key] is not None:
            sums[r[key]] = sums.get(r[key], Decimal(0)) + Decimal(r["amount"])
    return len(sums), sum(1 for s in sums.values() if s != 0)


def selftest() -> None:
    assert lit(None, "bigint") == "CAST(NULL AS bigint)"
    assert lit("it's", "varchar") == "'it''s'"
    assert lit(Decimal("1.50"), "decimal(14,2)") == "CAST('1.50' AS decimal(14,2))"
    assert lit(date(2026, 1, 2), "date") == "DATE '2026-01-02'"
    assert lit(at(1, 2, 3), "timestamp(6) with time zone").startswith("CAST(TIMESTAMP '2026-01-01 02:03:00.000000 UTC'")
    assert "00.123000 UTC" in lit(orc.epoch_ms(at(1, 0)) + 123, "timestamp(6) with time zone")
    a = [{"x": Decimal("1.50"), "t": at(1, 0), "d": date(2026, 1, 1)}, {"x": Decimal("2"), "t": at(1, 1), "d": None}]
    b = [{"x": Decimal("2.0"), "t": at(1, 1), "d": None}, {"x": Decimal("1.5"), "t": at(1, 0), "d": date(2026, 1, 1)}]
    assert rows_digest(a) == rows_digest(b) and rows_digest(a)[0] == 2
    assert rows_digest(a) != rows_digest(a[:1])
    assert diff_dicts({1: "a"}, {1: "a"}) == (0, "")
    assert diff_dicts({1: "a", 3: "c"}, {1: "b", 2: "x"})[0] == 3
    ledger: list[dict[str, Any]] = [
        {"j": "p:1", "amount": Decimal("5")},
        {"j": "p:1", "amount": Decimal("-5")},
        {"j": None, "amount": 1},
    ]
    assert nets(ledger, "j") == (1, 0)
    assert nets([{"j": 1, "amount": Decimal("1")}], "j") == (1, 1)
    assert orc.epoch_ms(at(1, 0)) == 1767225600000
    print("selftest ok")


# ---- Trino ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Table:
    schema: str
    name: str
    columns: tuple[tuple[str, str], ...]
    partitioning: tuple[str, ...]

    @property
    def fq(self) -> str:
        return f"{CATALOG}.{self.schema}.{self.name}"

    def ddl(self) -> str:
        cols = ", ".join(f"{n} {t}" for n, t in self.columns)
        parts = ", ".join(f"'{p}'" for p in self.partitioning)
        return f"CREATE TABLE {self.fq} ({cols}) WITH (format_version = 2, partitioning = ARRAY[{parts}])"


TS = "timestamp(6) with time zone"
_META = (("lsn", "bigint"), ("is_deleted", "boolean"), ("op", "varchar"), ("source_ts", TS))
# lake.bronze.cdc_events (streams.py ensure_bronze): partitioned by (source_table, days(kafka_ts), bucket(16, pk))
BRONZE = Table(
    BRONZE_SCHEMA,
    "cdc_events",
    (
        ("topic", "varchar"),
        ("kafka_partition", "integer"),
        ("kafka_offset", "bigint"),
        ("kafka_ts", TS),
        ("key_bytes", "varbinary"),
        ("value_bytes", "varbinary"),
        ("schema_id", "integer"),
        ("source_table", "varchar"),
        ("op", "varchar"),
        ("lsn", "bigint"),
        ("txid", "bigint"),
        ("source_ts", TS),
        ("pk", "varchar"),
        ("before", "varchar"),
        ("after", "varchar"),
    ),
    ("source_table", "day(kafka_ts)", "bucket(pk, 16)"),
)
# lake.silver.<t> (silver.py create_silver): Postgres columns (char(n) and text as varchar, numeric(p,s) as decimal,
# timestamptz as timestamp(6) with time zone) plus lsn, is_deleted, op, source_ts; bucket(16, id).
_SILVER_COLS: dict[str, tuple[tuple[str, str], ...]] = {
    "customers": (
        ("id", "bigint"),
        ("full_name", "varchar"),
        ("email", "varchar"),
        ("phone", "varchar"),
        ("address", "varchar"),
        ("date_of_birth", "date"),
        ("created_at", TS),
        ("updated_at", TS),
    ),
    "merchants": (
        ("id", "bigint"),
        ("name", "varchar"),
        ("category", "varchar"),
        ("country", "varchar"),
        ("created_at", TS),
        ("updated_at", TS),
    ),
    "accounts": (
        ("id", "bigint"),
        ("owner_type", "varchar"),
        ("customer_id", "bigint"),
        ("merchant_id", "bigint"),
        ("currency", "varchar"),
        ("balance", "decimal(14,2)"),
        ("created_at", TS),
        ("updated_at", TS),
    ),
    "transactions": (
        ("id", "bigint"),
        ("txn_type", "varchar"),
        ("customer_id", "bigint"),
        ("merchant_id", "bigint"),
        ("payer_account_id", "bigint"),
        ("payee_account_id", "bigint"),
        ("amount", "decimal(12,2)"),
        ("currency", "varchar"),
        ("status", "varchar"),
        ("description", "varchar"),
        ("created_at", TS),
    ),
    "refunds": (
        ("id", "bigint"),
        ("transaction_id", "bigint"),
        ("amount", "decimal(12,2)"),
        ("reason", "varchar"),
        ("created_at", TS),
    ),
}
SILVER = {t: Table(SILVER_SCHEMA, t, (*cols, *_META), ("bucket(id, 16)",)) for t, cols in _SILVER_COLS.items()}


class Trino:
    def __init__(self, host: str, port: int) -> None:
        self.conn = trino.dbapi.connect(  # type: ignore[no-untyped-call]
            host=host,
            port=port,
            user="g4fx",
            catalog=CATALOG,
            schema=BRONZE_SCHEMA,
            http_scheme="http",
            request_timeout=300,
        )

    def run(self, sql: str) -> list[list[Any]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()  # always drain: a statement is only finished once its results are read

    def rows(self, sql: str) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        names = [c[0] for c in cur.description]
        return [dict(zip(names, r, strict=True)) for r in data]

    def insert(self, table: Table, rows: Sequence[Mapping[str, Any]], batch: int = 100) -> None:
        cols = [c for c, _ in table.columns]
        for i in range(0, len(rows), batch):
            values = ", ".join(
                "(" + ", ".join(lit(r.get(c), t) for c, t in table.columns) + ")" for r in rows[i : i + batch]
            )
            self.run(f"INSERT INTO {table.fq} ({', '.join(cols)}) VALUES {values}")

    def drop_schema(self, schema: str) -> None:
        """Drop every table and view of the fx schema, then the schema. A missing schema is not an error."""
        try:
            names = [r[0] for r in self.run(f"SHOW TABLES FROM {CATALOG}.{schema}")]
        except trino.exceptions.TrinoQueryError:
            return
        for n in names:
            try:
                self.run(f'DROP TABLE IF EXISTS {CATALOG}.{schema}."{n}"')
            except trino.exceptions.TrinoQueryError:
                self.run(f'DROP VIEW IF EXISTS {CATALOG}.{schema}."{n}"')
        self.run(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema}")


def wait_ready(tr: Trino, timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            tr.run("SELECT 1")
            return
        except (trino.exceptions.Error, OSError) as e:
            if time.time() > deadline:
                raise SystemExit(f"Trino not ready after {timeout:.0f}s: {e}") from e
            time.sleep(2)


# ---- scripted world ---------------------------------------------------------------------------


def bronze_dict(r: orc.BronzeRow) -> dict[str, Any]:
    return {
        "topic": r.topic,
        "kafka_partition": r.kafka_partition,
        "kafka_offset": r.kafka_offset,
        "kafka_ts": r.kafka_ts_ms,
        "source_table": r.source_table,
        "op": r.op,
        "lsn": r.lsn,
        "txid": r.txid,
        "source_ts": r.source_ts_ms,
        "pk": r.pk,
        "before": r.before,
        "after": r.after,
    }


def dbz(row: Mapping[str, Any] | None) -> str | None:
    return None if row is None else canonical_json(debezium_row(row))


class World:
    """Bronze events (the oracle's input) and silver rows (the dbt sources), written to Trino by flush().

    Silver is maintained by the script exactly as the real MERGE would leave it: the latest row per key with its
    LSN, op and commit time; a delete is a tombstone (key only, is_deleted). `book` is an independent running
    balance per scripted account, so the ledger is checked against more than the oracle's own arithmetic.
    """

    def __init__(self, tr: Trino) -> None:
        self.tr = tr
        rows, silver = orc.events_from_generator(SEED, GEN_TXNS)
        keep = {f"public.{t}" for t in SILVER_TABLES}  # other tables (tickets, articles) are not read by gold
        self.bronze: list[orc.BronzeRow] = [r for r in rows if r.source_table in keep]
        self.pending: list[orc.BronzeRow] = list(self.bronze)
        self.silver: dict[str, dict[int, dict[str, Any]]] = silver
        self.dirty: dict[str, set[int]] = {t: set(silver[t]) for t in SILVER_TABLES}
        self.cur: dict[tuple[str, int], dict[str, Any]] = {}  # latest plain row of each scripted entity
        self.book: dict[int, Decimal] = {}
        self.lsn = 10_000_000  # above every generator LSN (seq * 100)
        self.txid = 5_000
        self.kclock = orc.epoch_ms(at(2, 0))
        self.offsets: Counter[int] = Counter()

    # -- identifiers and clocks
    def next_lsn(self) -> int:
        self.lsn += 100
        return self.lsn

    def next_txid(self) -> int:
        self.txid += 1
        return self.txid

    # -- bronze
    def make(
        self,
        table: str,
        op: str,
        pk: int,
        after: Mapping[str, Any] | None,
        before: Mapping[str, Any] | None,
        *,
        lsn: int,
        txid: int,
        ts: datetime,
    ) -> orc.BronzeRow:
        return orc.BronzeRow(
            topic=f"cdc.public.{table}",
            kafka_partition=pk % 3,
            kafka_offset=0,
            kafka_ts_ms=0,
            source_table=f"public.{table}",
            op=op,
            lsn=lsn,
            txid=txid,
            source_ts_ms=orc.epoch_ms(ts),
            pk=canonical_json({"id": pk}),
            before=dbz(before),
            after=dbz(after),
        )

    def deliver(self, row: orc.BronzeRow) -> orc.BronzeRow:
        """Append to the log: a fresh offset and the next broker time. Delivering a row twice is a duplicate."""
        self.offsets[row.kafka_partition] += 1
        out = replace(row, kafka_offset=OFFSET_BASE + self.offsets[row.kafka_partition], kafka_ts_ms=self.kclock)
        self.kclock += 1000
        self.pending.append(out)
        self.bronze.append(out)
        return out

    def emit(
        self,
        table: str,
        op: str,
        pk: int,
        after: Mapping[str, Any] | None,
        before: Mapping[str, Any] | None,
        *,
        lsn: int,
        txid: int,
        ts: datetime,
    ) -> orc.BronzeRow:
        return self.deliver(self.make(table, op, pk, after, before, lsn=lsn, txid=txid, ts=ts))

    # -- silver
    def put_silver(
        self, table: str, pk: int, row: Mapping[str, Any] | None, *, lsn: int, ts: datetime, op: str
    ) -> None:
        ms = orc.epoch_ms(ts)
        if row is None:
            self.silver[table][pk] = {"id": pk, "lsn": lsn, "is_deleted": True, "op": op, "source_ts_ms": ms}
        else:
            self.silver[table][pk] = {**row, "lsn": lsn, "is_deleted": False, "op": op, "source_ts_ms": ms}
            self.cur[(table, pk)] = dict(row)
        self.dirty[table].add(pk)

    def flush(self) -> None:
        if self.pending:
            self.tr.insert(BRONZE, [bronze_dict(r) for r in self.pending])
            self.pending.clear()
        for table, pks in self.dirty.items():
            if not pks:
                continue
            ids = ", ".join(str(p) for p in sorted(pks))
            self.tr.run(f"DELETE FROM {SILVER[table].fq} WHERE id IN ({ids})")
            recs = [{**self.silver[table][p], "source_ts": self.silver[table][p]["source_ts_ms"]} for p in sorted(pks)]
            self.tr.insert(SILVER[table], recs)
            pks.clear()

    # -- scripted entities
    def new_customer(self, pk: int, name: str, email: str, phone: str, address: str, dob: date, ts: datetime) -> None:
        row = {
            "id": pk,
            "full_name": name,
            "email": email,
            "phone": phone,
            "address": address,
            "date_of_birth": dob,
            "created_at": ts,
            "updated_at": ts,
        }
        lsn = self.next_lsn()
        self.emit("customers", "c", pk, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("customers", pk, row, lsn=lsn, ts=ts, op="c")

    def new_merchant(self, pk: int, name: str, category: str, country: str, ts: datetime) -> None:
        row = {"id": pk, "name": name, "category": category, "country": country, "created_at": ts, "updated_at": ts}
        lsn = self.next_lsn()
        self.emit("merchants", "c", pk, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("merchants", pk, row, lsn=lsn, ts=ts, op="c")

    def open_account(
        self, pk: int, owner_type: str, customer_id: int | None, merchant_id: int | None, balance: Decimal, ts: datetime
    ) -> None:
        row = {
            "id": pk,
            "owner_type": owner_type,
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "currency": "USD",
            "balance": balance,
            "created_at": ts,
            "updated_at": ts,
        }
        lsn = self.next_lsn()
        self.emit("accounts", "c", pk, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("accounts", pk, row, lsn=lsn, ts=ts, op="c")
        self.book[pk] = balance

    def _touch(self, acct: int, delta: Decimal, lsn: int, ts: datetime) -> None:
        """Silver accounts.balance after a money movement (the bronze account update event is not modelled:
        gold reads only the first bronze event of an account)."""
        self.book[acct] += delta
        row = {**self.cur[("accounts", acct)], "balance": self.book[acct], "updated_at": ts}
        self.put_silver("accounts", acct, row, lsn=lsn, ts=ts, op="u")

    def _txn_row(
        self,
        tid: int,
        kind: str,
        cust: int,
        merch: int | None,
        payer: int | None,
        payee: int,
        amount: Decimal,
        ts: datetime,
    ) -> dict[str, Any]:
        return {
            "id": tid,
            "txn_type": kind,
            "customer_id": cust,
            "merchant_id": merch,
            "payer_account_id": payer,
            "payee_account_id": payee,
            "amount": amount,
            "currency": "USD",
            "status": "settled",
            "description": "fixture",
            "created_at": ts,
        }

    def topup(self, tid: int, acct: int, cust: int, amount: Decimal, ts: datetime, *, lsn: int | None = None) -> None:
        lsn = lsn or self.next_lsn()
        row = self._txn_row(tid, "topup", cust, None, None, acct, amount, ts)
        self.emit("transactions", "c", tid, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("transactions", tid, row, lsn=lsn, ts=ts, op="c")
        self._touch(acct, amount, lsn, ts)

    def payment(
        self,
        tid: int,
        payer: int,
        payee: int,
        cust: int,
        merch: int,
        amount: Decimal,
        ts: datetime,
        *,
        lsn: int | None = None,
    ) -> None:
        lsn = lsn or self.next_lsn()
        row = self._txn_row(tid, "payment", cust, merch, payer, payee, amount, ts)
        self.emit("transactions", "c", tid, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("transactions", tid, row, lsn=lsn, ts=ts, op="c")
        self._touch(payer, -amount, lsn, ts)
        self._touch(payee, amount, lsn, ts)

    def refund(self, rid: int, tid: int, amount: Decimal, ts: datetime) -> None:
        pay = self.silver["transactions"][tid]
        row = {"id": rid, "transaction_id": tid, "amount": amount, "reason": "fixture", "created_at": ts}
        lsn = self.next_lsn()
        self.emit("refunds", "c", rid, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("refunds", rid, row, lsn=lsn, ts=ts, op="c")
        self._touch(pay["payee_account_id"], -amount, lsn, ts)
        self._touch(pay["payer_account_id"], amount, lsn, ts)

    def rekey(self, tid: int, to_customer: int, ts: datetime, lsn: int) -> None:
        """Forget's re-key of a transaction to the placeholder customer: silver and bronze both see an update."""
        row = {**self.cur[("transactions", tid)], "customer_id": to_customer}
        self.emit("transactions", "u", tid, row, None, lsn=lsn, txid=self.next_txid(), ts=ts)
        self.put_silver("transactions", tid, row, lsn=lsn, ts=ts, op="u")

    # -- oracle views
    def expected_ledger(self, missing: frozenset[int] = frozenset()) -> list[orc.Entry]:
        entries = orc.ledger_entries(*orc.ledger_inputs_from_bronze(self.bronze))
        return [e for e in entries if e.transaction_id not in missing]

    def expected_fct(self, missing: frozenset[int] = frozenset()) -> dict[int, tuple[Any, ...]]:
        refunds = {r["transaction_id"]: r for r in self.silver["refunds"].values() if not r["is_deleted"]}
        out: dict[int, tuple[Any, ...]] = {}
        for tid, t in self.silver["transactions"].items():
            if t["is_deleted"] or tid in missing:
                continue
            r = refunds.get(tid)
            d = t["created_at"].astimezone(timezone.utc).date()
            out[tid] = (
                t["txn_type"],
                t["customer_id"],
                t["merchant_id"],
                t["payer_account_id"],
                t["payee_account_id"],
                t["amount"],
                t["currency"],
                t["status"],
                d,
                d.year * 10000 + d.month * 100 + d.day,
                r["id"] if r else None,
                r["amount"] if r else None,
            )
        return out

    def first_lsns(self) -> dict[int, tuple[int, int]]:
        """transaction id -> (lsn of its insert event, customer id at insert)."""
        out: dict[int, tuple[int, int]] = {}
        for r in self.bronze:
            if r.source_table == "public.transactions" and r.op == "c" and r.after is not None:
                out[int(json.loads(r.pk)["id"])] = (r.lsn, int(json.loads(r.after)["customer_id"]))
        return out


# ---- stage context --------------------------------------------------------------------------------


@dataclass
class DbtRun:
    rc: int
    results: dict[str, dict[str, Any]]
    tail: str
    seconds: float

    @staticmethod
    def short(uid: str) -> str:
        return ".".join(uid.split(".")[2:])

    def counts(self) -> dict[str, int]:
        return dict(Counter(r["status"] for r in self.results.values()))

    def bad(self) -> list[str]:
        return [self.short(u) for u, r in self.results.items() if r["status"] not in OK_STATUSES]

    def failed_tests(self) -> set[str]:
        return {
            self.short(u) for u, r in self.results.items() if u.startswith("test.") and r["status"] in ("fail", "error")
        }

    def failed_nodes(self) -> list[str]:
        return [
            self.short(u)
            for u, r in self.results.items()
            if not u.startswith("test.") and r["status"] in ("fail", "error", "skipped")
        ]

    def n_unit_passed(self) -> int:
        return sum(1 for u, r in self.results.items() if u.startswith("unit_test.") and r["status"] == "pass")


@dataclass
class Fx:
    tr: Trino
    w: World
    env: dict[str, str]
    snap_schema: str = SNAP_SCHEMA
    state: dict[str, Any] = field(default_factory=dict)

    def dbt(self, *, unit: bool = False, select: str | None = None, vars: Mapping[str, Any] | None = None) -> DbtRun:
        exe = Path(sys.executable).parent / "dbt"
        cmd = [
            str(exe) if exe.exists() else (shutil.which("dbt") or "dbt"),
            "--no-use-colors",
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            "fx",
        ]
        if not unit:
            cmd += ["--exclude", "test_type:unit"]
        if select:
            cmd += ["--select", select]
        cmd += ["--vars", json.dumps({"strict_reconcile": True, **(vars or {})})]
        rr = DBT_DIR / "target" / "run_results.json"
        rr.unlink(missing_ok=True)  # never read a stale file
        t0 = time.time()
        p = subprocess.run(
            cmd,
            cwd=ROOT,
            env=self.env,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=1800,
            check=False,
        )
        results: dict[str, dict[str, Any]] = {}
        if rr.exists():
            results = {r["unique_id"]: r for r in json.loads(rr.read_text())["results"]}
        tail = "\n".join((p.stdout + p.stderr).splitlines()[-25:])
        return DbtRun(p.returncode, results, tail, time.time() - t0)

    def expect_clean(self, label: str, run: DbtRun) -> bool:
        ok = run.rc == 0 and bool(run.results) and not run.bad()
        detail = f"{run.seconds:.0f}s {run.counts()}"
        if not ok:
            detail += f" not ok: {run.bad()[:4]}"
            print(run.tail, flush=True)
        return check("ALL", f"{label}: dbt build clean (exit 0, every node success/pass)", ok, detail)

    def table(self, name: str) -> list[dict[str, Any]]:
        return self.tr.rows(f"SELECT * FROM {GOLD_FQ}.{name}")

    def digest(self, name: str) -> tuple[int, int]:
        return rows_digest(self.table(name))


# ---- shared assertions ----------------------------------------------------------------------------


def _vtuple(v: orc.Version) -> tuple[Any, ...]:
    return (v.valid_to_lsn, v.valid_from_ms, v.valid_to_ms, v.txid, v.attrs, v.is_current, v.closed_by_delete)


def _ms(dt: datetime | None) -> int | None:
    return None if dt is None else orc.epoch_ms(dt)


def _attr(v: Any) -> Any:
    return v.isoformat() if isinstance(v, date) else v


def assert_dims(fx: Fx, label: str) -> None:
    for table, key, tracked, dim in (
        ("customers", "customer_id", CUST_TRACKED, "dim_customer"),
        ("merchants", "merchant_id", MERCH_TRACKED, "dim_merchant"),
    ):
        exp = orc.scd2_versions(orc.events_from_bronze(fx.w.bronze, table), tracked)
        expected = {(v.pk, v.valid_from_lsn): _vtuple(v) for v in exp}
        actual = {
            (r[key], r["valid_from_lsn"]): (
                r["valid_to_lsn"],
                _ms(r["valid_from"]),
                _ms(r["valid_to"]),
                r["txid"],
                tuple(_attr(r[c]) for c in tracked),
                r["is_current"],
                r["closed_by_delete"],
            )
            for r in fx.table(dim)
        }
        n, ex = diff_dicts(actual, expected)
        check(
            "FX1",
            f"{label}: {dim} equals the oracle (valid_from/to by LSN and commit ms, txid, attrs)",
            n == 0,
            f"{len(exp)} versions, {n} differ {ex}",
        )


def assert_marts(fx: Fx, label: str, *, missing: frozenset[int] = frozenset()) -> None:
    """fct_transactions, fct_ledger_entries and the balance reconciliation against oracle and silver.
    `missing` are transactions silver holds but gold is expected not to have yet (late beyond the lookback)."""
    w, tr = fx.w, fx.tr
    # fct_transactions: one row per live silver transaction, columns as silver, refund folded in
    fct = {r["transaction_id"]: r for r in fx.table("fct_transactions")}
    actual = {
        tid: (
            r["txn_type"],
            r["customer_id"],
            r["merchant_id"],
            r["payer_account_id"],
            r["payee_account_id"],
            r["amount"],
            r["currency"],
            r["status"],
            r["business_date"],
            r["date_key"],
            r["refund_id"],
            r["refunded_amount"],
        )
        for tid, r in fct.items()
    }
    n, ex = diff_dicts(actual, w.expected_fct(missing))
    check(
        "FX4",
        f"{label}: fct_transactions equals silver (one row per transaction, refund folded in)",
        n == 0,
        f"{len(fct)} rows, {n} differ {ex}",
    )
    # point-in-time keys: customer_sk and merchant_sk name the dim version in force at the insert LSN
    first = w.first_lsns()
    dc = tr.rows(f"SELECT customer_sk, customer_id, valid_from_lsn FROM {GOLD_FQ}.dim_customer")
    dm = tr.rows(f"SELECT merchant_sk, merchant_id, valid_from_lsn FROM {GOLD_FQ}.dim_merchant")
    cust_by_sk = {r["customer_sk"]: (r["customer_id"], r["valid_from_lsn"]) for r in dc}
    merch_by_sk = {r["merchant_sk"]: (r["merchant_id"], r["valid_from_lsn"]) for r in dm}

    bad_c = bad_m = bad_l = 0
    for tid, r in fct.items():
        lsn, orig_cust = first[tid]
        bad_l += r["first_lsn"] != lsn  # carried forward across the re-key
    versions_c: dict[int, list[orc.Version]] = {}
    for v in orc.scd2_versions(orc.events_from_bronze(w.bronze, "customers"), CUST_TRACKED):
        versions_c.setdefault(v.pk, []).append(v)
    versions_m: dict[int, list[orc.Version]] = {}
    for v in orc.scd2_versions(orc.events_from_bronze(w.bronze, "merchants"), MERCH_TRACKED):
        versions_m.setdefault(v.pk, []).append(v)

    def pit(versions: dict[int, list[orc.Version]], pk: int, lsn: int) -> int | None:
        for v in versions.get(pk, []):
            if v.valid_from_lsn <= lsn and (v.valid_to_lsn is None or lsn < v.valid_to_lsn):
                return v.valid_from_lsn
        return None

    merch_of = {t: s["merchant_id"] for t, s in w.silver["transactions"].items()}
    for tid, r in fct.items():
        lsn, orig_cust = first[tid]
        got = cust_by_sk.get(r["customer_sk"])
        bad_c += got != (orig_cust, pit(versions_c, orig_cust, lsn))
        if merch_of[tid] is not None:
            gm = merch_by_sk.get(r["merchant_sk"])
            bad_m += gm != (merch_of[tid], pit(versions_m, merch_of[tid], lsn))
        else:
            bad_m += r["merchant_sk"] is not None
    check(
        "FX1",
        f"{label}: fct customer_sk / merchant_sk are the dim versions in force at the insert LSN (point-in-time join)",
        bad_c == 0 and bad_m == 0,
        f"{len(fct)} rows, {bad_c} customer, {bad_m} merchant",
    )
    check("FX6", f"{label}: fct first_lsn is the insert LSN of every transaction", bad_l == 0, f"{bad_l} differ")
    # ledger: entries equal the oracle's, every journal and every transaction nets to zero
    led = fx.table("fct_ledger_entries")
    actual_l = {
        r["entry_id"]: (
            r["journal_id"],
            r["journal_type"],
            r["transaction_id"],
            r["refund_id"],
            r["account_id"],
            r["amount"],
            r["currency"],
            r["business_date"],
        )
        for r in led
    }
    entries = w.expected_ledger(missing)
    exp_l = {
        e.entry_id: (
            e.journal_id,
            e.journal_type,
            e.transaction_id,
            e.refund_id,
            e.account_id,
            e.amount,
            e.currency,
            e.business_date,
        )
        for e in entries
    }
    n, ex = diff_dicts(actual_l, exp_l)
    types = Counter(r["journal_type"] for r in led)
    check(
        "FX4",
        f"{label}: fct_ledger_entries equals the oracle (entry ids, accounts, signed amounts, dates)",
        n == 0,
        f"{len(led)} entries {dict(types)}, {n} differ {ex}",
    )
    g_j, bad_j = nets(led, "journal_id")
    g_t, bad_t = nets(led, "transaction_id")
    check(
        "FX4",
        f"{label}: every journal and every transaction (payment plus its refund) nets to zero",
        bad_j == 0 and bad_t == 0,
        f"{g_j} journals, {g_t} transactions, {bad_j + bad_t} not zero",
    )
    # balances: ledger sum per real account against silver accounts.balance (and the oracle against silver)
    ledger_bal: dict[int, Decimal] = {}
    for r in led:
        if r["account_id"] > 0:
            ledger_bal[r["account_id"]] = ledger_bal.get(r["account_id"], Decimal(0)) + r["amount"]
    oracle_bal = orc.balances(entries)
    silver_bal = {a: Decimal(r["balance"]) for a, r in w.silver["accounts"].items() if not r["is_deleted"]}
    book_bad = (
        sum(1 for a, b in w.book.items() if silver_bal[a] != b or oracle_bal.get(a, Decimal(0)) != b)
        if not missing
        else 0
    )
    check(
        "FX4",
        f"{label}: fixture self-check, the independent book of scripted balances equals silver and the oracle",
        book_bad == 0,
        f"{len(w.book)} scripted accounts, {book_bad} differ",
    )
    ref = oracle_bal if missing else silver_bal  # with a missing transaction gold legitimately trails silver
    what = "oracle (silver is ahead by the missing transaction)" if missing else "silver accounts.balance"
    mism = [a for a in set(ref) | set(ledger_bal) if ledger_bal.get(a, Decimal(0)) != ref.get(a, Decimal(0))]
    no_entries = sum(1 for a in silver_bal if a not in ledger_bal)
    check(
        "FX4",
        f"{label}: ledger balance equals {what} for every account",
        not mism,
        f"{len(silver_bal)} accounts, {no_entries} without entries, {len(mism)} mismatches",
    )


def snapshot_rows(fx: Fx) -> list[dict[str, Any]]:
    schemas = [
        r[0] for r in fx.tr.run(f"SHOW SCHEMAS FROM {CATALOG}") if str(r[0]).startswith("g4fx") and "snap" in r[0]
    ]
    fx.snap_schema = schemas[0] if schemas else SNAP_SCHEMA
    return fx.tr.rows(f"SELECT customer_id, attr_hash FROM {CATALOG}.{fx.snap_schema}.snap_customer_attrs")


def snapshots_of(fx: Fx, table: str) -> list[dict[str, Any]]:
    return fx.tr.rows(
        f'SELECT snapshot_id, operation, summary FROM {GOLD_FQ}."{table}$snapshots" ORDER BY committed_at'
    )


def evidence(before_ids: set[Any], after: Sequence[Mapping[str, Any]]) -> tuple[int, int, str]:
    """(rows or files removed, rows added, description) over the Iceberg snapshots created since `before_ids`.
    Trino removes rows with position deletes (added-position-deletes) or whole files (deleted-records)."""
    new = [s for s in after if s["snapshot_id"] not in before_ids]
    removed = added = 0
    for s in new:
        sm = s["summary"]
        removed += sum(
            int(sm.get(k, 0)) for k in ("deleted-records", "added-position-deletes", "added-equality-deletes")
        )
        added += int(sm.get("added-records", 0))
    ops = ", ".join(
        f"{s['operation']}:{ {k: v for k, v in s['summary'].items() if 'records' in k or 'deletes' in k} }" for s in new
    )
    return removed, added, f"{len(new)} new snapshots: {ops}"


# ---- scripted batches --------------------------------------------------------------------------------


def script_baseline(w: World) -> None:
    w.kclock = orc.epoch_ms(at(2, 0))
    w.new_customer(
        CUST_P, "Placeholder Subject", "placeholder@fx.test", "000-0000", "n/a", date(1900, 1, 1), at(2, 0, 0)
    )
    w.new_customer(CUST_A, "Ada Fixture", "a1@fx.test", "555-0001", "1 First St", date(1990, 1, 1), at(2, 0, 5))
    w.new_customer(CUST_B, "Bob Fixture", "b1@fx.test", "555-0100", "9 Ninth St", date(1985, 5, 5), at(2, 0, 7))
    w.new_merchant(MERCH_M, "Mart Fixture", "grocery", "US", at(2, 0, 10))
    w.open_account(ACCT_A, "customer", CUST_A, None, Decimal("100.00"), at(2, 0, 15))
    w.open_account(ACCT_M, "merchant", None, MERCH_M, Decimal("0.00"), at(2, 0, 20))
    w.topup(T1, ACCT_A, CUST_A, Decimal("50.00"), at(2, 1, 0))
    w.payment(T2, ACCT_A, ACCT_M, CUST_A, MERCH_M, Decimal("30.00"), at(2, 2, 0))
    w.refund(R1, T2, Decimal("30.00"), at(2, 3, 0))  # full refund
    w.payment(T3, ACCT_A, ACCT_M, CUST_A, MERCH_M, Decimal("40.00"), at(3, 1, 0))
    w.refund(R2, T3, Decimal("20.00"), at(3, 2, 0))  # partial refund (half)
    w.flush()


@dataclass
class ChangeLsns:
    u1: int
    m: int
    u2: int
    t4: int  # T4, a payment that falls between A's second change and the two-update transaction
    ta: int
    tb: int
    bn: int
    bd: int
    rk: int


def script_changes(w: World) -> ChangeLsns:
    """Everything between run 1 and run 2. LSNs are allocated in commit order; delivery order is scrambled."""
    w.kclock = orc.epoch_ms(at(3, 3))
    ls = ChangeLsns(*(w.next_lsn() for _ in range(9)))
    ts_u1, ts_m, ts_u2, ts_t4, ts_tx, ts_bn, ts_bd, ts_rk = (
        at(3, 3, 0),
        at(3, 3, 5),
        at(3, 3, 10),
        at(3, 3, 20),
        at(3, 3, 30),
        at(3, 3, 40),
        at(3, 3, 50),
        at(3, 4, 0),
    )
    tx_u1, tx_m, tx_u2, tx_tx, tx_bn, tx_bd = (w.next_txid() for _ in range(6))
    a1 = w.cur[("customers", CUST_A)]
    a_u1 = {**a1, "email": "a2@fx.test", "updated_at": ts_u1}
    a_u2 = {**a_u1, "phone": "555-0002", "updated_at": ts_u2}
    a_ta = {**a_u2, "address": "2 Second St", "updated_at": ts_tx}  # first update of the two-update transaction
    a_tb = {**a_ta, "email": "a3@fx.test"}  # second update, same transaction: only this state counts
    ev_u1 = w.make("customers", "u", CUST_A, a_u1, a1, lsn=ls.u1, txid=tx_u1, ts=ts_u1)
    ev_u2 = w.make("customers", "u", CUST_A, a_u2, a_u1, lsn=ls.u2, txid=tx_u2, ts=ts_u2)
    ev_ta = w.make("customers", "u", CUST_A, a_ta, a_u2, lsn=ls.ta, txid=tx_tx, ts=ts_tx)
    ev_tb = w.make("customers", "u", CUST_A, a_tb, a_ta, lsn=ls.tb, txid=tx_tx, ts=ts_tx)
    m1 = w.cur[("merchants", MERCH_M)]
    m_u = {**m1, "category": "dining", "updated_at": ts_m}
    ev_m = w.make("merchants", "u", MERCH_M, m_u, m1, lsn=ls.m, txid=tx_m, ts=ts_m)
    b1 = w.cur[("customers", CUST_B)]
    b_n = {**b1, "updated_at": ts_bn}  # only updated_at changes: a no-op for the tracked attributes
    ev_bn = w.make("customers", "u", CUST_B, b_n, b1, lsn=ls.bn, txid=tx_bn, ts=ts_bn)
    ev_bd = w.make("customers", "d", CUST_B, None, b_n, lsn=ls.bd, txid=tx_bd, ts=ts_bd)
    # arrival order: u2 before u1, the second update of the two-update transaction before its first, then duplicates
    for ev in (ev_u2, ev_tb, ev_u1, ev_ta, ev_m, ev_bn, ev_bd, ev_u1, ev_tb, ev_bn):
        w.deliver(ev)
    w.put_silver("customers", CUST_A, a_tb, lsn=ls.tb, ts=ts_tx, op="u")
    w.put_silver("merchants", MERCH_M, m_u, lsn=ls.m, ts=ts_m, op="u")
    w.put_silver("customers", CUST_B, None, lsn=ls.bd, ts=ts_bd, op="d")
    w.payment(T4, ACCT_A, ACCT_M, CUST_A, MERCH_M, Decimal("10.00"), ts_t4, lsn=ls.t4)
    w.rekey(T3, CUST_P, ts_rk, ls.rk)
    w.flush()
    return ls


def script_late(w: World) -> None:
    """T5 is older than the newest gold row but inside the 60 minute lookback; T6 is older than the lookback."""
    w.kclock = orc.epoch_ms(at(3, 5))
    w.topup(T5, ACCT_A, CUST_A, Decimal("5.00"), at(3, 3, 45))
    w.topup(T6, ACCT_A, CUST_A, Decimal("7.00"), at(3, 0, 15))
    w.flush()


# ---- stages ------------------------------------------------------------------------------------------


def stage_b1(fx: Fx) -> None:
    w = fx.w
    script_baseline(w)
    fx.state["cut1"] = w.lsn
    run = fx.dbt(unit=True)
    fx.expect_clean("build 1", run)
    check(
        "ALL",
        "build 1: the dbt unit tests ran and passed",
        run.n_unit_passed() > 0,
        f"{run.n_unit_passed()} unit tests",
    )
    assert_dims(fx, "build 1")
    snap = snapshot_rows(fx)
    n_live = sum(1 for r in w.silver["customers"].values() if not r["is_deleted"])
    check(
        "FX2",
        "build 1: the snapshot captured every live customer once",
        len(snap) == n_live,
        f"{len(snap)} snapshot rows, {n_live} customers",
    )
    assert_marts(fx, "build 1")
    bl = fx.tr.run(f"SELECT count(DISTINCT model_name) FROM {GOLD_FQ}.build_log")[0][0]
    check("FX8", "build 1: on-run-end build_log has a row for each of the four tracked models", bl == 4, f"{bl} models")
    fx.state["t3_before"] = next(r for r in fx.table("fct_transactions") if r["transaction_id"] == T3)
    fx.state["ledger_before"] = {r["entry_id"]: r for r in fx.table("fct_ledger_entries")}


def stage_b2(fx: Fx) -> None:
    w, tr = fx.w, fx.tr
    ls = script_changes(w)
    fx.state["cut2"] = w.lsn
    fx.state["ls"] = ls
    # preconditions: the scripted events really contain duplicates and out-of-order arrival
    rows = tr.rows(
        f"SELECT lsn, count(*) AS n, min(kafka_ts) AS k FROM {BRONZE_FQ} "
        f"WHERE source_table = 'public.customers' AND pk = {sql_str(canonical_json({'id': CUST_A}))} GROUP BY lsn"
    )
    by = {r["lsn"]: r for r in rows}
    dups = sum(1 for r in rows if r["n"] > 1)
    ooo = by[ls.u2]["k"] < by[ls.u1]["k"] and by[ls.tb]["k"] < by[ls.ta]["k"]
    check(
        "FX1",
        "build 2 inputs: A's bronze has duplicate (pk, lsn) rows and LSN order differs from arrival order",
        dups >= 2 and ooo,
        f"{dups} duplicated LSNs, out of order: {ooo}",
    )
    run = fx.dbt()
    fx.expect_clean("build 2", run)
    assert_dims(fx, "build 2")
    va = [v for v in orc.scd2_versions(orc.events_from_bronze(w.bronze, "customers"), CUST_TRACKED) if v.pk == CUST_A]
    vb = [v for v in orc.scd2_versions(orc.events_from_bronze(w.bronze, "customers"), CUST_TRACKED) if v.pk == CUST_B]
    vm = [v for v in orc.scd2_versions(orc.events_from_bronze(w.bronze, "merchants"), MERCH_TRACKED) if v.pk == MERCH_M]
    act = {
        r["valid_from_lsn"]: r
        for r in tr.rows(f"SELECT * FROM {GOLD_FQ}.dim_customer WHERE customer_id IN ({CUST_A}, {CUST_B})")
    }
    check(
        "FX1",
        "build 2: A has 4 versions (3 changes, the two updates of one transaction collapsed to one), "
        "B 1 closed by its delete with the no-op merged, M 2",
        len(va) == 4
        and len(vb) == 1
        and len(vm) == 2
        and sum(1 for r in act.values() if r["customer_id"] == CUST_A) == 4
        and sum(1 for r in act.values() if r["customer_id"] == CUST_B and r["closed_by_delete"]) == 1,
        f"oracle: A {len(va)}, B {len(vb)}, M {len(vm)}",
    )
    # FX2: the dbt snapshot, run twice, saw only the first and the last state of A
    cuts = [fx.state["cut1"], fx.state["cut2"]]
    all_v = orc.scd2_versions(orc.events_from_bronze(w.bronze, "customers"), CUST_TRACKED)
    snap = snapshot_rows(fx)
    a_snap = sum(1 for r in snap if r["customer_id"] == CUST_A)
    lost_a = orc.lost_intermediate(va, cuts)
    check(
        "FX2",
        "build 2: the snapshot captured exactly 2 states of A while SCD2 has 4: exactly 2 are lost",
        a_snap == 2 and len(va) == 4 and lost_a == 2 and len(va) - a_snap == 2,
        f"snapshot {a_snap}, scd2 {len(va)}, oracle lost {lost_a}",
    )
    exp_caps = len(orc.snapshot_captures(all_v, cuts))
    check(
        "FX2",
        "build 2: snapshot rows over all customers equal the oracle's capture model",
        len(snap) == exp_caps,
        f"{len(snap)} rows, oracle {exp_caps}; lost {len(all_v) - len(snap)} of {len(all_v)} versions at 2 runs",
    )
    assert_marts(fx, "build 2")
    # FX6: the re-key reaches fct_transactions, leaves the surrogate keys and the ledger alone
    t3a = next(r for r in fx.table("fct_transactions") if r["transaction_id"] == T3)
    t3b = fx.state["t3_before"]
    check(
        "FX6",
        "re-key: the transaction's customer_id is the placeholder; customer_sk, merchant_sk and first_lsn are kept",
        t3a["customer_id"] == CUST_P
        and t3b["customer_id"] == CUST_A
        and t3a["customer_sk"] == t3b["customer_sk"]
        and t3a["merchant_sk"] == t3b["merchant_sk"]
        and t3a["first_lsn"] == t3b["first_lsn"]
        and t3a["src_lsn"] > t3b["src_lsn"],
        f"customer_id {t3b['customer_id']} -> {t3a['customer_id']}",
    )
    after = {r["entry_id"]: r for r in fx.table("fct_ledger_entries")}
    before = fx.state["ledger_before"]
    changed = sum(1 for k, v in before.items() if after.get(k) != v)
    check(
        "FX6",
        "re-key: every ledger entry that existed before is unchanged (the ledger has no customer_id)",
        changed == 0 and len(after) > len(before),
        f"{len(before)} before, {len(after)} after, {changed} changed",
    )
    # T4 sits between A's second change and the two-update transaction: its customer_sk is A's third version
    t4 = tr.rows(
        f"SELECT d.valid_from_lsn AS cv, m.valid_from_lsn AS mv FROM {GOLD_FQ}.fct_transactions f "
        f"JOIN {GOLD_FQ}.dim_customer d ON d.customer_sk = f.customer_sk "
        f"JOIN {GOLD_FQ}.dim_merchant m ON m.merchant_sk = f.merchant_sk WHERE f.transaction_id = {T4}"
    )
    check(
        "FX1",
        "build 2: a payment between A's changes points at the version in force at its LSN (A's 3rd, M's 2nd)",
        len(t4) == 1 and t4[0]["cv"] == ls.u2 and t4[0]["mv"] == ls.m,
        f"{t4}",
    )
    fx.state["digests_b2"] = {t: fx.digest(t) for t in ("fct_transactions", "fct_ledger_entries")}


def stage_b3(fx: Fx) -> None:
    script_late(fx.w)
    run = fx.dbt()
    failed = run.failed_tests()
    allowed = {"assert_gold_complete_vs_silver", "assert_ledger_balance_matches_silver_accounts"}
    check(
        "FX3",
        "build 3 (late arrivals): the build fails on the completeness test, and only on tests that must",
        run.rc != 0 and "assert_gold_complete_vs_silver" in failed and failed <= allowed and not run.failed_nodes(),
        f"failed tests {sorted(failed)}, failed or skipped nodes {run.failed_nodes()[:3]}, {run.counts()}",
    )
    have = {r["transaction_id"] for r in fx.table("fct_transactions")}
    led_t = {r["transaction_id"] for r in fx.table("fct_ledger_entries")}
    check(
        "FX3",
        "late inside the lookback is picked up (fct and ledger); late beyond it is missed",
        T5 in have and T5 in led_t and T6 not in have and T6 not in led_t,
        f"T5 fct {T5 in have} ledger {T5 in led_t}; T6 fct {T6 in have} ledger {T6 in led_t}",
    )
    assert_marts(fx, "build 3", missing=frozenset({T6}))


def stage_b4(fx: Fx) -> None:
    run = fx.dbt(vars={"backfill_day": "2026-01-03"})
    fx.expect_clean("backfill 2026-01-03", run)
    have = {r["transaction_id"] for r in fx.table("fct_transactions")}
    led_t = {r["transaction_id"] for r in fx.table("fct_ledger_entries")}
    check(
        "FX3",
        "backfill of the missed row's day recovers it in fct and ledger; the completeness test passes",
        T6 in have and T6 in led_t and not run.failed_tests(),
        f"T6 fct {T6 in have} ledger {T6 in led_t}",
    )
    assert_marts(fx, "after backfill 2026-01-03")
    assert_dims(fx, "after backfill 2026-01-03")


def stage_b5(fx: Fx) -> None:
    day = "2026-01-02"
    before = {t: fx.digest(t) for t in ("fct_transactions", "fct_ledger_entries")}
    snaps = {t: {s["snapshot_id"] for s in snapshots_of(fx, t)} for t in before}
    n_day = {
        "fct_transactions": fx.tr.run(
            f"SELECT count(*) FROM {GOLD_FQ}.fct_transactions WHERE business_date = DATE '{day}'"
        )[0][0],
        "fct_ledger_entries": fx.tr.run(
            f"SELECT count(*) FROM {GOLD_FQ}.fct_ledger_entries WHERE business_date = DATE '{day}'"
        )[0][0],
    }
    run = fx.dbt(vars={"backfill_day": day})
    fx.expect_clean(f"backfill {day}", run)
    after = {t: fx.digest(t) for t in before}
    check(
        "FX5",
        f"backfill of {day}: count and checksum of fct_transactions and fct_ledger_entries identical",
        after == before,
        f"rows {before['fct_transactions'][0]} and {before['fct_ledger_entries'][0]}",
    )
    check(
        "FX5",
        f"backfill of {day}: the day holds rows to delete and re-add",
        all(n > 0 for n in n_day.values()),
        f"{n_day}",
    )
    for t in before:
        removed, added, desc = evidence(snaps[t], snapshots_of(fx, t))
        # fct_transactions reselects the day and replaces by key (delete+insert); the ledger deletes the day (pre_hook)
        check(
            "FX5",
            f"backfill of {day}: {t} snapshots show rows removed and rows added (not a no-op)",
            removed > 0 and added > 0,
            f"removed {removed}, added {added}; {desc}",
        )


def stage_b6(fx: Fx) -> None:
    tables = ("dim_customer", "dim_merchant", "dim_account", "dim_date", "fct_transactions", "fct_ledger_entries")
    before = {t: fx.digest(t) for t in tables}
    ids_before = {r["entry_id"] for r in fx.table("fct_ledger_entries")}
    run = fx.dbt()
    fx.expect_clean("rerun with no new data", run)
    after = {t: fx.digest(t) for t in tables}
    ids_after = {r["entry_id"] for r in fx.table("fct_ledger_entries")}
    same = [t for t in tables if before[t] == after[t]]
    check(
        "FX8",
        "rerun with no new data: count and checksum of every gold table unchanged",
        len(same) == len(tables),
        f"unchanged {len(same)} of {len(tables)}; differing {sorted(set(tables) - set(same))}",
    )
    check(
        "FX8",
        "rerun with no new data: the ledger's entry_id set is unchanged",
        ids_before == ids_after,
        f"{len(ids_after)} entries",
    )
    assert_marts(fx, "final")


def stage_b7(fx: Fx) -> None:
    q = (
        f"SELECT data_type FROM {CATALOG}.information_schema.columns WHERE table_schema = '{GOLD_SCHEMA}' "
        "AND table_name = 'dim_date' AND column_name = 'date_key'"
    )
    n0 = fx.tr.run(f"SELECT count(*) FROM {GOLD_FQ}.dim_date")[0][0]
    run = fx.dbt(select="dim_date", vars={"contract_break_test": True})
    node = next((r for u, r in run.results.items() if u.startswith("model.") and u.endswith(".dim_date")), None)
    msg = str(node["message"]).lower() if node else ""
    check(
        "FX7",
        "contract break: the build fails and the dim_date node errors with a contract message",
        run.rc != 0 and node is not None and node["status"] == "error" and "contract" in msg,
        f"rc {run.rc}, status {node['status'] if node else None}, message {msg[:120]!r}",
    )
    typ = fx.tr.run(q)
    n1 = fx.tr.run(f"SELECT count(*) FROM {GOLD_FQ}.dim_date")[0][0]
    check(
        "FX7",
        "contract break: dim_date is left intact (date_key still integer, same row count)",
        typ == [["integer"]] and n0 == n1,
        f"{typ} {n0} -> {n1} rows",
    )


STAGES: list[tuple[str, Callable[[Fx], None], frozenset[str]]] = [
    ("B1 baseline", stage_b1, frozenset({"FX1", "FX2", "FX4", "FX8"})),
    ("B2 changes between runs", stage_b2, frozenset({"FX1", "FX2", "FX4", "FX6"})),
    ("B3 late arrivals", stage_b3, frozenset({"FX3"})),
    ("B4 backfill recovery", stage_b4, frozenset({"FX3"})),
    ("B5 backfill identity", stage_b5, frozenset({"FX5"})),
    ("B6 idempotent rerun", stage_b6, frozenset({"FX8"})),
    ("B7 contract break", stage_b7, frozenset({"FX7"})),
]


# ---- main ----------------------------------------------------------------------------------------


def main() -> int:
    global ONLY
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", action="store_true", help="do not drop the fx schemas at the end (for diagnosis)")
    ap.add_argument(
        "--only", choices=SCENARIOS, help="run the stages up to the one that owns this scenario; print only it"
    )
    ap.add_argument("--selftest", action="store_true", help="unit-test the pure helpers and exit (no Trino needed)")
    ap.add_argument("--host", default=os.environ.get("TRINO_HOST", "localhost"))
    ap.add_argument(
        "--port", type=int, default=int(os.environ.get("TRINO_PORT") or os.environ.get("TRINO_FX_PORT") or 28080)
    )
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return 0
    ONLY = args.only
    last = max((i for i, (_, _, tags) in enumerate(STAGES) if ONLY is None or ONLY in tags), default=len(STAGES) - 1)
    env = {**os.environ, "TRINO_HOST": args.host, "TRINO_PORT": str(args.port), "TRINO_FX_PORT": str(args.port)}
    started = time.time()
    print(
        f"g4 fixture: Trino {args.host}:{args.port}, catalog {CATALOG}, seed {SEED}, {GEN_TXNS} generator txns",
        flush=True,
    )
    tr = Trino(args.host, args.port)
    wait_ready(tr)
    try:
        catalogs = {r[0] for r in tr.run("SHOW CATALOGS")}
        if not check(
            "ALL", "Trino is up and has the `fx` catalog", CATALOG in catalogs, f"catalogs {sorted(catalogs)}"
        ):
            return 1
        for s in FX_SCHEMAS:
            tr.drop_schema(s)  # leftovers of an earlier --keep run
        for s in (BRONZE_SCHEMA, SILVER_SCHEMA):
            tr.run(f"CREATE SCHEMA {CATALOG}.{s}")
        for t in (BRONZE, *SILVER.values()):
            tr.run(t.ddl())
        w = World(tr)
        fx = Fx(tr, w, env)
        w.flush()
        n_b = tr.run(f"SELECT count(*) FROM {BRONZE_FQ}")[0][0]
        check(
            "ALL",
            "generator events loaded (bronze and silver)",
            n_b > 0,
            f"{n_b} bronze rows, " + ", ".join(f"{t} {len(w.silver[t])}" for t in SILVER_TABLES),
        )
        for name, fn, _tags in STAGES[: last + 1]:
            print(f"--- {name}", flush=True)
            try:
                fn(fx)
            except Exception as e:  # noqa: BLE001 - a stage that cannot run fails the run; later stages depend on it
                traceback.print_exc()
                check("ALL", f"{name}: stage ran to completion", False, f"{type(e).__name__}: {e}"[:300])
                break
    finally:
        if args.keep:
            print("--keep: fx schemas left in place", flush=True)
        else:
            try:
                for s in FX_SCHEMAS:
                    tr.drop_schema(s)
                left = [r[0] for r in tr.run(f"SHOW SCHEMAS FROM {CATALOG}") if str(r[0]).startswith(("g4fx", "src_"))]
                check("ALL", "fx schemas dropped", not left, f"left: {left}")
            except (trino.exceptions.Error, OSError) as e:
                check("ALL", "fx schemas dropped", False, f"{type(e).__name__}: {e}"[:200])
    n_ok = sum(r["ok"] for r in RESULTS)
    passed = bool(RESULTS) and all(r["ok"] for r in RESULTS)
    print(f"{'PASSED' if passed else 'FAILED'} ({n_ok}/{len(RESULTS)}) in {time.time() - started:.0f}s", flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
