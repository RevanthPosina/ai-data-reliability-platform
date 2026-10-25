"""Apply the generator's logical transactions to Postgres (PLAN G1).

- One logical transaction = one Postgres transaction, committed with an update of `gen.state.next_txn`
  in the same transaction, so a rerun resumes the deterministic sequence exactly where the DB is.
- Ground-truth log (logical ops, contains synthetic PII) and run log (txid, commit time) are written
  as separate Parquet datasets under RUN_DATA_DIR/<run_id>/.
- Rate: target row changes per second (0 = unthrottled). Achieved rate is reported after warmup.

Prints one line `SUMMARY {json}` at the end, which the host wrapper puts into the run manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from adrp.common import canonical_json
from adrp.gen.model import Generator, Op, StreamHash

COLUMNS = {
    "customers": ["id", "full_name", "email", "phone", "address", "date_of_birth", "created_at", "updated_at"],
    "merchants": ["id", "name", "category", "country", "created_at", "updated_at"],
    "accounts": [
        "id", "owner_type", "customer_id", "merchant_id", "currency", "balance", "created_at", "updated_at",
    ],
    "transactions": [
        "id", "txn_type", "customer_id", "merchant_id", "payer_account_id", "payee_account_id",
        "amount", "currency", "status", "description", "created_at",
    ],
    "refunds": ["id", "transaction_id", "amount", "reason", "created_at"],
    "support_tickets": ["id", "customer_id", "subject", "body", "status", "priority", "created_at", "updated_at"],
    "help_articles": ["id", "title", "body", "category", "view_count", "created_at", "updated_at"],
}  # fmt: skip


def _sql(table: str) -> dict[str, str]:
    cols = COLUMNS[table]
    non_key = [c for c in cols if c != "id"]
    return {
        "I": f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
        "U": f"UPDATE {table} SET {', '.join(f'{c} = %s' for c in non_key)} WHERE id = %s",
        "D": f"DELETE FROM {table} WHERE id = %s",
    }


SQL = {t: _sql(t) for t in COLUMNS}


def params(op: Op) -> tuple[Any, ...]:
    cols = COLUMNS[op.table]
    if op.op == "I":
        assert op.row is not None
        return tuple(op.row[c] for c in cols)
    if op.op == "U":
        assert op.row is not None
        return (*(op.row[c] for c in cols if c != "id"), op.pk)
    return (op.pk,)


class ParquetLog:
    """Buffered, chunked Parquet writer (one file per chunk keeps a crash from losing earlier chunks)."""

    def __init__(self, directory: Path, schema: pa.Schema, chunk_rows: int = 50_000) -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.chunk_rows = chunk_rows
        self.rows: list[dict[str, Any]] = []
        self.part = 0

    def add(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.chunk_rows:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        pq.write_table(table, self.dir / f"part-{self.part:05d}.parquet")
        self.part += 1
        self.rows = []


GROUND_TRUTH_SCHEMA = pa.schema(
    [
        ("seq", pa.int64()),
        ("txn_no", pa.int64()),
        ("op", pa.string()),
        ("table", pa.string()),
        ("pk", pa.int64()),
        ("event_seq", pa.int64()),
        ("row_json", pa.string()),
    ]
)
RUN_LOG_SCHEMA = pa.schema(
    [
        ("txn_no", pa.int64()),
        ("kind", pa.string()),
        ("n_ops", pa.int32()),
        ("txid", pa.int64()),  # xid8 (epoch-extended)
        ("xid32", pa.int64()),  # 32-bit xid, as reported by Debezium source.txId
        ("client_begin_ns", pa.int64()),
        ("client_commit_ns", pa.int64()),
        ("commit_ts", pa.timestamp("us", tz="UTC")),  # Postgres commit time (track_commit_timestamp)
    ]
)


def resolve_commit_ts(conn: psycopg.Connection[Any], txids: list[int]) -> dict[int, datetime | None]:
    out: dict[int, datetime | None] = {}
    with conn.cursor() as cur:
        for i in range(0, len(txids), 10_000):
            batch = txids[i : i + 10_000]
            cur.execute(
                "SELECT x, pg_xact_commit_timestamp((x %% 4294967296)::text::xid) FROM unnest(%s::bigint[]) AS x",
                (batch,),
            )
            out.update(dict(cur.fetchall()))
    conn.commit()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--duration", type=float, default=30.0, help="seconds of applying changes")
    ap.add_argument("--max-txns", type=int, default=0, help="stop after N txns (0 = duration only)")
    ap.add_argument("--rate", type=float, default=0.0, help="target row changes/s (0 = unthrottled)")
    ap.add_argument("--warmup", type=float, default=0.0, help="seconds excluded from achieved-rate stats")
    ap.add_argument("--run-id", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    args = ap.parse_args()

    dsn = os.environ["SHOP_DSN"]
    out_dir = Path(os.environ.get("RUN_DATA_DIR", "/runs/data")) / args.run_id
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    conn = psycopg.connect(dsn, autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SELECT seed, next_txn FROM gen.state WHERE id = 1")
        found = cur.fetchone()
        if found is None:
            cur.execute("INSERT INTO gen.state (id, seed, next_txn) VALUES (1, %s, 0)", (args.seed,))
            start_txn = 0
        else:
            if found[0] != args.seed:
                raise SystemExit(f"database was generated with seed {found[0]}, not {args.seed}; reset volumes")
            start_txn = int(found[1])
    conn.commit()

    gen = Generator(args.seed)
    t_ff = time.monotonic()
    gen.skip(start_txn)
    fast_forward_s = time.monotonic() - t_ff

    gt = ParquetLog(out_dir / "ground_truth", GROUND_TRUTH_SCHEMA)
    runlog_rows: list[dict[str, Any]] = []
    digest = StreamHash()
    changes_total = changes_after_warmup = txns_after_warmup = 0
    t0 = time.monotonic()
    warm_t: float | None = None if args.warmup > 0 else t0
    cur = conn.cursor()
    while not stop["flag"]:
        now = time.monotonic()
        if now - t0 >= args.duration or (args.max_txns and gen.txn_no - start_txn >= args.max_txns):
            break
        if warm_t is None and now - t0 >= args.warmup:
            warm_t = now
        txn = gen.next_txn()
        if args.rate > 0:  # pace by cumulative row changes
            due = t0 + changes_total / args.rate
            if due > now:
                time.sleep(due - now)
        begin_ns = time.time_ns()
        cur.execute("SELECT pg_current_xact_id()::text::bigint")
        row = cur.fetchone()
        assert row is not None
        txid = int(row[0])
        for op in txn.ops:
            cur.execute(SQL[op.table][op.op], params(op))
        cur.execute("UPDATE gen.state SET next_txn = %s, updated_at = now() WHERE id = 1", (txn.txn_no + 1,))
        conn.commit()
        commit_ns = time.time_ns()
        for op in txn.ops:
            digest.update(op)
            gt.add(
                {
                    "seq": op.seq,
                    "txn_no": op.txn_no,
                    "op": op.op,
                    "table": op.table,
                    "pk": op.pk,
                    "event_seq": op.event_seq,
                    "row_json": canonical_json(op.row) if op.row is not None else None,
                }
            )
        runlog_rows.append(
            {
                "txn_no": txn.txn_no,
                "kind": txn.kind,
                "n_ops": len(txn.ops),
                "txid": txid,
                "xid32": txid % 4294967296,
                "client_begin_ns": begin_ns,
                "client_commit_ns": commit_ns,
            }
        )
        changes_total += len(txn.ops)
        if warm_t is not None:
            changes_after_warmup += len(txn.ops)
            txns_after_warmup += 1
    t_end = time.monotonic()
    gt.flush()

    commit_ts = resolve_commit_ts(conn, [r["txid"] for r in runlog_rows])
    for r in runlog_rows:
        r["commit_ts"] = commit_ts.get(r["txid"])
    runlog = ParquetLog(out_dir / "run_log", RUN_LOG_SCHEMA, chunk_rows=len(runlog_rows) + 1)
    for r in runlog_rows:
        runlog.add(r)
    runlog.flush()
    conn.close()

    measured_s = (t_end - warm_t) if warm_t is not None else 0.0
    end_txn = start_txn + len(runlog_rows)
    summary = {
        "run_id": args.run_id,
        "seed": args.seed,
        "txn_range": [start_txn, end_txn],
        "ops": digest.ops,
        "stream_sha256": digest.hexdigest(),
        "target_rate_changes_per_s": args.rate,
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "measured_s": round(measured_s, 3),
        "achieved_changes_per_s": round(changes_after_warmup / measured_s, 1) if measured_s > 0 else None,
        "achieved_txns_per_s": round(txns_after_warmup / measured_s, 1) if measured_s > 0 else None,
        "fell_behind": bool(args.rate > 0 and measured_s > 0 and changes_after_warmup / measured_s < 0.95 * args.rate),
        "commit_ts_missing": sum(1 for r in runlog_rows if r["commit_ts"] is None),
        "fast_forward_s": round(fast_forward_s, 3),
        "data_dir": str(out_dir),
    }
    print("SUMMARY " + json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
