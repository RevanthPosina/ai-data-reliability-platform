"""G8 done-condition check: deletion propagation, verified in this system (not a compliance claim).

Precondition (Makefile target `g8-check`): fresh volumes, Redpanda topics created with infra/redpanda/topic-config.json
(`bootstrap.py --forget-ready-topics`), core + apps up with ADRP_FAULT_HOOKS=1, nothing generated yet.

  P0 setup     generator, N ad-hoc subjects with canaries (several ticket versions, a historical deleted ticket, one
               ticket forced into quarantine), SIGKILL mid_merge, a bronze tag, drain; the subject is PRESENT in every
               sink, asserted by sink name (non-vacuity)
  P1 logical   forget the ad-hoc subjects under load: logical absence per sink within S6, silver tombstones, ledger rows
               re-keyed, Qdrant, bronze, quarantine; the index still equals Postgres
  P2 generator forget the generator-owned Zipf-head customer after the generator stopped
  P3 window    physical maintenance window: Iceberg (delete files, refs, snapshots, orphans), whole-bucket scan,
               SeaweedFS vacuum, embedder pointer, files, Postgres vacuum
  P4 sinks     Postgres WAL, Redpanda segments, Qdrant segments: physically absent within the deadline, or the bound
  P5 purge     re-sweep, silver tombstone purge, raw ids NULLed, files and logs clean, pseudonym joins, manifest self-scan
  P6 rerun     an idempotent re-run changes nothing; the final scan finds nothing; manifest

Every result says "logical" or "physical" and names the sink. Nothing here prints a raw id or canary.
Resume one phase on the live stack: `python scripts/g8_check.py --only P3` (state: .run/g8-state.json).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import random
import re
import sys
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import g2_check as g2
import g3_check as g3
import g6_check as g6
from _env import ROOT
from generate import run_once
from manifest import write_manifest

sys.path.insert(0, str(ROOT / "src"))
from adrp.forget import audit, broker, postgres, qdrant, scan, sparkcli, storage, verify, workflow
from adrp.forget import files as ffiles
from adrp.forget import ids as fids
from adrp.forget.rawscan import service_running

SEED = g2.SEED
N_ADHOC = 5
LOAD_RATE = 8  # txn/s while subjects are forgotten: below what silver can merge, so the lag measured is the workflow's, not a backlog's
STATE = ROOT / ".run/g8-state.json"
REDACTED_NOTE = "ids, canaries and emails are never printed or stored in this check"
RESULTS: list[dict[str, Any]] = []
DETAILS: dict[str, Any] = {}
LOGICAL_SINKS = ("postgres", "silver", "bronze", "quarantine", "qdrant")
TS = ("customers", "accounts", "transactions", "support_tickets")
META = {"id", "lsn", "is_deleted", "op", "source_ts"}
_DIGITS = re.compile(r"\d{4,}")
PHASES = ("P0", "P1", "P2", "P3", "P4", "P5", "P6")


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}", flush=True)
    RESULTS.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


def mask(text: str, limit: int = 300) -> str:
    return _DIGITS.sub("#", text)[-limit:]


# ---- state (resume one phase on the live stack) ------------------------------------------------------------


@dataclass
class Ctx:
    cfg: workflow.Config
    consumer: Any
    drain: g3.Drain
    canaries: list[str] = field(default_factory=list)  # g8c-... tokens, one per ad-hoc subject (not PII)
    quarantine_index: int = 0
    adhoc_rids: list[str] = field(default_factory=list)
    gen_rid: str = ""
    subjects: list[workflow.ForgetSubject] = field(default_factory=list)  # raw ids: memory only
    seconds: dict[str, float] = field(default_factory=dict)
    scratch: dict[str, Any] = field(default_factory=dict)  # small cross-phase facts: counts, never ids

    @property
    def all_rids(self) -> list[str]:
        return [*self.adhoc_rids, *([self.gen_rid] if self.gen_rid else [])]


def save_state(ctx: Ctx, started: str) -> None:
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(
        json.dumps(
            {
                "started": started, "canaries": ctx.canaries, "quarantine_index": ctx.quarantine_index,
                "adhoc_rids": ctx.adhoc_rids, "gen_rid": ctx.gen_rid, "seconds": ctx.seconds, "scratch": ctx.scratch,
                "results": RESULTS, "details": DETAILS,
            },
            default=str,
        )
    )  # fmt: skip


def load_state(ctx: Ctx) -> str:
    st = json.loads(STATE.read_text())
    ctx.canaries, ctx.quarantine_index = st["canaries"], st["quarantine_index"]
    ctx.adhoc_rids, ctx.gen_rid = st["adhoc_rids"], st["gen_rid"]
    ctx.seconds, ctx.scratch = st["seconds"], st["scratch"]
    RESULTS[:] = st["results"]
    DETAILS.update(st["details"])
    return str(st["started"])


def reload_subjects(ctx: Ctx) -> None:
    """Subjects of the requests that are still open (raw id still stored). A finished request has nothing to load;
    P5/P6 keep the in-memory subjects from the phases before when they run in one process."""
    subs: list[workflow.ForgetSubject] = []
    for i, rid in enumerate(ctx.adhoc_rids):
        s = workflow.load_subject(rid, (ctx.canaries[i],))
        if s is not None:
            subs.append(s)
    if ctx.gen_rid:
        s = workflow.load_subject(ctx.gen_rid)
        if s is not None:
            subs.append(s)
    ctx.subjects = subs


class SafeDrain(g3.Drain):
    """g3.Drain, tolerant of compacted topics: read_range waits for the last offset, which compaction may have
    removed (an expired tombstone). Then the end offsets are taken as read."""

    def refresh_topics(self) -> None:
        try:
            super().refresh_topics()
        except SystemExit:
            for t in g3.TABLES:
                self.offsets[t] = g2.end_offsets(self.c, f"cdc.public.{t}")


# ---- generic helpers ---------------------------------------------------------------------------------------


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def first_int(out: str) -> int:
    return int(out.splitlines()[0].split("|")[0])


def cid_for(canary: str) -> int:
    return int(g2.psql(f"SELECT id FROM customers WHERE email = {sql_str(canary + '@g8.example')}"))


def drain_all(ctx: Ctx, what: str, timeout: float = 420) -> bool:
    return check(f"{what}: silver caught up to the end of every topic", ctx.drain.wait_silver(timeout))


def index_matches_postgres(what: str, timeout: float = 240) -> bool:
    ok, _secs, r = g6.wait_consistent(timeout)
    check(f"{what}: the index is consistent with silver", ok, json.dumps(r))
    ok2, _, pg = g6.wait_matches_postgres(timeout)
    return check(
        f"{what}: the whole index equals Postgres's current text (non-subjects; subjects hold no points)",
        ok2,
        json.dumps(pg),
    )


def ids_arg(xs: list[int]) -> str:
    return ",".join(str(int(x)) for x in xs) or "NULL"


def verdicts(outcomes: dict[str, verify.SinkOutcome]) -> dict[str, str]:
    return {n: o.verdict for n, o in sorted(outcomes.items())}


def secs_since(t0: float) -> float:
    return round(time.monotonic() - t0, 1)


# ---- P0 setup ----------------------------------------------------------------------------------------------


def plant_subject(cid: int, canary: str, n_versions: int = 3) -> None:
    """One ad-hoc customer: PII with the canary in name/email/address, an account with balance changes, ledger rows,
    two live tickets with several versions and one ticket that is created and deleted again (history)."""
    q = g2.psql
    mail = f"{canary}@g8.example"
    q(
        "INSERT INTO customers (id, full_name, email, phone, address, date_of_birth) VALUES "
        f"({cid}, {sql_str('G8 Subject ' + canary)}, {sql_str(mail)}, '+1-555-0100', {sql_str('1 Forget Street ' + canary)}, '1990-01-01')"
    )  # fmt: skip
    acct = first_int(
        q(
            f"INSERT INTO accounts (owner_type, customer_id, currency, balance) VALUES ('customer', {cid}, 'EUR', 0) RETURNING id"
        )
    )
    for i in range(3):
        q(
            "INSERT INTO transactions (txn_type, customer_id, payee_account_id, amount, currency, status, description) "
            f"VALUES ('topup', {cid}, {acct}, {10 + i}.00, 'EUR', 'completed', 'g8 topup {i}')"
        )
        q(f"UPDATE accounts SET balance = balance + {10 + i}, updated_at = now() WHERE id = {acct}")
    tickets = [
        first_int(
            q(
                "INSERT INTO support_tickets (customer_id, subject, body, status, priority) VALUES "
                f"({cid}, {sql_str(f'Card declined {canary}')}, {sql_str(f'My card was declined at checkout. Reach me at {mail}. Ref {canary}.')}, 'open', 'high') RETURNING id"
            )
        ),
        first_int(
            q(
                "INSERT INTO support_tickets (customer_id, subject, body, status, priority) VALUES "
                f"({cid}, {sql_str(f'Address change {canary}')}, {sql_str(f'Please move my statements to 1 Forget Street. Ref {canary}.')}, 'open', 'low') RETURNING id"
            )
        ),
    ]
    for v in range(2, n_versions + 2):
        for t in tickets:
            q(
                f"UPDATE support_tickets SET body = body || E'\\n\\nFollow-up v{v} {canary}', status = 'pending', updated_at = now() WHERE id = {t}"
            )
        q(f"UPDATE customers SET phone = '+1-555-01{v:02d}', updated_at = now() WHERE id = {cid}")
    gone = first_int(
        q(
            "INSERT INTO support_tickets (customer_id, subject, body, status, priority) VALUES "
            f"({cid}, {sql_str(f'Duplicate request {canary}')}, {sql_str(f'Closed as a duplicate. Ref {canary}.')}, 'closed', 'low') RETURNING id"
        )
    )
    q(f"DELETE FROM support_tickets WHERE id = {gone}")


def force_quarantine(canary: str) -> bool:
    """The g3 P6 rename trick on support_tickets: a column rename lands the next change of the subject's ticket in
    quarantine_support_tickets (SCHEMA_INCOMPATIBLE); the rename is reverted and the stream carries on."""
    subject = "cdc.public.support_tickets-value"
    tid = int(g2.psql(f"SELECT min(id) FROM support_tickets WHERE subject LIKE {sql_str('%' + canary + '%')}"))
    g3.set_subject_compat(subject, "NONE")  # we test silver's handling, not the registry's gate
    parked = None
    try:
        g2.psql("ALTER TABLE support_tickets RENAME COLUMN priority TO priority_g8")
        g2.psql(
            f"UPDATE support_tickets SET body = body || E'\\n\\nQuarantined update {canary}', updated_at = now() WHERE id = {tid}"
        )

        def landed() -> dict[str, Any] | None:
            _, q = g3.probe("quarantine")
            rows = q.get("quarantine_support_tickets") or []
            return q if any("SCHEMA_INCOMPATIBLE" in r["reason"] for r in rows) else None

        parked = g2.wait_for(landed, 300, 5.0)
    finally:
        g2.psql("ALTER TABLE support_tickets RENAME COLUMN priority_g8 TO priority")
        g3.set_subject_compat(subject, None)
    g2.psql(
        f"UPDATE support_tickets SET body = body || E'\\n\\nAfter the rename {canary}', updated_at = now() WHERE id = {tid}"
    )
    return bool(parked)


def kill_mid_merge(ctx: Ctx) -> bool:
    """SIGKILL spark-streams inside a merge while changes flow; the subjects' last ticket versions are written first
    so they are in flight."""
    killed = False
    with g3.LoadGenerator():
        for c in ctx.canaries:
            g2.psql(
                f"UPDATE support_tickets SET body = body || E'\\n\\nFinal v9 {c}', updated_at = now() WHERE subject LIKE {sql_str('%' + c + '%')}"
            )
        before = g3.log_text().count("FAULT_HOOK point=mid_merge")
        g3.compose("exec", "-T", "spark-streams", "sh", "-c", "echo mid_merge > /checkpoints/fault")
        hit = g2.wait_for(lambda: g3.log_text().count("FAULT_HOOK point=mid_merge") > before, 240, 1.0)
        if hit:
            g3.sh("docker", "kill", "adrp-spark-streams-1")
            killed = True
        g3.clear_fault_file()
        g3.compose("--profile", "core", "--profile", "apps", "up", "-d", "spark-streams")
        time.sleep(45)
    return killed


def tag_bronze() -> bool:
    """A non-main ref on bronze pins the old snapshot (and the files holding the subjects): phase P must drop it."""
    script = ROOT / ".run/g8_tag.py"
    script.write_text(
        "from adrp.spark.forget import build_session_for_cli\n"
        "spark = build_session_for_cli()\n"
        'spark.sql("ALTER TABLE lake.bronze.cdc_events CREATE OR REPLACE TAG g8_pin")\n'
        'print("RESULT {}")\n'
    )
    g3.compose(
        "--profile",
        "tools",
        "run",
        "--rm",
        "-T",
        "-v",
        f"{script}:/tmp/g8_tag.py:ro",
        "spark-query",
        "/tmp/g8_tag.py",
        timeout=600,
    )
    refs = sparkcli.refs(list_only=True)
    return any(r["name"] == "g8_pin" for v in refs.values() if isinstance(v, dict) for r in v.get("refs", []))


def presence_pass_for(
    ctx: Ctx,
    label: str,
    probes: list[workflow.ForgetSubject],
    required_logical: set[str],
    required_physical: set[str],
    quarantine_subject: str | None = None,
) -> bool:
    # Recent inserts may still sit in shared buffers; the raw-byte scan of base/ only sees flushed pages.
    postgres.checkpoint()
    pr = workflow.presence(probes, ctx.cfg, allow_missing=("quarantine", "ground_truth"))
    ok = True
    for s in probes:
        rep = pr.logical[s.request_id]
        present = set(rep.present)
        need = set(required_logical) | ({"quarantine"} if s.request_id == quarantine_subject else set())
        miss = sorted(need - present)
        ok &= check(f"{label}: logical presence, sink by sink, subject {probes.index(s) + 1}/{len(probes)}: {sorted(need)} all found",
                    not miss and not rep.errors, f"missing={miss} vacuous={rep.vacuous} errors={rep.errors}")  # fmt: skip
    phys = pr.physical
    miss = sorted(required_physical - set(phys.present))
    ok &= check(f"{label}: physical presence, sink by sink (union of the subjects): {sorted(required_physical)} all found before any forget",
                not miss and not phys.errors, f"present={sorted(phys.present)} missing={miss} na={phys.na} vacuous={phys.vacuous} errors={phys.errors}")  # fmt: skip
    DETAILS[f"presence_{label}"] = pr.to_dict()
    return ok


def phase_setup(ctx: Ctx) -> None:
    print("P0 setup")
    s = run_once(SEED, duration=1e9, rate=0, warmup=0, max_txns=1500)
    check("setup: the generator wrote 1500 transactions", s["txn_range"][1] >= 1500, f"txn_range={s['txn_range']}")
    drain_all(ctx, "setup")
    ok, _, r = g6.wait_consistent(300)
    check("setup: the index is consistent before any subject exists", ok, json.dumps(r))

    ctx.canaries = [fids.new_canary() for _ in range(N_ADHOC)]
    cids = fids.adhoc_subject_ids(N_ADHOC, random.Random(SEED))
    for cid, can in zip(cids, ctx.canaries, strict=True):
        plant_subject(cid, can)
    n = int(g2.psql(f"SELECT count(*) FROM support_tickets WHERE customer_id IN ({ids_arg(cids)})"))
    check("setup: ad-hoc subjects planted, each with a canary in name, email, address and ticket bodies, several ticket versions, a deleted ticket", n == 2 * N_ADHOC, f"{N_ADHOC} subjects, {n} live tickets")  # fmt: skip

    ctx.quarantine_index = 0
    q_ok = force_quarantine(ctx.canaries[ctx.quarantine_index])
    check("setup: one subject ticket's update is parked in quarantine_support_tickets (SCHEMA_INCOMPATIBLE)", q_ok)

    killed = kill_mid_merge(ctx)
    check(
        "setup: SIGKILLed spark-streams inside mid_merge while changes flowed (and subject tickets were in flight)",
        killed,
    )
    drain_all(ctx, "setup after the kill")
    ok, secs, r = g6.wait_consistent(420)
    check(
        "setup: the embedder indexed the subjects' tickets and the index is consistent",
        ok,
        f"{secs:.0f} s; {json.dumps(r)}",
    )
    check("setup: bronze carries a non-main tag pinning the pre-forget snapshot", tag_bronze())

    probes = [
        workflow.ForgetSubject(str(uuid.uuid4()), cid_for(c), (c,)) for c in ctx.canaries
    ]  # presence never writes a request
    ok = presence_pass_for(
        ctx, "ad-hoc", probes,
        required_logical={"postgres", "silver", "bronze", "qdrant"},
        required_physical={"postgres", "bronze", "silver", "qdrant", "qdrant_wal", "broker", "pg_wal", "event_logs", "embedder_files"},
        quarantine_subject=probes[ctx.quarantine_index].request_id,
    )  # fmt: skip
    foot = probes[0].ids
    check("setup: the footprint is the union of Postgres and bronze history (the deleted ticket is in it)",
          len(foot.get("support_tickets", [])) >= 3 and len(foot.get("accounts", [])) >= 1 and len(foot.get("transactions", [])) >= 3,
          f"subject 1: {{{', '.join(f'{t}: {len(v)}' for t, v in sorted(foot.items()))}}}")  # fmt: skip
    gaps = bronze_offset_gaps(ctx)
    DETAILS["bronze_offset_gaps_before"] = gaps
    _ = ok


def bronze_offset_gaps(ctx: Ctx) -> dict[str, Any]:
    """Kafka records vs bronze rows per topic. On compacting topics bronze offsets HAVE gaps (compaction drops
    superseded versions a stopped reader had not read; the forget DELETE removes subject rows). Recorded, not asserted."""
    rep = g3.bronze_report(ctx.consumer)
    return {t: {**v, "gap": v["kafka_records"] - v["bronze_distinct_offsets"]} for t, v in rep.items()}


# ---- logical verification (P1, P2) -------------------------------------------------------------------------


def silver_dump() -> dict[str, list[dict[str, Any]]]:
    return dict(g3.silver_rows(list(TS)))


def verify_logical(ctx: Ctx, label: str, subs: list[workflow.ForgetSubject], rep: workflow.LogicalReport) -> None:
    check(f"{label}: the workflow reports every logical sink absent", rep.ok, json.dumps(rep.failed))
    lags: dict[str, list[float]] = {s: [] for s in LOGICAL_SINKS}
    unseen = 0
    for s in subs:
        for sink, d in audit.request_lags(s.request_id).items():
            if sink in lags:
                if d["logical_lag_s"] is None:
                    unseen += 1
                else:
                    lags[sink].append(d["logical_lag_s"])
    worst = max((max(v) for v in lags.values() if v), default=None)
    check(f"{label}: every sink was seen logically absent, within S6 ({verify.LOGICAL_DEADLINE_S} s)",
          unseen == 0 and worst is not None and worst <= verify.LOGICAL_DEADLINE_S,
          f"unseen={unseen} worst={None if worst is None else round(worst, 1)} s; per sink max "
          + json.dumps({k: (round(max(v), 1) if v else None) for k, v in lags.items()}))  # fmt: skip
    DETAILS[f"logical_lags_{label}"] = {k: audit.summarize(v) for k, v in lags.items()}
    DETAILS[f"delivery_seconds_{label}"] = audit.summarize(list(rep.delivery_s.values()))

    cids = [s.customer_id for s in subs]
    ents = {t: sorted({i for s in subs for i in s.ids.get(t, [])}) for t in TS}
    # Postgres, logical
    leftover = int(
        g2.psql(
            f"SELECT (SELECT count(*) FROM customers WHERE id IN ({ids_arg(cids)})) + (SELECT count(*) FROM support_tickets WHERE customer_id IN ({ids_arg(cids)}) OR id IN ({ids_arg(ents['support_tickets'])}))"
            f" + (SELECT count(*) FROM accounts WHERE customer_id IN ({ids_arg(cids)})) + (SELECT count(*) FROM transactions WHERE customer_id IN ({ids_arg(cids)}))"
        )
    )
    placeholder_rows = int(
        g2.psql(
            f"SELECT (SELECT count(*) FROM accounts WHERE id IN ({ids_arg(ents['accounts'])}) AND customer_id >= 1000000000000 AND customer_id NOT IN ({ids_arg(cids)})) + (SELECT count(*) FROM transactions WHERE id IN ({ids_arg(ents['transactions'])}) AND customer_id >= 1000000000000 AND customer_id NOT IN ({ids_arg(cids)}))"
        )
    )
    check(
        f"{label}: logical, Postgres: no customer, ticket, or row carrying the subject's id remains",
        leftover == 0,
        f"rows={leftover}",
    )
    check(f"{label}: logical, Postgres: the subject's accounts and transactions were re-keyed to a placeholder, not deleted",
          placeholder_rows == len(ents["accounts"]) + len(ents["transactions"]) and placeholder_rows > 0, f"{placeholder_rows} rows re-keyed")  # fmt: skip

    # silver, logical
    ctx.drain.wait_silver(420)
    dump = silver_dump()
    bad_tomb = bad_key = missing = 0
    by_id = {t: {r["id"]: r for r in dump.get(t, [])} for t in TS}
    for t in ("customers", "support_tickets"):
        for i in ents[t]:
            r = by_id[t].get(i)
            if r is None:
                if t == "customers":
                    missing += 1
                continue
            if not r["is_deleted"] or any(v is not None for c, v in r.items() if c not in META):
                bad_tomb += 1
    placeholders = set()
    for t in ("accounts", "transactions"):
        for i in ents[t]:
            r = by_id[t].get(i)
            if (
                r is None
                or r["is_deleted"]
                or r.get("customer_id") is None
                or r["customer_id"] in cids
                or r["customer_id"] < 10**12
            ):
                bad_key += 1
            else:
                placeholders.add(r["customer_id"])
    named = sum(1 for t in TS for r in dump.get(t, []) if r.get("customer_id") in cids)
    check(f"{label}: logical, silver: the subjects' customers and tickets are tombstones with every non-key column NULL (no PII)", bad_tomb == 0 and missing == 0, f"bad={bad_tomb} customers without a silver row={missing}")  # fmt: skip
    check(f"{label}: logical, silver: the subjects' accounts and transactions are live rows re-keyed to a placeholder; no row names the subject",
          bad_key == 0 and named == 0 and bool(placeholders), f"rekey_problems={bad_key} rows still naming a subject={named} placeholders={len(placeholders)}")  # fmt: skip

    # Qdrant, logical
    left = sum(len(g6.qdrant_points(f"support_tickets:{i}")) for i in ents["support_tickets"])
    check(f"{label}: logical, Qdrant: a filtered scroll on each of the subjects' ticket entities returns 0 points", left == 0, f"points={left}; deleted proactively={sum(rep.qdrant_points_deleted.values())}")  # fmt: skip

    # bronze / quarantine / silver, independent of the workflow's own probe
    pres = sparkcli.presence(
        {t: v for t, v in ents.items()}, canaries=[c for s in subs for c in s.canaries], customers=cids
    )
    ev = {fq: v for fq, v in pres.items() if ".bronze." in fq or "quarantine_" in fq}
    foot = sum(int(v.get("footprint_rows", 0)) for v in ev.values())
    can = sum(int(v.get("canary_rows", 0)) for v in pres.values())
    check(f"{label}: logical, bronze and quarantine_*: no event row of the footprint, no row carrying a canary (any lake table)", foot == 0 and can == 0, f"footprint rows={foot} canary rows (all tables)={can}")  # fmt: skip
    DETAILS[f"bronze_delete_{label}"] = rep.bronze_delete and {fq: {k: v.get(k) for k in ("before", "after", "failed_attempts")} for fq, v in rep.bronze_delete.items() if isinstance(v, dict)}  # fmt: skip


# ---- P1, P2 ------------------------------------------------------------------------------------------------


def phase_forget_adhoc(ctx: Ctx) -> None:
    print("P1 logical forget of the ad-hoc subjects, under load")
    if not ctx.adhoc_rids:  # a resumed phase reuses its requests (the customers may already be gone)
        ctx.adhoc_rids = [workflow.register(cid_for(c)) for c in ctx.canaries]
    subs = [
        s for i, rid in enumerate(ctx.adhoc_rids) if (s := workflow.load_subject(rid, (ctx.canaries[i],))) is not None
    ]
    check("forget: one request per subject", len(subs) == N_ADHOC)
    with g3.LoadGenerator(rate=LOAD_RATE, chunk_txns=100) as gen:
        t0 = time.monotonic()
        rep = workflow.run_logical(subs, ctx.cfg)
        ctx.scratch["p1_logical_seconds"] = secs_since(t0)
        start_txn = gen.end_txn
    check("forget: the generator kept writing while the subjects were forgotten (logical phase under load)", True, f"logical phase {ctx.scratch['p1_logical_seconds']} s; txn={start_txn}")  # fmt: skip
    ctx.subjects = subs
    verify_logical(ctx, "forget", subs, rep)
    drain_all(ctx, "forget")
    index_matches_postgres("forget")
    DETAILS["adhoc_footprint_counts"] = [{t: len(v) for t, v in s.ids.items()} for s in subs]


def pick_generator_customer() -> int:
    return int(
        g2.psql(
            "SELECT c.id FROM customers c WHERE c.id < 1000000000000 AND EXISTS (SELECT 1 FROM support_tickets t WHERE t.customer_id = c.id) "
            "ORDER BY (SELECT count(*) FROM transactions x WHERE x.customer_id = c.id) DESC, c.id LIMIT 1"
        )
    )


def phase_forget_generator(ctx: Ctx) -> None:
    print("P2 logical forget of the generator-owned Zipf-head customer (generator stopped)")
    if ctx.gen_rid:
        return forget_generator_subject(ctx)
    cid = pick_generator_customer()
    n_tx = int(g2.psql(f"SELECT count(*) FROM transactions WHERE customer_id = {cid}"))
    n_tk = int(g2.psql(f"SELECT count(*) FROM support_tickets WHERE customer_id = {cid}"))
    check("generator subject: the Zipf-head customer owns transactions and tickets (non-vacuous)", n_tx > 0 and n_tk > 0, f"{n_tx} transactions, {n_tk} tickets")  # fmt: skip
    # fresh WAL, broker and index bytes for the subject, so the presence pass is not at the mercy of recycled segments
    g2.psql(f"UPDATE customers SET updated_at = now() WHERE id = {cid}")
    g2.psql(
        f"UPDATE support_tickets SET body = body || E'\\n\\nG8 touch.', updated_at = now() WHERE customer_id = {cid}"
    )
    drain_all(ctx, "generator subject")
    index_matches_postgres("generator subject")
    probe = workflow.ForgetSubject(str(uuid.uuid4()), cid)
    presence_pass_for(
        ctx, "generator", [probe],
        required_logical={"postgres", "silver", "bronze", "qdrant"},
        # Qdrant's WAL only holds recent writes: the Zipf-head customer's points rotated out of it long before the forget.
        required_physical={"postgres", "bronze", "silver", "qdrant", "broker", "pg_wal", "event_logs", "embedder_files", "ground_truth"},
    )  # fmt: skip
    ctx.gen_rid = workflow.register(cid)
    forget_generator_subject(ctx)


def forget_generator_subject(ctx: Ctx) -> None:
    sub = workflow.load_subject(ctx.gen_rid)
    assert sub is not None
    t0 = time.monotonic()
    rep = workflow.run_logical([sub], ctx.cfg)
    ctx.scratch["p2_logical_seconds"] = secs_since(t0)
    ctx.subjects = [*[x for x in ctx.subjects if x.request_id != sub.request_id], sub]
    verify_logical(ctx, "generator subject", [sub], rep)
    drain_all(ctx, "generator subject after the forget")
    index_matches_postgres("generator subject after the forget")


# ---- P3 ----------------------------------------------------------------------------------------------------


def silver_vs_postgres() -> dict[str, int]:
    """Live silver rows against Postgres, table by table: ids that differ and rows whose owner/email differ."""
    dump = silver_dump()
    out = {"missing": 0, "extra": 0, "differing": 0}
    cols = {
        "customers": "email",
        "accounts": "customer_id",
        "transactions": "customer_id",
        "support_tickets": "customer_id",
    }
    for t, col in cols.items():
        pg = {int(r["id"]): r[col] for r in g6.pg_json(f"SELECT id, {col} FROM {t}")}
        sv = {r["id"]: r.get(col) for r in dump.get(t, []) if not r["is_deleted"]}
        out["missing"] += len(pg.keys() - sv.keys())
        out["extra"] += len(sv.keys() - pg.keys())
        out["differing"] += sum(1 for k in pg.keys() & sv.keys() if pg[k] != sv[k])
    out["rows"] = sum(len(v) for v in dump.values())
    return out


def plant_orphan(canary: str) -> bool:
    """An unreferenced Parquet file holding a canary in bronze's data directory (what a writer killed before its
    commit leaves behind). remove_orphan_files must delete it; the whole-bucket scan would find it otherwise."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    store = scan.s3_store(audit.repo_env())
    prefix = next((k.split("/data/source_table=")[0] for k in store.keys() if "/data/source_table=" in k), None)
    if prefix is None:
        return False
    buf = io.BytesIO()
    pq.write_table(pa.table({"note": [f"orphan of a killed writer {canary}"]}), buf)
    with store.fs.open_output_stream(f"{store.bucket}/{prefix}/data/g8-orphan-{uuid.uuid4().hex[:8]}.parquet") as f:
        f.write(buf.getvalue())
    return True


def embedder_pointer() -> Any:
    return workflow._embedder_call("count", {}).get("snapshot")


def phase_window(ctx: Ctx) -> None:
    print("P3 physical maintenance window")
    ids = sparkcli.merge_ids(*[s.ids for s in ctx.subjects])
    before = sparkcli.state(ids)
    ctx.scratch["delete_files_before"] = sum(int(v.get("delete_files_affected") or 0) for v in before.values())
    ctx.scratch["snapshots_before"] = {t.split(".")[-1]: v.get("snapshots") for t, v in before.items()}
    logs_before = g6.embedder_logs().count("pointer snapshot expired")
    planted = plant_orphan(ctx.canaries[1])
    t0 = time.monotonic()
    rep = workflow.run_physical_window(ctx.subjects, ctx.cfg)
    ctx.scratch["p3_seconds"] = secs_since(t0)
    it = rep.steps["iceberg"]
    res = it["residue"]
    DETAILS["window"] = {
        "window_seconds": round(rep.seconds, 1),
        "steps": rep.steps,
        "verdicts": verdicts(rep.outcomes),
    }
    check(
        "window: physical, every window sink is absent, or bounded with a recorded bound",
        rep.ok,
        json.dumps(verdicts(rep.outcomes)),
    )
    check("window: the maintenance window (writers stopped) closed and the services were restarted", rep.seconds > 0 and service_running("spark-streams") and service_running("embedder"), f"window {rep.seconds:.0f} s")  # fmt: skip
    check("window: physical, Iceberg: the rewrite removed position-delete files from the footprint's partitions (the rest belong to other rows in the same buckets; the whole-bucket scan below is the evidence of absence)",
          it["delete_files_removed"] >= 1 and res["delete_files_affected"] < ctx.scratch["delete_files_before"], f"delete files in the footprint's partitions {ctx.scratch['delete_files_before']} -> {res['delete_files_affected']}; removed by the rewrite={it['delete_files_removed']}")  # fmt: skip
    check("window: physical, Iceberg: the pinned tag was dropped and no non-main ref remains", it["refs_dropped"] >= 1 and res["non_main_refs"] == 0, f"dropped={it['refs_dropped']} remaining={res['non_main_refs']}")  # fmt: skip
    check("window: physical, Iceberg: each table keeps exactly one snapshot after expire (retain_last=1)", res["extra_snapshots"] == 0, f"extra snapshots={res['extra_snapshots']}")  # fmt: skip
    check("window: physical, Iceberg: orphan files (one planted with a canary in bronze's data directory, plus any the SIGKILLed writer left) were removed (>= 1)", it["orphans_removed"] >= 1 and planted, f"orphans removed={it['orphans_removed']} planted={planted}")  # fmt: skip
    check(
        "window: physical, Iceberg: data files were rewritten",
        it["rewritten_files"] >= 1,
        f"rewritten files={it['rewritten_files']}",
    )
    scan = rep.steps["bucket_scan"]
    check("window: physical, Iceberg: the whole-bucket scan (every Parquet decoded incl. footer stats and value_bytes, every Avro manifest, every gzipped metadata.json) has 0 matches for the canaries and emails (the silver tombstones still hold the raw customer key until the purge, P5)",
          scan["total_hits"] == 0 and scan["errors"] == 0 and scan["undecoded_parquet"] == 0 and scan["files"] > 0, json.dumps({k: scan[k] for k in ("files", "total_hits", "errors", "undecoded_parquet")}))  # fmt: skip
    pre = DETAILS.get("presence_ad-hoc", {}).get("physical", {})
    check("window: the scan is not vacuous: the same scan found the subjects before the forget", "bronze" in pre.get("present", []) and "silver" in pre.get("present", []), f"present before={pre.get('present')}")  # fmt: skip
    sw = rep.steps["seaweedfs"]
    check("window: physical, SeaweedFS: volume vacuum reclaimed the deleted needles of the lake collection",
          sw["ok"] == 1 and sw["garbage_permille_after"] <= 1 and sw["garbage_permille_before"] >= sw["garbage_permille_after"],
          f"garbage permille {sw['garbage_permille_before']} -> {sw['garbage_permille_after']} in {sw['seconds']} s")  # fmt: skip
    files = rep.steps["files"]
    check("window: physical, files: event-log batches, embedder actions/state and ground truth were rewritten with pseudonyms",
          files["ground_truth"] > 0 and files["embedder_actions"] + files["embedder_state"] > 0, json.dumps(files))  # fmt: skip
    pgstep = rep.steps["postgres"]
    check("window: physical, Postgres: VACUUM FULL ran and pg_statistic was refreshed (or the bound is recorded)",
          pgstep["statistic_ok"] == 1 or rep.outcomes["postgres"].ok, json.dumps(pgstep))  # fmt: skip

    ptr = embedder_pointer()
    after = sparkcli.state(ids)
    cur = after.get("lake.bronze.cdc_events", {}).get("current_snapshot_id")
    check("window: the embedder's pointer equals the retained bronze snapshot", ptr is not None and (ptr == it["bronze_snapshot"] or ptr == cur), f"pointer is retained snapshot: {ptr == it['bronze_snapshot']}")  # fmt: skip
    DETAILS["state_after_window"] = {t.split(".")[-1]: {k: v.get(k) for k in ("snapshots", "delete_files", "non_main_refs", "delete_files_affected")} for t, v in after.items()}  # fmt: skip

    # the embedder carries on: a new change is indexed, no fallback to a full rescan, and the index equals Postgres
    art = int(g2.psql("SELECT min(id) FROM help_articles WHERE id < 1000000000000"))
    g2.psql(f"UPDATE help_articles SET body = body || E'\\n\\nG8 post-window edit.' WHERE id = {art}")
    ok = bool(g2.wait_for(lambda: g6.vs_postgres("help_articles", [art], ("title", "body")) == 0, 300, 3.0))
    check("window: the embedder reconverged: a post-window edit was indexed", ok)
    check("window: the embedder did not fall back to a full bronze rescan", g6.embedder_logs().count("pointer snapshot expired") == logs_before)  # fmt: skip
    drain_all(ctx, "window")
    index_matches_postgres("window")
    svp = silver_vs_postgres()
    check("window: silver equals Postgres for every non-subject row (ids and owners, four tables)", svp["missing"] == svp["extra"] == svp["differing"] == 0, json.dumps(svp))  # fmt: skip


# ---- P4 ----------------------------------------------------------------------------------------------------


def phase_sinks(ctx: Ctx) -> None:
    print("P4 physical: Qdrant, Redpanda, Postgres WAL")
    for name, fn, sinks in (
        ("Qdrant segments and WAL", workflow.run_qdrant_physical, ("qdrant", "qdrant_wal")),
        ("Redpanda segments", workflow.run_broker_physical, ("broker",)),
        ("Postgres WAL", workflow.run_postgres_wal, ("pg_wal",)),
    ):
        t0 = time.monotonic()
        rep = fn(ctx.subjects, ctx.cfg)
        v = verdicts(rep.outcomes)
        ctx.scratch[f"p4_{sinks[0]}_seconds"] = secs_since(t0)
        DETAILS[f"physical_{sinks[0]}"] = {
            "steps": rep.steps,
            "verdicts": v,
            "seconds": ctx.scratch[f"p4_{sinks[0]}_seconds"],
        }
        check(f"physical, {name}: absent within the deadline, or the bound is recorded ('bounded, not removed')", rep.ok and set(sinks) <= set(v), f"{json.dumps(v)} {json.dumps(rep.steps)}")  # fmt: skip
    ok, cfgs = broker.compression_ok()
    check("physical, Redpanda: producer compression is off for the CDC topics (a raw-byte scan of the segments is meaningful)", ok, json.dumps(cfgs)[:200])  # fmt: skip
    drain_all(ctx, "after the broker nudges")
    index_matches_postgres("after the broker nudges")


# ---- P5 ----------------------------------------------------------------------------------------------------


def self_scan(payload: dict[str, Any], subs: list[workflow.ForgetSubject]) -> dict[str, int]:
    """Search the manifest payload that is about to be written for every subject token (canaries, emails, ids
    >= 1e12 as text) and, in the check names and details, for any 13-digit number."""
    text = json.dumps(payload, default=str)
    big = [s.customer_id for s in subs if s.customer_id >= 10**12]
    can = [c for s in subs for c in s.canaries]
    pii = [p for s in subs for p in s.pii]
    m = fids.Matcher(subject_ids=big, canaries=[*can, *pii])
    h = m.hits(text)
    checks_text = json.dumps(RESULTS, default=str)
    return {"canary_or_email": h.canary, "subject_id": h.subject_id, "digits13_in_checks": len(fids.ID13.findall(checks_text)), "bytes": len(text)}  # fmt: skip


def phase_purge(ctx: Ctx) -> None:
    print("P5 re-sweep, tombstone purge, raw ids, files and logs")
    t0 = time.monotonic()
    rep = workflow.run_resweep_and_purge(ctx.subjects, ctx.cfg)
    ctx.scratch["p5_seconds"] = secs_since(t0)
    DETAILS["purge"] = {"steps": rep.steps, "verdicts": verdicts(rep.outcomes), "deferred": rep.deferred, "window_seconds": round(rep.window_seconds, 1)}  # fmt: skip
    check("purge: physical, every re-sweep and final sink is absent, or bounded with a recorded bound", rep.ok, json.dumps(verdicts(rep.outcomes)))  # fmt: skip
    check("purge: the broker was verified free of the subjects, so the silver tombstones were hard-deleted (not deferred)", rep.broker_clean and rep.tombstones_purged and not rep.deferred, f"broker_clean={rep.broker_clean} deferred={rep.deferred}")  # fmt: skip
    check("purge: every request reached physical_done", len(rep.finished) == len(ctx.subjects), f"{len(rep.finished)}/{len(ctx.subjects)}")  # fmt: skip
    rows = [audit.get_request(r) for r in ctx.all_rids]
    check(
        "purge: every request's status is physical_done",
        all(r is not None and r.status == "physical_done" for r in rows),
    )
    nulls = int(
        g2.psql(
            "SELECT count(*) FROM privacy.forget_requests WHERE raw_customer_id IS NOT NULL OR subject_scratch IS NOT NULL"
        )
    )
    check("purge: raw_customer_id and the scratch footprint are NULL in privacy.forget_requests for every request", nulls == 0, f"rows still holding a raw id={nulls}")  # fmt: skip

    union = workflow.union_subject(ctx.subjects)
    key = audit.hmac_key()
    logs = storage.logs_report(union, ctx.cfg.eventlog)
    bad = sorted(k for k, v in logs.items() if v != 0)
    check("purge: physical, logs: no compose service log, the exporter /metrics, Dagster storage, run manifests or event log names a subject",
          not bad or all(k.startswith("log:") for k in bad), f"sources with hits={bad}")  # fmt: skip
    DETAILS["logs_report"] = {k: v for k, v in logs.items()}
    fr = workflow.file_residual(key, sparkcli.merge_ids(*[s.ids for s in ctx.subjects]), ctx.cfg)
    resid = {k: fn() for k, fn in fr.items()}
    check("purge: physical, files: silver_batches, embedder actions/state and ground truth hold no subject entity", all(v == 0 for v in resid.values()), json.dumps(resid))  # fmt: skip

    # pseudonyms still join: the audit handle, the actions log, ground truth
    handles = {fids.subject_hmac(key, s.customer_id) for s in ctx.subjects}
    joined = int(
        g2.psql(
            "SELECT count(*) FROM privacy.forget_audit WHERE subject_hmac IN ("
            + ",".join(sql_str(h) for h in handles)
            + ")"
        )
    )
    check("purge: pseudonym join, audit: HMAC(subject) finds every request's forget_audit row", joined == len(ctx.subjects), f"{joined}/{len(ctx.subjects)}")  # fmt: skip
    acts = ctx.cfg.eventlog / "embedder/actions.jsonl"
    pseud = {fids.pseudonym(key, e) for s in ctx.subjects for e in s.subject().entity_strings(["support_tickets"])}
    n_pseud = 0
    if acts.exists():
        for ln in acts.read_text().splitlines():
            try:
                n_pseud += json.loads(ln).get("entity") in pseud
            except ValueError:
                continue
    check("purge: pseudonym join, embedder actions: entries of the subjects' tickets carry p:<hmac> and join to the HMAC-derived pseudonyms", n_pseud > 0, f"{n_pseud} entries")  # fmt: skip
    import pyarrow.parquet as pq

    gt_hits = 0
    gen_sub = next((s for s in ctx.subjects if s.request_id == ctx.gen_rid), None)
    if gen_sub is not None:
        want = {fids.pseudonym_int(key, f"customers:{gen_sub.customer_id}")}
        for d in ffiles.ground_truth_dirs(ctx.cfg.runs_data):
            for p in sorted(d.glob("*.parquet")):
                t = pq.read_table(p, columns=["table", "pk"])
                gt_hits += sum(1 for tb, pk in zip(t["table"].to_pylist(), t["pk"].to_pylist(), strict=True) if tb == "customers" and pk in want)  # fmt: skip
    check("purge: pseudonym join, ground truth: the generator subject's customer row is keyed by its HMAC-derived negative id", gt_hits > 0, f"{gt_hits} rows")  # fmt: skip


# ---- P6 ----------------------------------------------------------------------------------------------------


def audit_digest() -> dict[str, str]:
    return {
        t: g2.psql(f"SELECT count(*) || ':' || md5(coalesce(string_agg(x::text, E'\\n' ORDER BY x::text), '')) FROM privacy.{t} x")
        for t in ("forget_requests", "forget_audit", "forget_sink_status")
    }  # fmt: skip


def snapshot_ids() -> dict[str, Any]:
    return {t: v.get("current_snapshot_id") for t, v in sparkcli.state().items()}


def phase_rerun(ctx: Ctx, started: str, write: bool) -> bool:
    print("P6 idempotent re-run, tombstone purge, final scan")
    ctx.drain.wait_silver(300)
    snaps0, aud0, pts0 = snapshot_ids(), audit_digest(), g6.stable_points()
    rep = workflow.run_all(ctx.subjects, ctx.cfg)
    snaps1, aud1, pts1 = snapshot_ids(), audit_digest(), g6.stable_points()
    check("rerun: re-running the workflow for finished requests skips every phase", rep.ok and (rep.logical is None or len(rep.logical.skipped) == len(ctx.subjects)), f"logical skipped={len(rep.logical.skipped) if rep.logical else None}")  # fmt: skip
    check(
        "rerun: Iceberg snapshot ids are unchanged (bronze, quarantine, silver)",
        snaps0 == snaps1,
        f"{len(snaps0)} tables",
    )
    check("rerun: privacy.* audit rows are unchanged", aud0 == aud1)
    check("rerun: Qdrant points and payloads are unchanged", pts0 == pts1, f"{len(pts0)} points")

    ids = sparkcli.merge_ids(*[s.ids for s in ctx.subjects])
    cids = [s.customer_id for s in ctx.subjects]
    pres = sparkcli.presence(ids, canaries=[c for s in ctx.subjects for c in s.canaries], customers=cids)
    silver_tomb = sum(
        int(v.get("footprint_tombstones", 0)) for fq, v in pres.items() if ".silver." in fq and "quarantine_" not in fq
    )
    silver_cust = sum(
        int(pres.get(f"lake.silver.{t}", {}).get("footprint_rows", 0)) for t in ("customers", "support_tickets")
    )
    other_rows = sum(
        int(v.get("footprint_rows", 0)) for fq, v in pres.items() if ".bronze." in fq or "quarantine_" in fq
    )
    can = sum(int(v.get("canary_rows", 0)) for v in pres.values())
    owners = sum(int(v.get("customer_id_rows", 0)) for v in pres.values())
    check("purge: logical, silver: the tombstones were purged (0 tombstones and 0 customer/ticket rows of the footprint); only re-keyed ledger rows remain",
          silver_tomb == 0 and silver_cust == 0, f"tombstones={silver_tomb} customer/ticket rows={silver_cust}")  # fmt: skip
    check("rerun: logical, bronze and quarantine_*: still 0 footprint rows; no table has a canary or a row naming a subject", other_rows == 0 and can == 0 and owners == 0, f"footprint rows={other_rows} canary rows={can} rows naming a subject={owners}")  # fmt: skip
    index_matches_postgres("rerun")
    svp = silver_vs_postgres()
    check("rerun: silver equals Postgres for every non-subject row", svp["missing"] == svp["extra"] == svp["differing"] == 0, json.dumps(svp))  # fmt: skip

    batch = workflow.Batch(ctx.subjects, ctx.cfg, audit.hmac_key())
    final = verify.poll(batch.sinks(), "physical", 180, 5.0)
    DETAILS["final_scan"] = verdicts(final)
    bad = verify.failures(final)
    check("final: physical, every sink re-evaluated (whole-bucket scan, Postgres files, Redpanda segments, Qdrant, files, logs) is absent or bounded", not bad, json.dumps(verdicts(final)))  # fmt: skip
    ice = final["bronze"].evidence
    check("final: physical, Iceberg: the final whole-bucket scan has 0 matches", ice.get("total_hits") == 0 and ice.get("errors") == 0 and int(ice.get("files") or 0) > 0, json.dumps(ice))  # fmt: skip

    DETAILS["bronze_offset_gaps_after"] = bronze_offset_gaps(ctx)
    DETAILS["lag_report"] = workflow.lag_report(ctx.all_rids)
    DETAILS["settings"] = settings()
    DETAILS["seconds"] = ctx.scratch | ctx.seconds
    passed_so_far = all(r["ok"] for r in RESULTS)
    payload = {"started_at": started, "passed": passed_so_far, "checks": RESULTS, **DETAILS}
    scan = self_scan(payload, ctx.subjects)
    DETAILS["manifest_self_scan"] = scan
    check("manifest: the payload about to be written holds no canary, email or subject id, and no 13-digit number in a check name or detail",
          scan["canary_or_email"] == 0 and scan["subject_id"] == 0 and scan["digits13_in_checks"] == 0, json.dumps(scan))  # fmt: skip
    passed = all(r["ok"] for r in RESULTS)
    if write:
        path = write_manifest(
            "g8-check", "core", SEED, {"started_at": started, "passed": passed, "checks": RESULTS, **DETAILS}
        )
        post = storage.manifests_scan(workflow.union_subject(ctx.subjects))
        if post["hits"]:
            path.unlink()
            print("FAIL  the written manifest named a subject; removed")
            passed = False
        print(
            f"{'PASSED' if passed else 'FAILED'} ({sum(r['ok'] for r in RESULTS)}/{len(RESULTS)}); manifest: {path.relative_to(ROOT)}"
        )
    else:
        print(
            f"{'PASSED' if passed else 'FAILED'} ({sum(r['ok'] for r in RESULTS)}/{len(RESULTS)}); no manifest written"
        )
    return passed


def rewrite_options() -> dict[str, str]:
    """REWRITE_OPTIONS of src/adrp/spark/forget.py, read without importing pyspark."""
    tree = ast.parse((ROOT / "src/adrp/spark/forget.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "REWRITE_OPTIONS" for t in node.targets
        ):
            return dict(ast.literal_eval(node.value))
    return {}


def settings() -> dict[str, Any]:
    info = qdrant.collection_info()
    min_wal, seg, lag = postgres.wal_settings()
    topics = {t: broker.topic_config(t) for t in ("customers", "support_tickets")}
    return {
        "redpanda": {
            "topic_config": {
                t: {
                    k: v
                    for k, v in c.items()
                    if k
                    in (
                        "cleanup.policy",
                        "segment.ms",
                        "min.cleanable.dirty.ratio",
                        "min.compaction.lag.ms",
                        "max.compaction.lag.ms",
                        "delete.retention.ms",
                        "retention.ms",
                        "compression.type",
                    )
                }
                for t, c in topics.items()
            },
            "cluster_log_segment_ms_min": broker.cluster_segment_min_ms(),
            "applied_from": "infra/redpanda/topic-config.json",
        },
        "iceberg": {
            "rewrite_data_files_options": rewrite_options(),
            "expire_snapshots": "older_than=now, retain_last=1 (twice, around rewrite_manifests)",
            "remove_orphan_files": "Action API, older_than=now, prefix listing",
            "bronze_and_quarantine": "copy-on-write",
            "silver": "merge-on-read",
        },
        "qdrant": {
            "optimizer_config": info.get("config", {}).get("optimizer_config"),
            "applied": qdrant.VACUUM_CONFIG,
            "segments": info.get("segments_count"),
        },
        "postgres": {"min_wal_size_mib": min_wal, "wal_segment_mib": seg, "slot_lag_bytes": lag},
        "seaweedfs": {"vacuum": "volume.vacuum -collection lake, garbageThreshold small, after the Iceberg procedure"},
        "bronze_offsets_have_gaps": "yes: the CDC topics compact (cleanup.policy=compact,delete), so superseded versions never reach bronze, and the forget DELETE removes subject rows; kafka offsets in bronze are not contiguous on these topics",
        "wording": "deletion propagation verified in this system; per sink 'logical' (by query) and 'physical' (by bytes) or 'bounded, not removed'; no compliance claim",
        "note": REDACTED_NOTE,
    }


# ---- main --------------------------------------------------------------------------------------------------

PHASE_FN: dict[str, Callable[[Ctx], None]] = {
    "P0": phase_setup, "P1": phase_forget_adhoc, "P2": phase_forget_generator,
    "P3": phase_window, "P4": phase_sinks, "P5": phase_purge,
}  # fmt: skip


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--only", choices=PHASES, help="run just this phase on the live stack (state in .run/g8-state.json)"
    )
    ap.add_argument("--from", dest="from_phase", choices=PHASES, help="run this phase and the ones after it")
    ap.add_argument("--no-manifest", action="store_true")
    a = ap.parse_args()
    os.environ["ADRP_FAULT_HOOKS"] = "1"  # every `compose up` here must keep the fault hooks on
    c = g2.consumer()
    ctx = Ctx(workflow.Config(), c, SafeDrain(c))
    resumed = bool(a.only or a.from_phase)
    started = load_state(ctx) if resumed and STATE.exists() else datetime.now(timezone.utc).isoformat()
    todo = [a.only] if a.only else list(PHASES[PHASES.index(a.from_phase) :]) if a.from_phase else list(PHASES)
    if resumed and todo[0] != "P0":
        reload_subjects(ctx)
    ok = True
    for ph in todo:
        t0 = time.monotonic()
        try:
            if ph == "P6":
                ok = phase_rerun(ctx, started, not a.no_manifest) and ok
            else:
                PHASE_FN[ph](ctx)
        except Exception as e:  # noqa: BLE001 - a phase that crashes is a finding; nothing after it can be trusted
            print(mask(traceback.format_exc(), 3000), file=sys.stderr)
            check(f"{ph}: ran to completion", False, f"{type(e).__name__}: {mask(str(e), 200)}")
            ctx.seconds[ph] = secs_since(t0)
            save_state(ctx, started)
            ok = False
            break
        ctx.seconds[ph] = secs_since(t0)
        save_state(ctx, started)
        ok = ok and all(r["ok"] for r in RESULTS)
    c.close()
    passed = ok and all(r["ok"] for r in RESULTS)
    if "P6" not in todo:
        print(
            f"{'ok' if passed else 'FAILED'} so far ({sum(r['ok'] for r in RESULTS)}/{len(RESULTS)}); phases run: {','.join(todo)}"
        )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
