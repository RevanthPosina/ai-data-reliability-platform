"""Audit and request bookkeeping for the forget workflow (G8): a thin layer over adrp.forget.postgres privacy.*.

What lives where (ADR-0017): privacy.forget_requests holds the only copy of the raw id (NULLed on completion);
privacy.forget_audit holds the HMAC of the subject, the xid and begin LSN of the forget transaction and per-table
counts; privacy.forget_sink_status holds, per (request, sink), when the subject was first seen logically and
physically absent, the bound when removal is bounded rather than verified, and evidence (counts and sizes only).

Everything returned from this module is safe to print: request ids (uuids), statuses, times, counts. The raw id
only comes out of raw_customer_id(), which is in postgres.py and is for in-process use.

Times are Postgres epoch seconds (`extract(epoch from ...)`), so lag = absent_at - requested_at uses one clock.
"""

from __future__ import annotations

import json
import os
import re
import statistics
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from adrp.forget import ids as fids
from adrp.forget import postgres
from adrp.forget.rawscan import ROOT

STATUS_ORDER = {"requested": 0, "logical_done": 1, "physical_done": 2, "failed": -1}
# allowed transitions of privacy.forget_requests.status. `failed` keeps the error and can be retried from either
# phase: back to `requested` (the forget transaction is skipped on resume if an audit row exists) or on to
# `logical_done` when the logical phase had in fact finished.
TRANSITIONS: dict[str, frozenset[str]] = {
    "requested": frozenset({"logical_done", "failed"}),
    "logical_done": frozenset({"physical_done", "failed"}),
    "physical_done": frozenset(),
    "failed": frozenset({"requested", "logical_done"}),
}
_DIGITS = re.compile(r"\d{4,}")


class TransitionError(ValueError):
    pass


def can_transition(old: str, new: str) -> bool:
    return old == new or new in TRANSITIONS.get(old, frozenset())


# ---- environment -----------------------------------------------------------------------------------------


def repo_env() -> dict[str, str]:
    """versions.env < .env.example < .env < os.environ, as scripts/_env.all_env() does (src cannot import scripts)."""
    out: dict[str, str] = {}
    for name in ("versions.env", ".env.example", ".env"):
        p = ROOT / name
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip()
    out.update({k: v for k, v in os.environ.items() if k in out or k.startswith(("FORGET_", "S3_", "AWS_"))})
    return out


def hmac_key(env: Mapping[str, str] | None = None) -> str:
    key = (env if env is not None else repo_env()).get("FORGET_HMAC_KEY", "")
    if not key:
        raise RuntimeError("FORGET_HMAC_KEY is not set (.env / .env.example)")
    return key


# ---- evidence guard --------------------------------------------------------------------------------------


def evidence_problem(obj: Any, path: str = "evidence") -> str | None:
    """Why `obj` must not be stored as evidence, or None. Evidence is counts, sizes, booleans and short labels:
    no string may hold a 13-digit id, a canary token or a long digit run (an id, a pk), and numbers must be plain."""
    if obj is None or isinstance(obj, (bool, int, float)):
        return None
    if isinstance(obj, str):
        if len(obj) > 2000:
            return f"{path}: string too long"
        if fids.CANARY_PREFIX in obj or fids.ID13.search(obj) or _DIGITS.search(obj.replace("0x", "")):
            # four digits is already more than any count or size we report as text; counts are ints
            return f"{path}: string contains an id-like digit run or a canary"
        return None
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            p = evidence_problem(str(k), f"{path} key")
            if p:
                return p
            p = evidence_problem(v, f"{path}.{k}")
            if p:
                return p
        return None
    if isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            p = evidence_problem(v, f"{path}[{i}]")
            if p:
                return p
        return None
    return f"{path}: unsupported type {type(obj).__name__}"


def safe_evidence(evidence: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if evidence is None:
        return None
    p = evidence_problem(evidence)
    if p:
        raise ValueError("refusing to store evidence: " + _DIGITS.sub("#", p))
    return json.loads(json.dumps(evidence))


def safe_bound(bound: str | None) -> str | None:
    """A bound statement is prose: non-empty when given, and without a 13-digit id or a canary (it may carry
    ordinary numbers such as sizes and timings)."""
    if bound is None:
        return None
    if not bound.strip():
        raise ValueError("a bound must not be empty")
    if fids.CANARY_PREFIX in bound or fids.ID13.search(bound):
        raise ValueError("refusing to store a bound that contains an id-like number")
    return bound


# ---- rows ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RequestRow:
    request_id: str
    status: str
    requested_at: float
    logical_done_at: float | None
    physical_done_at: float | None
    error: str | None
    raw_present: bool  # the raw id is still stored (False once null_raw_customer_id ran)


def _f(text: str) -> float | None:
    return float(text) if text else None


def parse_request_rows(out: str) -> list[RequestRow]:
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        rid, status, req, ldone, pdone, raw_present, err = line.split("|", 6)
        rows.append(RequestRow(rid, status, float(req), _f(ldone), _f(pdone), err or None, raw_present == "true"))
    return rows


_REQ_SELECT = (
    "SELECT request_id, status, extract(epoch from requested_at)::text, "
    "coalesce(extract(epoch from logical_done_at)::text, ''), "
    "coalesce(extract(epoch from physical_done_at)::text, ''), "
    "(raw_customer_id IS NOT NULL)::text, "
    "coalesce(replace(replace(error, E'\\n', ' '), '|', '/'), '') FROM privacy.forget_requests"
)


def get_request(request_id: str) -> RequestRow | None:
    rows = parse_request_rows(postgres.psql(f"{_REQ_SELECT} WHERE request_id = '{postgres._uuid(request_id)}';"))
    return rows[0] if rows else None


def list_requests(statuses: Iterable[str] | None = None) -> list[RequestRow]:
    where = ""
    if statuses is not None:
        where = " WHERE status IN (" + ", ".join(postgres.lit(s) for s in statuses) + ")"
    return parse_request_rows(postgres.psql(f"{_REQ_SELECT}{where} ORDER BY requested_at, request_id;"))


def db_now() -> float:
    """Postgres clock in epoch seconds: the one clock for requested_at, absent_at and verified-at."""
    return float(postgres.psql("SELECT extract(epoch from clock_timestamp());"))


# ---- request lifecycle -----------------------------------------------------------------------------------


def new_request(customer_id: int) -> str:
    """Create a request (status 'requested'). The raw id is stored in privacy.forget_requests only."""
    return postgres.create_request(customer_id)


def move(request_id: str, new_status: str, error: str | None = None) -> RequestRow:
    """Checked status change. Re-asserting the current status is a no-op, so a re-run of a finished request does
    nothing. `error` is scrubbed of digit runs."""
    row = get_request(request_id)
    if row is None:
        raise KeyError("unknown request")
    if row.status == new_status:
        return row
    if not can_transition(row.status, new_status):
        raise TransitionError(f"{row.status} -> {new_status} is not allowed")
    postgres.set_status(request_id, new_status, _DIGITS.sub("#", error)[:300] if error else None)
    return get_request(request_id) or row


def fail(request_id: str, error: str) -> None:
    row = get_request(request_id)
    if row is not None and row.status in ("requested", "logical_done"):
        postgres.set_status(request_id, "failed", _DIGITS.sub("#", error)[:300])


def audit_row(request_id: str) -> dict[str, Any] | None:
    """The forget_audit row of a request (HMAC, xid32, begin LSN as text, footprint counts), or None."""
    out = postgres.psql(
        "SELECT forget_xid32, forget_lsn::text, coalesce(footprint::text, '{}') FROM privacy.forget_audit "
        f"WHERE request_id = '{postgres._uuid(request_id)}';"
    )
    if not out:
        return None
    xid, lsn, fp = out.splitlines()[0].split("|", 2)
    return {"xid32": int(xid) if xid else None, "forget_lsn": lsn or None, "footprint": json.loads(fp)}


def record_audit_row(
    request_id: str, customer_id: int, txn: postgres.ForgetTxn, footprint: Mapping[str, Iterable[int]]
) -> str:
    """Insert the HMAC-keyed audit row; returns the subject HMAC. `footprint` is reduced to counts."""
    handle = fids.subject_hmac(hmac_key(), customer_id)
    postgres.record_audit(handle, request_id, txn, {t: len(list(v)) for t, v in footprint.items()})
    return handle


# ---- per-sink status -------------------------------------------------------------------------------------


def record_sink(
    request_id: str,
    sink: str,
    *,
    logical_absent: bool = False,
    physical_absent: bool = False,
    bound: str | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    """Record one observation. The first time a flag is set it stamps now(); a later call keeps that time. Evidence
    and bound are validated: counts and prose only, never an id or canary."""
    postgres.set_sink_status(
        request_id,
        sink,
        logical_absent=logical_absent,
        physical_absent=physical_absent,
        bound=safe_bound(bound),
        evidence=safe_evidence(evidence),
    )


@dataclass(frozen=True)
class SinkRow:
    sink: str
    logical_absent_at: float | None
    physical_absent_at: float | None
    bound: str | None
    evidence: Mapping[str, Any] = field(default_factory=dict)


def sink_rows(request_id: str) -> list[SinkRow]:
    out = postgres.psql(
        "SELECT sink, coalesce(extract(epoch from logical_absent_at)::text, ''), "
        "coalesce(extract(epoch from physical_absent_at)::text, ''), "
        "coalesce(replace(replace(bound, E'\\n', ' '), '|', '/'), ''), coalesce(evidence::text, '{}') "
        f"FROM privacy.forget_sink_status WHERE request_id = '{postgres._uuid(request_id)}' ORDER BY sink;"
    )
    rows = []
    for line in out.splitlines():
        sink, la, pa, bound, ev = line.split("|", 4)
        rows.append(SinkRow(sink, _f(la), _f(pa), bound or None, json.loads(ev)))
    return rows


# ---- lag -------------------------------------------------------------------------------------------------


def lag_s(requested_at: float, absent_at: float | None) -> float | None:
    """Seconds from the request to the first observation of absence; None if never observed. Not negative."""
    if absent_at is None:
        return None
    return max(0.0, absent_at - requested_at)


def summarize(values: Iterable[float | None]) -> dict[str, float | int | None]:
    """median / min / max over the observed lags and how many were observed (None entries are not observed)."""
    xs = [v for v in values if v is not None]
    if not xs:
        return {"n": 0, "median_s": None, "min_s": None, "max_s": None}
    return {"n": len(xs), "median_s": statistics.median(xs), "min_s": min(xs), "max_s": max(xs)}


def request_lags(request_id: str) -> dict[str, dict[str, Any]]:
    """{sink: {logical_lag_s, physical_lag_s, bound: bool}} for one request."""
    req = get_request(request_id)
    if req is None:
        raise KeyError("unknown request")
    return {
        r.sink: {
            "logical_lag_s": lag_s(req.requested_at, r.logical_absent_at),
            "physical_lag_s": lag_s(req.requested_at, r.physical_absent_at),
            "bounded": bool(r.bound),
        }
        for r in sink_rows(request_id)
    }


def lag_table(request_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Per sink across requests: {sink: {logical: summary, physical: summary, bounded: n requests with a bound}}.
    This is what a manifest records. No ids."""
    per: dict[str, dict[str, list[Any]]] = {}
    for rid in request_ids:
        for sink, d in request_lags(rid).items():
            e = per.setdefault(sink, {"logical": [], "physical": [], "bounded": []})
            e["logical"].append(d["logical_lag_s"])
            e["physical"].append(d["physical_lag_s"])
            e["bounded"].append(d["bounded"])
    return {
        s: {"logical": summarize(e["logical"]), "physical": summarize(e["physical"]), "bounded": sum(e["bounded"])}
        for s, e in sorted(per.items())
    }


def public_summary(row: RequestRow) -> dict[str, Any]:
    """A request as the CLI prints it: no ids but the request uuid."""
    return {
        "request_id": row.request_id,
        "status": row.status,
        "requested_at": row.requested_at,
        "logical_lag_s": lag_s(row.requested_at, row.logical_done_at),
        "physical_lag_s": lag_s(row.requested_at, row.physical_done_at),
        "raw_id_stored": row.raw_present,
        "error": row.error,
    }


# ---- subject scratch (raw, in the same class as raw_customer_id) ------------------------------------------


def save_scratch(request_id: str, scratch: Mapping[str, Any]) -> None:
    """Store the footprint pks and PII tokens the physical phase needs (it may run in another process, after the
    rows are gone from Postgres and bronze). Same sensitivity as raw_customer_id: NULLed with it, then vacuumed."""
    postgres.psql(
        f"UPDATE privacy.forget_requests SET subject_scratch = {postgres.lit(json.dumps(dict(scratch)))}::jsonb "
        f"WHERE request_id = '{postgres._uuid(request_id)}';"
    )


def load_scratch(request_id: str) -> dict[str, Any] | None:
    out = postgres.psql(
        "SELECT coalesce(subject_scratch::text, '') FROM privacy.forget_requests "
        f"WHERE request_id = '{postgres._uuid(request_id)}';"
    )
    return json.loads(out) if out else None


def null_subject(request_id: str) -> None:
    """Drop both raw copies (raw id and scratch). The dead tuples go with VACUUM FULL of the privacy tables."""
    postgres.psql(
        "UPDATE privacy.forget_requests SET raw_customer_id = NULL, subject_scratch = NULL "
        f"WHERE request_id = '{postgres._uuid(request_id)}';"
    )
