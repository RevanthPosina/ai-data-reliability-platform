"""Pure summaries and README strings for G11a (reduced: R2 and R4 from existing manifests; R1 deferred, ADR-0024).

No I/O. The notebook (notebooks/g11a_results.py) reads the manifests with DuckDB and hands plain dicts to these
functions; every number in the README row cells is rendered here, so the README can be checked mechanically.

Wording rules (design must-never-claim): logical or physical for deletions, "idempotent under the tested faults",
"0 in N runs" with no rate, silver only (bronze holds duplicate rows by design), one machine, no extrapolation, and
no percentile at these sample sizes: median, min, max and n only.
"""

from __future__ import annotations

import math
import re
import statistics
from collections.abc import Mapping, Sequence
from typing import Any

R1_CELL = "not measured yet (deferred, ADR-0024)"
R1_MANIFEST_CELL = "none (deferred)"

# R4 fault groups (G11a plan 1.4)
GROUPS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("replay", "replay", ("F1", "F2")),
    ("crash_restart", "crash and restart", ("F3", "F4", "F5")),
    ("schema", "schema", ("F6a", "F6b")),
)
OUTCOMES = ("caught", "self_healed", "gate_failed")
# the three counts R4 is about, in the order they are printed everywhere
COUNTS = ("dupes", "losses", "resurrections")

# bounded-not-removed locations of design S8 (reported as bounded, never as removed)
_BOUNDED_NOTE = "bounded, not removed"

_BANNED: list[tuple[str, re.Pattern[str]]] = [
    ("real-time", re.compile(r"\breal[- ]?time\b", re.I)),
    ("guarantee", re.compile(r"\bguarantee\w*", re.I)),
    ("SLA met", re.compile(r"\bSLA met\b", re.I)),
    ("meets the SLO", re.compile(r"\bmeets? (?:the )?SLOs?\b", re.I)),
    ("capacity", re.compile(r"\bcapacity\b", re.I)),
    ("maximum throughput", re.compile(r"\b(?:maximum|max(?:imal)?) throughput\b", re.I)),
    ("exactly-once", re.compile(r"\bexactly[- ]once\b", re.I)),
    ("production", re.compile(r"\bproduction\b", re.I)),
    ("percentile", re.compile(r"\bp\d{2,3}(?:\.\d+)?\b|\bpercentiles?\b|\bquantiles?\b", re.I)),
]


def wording_violations_g11a(text: str) -> list[str]:
    """G11a-specific banned phrases in `text`, as `rule: 'matched text'`. Empty means clean. Percentiles are banned
    outright: n is 2 runs of 6 requests (R2) or 3 runs (R4), so only median, min, max and n are reported."""
    return [f"{label}: {m.group(0)!r}" for label, pat in _BANNED for m in pat.finditer(text)]


# ---------------------------------------------------------------- small helpers


def _mmm(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "median": None, "min": None, "max": None}
    return {
        "n": len(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def fmt_s(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.0f}"


def fmt_min(x: float | None) -> str:
    return "n/a" if x is None else f"{x / 60:.1f}"


def _r2(x: float | None) -> float | None:
    return None if x is None else round(x, 2)


def _round_stats(d: Mapping[str, Any]) -> dict[str, Any]:
    return {k: (_r2(v) if isinstance(v, float) else v) for k, v in d.items()}


# ---------------------------------------------------------------- R2


def r2_summary(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """R2 from g8 run manifests. Each input run: `label`, `manifest`, `code` (short sha), `requests` (a list of
    `requested_at`, `logical_lag_s`, `physical_lag_s`) and `sinks` (the manifest's `lag_report.sinks`).

    Per run and pooled: median, min, max and n of the logical and the physical lag over requests. The five ad-hoc
    requests of a run are submitted back to back (not independent) and the sixth is the generator-owned subject, so
    the two groups are also reported apart. Per-sink values are the manifest's per-run summaries; pooling those
    would need the raw per-request rows, which the manifests do not hold, so only their per-run values are kept.
    """
    if not runs:
        raise ValueError("r2_summary needs at least one run")
    out_runs: list[dict[str, Any]] = []
    pooled_l: list[float] = []
    pooled_p: list[float] = []
    for run in runs:
        reqs = sorted(run["requests"], key=lambda r: r["requested_at"])
        if not reqs:
            raise ValueError(f"{run['label']}: no requests")
        t0 = reqs[0]["requested_at"]
        batch = [r for r in reqs if r["requested_at"] - t0 < 60.0]
        other = [r for r in reqs if r["requested_at"] - t0 >= 60.0]
        spread = max(r["requested_at"] for r in batch) - t0
        logical = [float(r["logical_lag_s"]) for r in reqs]
        physical = [float(r["physical_lag_s"]) for r in reqs]
        pooled_l += logical
        pooled_p += physical
        sinks: dict[str, Any] = {}
        for name, s in sorted(run["sinks"].items()):
            sinks[name] = {
                "logical": _round_stats({"n": s["logical"]["n"], **_pick(s["logical"])}),
                "physical": _round_stats({"n": s["physical"]["n"], **_pick(s["physical"])}),
                "bounded": int(s.get("bounded", 0)),
            }
        out_runs.append(
            {
                "label": run["label"],
                "manifest": run["manifest"],
                "link": run["link"],
                "code": run["code"],
                "n_requests": len(reqs),
                "n_batched": len(batch),
                "n_generator_owned": len(other),
                "batch_spread_s": math.ceil(round(spread * 10, 6)) / 10,
                "logical_s": _round_stats(_mmm(logical)),
                "physical_s": _round_stats(_mmm(physical)),
                "batched_logical_s": _round_stats(_mmm([float(r["logical_lag_s"]) for r in batch])),
                "batched_physical_s": _round_stats(_mmm([float(r["physical_lag_s"]) for r in batch])),
                "generator_logical_s": _round_stats(_mmm([float(r["logical_lag_s"]) for r in other])),
                "generator_physical_s": _round_stats(_mmm([float(r["physical_lag_s"]) for r in other])),
                "sinks": sinks,
            }
        )
    bounded = sorted({name for r in out_runs for name, s in r["sinks"].items() if s["bounded"] > 0})
    return {
        "n_runs": len(out_runs),
        "n_requests": len(pooled_l),
        "runs": out_runs,
        "pooled": {"logical_s": _round_stats(_mmm(pooled_l)), "physical_s": _round_stats(_mmm(pooled_p))},
        "bounded_locations": bounded,
        "bounded_note": _BOUNDED_NOTE,
        "spread_s": max(r["batch_spread_s"] for r in out_runs),
    }


def _pick(d: Mapping[str, Any]) -> dict[str, Any]:
    return {"median": d.get("median_s"), "min": d.get("min_s"), "max": d.get("max_s")}


def readme_r2(summary: Mapping[str, Any]) -> str:
    """The R2 result cell of README.md (one line, no table pipes)."""
    parts_l = []
    parts_p = []
    for r in summary["runs"]:
        lg, ph = r["logical_s"], r["physical_s"]
        parts_l.append(f"{r['label']} {fmt_s(lg['median'])} s [min {fmt_s(lg['min'])}, max {fmt_s(lg['max'])}]")
        parts_p.append(f"{r['label']} {fmt_min(ph['median'])} min [min {fmt_min(ph['min'])}, max {fmt_min(ph['max'])}]")
    pl, pp = summary["pooled"]["logical_s"], summary["pooled"]["physical_s"]
    gen = "; ".join(
        f"{r['label']} {fmt_s(r['generator_logical_s']['median'])} s logical, "
        f"{fmt_min(r['generator_physical_s']['median'])} min physical"
        for r in summary["runs"]
    )
    n_req = " and ".join(sorted({str(r["n_requests"]) for r in summary["runs"]}))
    locs = list(summary["bounded_locations"])
    bounded = ", ".join(locs[:-1]) + " and " + locs[-1] if len(locs) > 1 else ", ".join(locs)
    return (
        f"Logical lag (request to every sink verified absent by query), median over requests: {'; '.join(parts_l)}; "
        f"pooled {fmt_s(pl['median'])} s [min {fmt_s(pl['min'])}, max {fmt_s(pl['max'])}] "
        f"over {summary['n_requests']} requests. "
        f"Physical lag (request to the maintenance window's verified-absent time, all sinks): {'; '.join(parts_p)}; "
        f"pooled {fmt_min(pp['median'])} min [min {fmt_min(pp['min'])}, max {fmt_min(pp['max'])}] "
        f"over {summary['n_requests']} "
        f"requests. Physical lag is measured in this schedule (set by when the g8 check's maintenance window ran), "
        f"not a minimum latency. {summary['n_runs']} runs ({n_req} requests each) on one machine, compacting topics "
        f"(the g8 setting); each run has 5 ad-hoc requests submitted within {summary['spread_s']:.1f} s of each other "
        f"(not independent samples) and 1 generator-owned subject ({gen}). "
        f"{bounded} are {summary['bounded_note']}. No claim beyond these runs."
    )


def readme_r2_manifests(summary: Mapping[str, Any]) -> str:
    return ", ".join(f"[{r['label']}]({r['link']})" for r in summary["runs"])


# ---------------------------------------------------------------- R4


def r4_summary(g10a: Sequence[Mapping[str, Any]], g3: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """R4 from g10a runs and g3 kill runs (silver only).

    g10a input: `label`, `manifest`, `code`, `topic_mode`, `faults` ({id: dupes, losses, resurrections, outcome,
    expected_detection_fired}) and `bronze_duplicate_ledger` (F1 rows). g3 input: `manifest`, `code`, `kills` and
    `compare` (missing, differing, extra_live, resurrected, duplicates, tombstone_pii). The Connect-stall rule
    (ADR-0024): a run where the stall recurs stays in the denominator, its F5 outcome is `gate_failed`, and its
    `losses` are summed as observed. Nothing is dropped, so a stall shows up here as a non-zero total.
    """
    if not g10a and not g3:
        raise ValueError("r4_summary needs at least one run")
    groups: list[dict[str, Any]] = []
    tot = dict.fromkeys(COUNTS, 0)
    n_exec = 0
    for key, label, ids in GROUPS:
        g: dict[str, Any] = {"key": key, "label": label, "faults": list(ids), "executions": 0}
        g.update(dict.fromkeys(COUNTS, 0))
        g["outcomes"] = dict.fromkeys(OUTCOMES, 0)
        for run in g10a:
            for fid in ids:
                if fid not in run["faults"]:
                    raise ValueError(f"{run['label']}: fault {fid} missing")
                f = run["faults"][fid]
                g["executions"] += 1
                for c in COUNTS:
                    g[c] += int(f[c])
                outcome = str(f["outcome"])
                if outcome not in OUTCOMES:
                    raise ValueError(f"{run['label']} {fid}: unknown outcome {outcome!r}")
                g["outcomes"][outcome] += 1
        for c in COUNTS:
            tot[c] += g[c]
        n_exec += g["executions"]
        groups.append(g)
    extra = {fid for run in g10a for fid in run["faults"]} - {i for _, _, ids in GROUPS for i in ids}
    if extra:
        raise ValueError(f"faults outside the R4 groups: {sorted(extra)}")
    kill_rows: list[dict[str, Any]] = []
    for run in g3:
        c = run["compare"]
        row = {
            "manifest": run["manifest"],
            "link": run["link"],
            "code": run["code"],
            "kills": list(run["kills"]),
            "n_kills": len(run["kills"]),
            "dupes": int(c["duplicates"]),
            "losses": int(c["missing"]),
            "resurrections": int(c["resurrected"]),
            "differing": int(c["differing"]),
            "extra_live": int(c["extra_live"]),
            "tombstone_pii": int(c["tombstone_pii"]),
        }
        for k in COUNTS:
            tot[k] += row[k]
        kill_rows.append(row)
    runs_with_nonzero = sum(1 for run in g10a if any(_run_total(run, c) for c in COUNTS)) + sum(
        1 for r in kill_rows if any(r[c] for c in COUNTS)
    )
    return {
        "n_runs": len(g10a) + len(g3),
        "n_g10a_runs": len(g10a),
        "n_g3_runs": len(g3),
        "n_fault_executions": n_exec,
        "n_kills": sum(r["n_kills"] for r in kill_rows),
        "totals": tot,
        "runs_with_nonzero": runs_with_nonzero,
        "groups": groups,
        "kill": kill_rows,
        "g10a": [
            {
                "label": r["label"],
                "manifest": r["manifest"],
                "link": r["link"],
                "code": r["code"],
                "topic_mode": r["topic_mode"],
                "bronze_f1_ledger": int(r["bronze_duplicate_ledger"]["F1"]),
                "faults": {k: _fault_row(v) for k, v in r["faults"].items()},
            }
            for r in g10a
        ],
    }


def _run_total(run: Mapping[str, Any], c: str) -> int:
    return sum(int(f[c]) for f in run["faults"].values())


def _fault_row(f: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dupes": int(f["dupes"]),
        "losses": int(f["losses"]),
        "resurrections": int(f["resurrections"]),
        "outcome": f["outcome"],
        "expected_detection_fired": f.get("expected_detection_fired"),
    }


def _triple(d: Mapping[str, Any]) -> str:
    return " / ".join(str(d[c]) for c in COUNTS)


def outcome_text(o: Mapping[str, int]) -> str:
    names = {"caught": "caught", "self_healed": "self-healed in this run", "gate_failed": "gate failed"}
    return ", ".join(f"{names[k]} {o[k]}" for k in OUTCOMES if o[k])


def readme_r4(summary: Mapping[str, Any]) -> str:
    """The R4 result cell of README.md (one line, no table pipes)."""
    t = summary["totals"]
    n = summary["n_runs"]
    ledgers = " and ".join(str(r["bronze_f1_ledger"]) for r in summary["g10a"])
    groups = "; ".join(
        f"{g['label']} ({', '.join(g['faults'])}) {_triple(g)} in {g['executions']} executions "
        f"({outcome_text(g['outcomes'])})"
        for g in summary["groups"]
    )
    kills = summary["kill"]
    kill_txt = "; ".join(
        f"g3 kill {_triple(k)} in {k['n_kills']} SIGKILLs ({', '.join(k['kills'])}; also {k['differing']} differing, "
        f"{k['extra_live']} extra live rows)"
        for k in kills
    )
    modes = " and ".join(r["label"] for r in summary["g10a"])
    head = (
        f"Silver, idempotent under the tested faults: {t['dupes']} duplicates, {t['losses']} losses and "
        f"{t['resurrections']} resurrections in {n} runs ({summary['n_g10a_runs']} full fault runs, one per topic "
        f"setting ({modes}), {summary['n_fault_executions']} fault executions; {summary['n_g3_runs']} g3 kill run, "
        f"{summary['n_kills']} SIGKILLs)"
    )
    tail = (
        f"Counts are duplicates / losses / resurrections, from the reconciler gate after recovery (g10a) and the "
        f"ground-truth compare (g3). Silver only: bronze holds duplicate rows by design (F1 replay ledgers {ledgers}). "
        f"F4's retry ran with no concurrent writers. F5 is a graceful restart of a dev-container broker. "
        f"One machine, code {', '.join(sorted({r['code'] for r in summary['g10a']} | {k['code'] for k in kills}))}, "
        f"1 run per topic setting; a zero here is a count in these runs, not a rate, "
        f"and undetected faults may exist. Open finding (ADR-0020): Connect stalled silently once after a broker "
        f"restart in an earlier non-record run; cause unverified, decision open."
    )
    return f"{head}. By group: {groups}; {kill_txt}. {tail}"


def readme_r4_manifests(summary: Mapping[str, Any]) -> str:
    items = [f"[{r['label']}]({r['link']})" for r in summary["g10a"]]
    items += [f"[g3 kill]({k['link']})" for k in summary["kill"]]
    return ", ".join(items)
