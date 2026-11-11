"""G11a report logic (pure): R2 and R4 summaries, README strings and the G11a wording guard."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from adrp.measure import report
from adrp.recon.classify import wording_violations


def _sink(ln: int, p: float, bounded: int = 0) -> dict[str, Any]:
    return {
        "logical": {
            "n": ln,
            "median_s": 100.0 if ln else None,
            "min_s": 90.0 if ln else None,
            "max_s": 110.0 if ln else None,
        },
        "physical": {"n": 6, "median_s": p, "min_s": p - 10, "max_s": p + 10},
        "bounded": bounded,
    }


def _run(label: str, logical: list[float], physical: list[float], gap: float = 0.3) -> dict[str, Any]:
    """Five back-to-back requests, then a sixth 700 s later."""
    t = [1000.0 + gap * i for i in range(5)] + [1700.0]
    reqs = [{"requested_at": t[i], "logical_lag_s": logical[i], "physical_lag_s": physical[i]} for i in range(6)]
    return {
        "label": label,
        "manifest": f"{label}.json",
        "link": f"runs/manifests/{label}.json",
        "code": "abc1234",
        "requests": reqs,
        "sinks": {
            "silver": _sink(6, 900.0),
            "pg_wal": _sink(0, 1500.0, bounded=6),
            "qdrant_wal": _sink(0, 0.0, bounded=6),
        },
    }


R2_RUNS = [
    _run("run 1", [500, 501, 502, 503, 504, 150], [2600, 2601, 2602, 2603, 2604, 1800]),
    _run("run 2", [300, 301, 302, 303, 304, 100], [2000, 2001, 2002, 2003, 2004, 1500], gap=0.5),
]


def _fault(outcome: str = "self_healed", **kw: int) -> dict[str, Any]:
    return {"dupes": 0, "losses": 0, "resurrections": 0, "outcome": outcome, "expected_detection_fired": None, **kw}


def _g10a(label: str, ledger: int) -> dict[str, Any]:
    faults = {f: _fault() for f in ("F4", "F1", "F2", "F3", "F5", "F6a", "F6b")}
    faults["F4"]["outcome"] = "caught"
    faults["F6b"]["outcome"] = "caught"
    return {
        "label": label,
        "manifest": f"{label}.json",
        "link": f"runs/manifests/{label}.json",
        "code": "58340e0",
        "topic_mode": label,
        "bronze_duplicate_ledger": {"F1": ledger, "total": ledger},
        "faults": faults,
    }


G3: dict[str, Any] = {
    "manifest": "g3.json",
    "link": "runs/manifests/g3.json",
    "code": "58340e0",
    "kills": ["before_merge", "mid_merge", "after_merge"],
    "compare": {"missing": 0, "differing": 0, "extra_live": 0, "resurrected": 0, "duplicates": 0, "tombstone_pii": 0},
}


def test_r2_per_run_and_pooled() -> None:
    s = report.r2_summary(R2_RUNS)
    r1, r2 = s["runs"]
    assert (r1["n_requests"], r1["n_batched"], r1["n_generator_owned"]) == (6, 5, 1)
    assert r1["logical_s"] == {"n": 6, "median": 501.5, "min": 150, "max": 504}
    assert r1["batched_logical_s"]["median"] == 502
    assert r1["generator_logical_s"]["median"] == 150
    assert r2["physical_s"]["median"] == 2001.5
    pooled = s["pooled"]["logical_s"]
    assert pooled["n"] == 12 and pooled["min"] == 100 and pooled["max"] == 504
    assert s["n_runs"] == 2 and s["n_requests"] == 12


def test_r2_pooled_median_is_over_requests_not_runs() -> None:
    s = report.r2_summary(R2_RUNS)
    values = sorted([500, 501, 502, 503, 504, 150, 300, 301, 302, 303, 304, 100])
    assert s["pooled"]["logical_s"]["median"] == (values[5] + values[6]) / 2


def test_r2_batch_spread_rounds_up_and_bounded_locations() -> None:
    s = report.r2_summary(R2_RUNS)
    assert s["runs"][0]["batch_spread_s"] == 1.2
    assert s["runs"][1]["batch_spread_s"] == 2.0
    assert s["spread_s"] == 2.0
    assert s["bounded_locations"] == ["pg_wal", "qdrant_wal"]


def test_r2_rejects_empty() -> None:
    with pytest.raises(ValueError):
        report.r2_summary([])


def test_readme_r2_is_one_line_and_says_logical_physical_and_schedule() -> None:
    s = report.r2_summary(R2_RUNS)
    cell = report.readme_r2(s)
    assert "|" not in cell and "\n" not in cell
    assert "Logical lag" in cell and "Physical lag" in cell
    assert "measured in this schedule" in cell and "not a minimum latency" in cell
    assert "compacting topics" in cell and "not independent" in cell
    assert "pg_wal and qdrant_wal are bounded, not removed" in cell
    assert report.readme_r2_manifests(s) == "[run 1](runs/manifests/run 1.json), [run 2](runs/manifests/run 2.json)"


def test_r4_zero_totals_and_groups() -> None:
    s = report.r4_summary([_g10a("default", 667), _g10a("compacting", 435)], [G3])
    assert s["totals"] == {"dupes": 0, "losses": 0, "resurrections": 0}
    assert (s["n_runs"], s["n_fault_executions"], s["n_kills"]) == (3, 14, 3)
    by = {g["key"]: g for g in s["groups"]}
    assert (
        by["replay"]["executions"] == 4 and by["crash_restart"]["executions"] == 6 and by["schema"]["executions"] == 4
    )
    assert by["crash_restart"]["outcomes"] == {"caught": 2, "self_healed": 4, "gate_failed": 0}
    assert s["runs_with_nonzero"] == 0
    cell = report.readme_r4(s)
    assert "0 duplicates, 0 losses and 0 resurrections in 3 runs" in cell
    assert "idempotent under the tested faults" in cell
    assert "667 and 435" in cell and "Silver only" in cell and "no concurrent writers" in cell
    assert "|" not in cell and "\n" not in cell
    assert report.readme_r4_manifests(s).count("](") == 3


def test_r4_stall_run_stays_in_the_denominator() -> None:
    """The Connect-stall rule (ADR-0024): gate_failed and losses are kept and reported as observed."""
    stalled = _g10a("compacting", 435)
    stalled["faults"]["F5"].update(outcome="gate_failed", losses=12)
    s = report.r4_summary([_g10a("default", 667), stalled], [G3])
    assert s["n_runs"] == 3 and s["totals"]["losses"] == 12 and s["runs_with_nonzero"] == 1
    by = {g["key"]: g for g in s["groups"]}
    assert by["crash_restart"]["outcomes"]["gate_failed"] == 1 and by["crash_restart"]["losses"] == 12
    cell = report.readme_r4(s)
    assert "12 losses" in cell and "gate failed 1" in cell


def test_r4_g3_kill_compare_fields() -> None:
    g3 = copy.deepcopy(G3)
    g3["compare"].update(missing=2, duplicates=1, resurrected=3)
    s = report.r4_summary([_g10a("default", 1)], [g3])
    k = s["kill"][0]
    assert (k["dupes"], k["losses"], k["resurrections"]) == (1, 2, 3)
    assert s["totals"] == {"dupes": 1, "losses": 2, "resurrections": 3}


def test_r4_rejects_unknown_fault_missing_fault_and_bad_outcome() -> None:
    run = _g10a("default", 1)
    run["faults"]["F9"] = _fault()
    with pytest.raises(ValueError, match="outside"):
        report.r4_summary([run], [G3])
    run = _g10a("default", 1)
    del run["faults"]["F2"]
    with pytest.raises(ValueError, match="missing"):
        report.r4_summary([run], [G3])
    run = _g10a("default", 1)
    run["faults"]["F2"]["outcome"] = "fine"
    with pytest.raises(ValueError, match="unknown outcome"):
        report.r4_summary([run], [G3])
    with pytest.raises(ValueError):
        report.r4_summary([], [])


@pytest.mark.parametrize(
    "text",
    [
        "real-time freshness",
        "near real-time",
        "Near Real Time",
        "we guarantee it",
        "guaranteed",
        "SLA met",
        "meets the SLO",
        "maximum throughput",
        "the capacity of the stack",
        "exactly-once",
        "a production system",
        "p99 was 4 s",
        "P95 lag",
        "p99.9",
        "the 99th percentile",
        "quantile 0.5",
    ],
)
def test_g11a_wording_rejects(text: str) -> None:
    assert report.wording_violations_g11a(text), text


@pytest.mark.parametrize(
    "text",
    [
        "median 503 s [min 156, max 503] over 6 requests",
        "idempotent under the tested faults",
        "0 duplicates in 3 runs",
        "P0 and P1 are phases",  # a single digit is not a percentile
    ],
)
def test_g11a_wording_accepts(text: str) -> None:
    assert report.wording_violations_g11a(text) == []


def test_rendered_readme_cells_pass_both_wording_guards() -> None:
    r2 = report.readme_r2(report.r2_summary(R2_RUNS))
    r4 = report.readme_r4(report.r4_summary([_g10a("default", 667), _g10a("compacting", 435)], [G3]))
    for cell in (r2, r4, report.R1_CELL):
        assert wording_violations(cell) == [], cell
        assert report.wording_violations_g11a(cell) == [], cell
