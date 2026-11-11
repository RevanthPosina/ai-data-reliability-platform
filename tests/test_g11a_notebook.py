"""notebooks/g11a_results.py: main() on synthetic manifests in tmp_path (no Docker, no real manifests)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("g11a_results", ROOT / "notebooks" / "g11a_results.py")
assert spec is not None and spec.loader is not None
nb: ModuleType = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

SINK = {
    "logical": {"n": 6, "median_s": 100.0, "min_s": 90.0, "max_s": 110.0},
    "physical": {"n": 6, "median_s": 900.0, "min_s": 800.0, "max_s": 1000.0},
    "bounded": 0,
}
BOUNDED = {
    "logical": {"n": 0, "median_s": None, "min_s": None, "max_s": None},
    "physical": {"n": 0, "median_s": None, "min_s": None, "max_s": None},
    "bounded": 6,
}


def _g8(sha: str, base: float) -> dict[str, Any]:
    t = 1000.0
    reqs = [
        {
            "request_id": f"r{i}",
            "status": "physical_done",
            "requested_at": t + 0.4 * i,
            "logical_lag_s": base + i,
            "physical_lag_s": 10 * base + i,
            "error": None,
        }
        for i in range(5)
    ] + [
        {
            "request_id": "r5",
            "status": "physical_done",
            "requested_at": t + 700,
            "logical_lag_s": base / 3,
            "physical_lag_s": 7 * base,
            "error": None,
        }
    ]
    return {
        "git_sha": sha + "0" * 33,
        "result": {"passed": True, "lag_report": {"requests": reqs, "sinks": {"silver": SINK, "pg_wal": BOUNDED}}},
    }


def _fault(outcome: str) -> dict[str, Any]:
    return {
        "dupes": 0,
        "losses": 0,
        "resurrections": 0,
        "outcome": outcome,
        "expected_detection_fired": True if outcome == "caught" else None,
    }


def _g10a(mode: str, ledger: int) -> dict[str, Any]:
    faults = {f: _fault("self_healed") for f in ("F4", "F1", "F2", "F3", "F5", "F6a", "F6b")}
    faults["F4"] = _fault("caught")
    return {
        "git_sha": "58340e0" + "1" * 33,
        "result": {
            "passed": True,
            "topic_mode": mode,
            "faults": faults,
            "bronze_duplicate_ledger": {"F1": ledger, "total": ledger},
        },
    }


def _g3() -> dict[str, Any]:
    detail = json.dumps(
        {"missing": 0, "differing": 0, "extra_live": 0, "resurrected": 0, "duplicates": 0, "tombstone_pii": 0}
    )
    checks = [
        {"check": "kill: silver caught up after the restarts", "ok": True, "detail": ""},
        {
            "check": "kill: silver equals ground truth (no loss, no duplicates, no resurrection)",
            "ok": True,
            "detail": detail,
        },
    ]
    return {
        "git_sha": "58340e0" + "2" * 33,
        "result": {"passed": True, "checks": checks, "kill": {"kills": ["before_merge", "mid_merge", "after_merge"]}},
    }


@pytest.fixture
def world(tmp_path: Path) -> dict[str, Path]:
    man = tmp_path / "runs" / "manifests"
    man.mkdir(parents=True)
    files = {
        "g8a": ("a-g8-check.json", _g8("aaaaaaa", 500.0)),
        "g8b": ("b-g8-check.json", _g8("bbbbbbb", 300.0)),
        "g10a1": ("c-g10a-check.json", _g10a("default", 667)),
        "g10a2": ("d-g10a-check.json", _g10a("compact", 435)),
        "g3": ("e-g3-check.json", _g3()),
    }
    for _, (name, body) in files.items():
        (man / name).write_text(json.dumps(body))
    inputs = tmp_path / "inputs.json"

    def rel(k: str) -> str:
        return f"runs/manifests/{files[k][0]}"

    inputs.write_text(
        json.dumps(
            {"rounds": [], "g8": [rel("g8a"), rel("g8b")], "g10a": [rel("g10a1"), rel("g10a2")], "g3": [rel("g3")]}
        )
    )
    adr = tmp_path / "docs" / "adr"
    adr.mkdir(parents=True)
    (adr / "0020-fault-injection.md").write_text(
        "# ADR\n\n### Open finding: Connect stalled once after the broker restart\n"
        "Connect stalled once; cause unverified.\n\n### Next\nother\n"
    )
    readme = tmp_path / "README.md"
    readme.write_text("| ID | Q | Result | Manifest |\n|---|---|---|---|\n")
    return {"root": tmp_path, "inputs": inputs, "readme": readme, "out": tmp_path / "out"}


def _args(w: dict[str, Path], *extra: str) -> list[str]:
    return [
        "--inputs",
        str(w["inputs"]),
        "--out",
        str(w["out"]),
        "--base-dir",
        str(w["root"]),
        "--readme",
        str(w["readme"]),
        *extra,
    ]


def _cells(w: dict[str, Path]) -> dict[str, str]:
    cells: dict[str, str] = json.loads((w["out"] / "g11a-results.json").read_text())["readme_cells"]
    return cells


def test_main_writes_results_and_is_clean(world: dict[str, Path], capsys: pytest.CaptureFixture[str]) -> None:
    assert nb.main(_args(world)) == 0
    md = (world["out"] / "g11a-results.md").read_text()
    js = json.loads((world["out"] / "g11a-results.json").read_text())
    assert "not measured; deferred (ADR-0024)" in md
    assert "Connect stalled once; cause unverified." in md  # verbatim from the ADR section
    assert "### Next" not in md and "\n> other" not in md  # the quote stops at the next heading
    assert (
        js["r2"]["n_requests"] == 12
        and js["r4"]["n_runs"] == 3
        and js["r4"]["totals"] == {"dupes": 0, "losses": 0, "resurrections": 0}
    )
    assert js["forget_code_diff"]["status"] in {"unavailable", "identical"}  # synthetic shas are unknown to git
    assert nb.scan_text("x", md) == [] and nb.scan_text("x", json.dumps(js)) == []
    assert "g11a-results OK" in capsys.readouterr().out


def test_check_readme_passes_on_rendered_cells_and_fails_on_drift(
    world: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert nb.main(_args(world)) == 0
    c = _cells(world)
    rows = ["| ID | Q | Result | Manifest |", "|---|---|---|---|"]
    rows += [
        f"| R1 | q | {c['r1']} | {c['r1_manifests']} |",
        f"| R2 | q | {c['r2']} | {c['r2_manifests']} |",
        f"| R4 | q | {c['r4']} | {c['r4_manifests']} |",
    ]
    world["readme"].write_text("\n".join(rows) + "\n")
    assert nb.main(_args(world, "--check-readme")) == 0
    world["readme"].write_text("\n".join(rows).replace("0 duplicates", "1 duplicates") + "\n")
    capsys.readouterr()
    assert nb.main(_args(world, "--check-readme")) == 1
    assert "README R4 result cell differs" in capsys.readouterr().err
    world["readme"].write_text("\n".join(r for r in rows if not r.startswith("| R2")) + "\n")
    assert nb.main(_args(world, "--check-readme")) == 1


def test_rounds_listed_is_rejected(world: dict[str, Path]) -> None:
    d = json.loads(world["inputs"].read_text())
    d["rounds"] = ["runs/manifests/x-g11a-round.json"]
    world["inputs"].write_text(json.dumps(d))
    with pytest.raises(ValueError, match="R1"):
        nb.main(_args(world))


def test_failed_manifest_is_rejected(world: dict[str, Path]) -> None:
    p = world["root"] / "runs" / "manifests" / "c-g10a-check.json"
    d = json.loads(p.read_text())
    d["result"]["passed"] = False
    p.write_text(json.dumps(d))
    with pytest.raises(ValueError, match="did not pass"):
        nb.main(_args(world))


def test_scan_text_flags_epoch_ms_percentiles_and_banned_words() -> None:
    assert nb.scan_text("t", "ts 1794358713160 here")  # 13 digits: epoch ms or a raw id
    assert nb.scan_text("t", "p99 lag")
    assert nb.scan_text("t", "real-time")
    assert nb.scan_text("t", "exactly-once")
    assert nb.scan_text("t", "median 503 s over 6 requests; silver only, 0 duplicates") == []


def test_stall_finding_requires_heading(tmp_path: Path) -> None:
    p = tmp_path / "adr.md"
    p.write_text("# x\n\nnothing\n")
    with pytest.raises(ValueError, match="not found"):
        nb.stall_finding(p)


def test_git_diff_stat_unknown_sha_is_unavailable() -> None:
    r = nb.git_diff_stat(ROOT, "0000000", "1111111", ("src/adrp/forget",))
    assert r["status"] == "unavailable"
