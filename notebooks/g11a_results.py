# %% [markdown]
# # G11a results (reduced): R2 and R4 from existing runs, R1 deferred (ADR-0024)
#
# Percent-format notebook that also runs as a script:
#
#     python notebooks/g11a_results.py --inputs docs/results/g11a-inputs.json --out docs/results --check-readme
#
# DuckDB reads the committed run manifests (`read_json`) and computes the R2 and R4 tables. The pure logic and the
# README strings live in `adrp.measure.report`; this file only reads, cross-checks and writes. No Docker is used and
# nothing is measured here: every number comes from a manifest in `runs/manifests/`.

# %%
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402

from adrp.forget.ids import ID13  # noqa: E402
from adrp.measure import report  # noqa: E402
from adrp.recon.classify import wording_violations  # noqa: E402

STALL_HEADING = "### Open finding: Connect stalled once after the broker restart"
STALL_SOURCE = "docs/adr/0020-fault-injection.md"
FORGET_CODE_PATHS = ("src/adrp/forget", "scripts/g8_check.py")
_SQL_READ = "read_json(?, columns={'git_sha': 'VARCHAR', 'result': 'JSON'})"
_TOPIC_LABEL = {"default": "default", "compact": "compacting"}

# %% [markdown]
# ## Inputs and manifest readers (DuckDB `read_json`)


# %%
def load_inputs(path: Path) -> dict[str, Any]:
    d = json.loads(path.read_text())
    if d.get("rounds"):
        raise ValueError("g11a-inputs.json lists R1 rounds, but this reduced G11a does not analyse R1 (ADR-0024)")
    for k in ("g8", "g10a", "g3"):
        if not d.get(k):
            raise ValueError(f"g11a-inputs.json has no {k!r} manifests")
    return d


def _resolve(base: Path, rel: str) -> Path:
    p = base / rel
    if not p.is_file():
        raise FileNotFoundError(f"manifest listed in the inputs is missing: {rel}")
    return p


def _meta(con: duckdb.DuckDBPyConnection, p: Path) -> tuple[str, dict[str, Any]]:
    row = con.execute(f"SELECT git_sha, result FROM {_SQL_READ}", [str(p)]).fetchone()
    assert row is not None
    return str(row[0])[:7], json.loads(row[1])


def read_r2(con: duckdb.DuckDBPyConnection, base: Path, rels: Sequence[str]) -> list[dict[str, Any]]:
    """One dict per g8 manifest: per-request lags and the per-sink summaries, via DuckDB."""
    runs = []
    for i, rel in enumerate(rels, 1):
        p = _resolve(base, rel)
        code, result = _meta(con, p)
        reqs = con.execute(
            f"""SELECT (j.value->>'$.requested_at')::DOUBLE, (j.value->>'$.logical_lag_s')::DOUBLE,
                       (j.value->>'$.physical_lag_s')::DOUBLE, j.value->>'$.status'
                FROM {_SQL_READ} m, json_each(m.result, '$.lag_report.requests') j""",
            [str(p)],
        ).fetchall()
        if any(r[3] != "physical_done" for r in reqs):
            raise ValueError(f"{rel}: a request is not physical_done")
        if not result.get("passed"):
            raise ValueError(f"{rel}: the g8 run did not pass")
        runs.append(
            {
                "label": f"run {i}",
                "manifest": p.name,
                "link": rel,
                "code": code,
                "requests": [{"requested_at": r[0], "logical_lag_s": r[1], "physical_lag_s": r[2]} for r in reqs],
                "sinks": result["lag_report"]["sinks"],
            }
        )
    return runs


def read_g10a(con: duckdb.DuckDBPyConnection, base: Path, rels: Sequence[str]) -> list[dict[str, Any]]:
    runs = []
    for rel in rels:
        p = _resolve(base, rel)
        code, result = _meta(con, p)
        if not result.get("passed"):
            raise ValueError(f"{rel}: the g10a run did not pass")
        rows = con.execute(
            f"""SELECT f.key, (f.value->>'$.dupes')::BIGINT, (f.value->>'$.losses')::BIGINT,
                       (f.value->>'$.resurrections')::BIGINT, f.value->>'$.outcome',
                       f.value->>'$.expected_detection_fired'
                FROM {_SQL_READ} m, json_each(m.result, '$.faults') f""",
            [str(p)],
        ).fetchall()
        mode = str(result["topic_mode"])
        runs.append(
            {
                "label": _TOPIC_LABEL.get(mode, mode),
                "manifest": p.name,
                "link": rel,
                "code": code,
                "topic_mode": mode,
                "bronze_duplicate_ledger": result["bronze_duplicate_ledger"],
                "faults": {
                    r[0]: {
                        "dupes": r[1],
                        "losses": r[2],
                        "resurrections": r[3],
                        "outcome": r[4],
                        "expected_detection_fired": None if r[5] is None else r[5] == "true",
                    }
                    for r in rows
                },
            }
        )
    return runs


def read_g3(con: duckdb.DuckDBPyConnection, base: Path, rels: Sequence[str]) -> list[dict[str, Any]]:
    """The kill phase: `result.kill.kills` and the ground-truth compare in the check named
    'kill: silver equals ground truth', whose `detail` is the JSON of the compare."""
    runs = []
    for rel in rels:
        p = _resolve(base, rel)
        code, result = _meta(con, p)
        if not result.get("passed"):
            raise ValueError(f"{rel}: the g3 run did not pass")
        rows = con.execute(
            f"""SELECT c.value->>'$.detail', (c.value->>'$.ok')::BOOLEAN
                FROM {_SQL_READ} m, json_each(m.result, '$.checks') c
                WHERE (c.value->>'$.check') LIKE 'kill: silver equals ground truth%'""",
            [str(p)],
        ).fetchall()
        if len(rows) != 1 or not rows[0][1]:
            raise ValueError(f"{rel}: expected exactly one passing 'kill: silver equals ground truth' check")
        runs.append(
            {
                "manifest": p.name,
                "link": rel,
                "code": code,
                "kills": list(result["kill"]["kills"]),
                "compare": json.loads(rows[0][0]),
            }
        )
    return runs


# %% [markdown]
# ## Independent cross-check in SQL
#
# The summaries come from `adrp.measure.report` (plain Python). DuckDB recomputes the same medians, minima, maxima
# and sums from the manifests and the two must agree.


# %%
def sql_cross_check(
    con: duckdb.DuckDBPyConnection, base: Path, inputs: Mapping[str, Any], r2: Mapping[str, Any], r4: Mapping[str, Any]
) -> list[str]:
    problems: list[str] = []
    for run, rel in zip(r2["runs"], inputs["g8"], strict=True):
        row = con.execute(
            f"""SELECT count(*), median((j.value->>'$.logical_lag_s')::DOUBLE), min((j.value->>'$.logical_lag_s')::DOUBLE),
                       max((j.value->>'$.logical_lag_s')::DOUBLE), median((j.value->>'$.physical_lag_s')::DOUBLE),
                       min((j.value->>'$.physical_lag_s')::DOUBLE), max((j.value->>'$.physical_lag_s')::DOUBLE)
                FROM {_SQL_READ} m, json_each(m.result, '$.lag_report.requests') j""",
            [str(base / rel)],
        ).fetchone()
        assert row is not None
        mine = [
            run["n_requests"],
            run["logical_s"]["median"],
            run["logical_s"]["min"],
            run["logical_s"]["max"],
            run["physical_s"]["median"],
            run["physical_s"]["min"],
            run["physical_s"]["max"],
        ]
        if any(abs(float(a) - float(b)) > 0.01 for a, b in zip(row, mine, strict=True)):
            problems.append(f"R2 {run['label']}: DuckDB {row} differs from report {mine}")
    sums = dict.fromkeys(report.COUNTS, 0)
    for rel in inputs["g10a"]:
        row = con.execute(
            f"""SELECT sum((f.value->>'$.dupes')::BIGINT), sum((f.value->>'$.losses')::BIGINT),
                       sum((f.value->>'$.resurrections')::BIGINT)
                FROM {_SQL_READ} m, json_each(m.result, '$.faults') f""",
            [str(base / rel)],
        ).fetchone()
        assert row is not None
        for c, v in zip(report.COUNTS, row, strict=True):
            sums[c] += int(v)
    g10a_total = {c: sum(g[c] for g in r4["groups"]) for c in report.COUNTS}
    if sums != g10a_total:
        problems.append(f"R4 g10a sums: DuckDB {sums} differs from report {g10a_total}")
    return problems


# %% [markdown]
# ## Facts the notebook checks with git, and the open finding it quotes


# %%
def git_diff_stat(root: Path, a: str, b: str, paths: Sequence[str]) -> dict[str, str]:
    """`git diff --stat a b -- paths`: status is identical, differs, or unavailable (no git, or an unknown sha)."""
    cmd = ["git", "-C", str(root), "diff", "--stat", a, b, "--", *paths]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=60)
    except (OSError, subprocess.SubprocessError) as e:
        return {"command": " ".join(cmd[3:]), "status": "unavailable", "output": type(e).__name__}
    command = "git diff --stat " + " ".join([a, b, "--", *paths])
    if p.returncode != 0:
        return {"command": command, "status": "unavailable", "output": p.stderr.strip()[:200]}
    out = p.stdout.strip()
    return {"command": command, "status": "differs" if out else "identical", "output": out}


def stall_finding(adr: Path) -> str:
    """The open Connect-stall finding, verbatim from ADR-0020 (the text under its heading)."""
    lines = adr.read_text().splitlines()
    try:
        start = lines.index(STALL_HEADING) + 1
    except ValueError as e:
        raise ValueError(f"{adr}: heading {STALL_HEADING!r} not found") from e
    body: list[str] = []
    for ln in lines[start:]:
        if ln.startswith("#"):
            break
        body.append(ln)
    text = "\n".join(body).strip()
    if not text:
        raise ValueError(f"{adr}: the open finding has no text")
    return text


# %% [markdown]
# ## Rendering


# %%
def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def _mmm_s(d: Mapping[str, Any]) -> str:
    return f"{report.fmt_s(d['median'])} / {report.fmt_s(d['min'])} / {report.fmt_s(d['max'])}"


def _mmm_min(d: Mapping[str, Any]) -> str:
    return f"{report.fmt_min(d['median'])} / {report.fmt_min(d['min'])} / {report.fmt_min(d['max'])}"


def _sink_stat(d: Mapping[str, Any], unit: str) -> str:
    if not d["n"]:
        return "-"
    return f"{(_mmm_s if unit == 's' else _mmm_min)(d)} (n={d['n']})"


def render_md(
    r2: Mapping[str, Any],
    r4: Mapping[str, Any],
    cells: Mapping[str, str],
    stall: str,
    forget_diff: Mapping[str, str],
) -> str:
    L: list[str] = []
    L.append("# G11a results (reduced): R2 and R4 from existing runs, R1 deferred")
    L.append("")
    L.append(
        "Generated by `notebooks/g11a_results.py` from the committed manifests listed in "
        "`docs/results/g11a-inputs.json`. Nothing was measured by this notebook and no run was made for it. "
        "One machine (Apple M1 Pro, Docker Desktop VM with 4 CPUs and 8 GB). Scope and decisions: "
        "[ADR-0024](../adr/0024-g11a-r2-r4-from-existing-runs.md)."
    )
    L.append("")
    L.append("## R1: freshness per sink vs achieved change rate")
    L.append("")
    L.append("not measured; deferred (ADR-0024)")
    L.append("")
    L.append("## R2: deletion, logical and physical absence lag")
    L.append("")
    L.append(
        "Per forget request, on the Postgres clock: the logical lag is the time from the request until every sink "
        "was verified absent by query; the physical lag is the time from the request until the maintenance "
        "window's verified-absent time for every location. Lags are in seconds (logical) and minutes (physical). "
        "Topics are compacting (the g8 setting, which broker removal needs). The physical lag is measured in this "
        "schedule (set by when the g8 check's maintenance window ran) and is not a minimum latency. "
        "Each run has 5 ad-hoc requests submitted within "
        f"{r2['spread_s']:.1f} s of each other (not independent) and 1 generator-owned subject. "
        "Statistics are median / min / max and n; no tail statistic is reported at this sample size."
    )
    L.append("")
    L.append(
        f"Forget code identity between the two code SHAs: `{forget_diff['command']}` gave "
        f"**{forget_diff['status']}**" + (f" ({forget_diff['output']})" if forget_diff["output"] else "") + ". "
        "Between those SHAs G10a added the reconciler, a fault harness and Spark fault hooks (new hooks act "
        "only when the fault file names them); the forget code and `scripts/g8_check.py` did not change. "
        "Host sleep during both run windows was checked by hand against `pmset -g log` and is recorded in "
        "ADR-0024; it is not computed here because the power log rotates."
    )
    L.append("")
    rows: list[list[Any]] = []
    for r in r2["runs"]:
        rows.append(
            [r["label"], r["code"], "all requests", r["n_requests"], _mmm_s(r["logical_s"]), _mmm_min(r["physical_s"])]
        )
        rows.append(
            [
                r["label"],
                r["code"],
                "ad-hoc, batched",
                r["n_batched"],
                _mmm_s(r["batched_logical_s"]),
                _mmm_min(r["batched_physical_s"]),
            ]
        )
        rows.append(
            [
                r["label"],
                r["code"],
                "generator-owned",
                r["n_generator_owned"],
                _mmm_s(r["generator_logical_s"]),
                _mmm_min(r["generator_physical_s"]),
            ]
        )
    p = r2["pooled"]
    rows.append(["pooled", "-", "all requests", r2["n_requests"], _mmm_s(p["logical_s"]), _mmm_min(p["physical_s"])])
    L.append(
        _table(
            [
                "Run",
                "Code",
                "Requests",
                "n",
                "Logical lag, s (median / min / max)",
                "Physical lag, min (median / min / max)",
            ],
            rows,
        )
    )
    L.append("")
    L.append(
        "Per-sink summaries are the manifests' own per-run values (median / min / max over requests, with n). "
        "A '-' under logical means the manifest records no logical value for that sink. "
        "`bounded` counts requests for which the location is bounded, not removed."
    )
    for r in r2["runs"]:
        L.append("")
        L.append(
            f"### Sinks, {r['label']} ([{r['manifest']}]({'../../runs/manifests/' + r['manifest']}), code {r['code']})"
        )
        L.append("")
        L.append(
            _table(
                ["Sink", "Logical lag, s", "Physical lag, min", "bounded"],
                [
                    [n, _sink_stat(s["logical"], "s"), _sink_stat(s["physical"], "min"), s["bounded"] or "-"]
                    for n, s in r["sinks"].items()
                ],
            )
        )
    L.append("")
    L.append(f"Bounded, not removed: {', '.join(r2['bounded_locations'])}.")
    L.append("")
    L.append("## R4: idempotency after replay and crash")
    L.append("")
    t = r4["totals"]
    L.append(
        f"Idempotent under the tested faults: {t['dupes']} duplicates, {t['losses']} losses and "
        f"{t['resurrections']} resurrections in {r4['n_runs']} runs ({r4['n_g10a_runs']} full fault runs, one per "
        f"topic setting, {r4['n_fault_executions']} fault executions; {r4['n_g3_runs']} g3 kill run, "
        f"{r4['n_kills']} SIGKILLs). Silver only: bronze holds duplicate rows by design (F1 replay ledgers "
        + " and ".join(str(x["bronze_f1_ledger"]) for x in r4["g10a"])
        + "). F4's retry ran with no concurrent writers. F5 is a graceful restart of a dev-container broker. "
        "A zero is a count in these runs, not a rate."
    )
    L.append("")
    grows = [
        [
            g["label"],
            ", ".join(g["faults"]),
            g["executions"],
            g["dupes"],
            g["losses"],
            g["resurrections"],
            report.outcome_text(g["outcomes"]),
        ]
        for g in r4["groups"]
    ]
    for k in r4["kill"]:
        grows.append(
            [
                "g3 kill",
                ", ".join(k["kills"]),
                k["n_kills"],
                k["dupes"],
                k["losses"],
                k["resurrections"],
                f"ground-truth compare; also {k['differing']} differing, {k['extra_live']} extra live",
            ]
        )
    grows.append(
        ["total", "-", r4["n_fault_executions"] + r4["n_kills"], t["dupes"], t["losses"], t["resurrections"], "-"]
    )
    L.append(
        _table(
            ["Group", "Faults or kills", "Executions or kills", "Duplicates", "Losses", "Resurrections", "Outcomes"],
            grows,
        )
    )
    for run in r4["g10a"]:
        L.append("")
        L.append(
            f"### Faults, {run['label']} topics ([{run['manifest']}]({'../../runs/manifests/' + run['manifest']}), code {run['code']})"
        )
        L.append("")
        frows = []
        for fid, f in run["faults"].items():
            fired = "-" if f["expected_detection_fired"] is None else str(f["expected_detection_fired"]).lower()
            frows.append([fid, f["dupes"], f["losses"], f["resurrections"], f["outcome"], fired])
        L.append(
            _table(["Fault", "Duplicates", "Losses", "Resurrections", "Outcome", "Expected detection fired"], frows)
        )
    for k in r4["kill"]:
        L.append("")
        L.append(
            f"g3 kill phase: [{k['manifest']}]({'../../runs/manifests/' + k['manifest']}), code {k['code']}: "
            f"{k['n_kills']} SIGKILLs ({', '.join(k['kills'])}); silver compared with the ground truth: "
            f"{k['losses']} missing, {k['dupes']} duplicates, {k['resurrections']} resurrected, "
            f"{k['differing']} differing, {k['extra_live']} extra live, {k['tombstone_pii']} tombstone PII."
        )
    L.append("")
    L.append("### Open finding beside R4 (verbatim from ADR-0020)")
    L.append("")
    L.extend("> " + ln if ln else ">" for ln in stall.splitlines())
    L.append("")
    L.append(
        "Counting rule (ADR-0024): a future run where the stall recurs stays in the denominator, its F5 outcome is "
        "gate_failed, and its losses are reported as observed. Neither run of record here had the stall."
    )
    L.append("")
    L.append("## Exact README cells")
    L.append("")
    L.append("`make g11a-results` fails if these differ from `README.md`.")
    for key, title in (
        ("r1", "R1 result"),
        ("r2", "R2 result"),
        ("r2_manifests", "R2 manifests"),
        ("r4", "R4 result"),
        ("r4_manifests", "R4 manifests"),
    ):
        L.append("")
        L.append(f"{title}:")
        L.append("")
        L.append("```text")
        L.append(cells[key])
        L.append("```")
    L.append("")
    return "\n".join(L)


# %% [markdown]
# ## Checks on everything written, and the README comparison


# %%
def scan_text(name: str, text: str) -> list[str]:
    """Wording (G10a and G11a rules), 13-digit ids (epoch ms and raw ids) over one string that will be written."""
    out = [f"{name}: {v}" for v in wording_violations(text)]
    out += [f"{name}: {v}" for v in report.wording_violations_g11a(text)]
    out += [f"{name}: 13-digit number {m.group(0)!r}" for m in ID13.finditer(text)]
    return out


def readme_rows(text: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in text.splitlines():
        m = re.match(r"\|\s*(R[1-4])\s*\|", line)
        if m:
            rows[m.group(1)] = [c.strip() for c in line.strip().strip("|").split("|")]
    return rows


def check_readme(readme: Path, cells: Mapping[str, str]) -> list[str]:
    rows = readme_rows(readme.read_text())
    problems = []
    want = {
        "R1": (cells["r1"], cells["r1_manifests"]),
        "R2": (cells["r2"], cells["r2_manifests"]),
        "R4": (cells["r4"], cells["r4_manifests"]),
    }
    for rid, (result, manifests) in want.items():
        got = rows.get(rid)
        if got is None or len(got) < 4:
            problems.append(f"README has no complete {rid} row")
            continue
        if got[2] != result:
            problems.append(f"README {rid} result cell differs from the rendered string")
        if got[3] != manifests:
            problems.append(f"README {rid} manifest cell differs from the rendered string")
    return problems


# %% [markdown]
# ## Main


# %%
def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inputs", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--check-readme", action="store_true")
    ap.add_argument("--readme", type=Path, default=ROOT / "README.md")
    ap.add_argument("--base-dir", type=Path, default=ROOT, help="directory the manifest paths are relative to")
    ap.add_argument("--stall-source", type=Path, default=None, help=f"default: {STALL_SOURCE} under --base-dir")
    args = ap.parse_args(argv)
    base: Path = args.base_dir
    inputs = load_inputs(args.inputs)
    con = duckdb.connect()
    r2 = report.r2_summary(read_r2(con, base, inputs["g8"]))
    r4 = report.r4_summary(read_g10a(con, base, inputs["g10a"]), read_g3(con, base, inputs["g3"]))
    problems = sql_cross_check(con, base, inputs, r2, r4)
    forget_diff = git_diff_stat(ROOT, r2["runs"][0]["code"], r2["runs"][-1]["code"], FORGET_CODE_PATHS)
    if forget_diff["status"] == "differs":
        problems.append("forget code differs between the two g8 code SHAs; R2 cannot be pooled as one code version")
    stall = stall_finding(args.stall_source or base / STALL_SOURCE)
    cells = {
        "r1": report.R1_CELL,
        "r1_manifests": report.R1_MANIFEST_CELL,
        "r2": report.readme_r2(r2),
        "r2_manifests": report.readme_r2_manifests(r2),
        "r4": report.readme_r4(r4),
        "r4_manifests": report.readme_r4_manifests(r4),
    }
    md = render_md(r2, r4, cells, stall, forget_diff)
    js = json.dumps(
        {
            "scope": "reduced G11a: R2 and R4 from existing manifests; R1 deferred (ADR-0024)",
            "r1": {"status": "not measured; deferred (ADR-0024)"},
            "r2": r2,
            "r4": r4,
            "forget_code_diff": forget_diff,
            "readme_cells": cells,
            "inputs": {k: inputs[k] for k in ("g8", "g10a", "g3")},
        },
        indent=2,
        sort_keys=True,
    )
    for name, text in [("results.md", md), ("results.json", js), *[(f"cell {k}", v) for k, v in cells.items()]]:
        problems += scan_text(name, text)
    if args.check_readme:
        problems += check_readme(args.readme, cells)
    if problems:
        print("g11a-results FAILED:", *problems, sep="\n  ", file=sys.stderr)
        return 1
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "g11a-results.md").write_text(md)
    (args.out / "g11a-results.json").write_text(js + "\n")
    print(f"g11a-results OK: R2 {r2['n_runs']} runs, R4 {r4['n_runs']} runs; wrote {args.out}/g11a-results.{{md,json}}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
