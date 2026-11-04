"""Workflow pieces that need no live stack: entity maps, read-only file residuals, the embedder state script, the
Spark CLI wrapper's parsing, idempotence and window safety."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from adrp.forget import audit, postgres, sparkcli, workflow
from adrp.forget.audit import RequestRow
from adrp.forget.workflow import Config, ForgetSubject

RAW = 2_000_000_000_123
KEY = "unit-test-key"
IDS = {"customers": [RAW], "support_tickets": [RAW + 1, RAW + 2], "transactions": [77]}


def subj(rid: str = "r1") -> ForgetSubject:
    return ForgetSubject(rid, RAW, ids=dict(IDS), canaries=("g8c-" + "a" * 32,), pii=("who@example.org",))


# ---- maps and subjects -----------------------------------------------------------------------------------


def test_entity_maps_are_deterministic_and_free_of_ids() -> None:
    smap, imap = workflow.entity_maps(KEY, IDS)
    assert (
        set(smap)
        == set(imap)
        == {f"customers:{RAW}", f"support_tickets:{RAW + 1}", f"support_tickets:{RAW + 2}", "transactions:77"}
    )
    assert all(v.startswith("p:") and str(RAW) not in v for v in smap.values())
    assert all(v < 0 for v in imap.values())
    assert smap == workflow.entity_maps(KEY, IDS)[0]
    assert smap != workflow.entity_maps("other-key", IDS)[0]


def test_union_subject_merges_ids_and_tokens() -> None:
    a, b = subj("a"), subj("b")
    b.ids = {"customers": [5], "transactions": [77, 78]}
    u = workflow.union_subject([a, b])
    assert u.entities["transactions"] == [77, 78]
    assert sorted(u.entities["customers"]) == [5, RAW]
    assert "who@example.org" in u.canaries and any(c.startswith("g8c-") for c in u.canaries)


def test_expected_min_rows_counts_touched_rows_plus_placeholder() -> None:
    from adrp.forget.postgres import ForgetTxn

    s = subj()
    s.txn = ForgetTxn(
        True, 5, "0/10", "0/20", 9, {"accounts": 1, "transactions": 4, "support_tickets": 2, "customers": 1}
    )
    assert workflow._expected_min_rows(s) == 9


# ---- read-only residual counters ---------------------------------------------------------------------------


def write_logs(tmp_path: Path) -> Config:
    ev = tmp_path / "eventlog" / "silver_batches"
    ev.mkdir(parents=True)
    changes = pa.array(
        [[{"pk": json.dumps({"id": RAW + 1}, separators=(",", ":")), "lsn": 1}, {"pk": '{"id":5}', "lsn": 2}]],
        type=pa.list_(pa.struct([("pk", pa.string()), ("lsn", pa.int64())])),
    )
    pq.write_table(pa.table({"table": ["support_tickets"], "changes": changes}), ev / "part-0.parquet")
    gt = tmp_path / "data" / "run1" / "ground_truth"
    gt.mkdir(parents=True)
    pq.write_table(
        pa.table({"table": ["transactions", "transactions"], "pk": [77, 78], "row_json": ["{}", "{}"]}),
        gt / "p.parquet",
    )
    return Config(eventlog=tmp_path / "eventlog", runs_data=tmp_path / "data")


def test_residual_counters_count_and_never_write(tmp_path: Path) -> None:
    cfg = write_logs(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*.parquet")}
    res = workflow.file_residual(KEY, IDS, cfg)
    assert res["silver_batches"]() == 1
    assert res["ground_truth"]() == 1
    assert before == {p: p.read_bytes() for p in tmp_path.rglob("*.parquet")}  # a presence check must not mutate


# ---- the embedder state script (runs in a throwaway container; here with a temp path) ----------------------


def run_script(tmp: Path, req: dict[str, Any]) -> dict[str, Any]:
    script = workflow.EMBEDDER_SCRIPT.replace("/state/embedder.json", str(tmp / "embedder.json"))
    r = subprocess.run(
        [sys.executable, "-c", script], input=json.dumps(req), capture_output=True, text=True, check=True
    )
    return json.loads(r.stdout.splitlines()[-1])


def test_embedder_script_count_rewrite_pointer(tmp_path: Path) -> None:
    ent = f"support_tickets:{RAW + 1}"
    state = {"snapshot": 11, "pending": {ent: 5, "help_articles:1": 6}, "since": {ent: 1.0}, "batch": 3}
    (tmp_path / "embedder.json").write_text(json.dumps(state))
    mapping = {ent: "p:abc"}
    c = run_script(tmp_path, {"op": "count", "mapping": mapping})
    assert c == {"exists": True, "changed": 2, "snapshot": 11, "pending": 2, "pending_footprint": 1}
    assert json.loads((tmp_path / "embedder.json").read_text()) == state  # count does not write
    r = run_script(tmp_path, {"op": "rewrite", "mapping": mapping, "snapshot": 99})
    new = json.loads((tmp_path / "embedder.json").read_text())
    assert r["snapshot"] == 99 and new["snapshot"] == 99 and "p:abc" in new["pending"] and ent not in new["pending"]
    assert str(RAW) not in json.dumps(new)
    assert run_script(tmp_path, {"op": "count", "mapping": mapping})["changed"] == 0  # second pass: clean


def test_embedder_script_without_state() -> None:
    script = workflow.EMBEDDER_SCRIPT.replace("/state/embedder.json", "/nonexistent/embedder.json")
    r = subprocess.run(
        [sys.executable, "-c", script], input='{"op":"count"}', capture_output=True, text=True, check=True
    )
    assert json.loads(r.stdout)["exists"] is False


# ---- sparkcli --------------------------------------------------------------------------------------------


def test_lsn_to_int() -> None:
    assert sparkcli.lsn_to_int("0/1200A208") == 0x1200A208
    assert sparkcli.lsn_to_int("1/0") == 1 << 32


def test_footprint_roundtrip_and_shape() -> None:
    fp = sparkcli.to_footprint({"support_tickets": [3, 1], "accounts": []})
    assert fp == {"public.support_tickets": ['{"id":1}', '{"id":3}']}
    assert sparkcli.from_footprint(fp) == {"support_tickets": [1, 3]}
    assert sparkcli.merge_ids({"a": [1]}, {"a": [1, 2], "b": [3]}) == {"a": [1, 2], "b": [3]}


def test_parse_output_and_mask() -> None:
    rows, res = sparkcli.parse_output('noise\nROW\t12\tpublic.accounts\t{"id":3}\nRESULT {"total": 1}\n')
    assert rows == [["12", "public.accounts", '{"id":3}']] and res == {"total": 1}
    assert "2000000000123" not in sparkcli.mask("failed for 2000000000123 and 99")


def test_total_residue() -> None:
    res = {
        "lake.bronze.cdc_events": {"snapshots": 1, "delete_files_affected": 0, "non_main_refs": []},
        "lake.silver.customers": {"snapshots": 3, "delete_files_affected": 2, "non_main_refs": ["t1"]},
    }
    assert sparkcli.total_residue(res) == {"delete_files_affected": 2, "non_main_refs": 1, "extra_snapshots": 2}


def test_logical_sends_pk_strings_and_no_extra_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_call(script: str, cmd: str, *args: str, **kw: Any) -> tuple[list[list[str]], dict[str, Any]]:
        seen.update(cmd=cmd, groups=json.loads(args[1]))
        return [], {"x": {"bronze_rows": 0}}

    monkeypatch.setattr(sparkcli, "call", fake_call)
    out = sparkcli.logical({"x": {"footprint": {"customers": [RAW]}, "customers": [RAW], "txid": 7, "lsn": 9}})
    assert out == {"x": {"bronze_rows": 0}} and seen["cmd"] == "logical"
    assert seen["groups"]["x"] == {
        "footprint": {"public.customers": [f'{{"id":{RAW}}}']},
        "customers": [RAW],
        "txid": 7,
        "lsn": 9,
    }


# ---- idempotence and window safety -----------------------------------------------------------------------


def row(status: str) -> RequestRow:
    return RequestRow("r1", status, 1.0, None, None, None, True)


def boom(*a: Any, **k: Any) -> Any:
    raise AssertionError("must not be called for a finished request")


def test_rerunning_a_finished_request_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audit, "get_request", lambda rid: row("physical_done"))
    monkeypatch.setattr(audit, "hmac_key", lambda env=None: KEY)
    monkeypatch.setattr(workflow, "capture", boom)
    monkeypatch.setattr(postgres, "forget_customer", boom)
    monkeypatch.setattr(sparkcli, "delete_events", boom)
    s = subj()
    lrep = workflow.run_logical([s], Config(log=lambda m: None))
    assert lrep.skipped == ["r1"] and lrep.ok
    wrep = workflow.run_physical_window([s], Config(log=lambda m: None))
    assert wrep.skipped == ["r1"] and wrep.ok
    prep = workflow.run_resweep_and_purge([s], Config(log=lambda m: None))
    assert prep.skipped == ["r1"] and prep.ok


def test_physical_phase_refuses_a_request_that_is_not_logical_done(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audit, "get_request", lambda rid: row("requested"))
    monkeypatch.setattr(audit, "hmac_key", lambda env=None: KEY)
    with pytest.raises(workflow.WorkflowError):
        workflow.run_physical_window([subj()], Config(log=lambda m: None))


def test_window_restarts_what_it_stopped_even_when_the_body_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class P:
        rc = 0
        err_text = ""

    def fake_run(argv: list[str], **kw: Any) -> P:
        calls.append(argv[-2] + " " + argv[-1])
        return P()

    monkeypatch.setattr(workflow, "run", fake_run)
    monkeypatch.setattr(workflow, "service_running", lambda s: True)
    with pytest.raises(RuntimeError), workflow.maintenance_window(Config(log=lambda m: None)) as w:
        w.stop("embedder")
        raise RuntimeError("step failed")
    assert calls[:2] == ["stop spark-streams", "stop embedder"]
    assert calls[2:4] == ["start embedder", "start spark-streams"]  # reverse order, both restarted
