"""Pure helpers of scripts/g10a_check.py: no Docker, Kafka or Spark."""

import ast
import json
import random
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import g10a_check as g
import pytest
import reconcile

from adrp.forget import ids as fids
from adrp.recon.classify import wording_violations

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "scripts/g10a_check.py"


# ---- wording: every check name and detail template, and the fixed texts --------------------------


def _text(node: ast.expr) -> str | None:
    """A string literal, or an f-string with each placeholder replaced by `X`."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(str(p.value) if isinstance(p, ast.Constant) else "X" for p in node.values)
    if isinstance(node, ast.IfExp):  # "a" if cond else "b": both branches are text
        return " ".join(t for t in (_text(node.body), _text(node.orelse)) if t)
    return None


def check_call_strings() -> list[str]:
    """Every string argument (name and detail) of a `check(...)` / `g3.check(...)` call in g10a_check.py."""
    out = []
    for node in ast.walk(ast.parse(SOURCE.read_text())):
        if isinstance(node, ast.Call):
            f = node.func
            if (isinstance(f, ast.Name) and f.id == "check") or (isinstance(f, ast.Attribute) and f.attr == "check"):
                for arg in [*node.args, *(k.value for k in node.keywords)]:
                    if (t := _text(arg)) is not None:
                        out.append(t)
    return out


def test_the_scan_finds_the_check_strings() -> None:
    names = check_call_strings()
    assert len(names) > 40
    assert any("F4: Connect was SIGKILLed inside the snapshot" in n for n in names)


def test_no_check_name_or_detail_template_breaks_a_wording_rule() -> None:
    bad = {s: v for s in check_call_strings() if (v := wording_violations(s))}
    assert bad == {}


def test_fixed_texts_break_no_wording_rule() -> None:
    texts = [g.SCOPE_NOTE, *g.LIMITS, *g.DETAIL_WORDS.values(), *g.INJECTION_AS_RUN.values(), *g.RECOVERY.values()]
    texts += [cell for row in g.PREREGISTERED.values() for cell in row.values()]
    assert [v for t in texts for v in wording_violations(t)] == []


def test_the_scope_note_is_the_fixed_sentence() -> None:
    assert g.SCOPE_NOTE == "Zero mismatches under the 6 tested faults at this cut; undetected faults may exist."


def test_a_forbidden_claim_would_be_caught_by_the_scan() -> None:
    assert wording_violations("exactly-once delivery") and wording_violations("a row was deleted")
    assert wording_violations("0 silent faults")


# ---- the pre-registered rows are the design rows, verbatim ---------------------------------------


def test_preregistered_rows_are_verbatim_from_design_section_5() -> None:
    lines = (ROOT / "docs/design.md").read_text().splitlines()
    for fid, row in g.PREREGISTERED.items():
        line = next(ln for ln in lines if ln.startswith(f"| {fid} |"))
        for cell in row.values():
            assert cell in line, (fid, cell)


def test_every_fault_has_a_record_template_and_text() -> None:
    assert set(g.FAULTS) <= set(g.PREREGISTERED)
    for fid in g.FAULTS:
        rec = g.new_record(fid)
        assert rec["outcome"] == "not_run" and rec["injection"] and rec["recovery"]
        assert fid in g.EXPECTS_DETECTION and fid in g.FAULT_FNS
    assert g.PHASES == ("F4", "F1", "F2", "F3", "F5", "F6a", "F6b", "END")


# ---- sampler -------------------------------------------------------------------------------------


def test_connector_down_needs_either_clause() -> None:
    assert g.connector_down({g.CONNECT_UP: 0.0})
    assert g.connector_down({g.CONNECT_SRC: 0.0})  # Connect down: the connector series is absent
    assert not g.connector_down({g.CONNECT_UP: 1.0, g.CONNECT_SRC: 1.0})
    assert not g.connector_down({})


def test_window_summary() -> None:
    samples = [
        g.Sample(1.0, True, False, 100.0),
        g.Sample(2.0, True, True, 5000.0),
        g.Sample(3.0, False, False, None),  # exporter silent: not a firing
        g.Sample(9.0, True, True, 1.0),  # outside the window
    ]
    w = g.window_summary(samples, 1.0, 4.0)
    assert w == {
        "samples": 3, "exporter_answered": 2, "connector_status_fired": True,
        "peak_slot_lag_bytes": 5000.0, "slot_lag_grew": True,
    }  # fmt: skip
    quiet = g.window_summary([g.Sample(1.0, False, False, None)], 0.0, 5.0)
    assert quiet["connector_status_fired"] is False and quiet["peak_slot_lag_bytes"] is None


def test_parse_metrics_skips_comments_and_reads_labelled_series() -> None:
    up, src = 'adrp_connector_up{connector="pg-shop"}', 'adrp_exporter_source_up{source="connect"}'
    text = f"# HELP x y\n# TYPE x gauge\n{up} 1.0\n\n{src} 0\n"
    assert g.parse_metrics(text) == {up: 1.0, src: 0.0}


def test_sampler_polls_at_half_a_second_and_never_overlaps_scrapes() -> None:
    assert g.SAMPLE_EVERY_S == 0.5 and g.CdcSampler().every_s == 0.5
    assert g.SCRAPE_TIMEOUT_S <= g.SAMPLE_EVERY_S * 2  # a blocked scrape costs at most two intervals
    assert g.next_wait(0.5, 10.0, 10.04) == pytest.approx(0.46)  # fixed cadence
    assert g.next_wait(0.5, 10.0, 11.0) == 0.0  # a slow scrape: no extra sleep, and the next starts only after it ended


def test_a_four_second_window_holds_at_least_three_samples_at_the_polling_interval() -> None:
    every = g.SAMPLE_EVERY_S
    # next_wait subtracts the scrape time, so cycles start `every` apart
    samples = [g.Sample(i * every, True, False, 1.0) for i in range(40)]
    assert g.window_summary(samples, 3.1, 7.1)["exporter_answered"] >= 3


# ---- END: compaction ---------------------------------------------------------------------------


def _offset_result(present: int, distinct: int) -> Any:
    return SimpleNamespace(kafka_present=present, distinct=distinct)


def test_compaction_effect_uses_real_end_offsets_not_bronze_distinct() -> None:
    topics = {"t.accounts": _offset_result(633, 14259), "t.refunds": _offset_result(10, 10)}
    ends = {"t.accounts": {0: 7000, 1: 7259}, "t.refunds": {0: 10}}
    effect = g.compaction_effect(topics, ends)
    assert effect == {"t.accounts": (633, 14259), "t.refunds": (10, 10)}
    assert g.compaction_removed(effect)
    # bronze distinct below the end must not be read as the end: present == distinct here, the end is larger
    assert g.compaction_removed(g.compaction_effect({"t": _offset_result(5, 5)}, {"t": {0: 8}}))


def test_compaction_removed_is_false_when_nothing_was_removed_or_nothing_was_read() -> None:
    assert not g.compaction_removed({"a": (10, 10), "b": (0, 0)})
    assert not g.compaction_removed({})
    assert g.compaction_effect({"t": _offset_result(3, 3)}, {}) == {"t": (3, 0)}  # missing end: not a removal


def test_compaction_detail_is_counts_only_and_clean() -> None:
    d = g.compaction_detail({"t.accounts": (633, 14259)})
    assert "633 of 14259" in d and not wording_violations(d)
    assert g.compaction_detail({}) == "no END gate report"


# ---- F1: checkpoint rewind and bronze ranges -----------------------------------------------------


def test_rewind_removes_everything_above_m_with_crc_siblings_and_uncommitted_offsets() -> None:
    commits = [".", "..", "1", ".1.crc", "2", ".2.crc", "3", ".3.crc", "4", ".4.crc", "5", ".5.crc"]
    offsets = [*commits, "6", ".6.crc"]  # offsets/6 was written but never committed
    plan = g.rewind_paths("/checkpoints/silver", commits, offsets, 3)
    assert plan is not None
    m, n, paths = plan
    assert (m, n) == (2, 5)
    assert "/checkpoints/silver/offsets/6" in paths and "/checkpoints/silver/offsets/.6.crc" in paths
    assert "/checkpoints/silver/commits/3" in paths and "/checkpoints/silver/commits/.5.crc" in paths
    assert not any(p.endswith(("/2", "/.2.crc", "/1", "/.1.crc")) for p in paths)
    assert len(paths) == 2 * 6 + 2


def test_rewind_needs_enough_committed_batches() -> None:
    assert g.rewind_paths("/c", ["0", "1"], ["0", "1"], 3) is None
    assert g.rewind_paths("/c", [".crc"], [], 1) is None
    assert g.rewind_paths("/c", ["3"], ["3"], 3) == (0, 3, ["/c/commits/3", "/c/offsets/3"])


def test_newest_offsets_file_ignores_crc_and_tmp() -> None:
    assert g.newest_offsets_file(["1", ".1.crc", "7", "9.tmp", "."]) == 7
    assert g.newest_offsets_file([]) is None


def test_replayed_ranges_and_size() -> None:
    start = {"t": {0: 10, 1: 5}}
    old_end = {"t": {0: 25, 1: 5, 2: 4}, "u": {0: 3}}
    r = g.replayed_ranges(start, old_end)
    assert r == {"t": {0: (10, 25), 2: (0, 4)}, "u": {0: (0, 3)}}  # an unread partition restarts at 0; empty ones drop
    assert g.range_size(r) == 15 + 4 + 3


def test_last_n_ranges() -> None:
    assert g.last_n_ranges({0: 500, 1: 120, 2: 0}, 200) == {"0": [300, 500], "1": [0, 120]}


def test_rerun_batches_counts_only_new_reused_batch_ids_with_events() -> None:
    before = {(5, "customers", "t1")}
    entries: list[dict[str, Any]] = [
        {"batch_id": 5, "table": "customers", "written_at": "t1", "n_events": 3},  # the original
        {"batch_id": 4, "table": "customers", "written_at": "t2", "n_events": 2},  # re-run of an old id
        {"batch_id": 4, "table": "accounts", "written_at": "t2", "n_events": 0},  # nothing carried
        {"batch_id": 9, "table": "customers", "written_at": "t3", "n_events": 7},  # a new id
    ]
    assert g.rerun_batches(before, entries, 5) == [4]


# ---- F2 ------------------------------------------------------------------------------------------


def _v(table: str, p: int, off: int, key: bytes, lsn: int | None, op: str | None = "u") -> g.Version:
    return g.Version(table, p, off, key, None if op is None else b"v", 1, lsn, op)


def test_pick_older_returns_the_lowest_lsn_of_keys_with_two_versions() -> None:
    vs = [
        _v("customers", 0, 1, b"a", 10, "c"), _v("customers", 0, 2, b"a", 20), _v("customers", 0, 3, b"a", 30),
        _v("customers", 1, 4, b"b", 15), _v("customers", 1, 5, b"b", 15),  # same LSN twice: not out of order
        _v("accounts", 0, 6, b"c", 5), _v("accounts", 0, 7, b"c", 9),
        _v("accounts", 0, 8, b"d", 7, None), _v("accounts", 0, 9, b"d", None, None),  # a tombstone never qualifies
    ]  # fmt: skip
    got = g.pick_older(vs, random.Random(42), 3)
    assert {(v.table, v.key, v.lsn) for v in got} == {("customers", b"a", 10), ("accounts", b"c", 5)}
    assert all(v.value is not None for v in got)
    assert g.pick_older(vs, random.Random(42), 3) == got  # seeded


def test_pick_older_limits_keys_per_table() -> None:
    vs = [_v("customers", 0, 2 * i, bytes([i]), 1) for i in range(10)] + [
        _v("customers", 0, 2 * i + 1, bytes([i]), 2) for i in range(10)
    ]
    assert len(g.pick_older(vs, random.Random(1), 3)) == 3
    assert [v.lsn for v in g.pick_older(vs, random.Random(1), 3)] == [1, 1, 1]


def test_changes_seen_matches_table_key_and_lsn_in_new_entries_only() -> None:
    before = {(1, "customers", "old")}
    entries = [
        {"batch_id": 1, "table": "customers", "written_at": "old", "changes": [{"pk": '{"id":7}', "lsn": 50}]},
        {
            "batch_id": 2,
            "table": "customers",
            "written_at": "new",
            "changes": [{"pk": '{"id":7}', "lsn": 50}, {"pk": "garbage", "lsn": 1}],
        },
        {"batch_id": 2, "table": "accounts", "written_at": "new", "changes": [{"pk": '{"id":8}', "lsn": 60}]},
    ]
    want = {("customers", 7): 50, ("accounts", 8): 61, ("accounts", 9): 1}
    assert g.changes_seen(entries, before, want) == 1  # accounts 8 has another LSN; 9 is absent
    assert g.changes_seen(entries[:1], before, {("customers", 7): 50}) == 0  # only the old entry


def test_expected_events_counts_a_tombstone_after_each_delete() -> None:
    ops = [SimpleNamespace(table="a", op="I"), SimpleNamespace(table="a", op="D"), SimpleNamespace(table="b", op="U")]
    assert g.expected_events(ops) == {"a": 3, "b": 1}


# ---- F4 ------------------------------------------------------------------------------------------


def test_dup_r_keys_counts_keys_with_two_snapshot_events() -> None:
    recs: list[tuple[int, bytes | None, str | None]] = [
        (0, b"a", "r"), (0, b"a", "r"), (0, b"a", "u"),  # a: two r
        (0, b"b", "r"), (1, b"b", "r"),  # same key bytes on two partitions are two keys
        (0, b"c", "r"), (0, b"c", None), (0, None, "r"),
    ]  # fmt: skip
    assert g.dup_r_keys(recs) == 1


# ---- scoring -------------------------------------------------------------------------------------


def test_a_dirty_gate_is_never_a_pass_even_when_classify_says_caught() -> None:
    cls, outcome, passed = g.score_fault("F5", False, ["connector_status"])
    assert cls == "caught" and outcome == "gate_failed" and not passed


def test_clean_gate_outcomes() -> None:
    assert g.score_fault("F1", True, []) == ("self_healed", "self_healed", True)
    assert g.score_fault("F4", True, ["connector_status"]) == ("caught", "caught", True)
    assert g.score_fault("F1", False, []) == ("silent", "gate_failed", False)


def test_f6b_must_be_caught() -> None:
    assert g.score_fault("F6b", True, ["quarantine_count"])[2]
    assert not g.score_fault("F6b", True, [])[2]  # self-healed is not enough


def test_counts_prefer_the_reconciler_then_the_oracle() -> None:
    t = {
        "customers": SimpleNamespace(duplicates=1, missing=2, resurrected=3),
        "accounts": SimpleNamespace(duplicates=0, missing=1, resurrected=0),
    }
    report = SimpleNamespace(tables=t)
    assert g.counts_from(report, None) == {"dupes": 1, "losses": 3, "resurrections": 3}
    assert g.counts_from(None, {"duplicates": 4, "missing": 5, "resurrected": 6}) == {
        "dupes": 4,
        "losses": 5,
        "resurrections": 6,
    }
    assert g.counts_from(None, None) == {"dupes": None, "losses": None, "resurrections": None}


# ---- the rendered table --------------------------------------------------------------------------


def _manifest(mode: str, outcomes: dict[str, str]) -> dict[str, Any]:
    faults = {}
    for fid in g.FAULTS:
        f = g.new_record(fid)
        f.update(
            outcome=outcomes.get(fid, "self_healed"),
            residual="2 items",
            detections_fired=["connector_status"] if fid == "F4" else [],
        )
        faults[fid] = f
    checks = [{"check": "F4 reconcile: no __debezium_unavailable_value in silver", "ok": True, "detail": ""}]
    return {
        "git_sha": "abcdef0123456789",
        "result": {"topic_mode": mode, "passed": True, "faults": faults, "checks": checks, "limits": g.LIMITS},
    }


def test_rendered_table_is_clean_and_complete() -> None:
    text = g.render_table([("a.json", _manifest("default", {"F6b": "caught"})), ("b.json", _manifest("compact", {}))])
    assert wording_violations(text) == []
    assert g.SCOPE_NOTE in text
    assert text.count("| F6b |") == 2 and text.count("| F13 |") == 2
    assert "0 silent corruption detected by the reconciler" in text
    assert "self-healed in this run" in text
    assert "Topic setting: default" in text and "Topic setting: compact" in text


def test_a_silent_fault_is_counted_in_the_table() -> None:
    text = g.render_table([("a.json", _manifest("default", {"F2": "silent"}))])
    assert "1 silent corruption detected by the reconciler" in text


def test_rendered_table_survives_json_round_trip() -> None:
    m = json.loads(json.dumps(_manifest("default", {})))
    assert "F1" in g.render_table([("m.json", m)])


# ---- identifiers ---------------------------------------------------------------------------------


def test_a_thirteen_digit_run_is_found_and_a_hex64_checksum_is_not() -> None:
    from adrp.recon.canonical import hex64

    assert fids.ID13.search(f"id {g.ADHOC_ARTICLE}")
    assert not any(fids.ID13.search(hex64(n)) for n in (10**13, 1234567890123456789, 2**64 - 1))


@pytest.mark.parametrize("fid", g.FAULTS)
def test_injection_and_recovery_texts_name_no_identifier(fid: str) -> None:
    assert not fids.ID13.search(g.INJECTION_AS_RUN[fid] + g.RECOVERY[fid])


# ---- F5 liveness ---------------------------------------------------------------------------------


def test_wait_flush_advanced_needs_the_slot_to_move_not_the_connector_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: F5 only checked that the connector and task read RUNNING. A Connect worker whose offset-store
    consumer spun after the broker restart stayed RUNNING while confirmed_flush_lsn sat frozen."""
    seen = iter(["0/3440C30", "0/3440C30", "0/3440C31"])
    monkeypatch.setattr(reconcile, "confirmed_flush_text", lambda: next(seen))
    monkeypatch.setattr(g, "wait_until", lambda fn, timeout, every: (_poll(fn, 3), 0.0))
    ok, last, _ = g.wait_flush_advanced("0/3440C30", 150.0)
    assert ok and last == "0/3440C31"

    frozen = iter(["0/3440C30"] * 5)
    monkeypatch.setattr(reconcile, "confirmed_flush_text", lambda: next(frozen))
    ok, last, _ = g.wait_flush_advanced("0/3440C30", 150.0)
    assert not ok and last == "0/3440C30"

    monkeypatch.setattr(reconcile, "confirmed_flush_text", lambda: None)  # no slot row: not a pass
    assert not g.wait_flush_advanced(None, 150.0)[0]


def _poll(fn: Any, tries: int) -> bool:
    return any(fn() for _ in range(tries))
