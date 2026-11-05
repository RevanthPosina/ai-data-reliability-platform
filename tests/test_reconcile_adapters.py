"""Pure parsing and report logic of scripts/reconcile.py. No Docker, Kafka or Spark."""

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import g2_check
import g3_check
import pytest
import reconcile as rc
from _env import ROOT

from adrp.forget import broker
from adrp.forget import ids as fids
from adrp.recon.canonical import hex64
from adrp.recon.classify import wording_violations
from adrp.recon.compare import FAILING, TableResult, compare_table
from adrp.recon.offsets import compare_offsets

TOPICS = [broker.topic_for(t) for t in rc.TABLES]
COLS = [("id", "bigint"), ("name", "text"), ("amount", "numeric")]


# ---- RangeTracker -------------------------------------------------------------------------------------------------


def test_tracker_closes_at_eof_even_when_end_minus_one_was_compacted_away() -> None:
    t = rc.RangeTracker({}, {0: 10})
    assert t.see(0, 3) and t.see(0, 7)
    assert not t.done  # offset 9 never arrives (compacted), so only EOF can close the partition
    t.eof(0)
    assert t.done


def test_tracker_closes_when_the_last_offset_is_seen_and_ignores_records_outside_the_range() -> None:
    t = rc.RangeTracker({0: 5}, {0: 10, 1: 4})
    assert not t.see(0, 4)  # below start
    assert t.see(0, 5) and t.open == {0, 1}
    assert t.see(0, 9)
    assert t.open == {1}
    assert not t.see(1, 4)  # at end: outside, and it closes the partition
    assert t.done


def test_tracker_ignores_empty_and_already_covered_partitions() -> None:
    assert rc.RangeTracker({}, {0: 0, 1: 0}).done
    assert rc.RangeTracker({0: 8}, {0: 8}).done
    assert rc.RangeTracker({0: 8}, {0: 5}).done


# ---- probe result parsing -----------------------------------------------------------------------------------------


def test_expand_ranges_and_bad_range() -> None:
    assert rc.expand_ranges([[0, 2], [5, 5], [9, 10]]) == {0, 1, 2, 5, 9, 10}
    assert rc.expand_ranges([]) == set()
    with pytest.raises(ValueError):
        rc.expand_ranges([[4, 3]])


def test_bronze_inputs_with_gaps_and_duplicates() -> None:
    entry = {
        "0": {"rows": 7, "distinct": 6, "ranges": [[0, 3], [10, 11]]},
        "2": {"rows": 0, "distinct": 0, "ranges": []},
    }
    rows, distinct = rc.bronze_inputs(entry)
    assert rows == {0: 7, 2: 0}
    assert distinct == {0: {0, 1, 2, 3, 10, 11}, 2: set()}
    assert rc.dup_rows(rows, distinct) == 1


def test_bronze_inputs_rejects_a_distinct_count_that_disagrees_with_the_ranges() -> None:
    with pytest.raises(ValueError):
        rc.bronze_inputs({"0": {"rows": 3, "distinct": 4, "ranges": [[0, 2]]}})


def test_parse_quarantine_takes_the_highest_lsn_per_key() -> None:
    pairs = [['{"id":7}', 100], ['{"id":7}', 250], ['{"id":9}', 5]]
    assert rc.parse_quarantine(pairs) == {7: 250, 9: 5}
    assert rc.parse_quarantine([]) == {}
    with pytest.raises(ValueError):
        rc.parse_quarantine([["not json", 1]])


def test_rebuild_summary_uses_hex64_and_marks_errors_and_missing_entries() -> None:
    ok = rc.rebuild_summary(
        {"rows": 5, "checksum": 2**64 - 1, "rebuilt_rows": 5, "rebuilt_checksum": 2**64 - 1, "equal": True}
    )
    assert ok == {
        "rows": 5, "equal": True, "checksum": "ffffffff_ffffffff", "rebuilt_checksum": "ffffffff_ffffffff",
        "rebuilt_rows": 5,
    }  # fmt: skip
    err = rc.rebuild_summary({"rows": 5, "checksum": 17, "error": "AnalysisException", "equal": False})
    assert err["equal"] is False and err["error"] == "AnalysisException" and "rebuilt_checksum" not in err
    assert rc.rebuild_summary(None) == {"equal": False, "error": "missing"}
    # an entry that claims equal but carries an error is not equal
    assert rc.rebuild_summary({"rows": 1, "checksum": 1, "error": "X", "equal": True})["equal"] is False


# ---- Postgres parsing ---------------------------------------------------------------------------------------------


def test_parse_columns_keeps_multiword_types_in_order() -> None:
    text = "id|bigint\ncreated_at|timestamp with time zone\ncountry|character\n\n"
    assert rc.parse_columns(text) == [
        ("id", "bigint"),
        ("created_at", "timestamp with time zone"),
        ("country", "character"),
    ]


def test_parse_pg_json_lines_parses_floats_as_decimal_and_keeps_ints() -> None:
    text = '{"id":1,"amount":10.10,"n":3,"note":"a\\nb"}\n{"id":2,"amount":0.1}\n'
    rows = rc.parse_pg_json_lines(text)
    assert rows[0]["amount"] == Decimal("10.10") and isinstance(rows[0]["amount"], Decimal)
    assert rows[0]["n"] == 3 and isinstance(rows[0]["n"], int)
    assert rows[0]["note"] == "a\nb"
    assert rows[1]["amount"] == Decimal("0.1")  # not the binary float 0.1000000000000000055...
    assert rc.parse_pg_json_lines("") == []


def test_pg_rows_rejects_other_tables() -> None:
    with pytest.raises(ValueError):
        rc.pg_rows("dbz_heartbeat")


# ---- pause state --------------------------------------------------------------------------------------------------


def test_parse_write_counters() -> None:
    assert rc.parse_write_counters("accounts|12\ncustomers|7\n") == {"accounts": 12, "customers": 7}
    assert rc.parse_write_counters("") == {}


def test_write_counter_sql_lists_the_seven_tables_and_not_the_heartbeat() -> None:
    assert all(f"'{t}'" in rc.WRITE_COUNTERS_SQL for t in rc.TABLES) and len(rc.TABLES) == 7
    assert "dbz_heartbeat" not in rc.WRITE_COUNTERS_SQL


def test_pause_stable_needs_no_generator_and_no_movement() -> None:
    a = rc.PauseState(False, 100, {"customers": 5})
    assert rc.pause_stable(a, rc.PauseState(False, 100, {"customers": 5}))
    assert not rc.pause_stable(a, rc.PauseState(False, 101, {"customers": 5}))  # next_txn moved
    assert not rc.pause_stable(a, rc.PauseState(False, 100, {"customers": 6}))  # a table was written
    assert not rc.pause_stable(a, rc.PauseState(True, 100, {"customers": 5}))  # generator container up
    assert not rc.pause_stable(rc.PauseState(True, 100, {}), rc.PauseState(True, 100, {}))


def test_cut_paused_needs_the_closing_sample() -> None:
    start = rc.PauseState(False, 10, {"customers": 1})
    cut = rc.Cut("0/10", 16, start, pause_settled=True)
    assert not cut.paused and not cut.ok  # no closing sample yet
    cut.pause_end = rc.PauseState(False, 10, {"customers": 1})
    assert cut.paused
    cut.pause_end = rc.PauseState(False, 11, {"customers": 1})
    assert not cut.paused
    cut.pause_end = start
    cut.pause_settled = False
    assert not cut.paused


def test_cut_reached_does_not_need_the_closing_sample_but_ok_does() -> None:
    start = rc.PauseState(False, 10, {"customers": 1})
    cut = rc.Cut("0/10", 16, start, pause_settled=True, flush_reached=True, checkpoints_reached=True)
    assert cut.reached and not cut.ok  # pause_end is only taken after the last read
    cut.pause_end = start
    assert cut.reached and cut.ok
    for name in ("flush_reached", "checkpoints_reached", "pause_settled"):
        broken = rc.Cut("0/10", 16, start, pause_settled=True, flush_reached=True, checkpoints_reached=True)
        setattr(broken, name, False)
        assert not broken.reached


def _stub_cut(monkeypatch: pytest.MonkeyPatch, flush: str, ends: dict[str, int], committed: dict[str, int]) -> None:
    start = rc.PauseState(False, 10, {"customers": 1})
    monkeypatch.setattr(rc, "settle_pause_state", lambda: (start, True))
    monkeypatch.setattr(g2_check, "psql", lambda sql: "0/10")
    monkeypatch.setattr(rc, "confirmed_flush_text", lambda: flush)
    monkeypatch.setattr(g2_check, "end_offsets", lambda c, topic: {int(p): e for p, e in ends.items()})
    monkeypatch.setattr(
        rc, "checkpoint_offsets", lambda q: {t: {int(p): o for p, o in committed.items()} for t in TOPICS}
    )


def test_consistent_cut_still_reads_end_offsets_and_checkpoints_when_the_slot_did_not_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a missed flush returned before any read, so the report showed no end offsets (every bronze offset
    "beyond end") and "lagging partitions {}" for a stall the broker and Spark knew nothing about."""
    _stub_cut(monkeypatch, flush="0/5", ends={"0": 7}, committed={"0": 7})
    cut = rc.consistent_cut(None, timeout=0.0, checkpoint_timeout=60.0)
    assert not cut.flush_reached and not cut.ok and not cut.reached
    assert cut.end and all(v == {0: 7} for v in cut.end.values())
    assert cut.checkpoints_reached and cut.checkpoint_wait_s < 5  # one sample, no wait of 60 s
    assert any("diagnosis only" in n for n in cut.notes)
    _stub_cut(monkeypatch, flush="0/5", ends={"0": 7}, committed={"0": 3})
    lagging = rc.consistent_cut(None, timeout=0.0, checkpoint_timeout=60.0)
    assert not lagging.checkpoints_reached and lagging.lagging() == {"silver": 7, "bronze": 7}
    assert lagging.lag["silver"]["cdc.public.customers"] == {0: 4}


def test_consistent_cut_with_a_flushed_slot_waits_for_the_checkpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_cut(monkeypatch, flush="0/10", ends={"0": 7}, committed={"0": 7})
    cut = rc.consistent_cut(None, timeout=0.0, checkpoint_timeout=0.0)
    assert cut.flush_reached and cut.checkpoints_reached and cut.reached and not cut.ok


def test_gate_detail_says_when_the_checkpoints_were_not_read() -> None:
    report = _report()
    report.cut.committed = {}
    assert "checkpoints NOT read" in report.gate_checks[2][2]
    report.cut.committed = {"silver": {}}
    assert "checkpoints read" in report.gate_checks[2][2]


def _stub_reconcile(monkeypatch: pytest.MonkeyPatch, cut: "rc.Cut") -> list[str]:
    """Everything reconcile() touches outside the cut, stubbed; returns the list `_qdrant_checks` appends to."""
    qdrant_runs: list[str] = []
    monkeypatch.setattr(rc, "consistent_cut", lambda consumer: cut)
    monkeypatch.setattr(rc, "detect_compacting", lambda: dict.fromkeys(rc.TABLES, False))
    monkeypatch.setattr(rc, "pg_rows", lambda t: ([], COLS))
    monkeypatch.setattr(g3_check, "probe", lambda *a: ([], {"bronze_offsets": {}, "rebuild": {}, "quarantine": {}}))
    monkeypatch.setattr(rc, "generator_deleted_ever", lambda n: {})
    monkeypatch.setattr(rc, "pause_state", lambda: cut.pause_start)
    monkeypatch.setattr(rc, "write_diff", lambda report: ROOT / ".run" / "g10a-diff-test.json")

    def fake_qdrant() -> dict[str, Any]:
        qdrant_runs.append("ran")
        return {"consistent": {"ok": True}, "matches_postgres": {"ok": True}}

    monkeypatch.setattr(rc, "_qdrant_checks", fake_qdrant)
    return qdrant_runs


def test_reconcile_runs_qdrant_when_the_cut_was_reached_before_the_closing_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: the Qdrant read was gated on Cut.ok, which needs the closing pause sample taken after that read, so
    it was skipped ("the cut was not reached") on every run even though every cut check passed."""
    start = rc.PauseState(False, 10, {"customers": 1})
    cut = rc.Cut("0/10", 16, start, pause_settled=True, flush_reached=True, checkpoints_reached=True)
    runs = _stub_reconcile(monkeypatch, cut)
    report = rc.reconcile(None, "T", 0, False)
    assert runs == ["ran"]
    assert "skipped" not in report.qdrant
    assert cut.paused and cut.pause_end is not None  # the closing sample was still taken, after Qdrant


def test_reconcile_skips_qdrant_when_the_waits_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    start = rc.PauseState(False, 10, {"customers": 1})
    cut = rc.Cut("0/10", 16, start, pause_settled=True, flush_reached=False, checkpoints_reached=True)
    runs = _stub_reconcile(monkeypatch, cut)
    report = rc.reconcile(None, "T", 0, False)
    assert runs == [] and report.qdrant == {"skipped": "the cut was not reached"}


# ---- report -------------------------------------------------------------------------------------------------------

LABEL = "F1 recovered"


def _clean_table(name: str) -> TableResult:
    row = {"id": 1, "name": "a", "amount": Decimal("1.50")}
    return compare_table(name, [row], [{**row, "lsn": 5, "is_deleted": False}], COLS)


def _report(**over: object) -> rc.ReconReport:
    start = rc.PauseState(False, 10, {"customers": 1})
    cut = rc.Cut("0/10", 16, start, pause_settled=True, pause_end=start, flush_lsn="0/20", flush_reached=True,
                 checkpoints_reached=True, end={"cdc.public.customers": {0: 3}})  # fmt: skip
    topic = compare_offsets({0: {0, 1, 2}}, {0: 3}, {0: {0, 1, 2}}, {0: 3}, compacting=False, expected_dups=0)
    base: dict[str, object] = {
        "label": LABEL, "cut": cut, "tables": {t: _clean_table(t) for t in rc.TABLES},
        "topics": {f"cdc.public.{t}": topic for t in rc.TABLES}, "ledger_total": True, "dup_rows_total": 0,
        "expected_dups_total": 0,
        "rebuild": {t: {"equal": True, "rows": 1} for t in rc.TABLES},
        "qdrant": {"consistent": {"ok": True, "counts": {"points": 9}}, "matches_postgres": {"ok": True, "counts": {}}},
        "compacting": dict.fromkeys(rc.TABLES, False), "compacting_unverified": [],
    }  # fmt: skip
    base.update(over)
    return rc.ReconReport(**base)  # type: ignore[arg-type]


GATE_NAMES = [
    "cut: the generator stayed paused during the reconcile",
    "cut: confirmed_flush_lsn reached the cut",
    "cut: silver and bronze committed offsets reached the topic end offsets",
    "reconcile: per-table counts equal Postgres",
    "reconcile: per-table and per-bucket checksums equal Postgres",
    "reconcile: silver tombstones have no live Postgres row and no non-key values (logical delete)",
    "reconcile: bronze holds every record present in Kafka; duplicate rows equal the replay ledger",
    "reconcile: silver equals its rebuild from bronze (count + checksum)",
    "reconcile: Qdrant has 0 stale, orphaned, duplicate, wrong-model and missing, and equals Postgres",
    "reconcile: no __debezium_unavailable_value in silver",
]


def _failed(report: rc.ReconReport) -> list[str]:
    return [n.removeprefix(f"{LABEL} ") for n, ok, _ in report.gate_checks if not ok]


def test_gate_checks_are_the_named_tuples_with_the_label_prefix() -> None:
    report = _report()
    names = [n for n, _, _ in report.gate_checks]
    assert names == [f"{LABEL} {n}" for n in GATE_NAMES]
    assert all(isinstance(ok, bool) and isinstance(d, str) for _, ok, d in report.gate_checks)
    assert report.ok and _failed(report) == []


def test_gate_check_names_and_details_make_no_forbidden_claim() -> None:
    text = "\n".join(f"{n} {d}" for n, _, d in _report().gate_checks)
    assert wording_violations(text) == []
    assert wording_violations(rc.SCOPE_NOTE) == []
    failing = _report(dup_rows_total=2, qdrant={"skipped": "the cut was not reached"})
    assert wording_violations("\n".join(f"{n} {d}" for n, _, d in failing.gate_checks)) == []


def test_each_cut_failure_fails_its_own_check() -> None:
    report = _report()
    report.cut.flush_reached = False
    assert _failed(report) == [GATE_NAMES[1]]
    report.cut.flush_reached = True
    report.cut.checkpoints_reached = False
    assert _failed(report) == [GATE_NAMES[2]]
    report.cut.checkpoints_reached = True
    report.cut.pause_end = rc.PauseState(False, 11, {"customers": 1})
    assert _failed(report) == [GATE_NAMES[0]]


def test_every_failing_table_count_fails_exactly_one_gate_check() -> None:
    assert set(FAILING) == {*rc.COUNT_FIELDS, *rc.JUDGED_ELSEWHERE}
    for name in FAILING:
        report = _report()
        res = report.tables["accounts"]
        setattr(res, name, 1)
        if name == "bucket_mismatches":
            res.digest_equal = False
        failed = _failed(report)
        assert len(failed) == 1, (name, failed)
        if name in rc.COUNT_FIELDS:
            assert failed == [GATE_NAMES[3]]
        elif name == "bucket_mismatches":
            assert failed == [GATE_NAMES[4]]
        elif name == "tombstone_nonnull":
            assert failed == [GATE_NAMES[5]]
        else:
            assert failed == [GATE_NAMES[9]]


def test_row_count_difference_with_clean_other_counts_still_fails_the_count_check() -> None:
    report = _report()
    report.tables["accounts"].silver_live = 5
    assert _failed(report) == [GATE_NAMES[3]]


def test_bronze_gate_covers_gaps_ledger_and_unverified_compaction() -> None:
    gap = compare_offsets({0: {0, 2}}, {0: 2}, {0: {0, 2}}, {0: 3}, compacting=False, expected_dups=0)
    assert _failed(_report(topics={"cdc.public.customers": gap})) == [GATE_NAMES[6]]
    allowed = compare_offsets({0: {0, 2}}, {0: 2}, {0: {0, 2}}, {0: 3}, compacting=True, expected_dups=0)
    assert _failed(_report(topics={"cdc.public.customers": allowed})) == []
    assert _failed(_report(dup_rows_total=1, expected_dups_total=0)) == [GATE_NAMES[6]]
    assert _failed(_report(compacting_unverified=["customers"])) == [GATE_NAMES[6]]
    assert _failed(_report(topics={})) == [GATE_NAMES[6]]  # nothing compared is not a pass


def test_per_table_ledger_failure() -> None:
    topic = compare_offsets({0: {0}}, {0: 2}, {0: {0}}, {0: 1}, compacting=False, expected_dups=0)
    report = _report(
        topics={"cdc.public.customers": topic}, ledger_total=False, dup_rows_total=1, expected_dups_total=0
    )
    assert not report.ledger_equal and _failed(report) == [GATE_NAMES[6]]


def test_rebuild_and_qdrant_gates() -> None:
    reb: dict[str, dict[str, Any]] = {t: {"equal": True} for t in rc.TABLES}
    reb["refunds"] = {"equal": False, "error": "AnalysisException"}
    assert _failed(_report(rebuild=reb)) == [GATE_NAMES[7]]
    assert _failed(_report(rebuild={})) == [GATE_NAMES[7]]
    assert _failed(_report(qdrant={"skipped": "x"})) == [GATE_NAMES[8]]
    half = {"consistent": {"ok": True}, "matches_postgres": {"ok": False}}
    assert _failed(_report(qdrant=half)) == [GATE_NAMES[8]]


def test_an_empty_report_passes_nothing() -> None:
    assert _failed(_report(tables={}, rebuild={}, topics={})) == [
        GATE_NAMES[3], GATE_NAMES[4], GATE_NAMES[5], GATE_NAMES[6], GATE_NAMES[7], GATE_NAMES[9],
    ]  # fmt: skip


# ---- manifest safety ----------------------------------------------------------------------------------------------


def test_to_dict_holds_no_row_ids_and_no_13_digit_run(tmp_path: Path) -> None:
    secret = 1_000_000_000_042
    row = {"id": secret, "name": "Ada", "amount": Decimal("2")}
    res = compare_table("customers", [row], [], COLS)  # missing in silver: the id goes to diff_keys only
    report = _report(tables={**{t: _clean_table(t) for t in rc.TABLES}, "customers": res})
    assert report.diff() == {"customers": {"missing": [secret]}}
    body = json.dumps(report.to_dict(), default=str)
    assert str(secret) not in body and not fids.ID13.search(body)
    assert "Ada" not in body
    path = rc.write_diff(report, tmp_path)
    written = json.loads(path.read_text())
    assert written["differences"] == {"customers": {"missing": [secret]}}
    assert "Ada" not in path.read_text()  # keys only, no payloads
    assert path.name == "g10a-diff-F1-recovered.json"


def test_write_diff_is_written_when_clean_and_label_is_made_filename_safe(tmp_path: Path) -> None:
    report = _report(label="F6b / after: ../x")
    path = rc.write_diff(report, tmp_path)
    assert path.parent == tmp_path and re.fullmatch(r"g10a-diff-[A-Za-z0-9_.-]+\.json", path.name)
    assert json.loads(path.read_text())["differences"] == {}


def test_to_dict_checksums_are_hex64_and_the_ledger_is_explained() -> None:
    res = _clean_table("customers")
    d = _report(tables={"customers": res}).to_dict()
    assert d["tables"]["customers"]["pg_checksum"] == hex64(res.pg_checksum)
    assert d["bronze_duplicates"] == {
        "scope": "total over all topics",
        "duplicate_rows": 0,
        "expected": 0,
        "equal": True,
    }
    assert "expected_dups" not in d["topics"]["cdc.public.customers"]  # judged over all topics, not per topic
    assert d["scope_note"] == rc.SCOPE_NOTE
    per_table = _report(ledger_total=False).to_dict()
    assert "expected_dups" in per_table["topics"]["cdc.public.customers"]
