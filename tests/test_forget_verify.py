"""Pure logic of adrp.forget.verify: the bound rule, first-seen-absent timing, presence (non-vacuity), judgements."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from adrp.forget import verify
from adrp.forget.verify import FnSink, SinkOutcome


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def run_poll(sinks: list[Any], phase: str, deadline: float = 100.0, **kw: Any) -> dict[str, SinkOutcome]:
    c = Clock()
    return verify.poll(sinks, phase, deadline, 5.0, clock=c.now, sleep=c.sleep, **kw)


# ---- bound rule ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("absent", "bound", "ok"),
    [
        (True, None, True),
        (None, None, True),  # not applicable
        (False, None, False),  # not absent and no bound: failure
        (False, "", False),
        (False, "   ", False),
        (False, "WAL: bounded, not removed", True),
        (True, "informational bound", True),
    ],
)
def test_physical_ok(absent: bool | None, bound: str | None, ok: bool) -> None:
    assert verify.physical_ok(absent, bound) is ok


@pytest.mark.parametrize(("absent", "ok"), [(True, True), (None, True), (False, False)])
def test_logical_has_no_bound_escape(absent: bool | None, ok: bool) -> None:
    assert verify.logical_ok(absent) is ok


def test_sink_without_bound_at_deadline_is_a_failure_with_bound_it_passes() -> None:
    never = FnSink("wal", "2", physical=lambda: (False, {"files": 3}, None))
    bounded = FnSink("spill", "19", physical=lambda: (False, {}, "ephemeral spill, bounded by container restart"))
    out = run_poll([never, bounded], "physical", 30.0)
    assert not out["wal"].ok and out["wal"].verdict == "FAIL"
    assert out["spill"].ok and out["spill"].verdict == "bounded"
    assert verify.failures(out) == ["wal"]


def test_logical_not_absent_fails_even_with_a_bound() -> None:
    s = FnSink("silver", "9", logical=lambda: False)
    assert not run_poll([s], "logical", 20.0)["silver"].ok


# ---- polling ---------------------------------------------------------------------------------------------


def test_first_seen_absent_is_recorded_once_and_polling_stops() -> None:
    state = {"n": 0}

    def logical() -> bool:
        state["n"] += 1
        return state["n"] >= 4  # absent on the 4th look

    seen: list[str] = []
    s = FnSink("qdrant", "14", logical=logical)
    out = run_poll([s], "logical", on_first_absent=lambda sink, ev: seen.append(sink.name), recheck=False)
    assert seen == ["qdrant"]
    assert out["qdrant"].polls == 4
    assert out["qdrant"].first_absent_s == pytest.approx(15.0)  # 3 sleeps of 5 s
    assert out["qdrant"].verdict == "absent"


def test_polling_respects_a_sink_specific_interval() -> None:
    calls = {"slow": 0, "fast": 0}

    def slow() -> bool:
        calls["slow"] += 1
        return False

    def fast() -> bool:
        calls["fast"] += 1
        return False

    run_poll(
        [FnSink("slow", "9", logical=slow, min_interval_s=30.0), FnSink("fast", "1", logical=fast)], "logical", 60.0
    )
    assert calls["fast"] > calls["slow"] >= 2


def test_a_sink_that_comes_back_fails_the_recheck() -> None:
    answers = iter([True, False])  # absent, then present again at the final look
    s = FnSink("qdrant", "14", logical=lambda: next(answers))
    o = run_poll([s], "logical")["qdrant"]
    assert o.reappeared and not o.ok and o.verdict == "FAIL"


def test_a_physical_sink_that_comes_back_with_a_bound_is_bounded_not_failed() -> None:
    answers: Iterator[Any] = iter(
        [(True, {}, None), (False, {}, "WAL: recycled segments keep bytes. Bounded, not removed.")]
    )
    s = FnSink("pg_wal", "2", physical=lambda: next(answers))
    o = run_poll([s], "physical")["pg_wal"]
    assert o.reappeared and o.ok and o.verdict == "bounded"
    again: Iterator[Any] = iter([(True, {}, None), (False, {}, None)])
    o2 = run_poll([FnSink("x", "2", physical=lambda: next(again))], "physical")["x"]
    assert o2.reappeared and not o2.ok and o2.verdict == "FAIL"


def test_exceptions_are_counted_and_do_not_stop_polling() -> None:
    n = {"i": 0}

    def flaky() -> bool:
        n["i"] += 1
        if n["i"] < 3:
            raise RuntimeError("boom with 1234567890123")
        return True

    o = run_poll([FnSink("pg", "1", logical=flaky)], "logical")["pg"]
    assert o.ok and o.errors == 2
    # the stored error evidence is the exception type, never its message
    o2 = run_poll(
        [FnSink("pg", "1", logical=lambda: (_ for _ in ()).throw(ValueError("x 1000000000123")))], "logical", 10
    )["pg"]
    assert o2.evidence == {"error": "ValueError", "errors": o2.errors} and not o2.ok


def test_not_applicable_sink_is_done_immediately() -> None:
    o = run_poll([FnSink("broker", "4")], "logical")["broker"]
    assert o.absent is None and o.ok and o.polls == 1 and o.verdict == "n/a"


# ---- presence (non-vacuity) ------------------------------------------------------------------------------


def test_presence_class() -> None:
    assert verify.presence_class(False, None) == "present"
    assert verify.presence_class(None, False) == "present"
    assert verify.presence_class(True, True) == "vacuous"
    assert verify.presence_class(True, False) == "present"
    assert verify.presence_class(None, None) == "na"


def test_presence_pass_flags_a_sink_that_finds_nothing() -> None:
    sinks = [
        FnSink("postgres", "1", logical=lambda: False),
        FnSink("quarantine", "10", logical=lambda: True),  # nothing there
        FnSink("seaweedfs", "28", physical=lambda: (True, {}, None), presence_expected=False),
    ]
    rep = verify.presence_pass(sinks)
    assert rep.present == ["postgres"] and rep.vacuous == ["quarantine"] and rep.skipped == ["seaweedfs"]
    assert not rep.ok
    assert verify.presence_pass(sinks, allow_missing=["quarantine"]).ok


def test_presence_pass_reports_unreadable_sinks_by_type_only() -> None:
    def broken() -> bool:
        raise OSError("1000000000123 secret")

    rep = verify.presence_pass([FnSink("qdrant", "14", logical=broken)])
    assert rep.errors == {"qdrant": "OSError"} and not rep.ok
    assert "1000000000123" not in str(rep.to_dict())


# ---- recording through the verifier ----------------------------------------------------------------------


def test_verifier_records_the_first_observation_and_the_final_bound() -> None:
    log: list[tuple[str, str, dict[str, Any]]] = []

    def rec(rid: str, sink: str, **kw: Any) -> None:
        log.append((rid, sink, kw))

    ok = FnSink("pg", "1", physical=lambda: (True, {"files": 0}, None))
    bounded = FnSink("pg_wal", "2", physical=lambda: (False, {"files": 2}, "bounded by min_wal_size"))
    c = Clock()
    v = verify.Verifier(["r1", "r2"], recorder=rec)
    out = v.run([ok, bounded], "physical", 10.0, 5.0, clock=c.now, sleep=c.sleep)
    assert out["pg"].ok and out["pg_wal"].ok
    firsts = [e for e in log if e[2]["physical_absent"]]
    assert {(r, s) for r, s, _ in firsts} == {("r1", "pg"), ("r2", "pg")}  # one observation per request
    finals = [e for e in log if not e[2]["physical_absent"]]
    assert {(r, s) for r, s, _ in finals} == {("r1", "pg_wal"), ("r2", "pg_wal")}
    assert all(e[2]["bound"] == "bounded by min_wal_size" for e in finals)
    assert all(e[2]["logical_absent"] is False for e in log)


def test_run_scoped_records_only_against_the_owning_request() -> None:
    log: list[tuple[str, str]] = []
    v = verify.Verifier((), recorder=lambda rid, sink, **kw: log.append((rid, sink)) if kw["logical_absent"] else None)
    c = Clock()
    out = v.run_scoped(
        {
            "r1": [FnSink("silver", "9", logical=lambda: True)],
            "r2": [FnSink("silver", "9", logical=lambda: False)],
        },
        "logical",
        10.0,
        5.0,
        clock=c.now,
        sleep=c.sleep,
    )
    assert log == [("r1", "silver")]
    assert out["r1"]["silver"].ok and not out["r2"]["silver"].ok
    assert out["r2"]["silver"].sink == "silver"


# ---- judgements ------------------------------------------------------------------------------------------


def silver_result(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "silver": {
            "customers": {
                "footprint_rows": 1,
                "live_rows": 0,
                "tombstones": 1,
                "tombstones_at_lsn": 1,
                "tombstones_with_values": 0,
            },
            "support_tickets": {
                "footprint_rows": 2,
                "live_rows": 0,
                "tombstones": 2,
                "tombstones_at_lsn": 2,
                "tombstones_with_values": 0,
            },
            "transactions": {"footprint_rows": 5, "live_rows": 5, "customer_id_rows": 0},
            "accounts": {"footprint_rows": 1, "live_rows": 1, "customer_id_rows": 0},
        }
    }
    for path, v in over.items():
        table, col = path.split("__")
        base["silver"][table][col] = v
    return base


def test_silver_judgement_accepts_tombstones_and_rekeyed_rows() -> None:
    ok, ev = verify.silver_judgement(silver_result())
    assert ok and ev["live_rows"] == 0 and ev["tables"] == 4


@pytest.mark.parametrize(
    "over",
    [
        {"customers__live_rows": 1},  # not tombstoned yet
        {"support_tickets__tombstones_with_values": 1},  # tombstone still holds PII
        {"transactions__customer_id_rows": 1},  # re-key not applied
        {"accounts__customer_id_rows": 2},
        {"customers__tombstones_at_lsn": 0},  # tombstone older than the forget lsn
    ],
)
def test_silver_judgement_rejects(over: dict[str, Any]) -> None:
    assert not verify.silver_judgement(silver_result(**over))[0]


def test_delivery_judgement() -> None:
    assert verify.delivery_judgement({"bronze_txn_rows": 12}, 12)[0]
    assert not verify.delivery_judgement({"bronze_txn_rows": 11}, 12)[0]
    assert not verify.delivery_judgement({"bronze_txn_rows": None}, 1)[0]


def test_memo_caches_within_ttl_only() -> None:
    c = Clock()
    m = verify.Memo(clock=c.now)
    calls = []
    for _ in range(2):
        m.get("k", lambda: calls.append(1), 10.0)
    assert len(calls) == 1
    c.t = 11.0
    m.get("k", lambda: calls.append(1), 10.0)
    assert len(calls) == 2


def test_outcome_dict_has_no_free_text_beyond_evidence() -> None:
    o = SinkOutcome("pg_wal", "2", "physical", False, None, "a bound with prose", {"files": 2}, 3)
    d = verify.outcome_dict(o)
    assert d["verdict"] == "bounded" and d["bounded"] is True and "bound" not in d


def test_privacy_files_are_not_evidence_about_the_shop_tables() -> None:
    pgdata = "/var/lib/postgresql/data"
    hits = [
        f"{pgdata}/base/16384/24601",  # privacy.forget_requests heap
        f"{pgdata}/base/16384/24601_fsm",
        f"{pgdata}/base/16384/24601.1",
        f"{pgdata}/base/16384/246011",  # a different relation whose number merely starts the same
        f"{pgdata}/base/16384/24700",  # customers heap
    ]
    kept, excluded = verify.exclude_privacy(hits, ["base/16384/24601"], pgdata)
    assert kept == [f"{pgdata}/base/16384/246011", f"{pgdata}/base/16384/24700"]
    assert len(excluded) == 3
