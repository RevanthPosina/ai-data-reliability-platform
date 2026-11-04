"""Pure logic of adrp.forget.audit and adrp.forget.queue: lag math, the request state machine, and the rule that
nothing stored or printed carries a raw id."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from adrp.forget import audit, postgres, queue, workflow
from adrp.forget.audit import RequestRow

RAW = 1_000_000_000_123


# ---- lag math --------------------------------------------------------------------------------------------


def test_lag_is_first_absent_minus_request_and_never_negative() -> None:
    assert audit.lag_s(100.0, 142.5) == 42.5
    assert audit.lag_s(100.0, None) is None
    assert audit.lag_s(100.0, 99.0) == 0.0  # clock noise does not produce a negative lag


def test_summarize_ignores_unobserved() -> None:
    s = audit.summarize([10.0, None, 30.0, 20.0])
    assert s == {"n": 3, "median_s": 20.0, "min_s": 10.0, "max_s": 30.0}
    assert audit.summarize([None, None]) == {"n": 0, "median_s": None, "min_s": None, "max_s": None}


def test_batched_requests_share_the_verified_time_but_not_the_lag() -> None:
    verified = 1000.0
    lags = [audit.lag_s(req, verified) for req in (940.0, 900.0, 995.0)]
    assert lags == [60.0, 100.0, 5.0]


def test_lag_table_aggregates_per_sink(monkeypatch: pytest.MonkeyPatch) -> None:
    per = {
        "a": {"silver": {"logical_lag_s": 10.0, "physical_lag_s": None, "bounded": False}},
        "b": {"silver": {"logical_lag_s": 30.0, "physical_lag_s": 600.0, "bounded": False}},
    }
    monkeypatch.setattr(audit, "request_lags", lambda rid: per[rid])
    t = audit.lag_table(["a", "b"])
    assert t["silver"]["logical"]["median_s"] == 20.0
    assert t["silver"]["physical"]["n"] == 1 and t["silver"]["bounded"] == 0


# ---- state machine ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("old", "new", "ok"),
    [
        ("requested", "logical_done", True),
        ("logical_done", "physical_done", True),
        ("requested", "physical_done", False),  # cannot skip the logical phase
        ("physical_done", "requested", False),  # finished is final
        ("physical_done", "failed", False),
        ("failed", "requested", True),  # retry
        ("failed", "logical_done", True),
        ("failed", "physical_done", False),
        ("logical_done", "logical_done", True),  # re-assertion is a no-op
        ("physical_done", "physical_done", True),
    ],
)
def test_transitions(old: str, new: str, ok: bool) -> None:
    assert audit.can_transition(old, new) is ok


def test_next_phase_per_status() -> None:
    assert [workflow.next_phase(s) for s in ("requested", "failed", "logical_done", "physical_done")] == [
        "logical", "logical", "physical", None,
    ]  # fmt: skip


def fake_db(monkeypatch: pytest.MonkeyPatch, status: str) -> list[tuple[str, str | None]]:
    state = {"status": status}
    writes: list[tuple[str, str | None]] = []

    def get(rid: str) -> RequestRow:
        return RequestRow(rid, state["status"], 1.0, None, None, None, True)

    def set_status(rid: str, new: str, error: str | None = None) -> None:
        writes.append((new, error))
        state["status"] = new

    monkeypatch.setattr(audit, "get_request", get)
    monkeypatch.setattr(postgres, "set_status", set_status)
    return writes


def test_move_is_idempotent_and_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    writes = fake_db(monkeypatch, "physical_done")
    audit.move("rid", "physical_done")  # a finished request re-run: nothing is written
    assert writes == []
    with pytest.raises(audit.TransitionError):
        audit.move("rid", "logical_done")


def test_move_forward_writes_once(monkeypatch: pytest.MonkeyPatch) -> None:
    writes = fake_db(monkeypatch, "requested")
    audit.move("rid", "logical_done")
    audit.move("rid", "logical_done")
    assert writes == [("logical_done", None)]


def test_fail_only_from_an_active_state_and_masks_digits(monkeypatch: pytest.MonkeyPatch) -> None:
    writes = fake_db(monkeypatch, "logical_done")
    audit.fail("rid", f"bronze delete failed for {RAW}")
    assert writes and str(RAW) not in (writes[0][1] or "")
    writes2 = fake_db(monkeypatch, "physical_done")
    audit.fail("rid", "late error")
    assert writes2 == []


# ---- no raw id in anything stored or printed -------------------------------------------------------------


def test_evidence_guard_refuses_ids_and_canaries() -> None:
    assert audit.evidence_problem({"files": 3, "ratio": 0.5, "ok": True, "label": "wal"}) is None
    assert audit.evidence_problem({"x": f"id {RAW}"})
    assert audit.evidence_problem({"x": ["g8c-" + "a" * 32]})
    assert audit.evidence_problem({"nested": {"deep": "1234567"}})
    assert audit.evidence_problem({"x": object()})
    with pytest.raises(ValueError) as e:
        audit.safe_evidence({"x": f"id {RAW}"})
    assert str(RAW) not in str(e.value)


def test_bound_must_be_nonempty_and_id_free() -> None:
    assert audit.safe_bound(None) is None
    assert audit.safe_bound("bounded by min_wal_size=80 MiB, 600000 ms") is not None
    with pytest.raises(ValueError):
        audit.safe_bound("  ")
    with pytest.raises(ValueError):
        audit.safe_bound(f"contains {RAW}")


def test_record_sink_validates_before_writing(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[Any] = []
    monkeypatch.setattr(postgres, "set_sink_status", lambda *a, **k: called.append((a, k)))
    audit.record_sink("rid", "pg_wal", physical_absent=False, bound="bounded", evidence={"files": 2})
    assert len(called) == 1
    with pytest.raises(ValueError):
        audit.record_sink("rid", "pg_wal", evidence={"leak": str(RAW)})
    assert len(called) == 1


def test_public_summary_and_parse_roundtrip_carry_no_id() -> None:
    out = "0b6f5f0e-5b0e-4a53-9f3a-2a5a3f1f3d11|logical_done|1700000000.5|1700000100.0||true|\n"
    (row,) = audit.parse_request_rows(out)
    s = audit.public_summary(row)
    assert s["status"] == "logical_done" and s["logical_lag_s"] == pytest.approx(99.5) and s["raw_id_stored"] is True
    assert str(RAW) not in json.dumps(s)


def test_forget_subject_repr_hides_ids_and_tokens() -> None:
    s = workflow.ForgetSubject(
        "rid", RAW, canaries=("g8c-" + "b" * 32,), pii=("someone@example.org",), ids={"customers": [RAW]}
    )
    text = repr(s) + str(s)
    assert str(RAW) not in text and "g8c-" not in text and "someone@" not in text


def test_hmac_key_required(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError):
        audit.hmac_key({})
    assert audit.hmac_key({"FORGET_HMAC_KEY": "k"}) == "k"


# ---- queue (Dagster -> executor) -------------------------------------------------------------------------


def test_queue_refuses_anything_that_is_not_a_request_uuid() -> None:
    with pytest.raises(ValueError):
        queue.clean_ids([str(RAW)])
    with pytest.raises(ValueError):
        queue.entry("logical", [])
    with pytest.raises(ValueError):
        queue.entry("sideways", ["0b6f5f0e-5b0e-4a53-9f3a-2a5a3f1f3d11"])


def test_queue_roundtrip(tmp_path: Path) -> None:
    a = "0b6f5f0e-5b0e-4a53-9f3a-2a5a3f1f3d11"
    b = "1b6f5f0e-5b0e-4a53-9f3a-2a5a3f1f3d11"
    p1 = queue.write_entry(tmp_path / "q", "logical", [b, a, a.upper()])
    queue.write_entry(tmp_path / "q", "physical", [a])
    (tmp_path / "q" / "junk.json").write_text("{not json")
    assert queue.pending_request_ids(tmp_path / "q", "logical") == [a, b]
    assert queue.pending_request_ids(tmp_path / "q", "physical") == [a]
    queue.done(p1)
    assert queue.pending_request_ids(tmp_path / "q", "logical") == []
    assert RAW.__str__() not in "".join(p.read_text() for p in (tmp_path / "q").glob("*.json"))
