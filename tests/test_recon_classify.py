"""Outcome classification and the never-claim wording guard (G10a)."""

import pytest

from adrp.recon.classify import Outcome, classify, wording_violations


def test_classify_branches() -> None:
    assert classify(True, ["connector-status"]) is Outcome.CAUGHT
    assert classify(True, []) is Outcome.SELF_HEALED
    assert classify(False, []) is Outcome.SILENT
    assert classify(False, ["connector-status"]) is Outcome.CAUGHT  # detected; the reconcile gate fails the run
    assert [o.value for o in Outcome] == ["caught", "self_healed", "silent"]


@pytest.mark.parametrize(
    "text",
    [
        "the sink is exactly-once",
        "Exactly once delivery",
        "0 silent faults",
        "no undetected faults remain",
        "a row was deleted",
        "the row is deleted from silver",
        "handles production-scale load",
        "GDPR compliant",
        "row-level lineage via OpenLineage",
        "the rebuild is reproducible",
        "F4 was self-healed",
        "the registry caught by the registry",
        "survives broker loss, so it is durable",
        "there are no duplicates",
    ],
)
def test_wording_violations_flag_bad_claims(text: str) -> None:
    assert wording_violations(text), text


@pytest.mark.parametrize(
    "text",
    [
        "0 silent corruption detected by the reconciler",
        "logical delete (silver tombstone)",
        "physically removed; the row was deleted logically",
        "idempotent under the tested faults",
        "self-healed in this run",
        "self-healed: silver unchanged",  # the preregistered design wording is not a claim about a run
        "no durability claim from a graceful restart",
        "silver has no duplicates; bronze holds duplicate rows by design",
        "is_deleted rows counted; deleted_ever keys",
        "the txid to LSN mapping is a reconciliation key",
        "silver equals its rebuild from bronze (count + checksum)",
    ],
)
def test_wording_violations_pass_good_text(text: str) -> None:
    assert wording_violations(text) == []


def test_wording_violations_name_the_rule() -> None:
    v = wording_violations("exactly-once and reproducible")
    assert len(v) == 2 and v[0].startswith("exactly-once") and v[1].startswith("reproducible")
