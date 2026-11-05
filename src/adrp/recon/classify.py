"""Fault outcome classification and the must-never-claim wording guard (G10a plan, section 6)."""

from __future__ import annotations

import re
from collections.abc import Sequence
from enum import Enum


class Outcome(str, Enum):
    CAUGHT = "caught"
    SELF_HEALED = "self_healed"
    SILENT = "silent"


def classify(recon_ok_after: bool, fired: Sequence[str]) -> Outcome:
    """Reconciler clean after recovery and a named detection fired: CAUGHT. Clean and nothing fired:
    SELF_HEALED. Not clean and nothing fired: SILENT. A detection that fired with a dirty reconciler is CAUGHT (it
    was detected); the reconcile gate fails the run separately."""
    if fired:
        return Outcome.CAUGHT
    return Outcome.SELF_HEALED if recon_ok_after else Outcome.SILENT


_NEAR_DELETED = re.compile(r"logical|physical|tombstone", re.I)
_DURABLE_NEGATED = re.compile(r"(?:\bno|\bnot|\bwithout|n't)\s+(?:\w+\s+){0,2}$", re.I)

# (label, pattern) for claims that are never allowed
_FORBIDDEN: list[tuple[str, re.Pattern[str]]] = [
    ("exactly-once", re.compile(r"\bexactly[- ]once\b", re.I)),
    (
        "zero silent without the reconciler scope",
        re.compile(r"\b0 silent\b(?! corruption detected by the reconciler)", re.I),
    ),
    (
        "absence-of-faults claim",
        re.compile(r"\b(?:no|zero) (?:undetected|silent) (?:faults?|corruption)\b|\bno undetected faults?\b", re.I),
    ),
    ("production scale", re.compile(r"\bproduction[- ](?:scale|ready|grade|safe)\b", re.I)),
    ("compliance", re.compile(r"\bcompliant\b|\bguarantee[sd]?\b", re.I)),
    ("row-level lineage", re.compile(r"\brow[- ]level lineage\b", re.I)),
    ("reproducible", re.compile(r"\breproducib(?:le|ility)\b", re.I)),
    (
        "self-healed without scope",
        re.compile(r"\b(?:was|were|is|are|been|always)\s+self[- ]healed\b(?! in this run)", re.I),
    ),
    (
        "registry credited for F6b",
        re.compile(r"\bcaught by (?:the )?(?:schema )?registry\b|\bregistry (?:caught|rejected|blocked)\b", re.I),
    ),
]


def wording_violations(text: str) -> list[str]:
    """Phrases in `text` that G10a must never claim, as `rule: 'matched text'`. Empty means clean."""
    out = [f"{label}: {m.group(0)!r}" for label, pat in _FORBIDDEN for m in pat.finditer(text)]
    for m in re.finditer(r"\bdeleted\b", text, re.I):  # needs "logical"/"physical" nearby
        if not _NEAR_DELETED.search(text[max(0, m.start() - 40) : m.end() + 40]):
            out.append(f"deleted without logical or physical: {m.group(0)!r}")
    for m in re.finditer(r"\bdurab\w*", text, re.I):  # a claim, unless negated just before
        if not _DURABLE_NEGATED.search(text[max(0, m.start() - 30) : m.start()]):
            out.append(f"durability claim: {m.group(0)!r}")
    if re.search(r"\b(?:no|zero) duplicates?\b", text, re.I) and not re.search(r"\bsilver\b", text, re.I):
        out.append("no duplicates not limited to silver")
    return out
