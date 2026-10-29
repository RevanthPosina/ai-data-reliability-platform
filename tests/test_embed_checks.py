from typing import Any

from adrp.embed.logic import evaluate

M = "m@1"


def pt(eid: str, ver: int, h: str = "h0", model: str = M) -> dict[str, Any]:
    return {"entity_id": eid, "source_version": ver, "chunk_hash": h, "embed_model_version": model}


SILVER = {"help_articles:1": (100, False, True), "help_articles:2": (50, True, False)}


def test_clean_index_has_no_findings() -> None:
    r = evaluate([pt("help_articles:1", 100)], SILVER, M)
    assert {k: v for k, v in r.items() if k not in ("points", "entities_in_index")} == {
        "stale_chunks": 0, "orphaned_chunks": 0, "duplicate_chunks": 0, "wrong_model_chunks": 0,
        "ahead_chunks": 0, "missing_entities": 0,
    }  # fmt: skip


def test_each_finding_is_counted() -> None:
    pts = [
        pt("help_articles:1", 90, "a"),  # stale: below silver's version
        pt("help_articles:1", 100, "b"),
        pt("help_articles:1", 100, "b"),  # duplicate of the line above
        pt("help_articles:2", 50, "c"),  # orphaned: entity is a tombstone
        pt("help_articles:9", 10, "d"),  # orphaned: entity unknown to silver
        pt("help_articles:1", 100, "e", model="m@0"),  # wrong model
    ]
    r = evaluate(pts, SILVER, M)
    assert (r["stale_chunks"], r["orphaned_chunks"], r["duplicate_chunks"], r["wrong_model_chunks"]) == (1, 2, 1, 1)


def test_live_entity_without_current_points_is_missing() -> None:
    assert evaluate([], SILVER, M)["missing_entities"] == 1
    assert evaluate([pt("help_articles:1", 90)], SILVER, M)["missing_entities"] == 1  # only an old version


def test_entity_without_text_is_not_missing() -> None:
    assert evaluate([], {"support_tickets:1": (5, False, False)}, M)["missing_entities"] == 0
