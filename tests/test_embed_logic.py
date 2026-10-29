import hashlib
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from adrp.common import chunk_hash, chunk_text, point_id
from adrp.embed import logic
from adrp.embed.logic import Point, process_entity

MODEL = "m@1"


class FakeStore:
    def __init__(self) -> None:
        self.points: dict[str, Point] = {}
        self.writes = 0

    def current_matches(self, eid: str, version: int, hashes: list[str], model: str) -> bool:
        ids = [point_id(eid, version, i) for i in range(len(hashes))]
        return all(
            i in self.points
            and self.points[i].payload["chunk_hash"] == h
            and self.points[i].payload["embed_model_version"] == model
            for i, h in zip(ids, hashes, strict=True)
        )

    def vectors_by_hash(self, eid: str, model: str) -> dict[str, list[float]]:
        return {
            p.payload["chunk_hash"]: p.vector
            for p in self.points.values()
            if p.payload["entity_id"] == eid and p.payload["embed_model_version"] == model
        }

    def upsert(self, points: list[Point]) -> None:
        self.writes += 1
        for p in points:
            self.points[p.id] = p

    def delete_older(self, eid: str, below_version: int) -> None:
        for k in [
            k
            for k, p in self.points.items()
            if p.payload["entity_id"] == eid and p.payload["source_version"] < below_version
        ]:
            del self.points[k]

    def delete_entity(self, eid: str) -> None:
        for k in [k for k, p in self.points.items() if p.payload["entity_id"] == eid]:
            del self.points[k]

    def of(self, eid: str) -> list[Point]:
        return sorted(
            (p for p in self.points.values() if p.payload["entity_id"] == eid),
            key=lambda p: (p.payload["source_version"], p.payload["chunk_no"]),
        )


class FakeEncoder:
    def __init__(self) -> None:
        self.calls = 0
        self.texts = 0

    def encode(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        self.texts += len(texts)
        return [[b / 255 for b in hashlib.sha256(t.encode()).digest()[:4]] for t in texts]


def row(lsn: int, title: str = "T", body: str = "body text", deleted: bool = False) -> dict[str, Any]:
    return {"id": 1, "lsn": lsn, "is_deleted": deleted, "title": title, "body": body}


EID = "help_articles:1"


def run(store: FakeStore, enc: FakeEncoder, r: dict[str, Any], now: str = "t0") -> logic.Action:
    return process_entity(EID, r, store, enc, MODEL, now)


def test_new_entity_gets_points_with_stable_ids_and_payload() -> None:
    s, e = FakeStore(), FakeEncoder()
    a = run(s, e, row(100, body="word " * 400))
    pts = s.of(EID)
    assert a.kind == "upsert" and a.chunks == len(pts) > 1
    assert [p.id for p in pts] == [point_id(EID, 100, i) for i in range(len(pts))]
    p0 = pts[0].payload
    assert p0["entity_id"] == EID and p0["source_version"] == 100 and p0["embed_model_version"] == MODEL
    assert p0["chunk_hash"] == chunk_hash(chunk_text("T\n\n" + "word " * 400)[0])


def test_text_change_embeds_new_chunks_and_removes_the_old_version() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100))
    a = run(s, e, row(200, body="different body"))
    assert a.embedded == 1 and a.reused == 0
    assert {p.payload["source_version"] for p in s.of(EID)} == {200}


def test_non_text_change_reuses_vectors_and_does_not_call_the_model() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100))
    before = e.texts
    a = run(s, e, row(200))  # same title/body: only a column the embedding ignores changed
    assert a.embedded == 0 and a.reused == a.chunks == 1 and e.texts == before
    assert {p.payload["source_version"] for p in s.of(EID)} == {200}


def test_tombstone_removes_every_chunk() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100, body="word " * 400))
    a = run(s, e, row(300, deleted=True))
    assert a.kind == "delete" and s.of(EID) == []


def test_rerun_is_a_no_op_even_the_payload_timestamp() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100, body="word " * 400), now="first")
    snapshot = {k: (p.vector, dict(p.payload)) for k, p in s.points.items()}
    writes, texts = s.writes, e.texts
    a = run(s, e, row(100, body="word " * 400), now="second")
    assert a.kind == "unchanged"
    assert {k: (p.vector, dict(p.payload)) for k, p in s.points.items()} == snapshot
    assert (s.writes, e.texts) == (writes, texts)


def test_interrupted_run_leaves_both_versions_and_the_next_run_cleans_up() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100))
    # crash after the upsert of v200 but before delete_older: simulate by upserting only
    chunks = chunk_text("T\n\nnew body")
    payload = {
        "entity_id": EID, "source_version": 200, "chunk_no": 0,
        "chunk_hash": chunk_hash(chunks[0]), "embed_model_version": MODEL,
    }  # fmt: skip
    s.upsert([Point(point_id(EID, 200, 0), [0.0], payload)])
    assert {p.payload["source_version"] for p in s.of(EID)} == {100, 200}
    run(s, e, row(200, body="new body"))
    assert {p.payload["source_version"] for p in s.of(EID)} == {200}


def test_wrong_model_vectors_are_not_reused() -> None:
    s, e = FakeStore(), FakeEncoder()
    run(s, e, row(100))
    a = process_entity(EID, row(200), s, e, "m@2", "t")
    assert a.embedded == 1 and a.reused == 0


def test_bronze_changes_keep_max_lsn_and_ignore_other_tables_and_tombstones() -> None:
    rows: list[dict[str, Any]] = [
        {"source_table": "public.help_articles", "pk": '{"id":1}', "lsn": 10},
        {"source_table": "public.help_articles", "pk": '{"id":1}', "lsn": 30},
        {"source_table": "public.support_tickets", "pk": '{"id":2}', "lsn": 20},
        {"source_table": "public.customers", "pk": '{"id":9}', "lsn": 99},
        {"source_table": "public.help_articles", "pk": '{"id":3}', "lsn": None},
    ]
    assert logic.bronze_changes(rows) == {"help_articles:1": 30, "support_tickets:2": 20}


def test_split_ready_waits_for_silver_to_catch_up() -> None:
    silver = {"help_articles:1": {"lsn": 50}, "help_articles:2": {"lsn": 10}}
    required = {"help_articles:1": 50, "help_articles:2": 40, "help_articles:3": 5}
    ready, pending = logic.split_ready(required, silver)
    assert ready == ["help_articles:1"]
    assert pending == {"help_articles:2": 40, "help_articles:3": 5}  # behind, and not in silver yet


@st.composite
def histories(draw: st.DrawFn) -> list[dict[str, Any]]:
    n = draw(st.integers(1, 12))
    out, lsn, deleted = [], 100, False
    for _ in range(n):
        lsn += draw(st.integers(1, 9))
        if not deleted and draw(st.integers(0, 9)) == 0:
            deleted = True
        text = draw(st.sampled_from(["alpha", "beta", "gamma " * 300]))
        out.append(row(lsn, body=text, deleted=deleted))
        if deleted:
            break
    return out


@settings(max_examples=300, deadline=None)
@given(histories(), st.data())
def test_any_subset_of_versions_ends_at_the_latest(history: list[dict[str, Any]], data: st.DataObject) -> None:
    """The worker sees silver at poll times, so it skips intermediate versions and may re-process; the result
    must depend only on the latest version."""
    s, e = FakeStore(), FakeEncoder()
    seen = [r for r in history[:-1] if data.draw(st.booleans())]
    dup = data.draw(st.lists(st.sampled_from(history), max_size=4))
    for r in [*seen, *dup, history[-1], history[-1]]:
        if r["lsn"] <= history[-1]["lsn"]:
            run(s, e, r)
    last = history[-1]
    run(s, e, last)
    pts = s.of(EID)
    if last["is_deleted"]:
        assert pts == []
    else:
        expected = chunk_text(logic.document("help_articles", last))
        assert [p.payload["chunk_hash"] for p in pts] == [chunk_hash(c) for c in expected]
        assert {p.payload["source_version"] for p in pts} == {last["lsn"]}
