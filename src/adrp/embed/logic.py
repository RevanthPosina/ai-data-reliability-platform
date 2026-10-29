"""Embedding pipeline logic (PLAN G6), free of Qdrant / Iceberg / model imports so it is unit-testable.

Entities are rows of the text tables. Their *version* is the silver row's LSN. For one entity at version v:
  - the points are uuid5(ns, entity_id|v|chunk_no) with payload {entity_id, source_version, chunk_hash, ...}
  - on a new version: upsert the new points first, then delete this entity's points with source_version < v
  - on a tombstone: delete every point of the entity
Chunks whose text did not change between versions (e.g. an article's view_count changed) reuse the stored vector.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

from adrp.common import chunk_hash, chunk_text, point_id

# silver table -> text columns that make up the embedded document
TEXT_TABLES: dict[str, tuple[str, ...]] = {
    "help_articles": ("title", "body"),
    "support_tickets": ("subject", "body"),
}
COLLECTION = "text_chunks"
CHUNK_CHARS = 800


def entity_id(table: str, row_id: int) -> str:
    return f"{table}:{row_id}"


def split_entity(eid: str) -> tuple[str, int]:
    table, _, rid = eid.partition(":")
    return table, int(rid)


def document(table: str, row: dict[str, Any]) -> str:
    return "\n\n".join(str(row[c]) for c in TEXT_TABLES[table] if row.get(c) is not None)


def bronze_changes(rows: list[dict[str, Any]]) -> dict[str, int]:
    """entity_id -> highest LSN seen in bronze rows (source_table, pk, lsn). Tombstones (no LSN) are ignored."""
    out: dict[str, int] = {}
    for r in rows:
        table = str(r["source_table"]).rsplit(".", 1)[-1]
        if table not in TEXT_TABLES or r.get("lsn") is None or r.get("pk") is None:
            continue
        eid = entity_id(table, int(json.loads(r["pk"])["id"]))
        out[eid] = max(out.get(eid, 0), int(r["lsn"]))
    return out


def merge_required(pending: dict[str, int], new: dict[str, int]) -> dict[str, int]:
    out = dict(pending)
    for eid, lsn in new.items():
        out[eid] = max(out.get(eid, 0), lsn)
    return out


def split_ready(required: dict[str, int], silver: dict[str, dict[str, Any]]) -> tuple[list[str], dict[str, int]]:
    """Silver and bronze are written by separate queries, so silver may not have applied a change bronze already has.
    An entity is ready once silver's version has reached the bronze LSN; otherwise it stays pending."""
    ready, pending = [], {}
    for eid, lsn in sorted(required.items()):
        row = silver.get(eid)
        if row is not None and int(row["lsn"]) >= lsn:
            ready.append(eid)
        else:
            pending[eid] = lsn
    return ready, pending


@dataclass(frozen=True)
class Point:
    id: str
    vector: list[float]
    payload: dict[str, Any]


class Store(Protocol):
    def current_matches(self, eid: str, version: int, hashes: list[str], model: str) -> bool: ...
    def vectors_by_hash(self, eid: str, model: str) -> dict[str, list[float]]: ...
    def upsert(self, points: list[Point]) -> None: ...
    def delete_older(self, eid: str, below_version: int) -> None: ...
    def delete_entity(self, eid: str) -> None: ...


class Encoder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...


@dataclass
class Action:
    entity: str
    version: int
    kind: str  # upsert | delete | unchanged
    chunks: int = 0
    embedded: int = 0
    reused: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


def process_entity(
    eid: str,
    row: dict[str, Any],
    store: Store,
    encoder: Encoder,
    model: str,
    now_iso: str,
    max_chars: int = CHUNK_CHARS,
) -> Action:
    table, _ = split_entity(eid)
    version = int(row["lsn"])
    if row["is_deleted"]:
        store.delete_entity(eid)
        return Action(eid, version, "delete")
    chunks = chunk_text(document(table, row), max_chars)
    hashes = [chunk_hash(c) for c in chunks]
    if chunks and store.current_matches(eid, version, hashes, model):
        store.delete_older(eid, version)  # idempotent; finishes an interrupted earlier run
        return Action(eid, version, "unchanged", chunks=len(chunks))
    known = store.vectors_by_hash(eid, model)
    todo = [i for i, h in enumerate(hashes) if h not in known]
    fresh = encoder.encode([chunks[i] for i in todo]) if todo else []
    new_vecs = dict(zip(todo, fresh, strict=True))
    points = [
        Point(
            point_id(eid, version, i),
            new_vecs[i] if i in new_vecs else known[hashes[i]],
            {
                "entity_id": eid,
                "source_table": table,
                "source_version": version,
                "chunk_no": i,
                "chunk_hash": hashes[i],
                "embed_model_version": model,
                "ingested_at": now_iso,
                "text": chunks[i],
            },
        )
        for i in range(len(chunks))
    ]
    if points:
        store.upsert(points)  # new version first (wait=true) ...
    store.delete_older(eid, version)  # ... then remove the older ones
    return Action(eid, version, "upsert", chunks=len(chunks), embedded=len(todo), reused=len(chunks) - len(todo))


def evaluate(points: list[dict[str, Any]], silver: dict[str, tuple[int, bool, bool]], model: str) -> dict[str, int]:
    stale = orphaned = ahead = wrong_model = 0
    seen: Counter[tuple[str, int, str]] = Counter()
    versions: dict[str, set[int]] = defaultdict(set)
    for p in points:
        eid, ver = p["entity_id"], int(p["source_version"])
        versions[eid].add(ver)
        seen[(eid, ver, p["chunk_hash"])] += 1
        if p["embed_model_version"] != model:
            wrong_model += 1
        s = silver.get(eid)
        if s is None or s[1]:
            orphaned += 1  # entity tombstoned or absent in silver
        elif ver < s[0]:
            stale += 1
        elif ver > s[0]:
            ahead += 1
    missing = sum(
        1
        for eid, (lsn, deleted, has_text) in silver.items()
        if not deleted and has_text and lsn not in versions.get(eid, ())
    )
    return {
        "points": len(points),
        "entities_in_index": len(versions),
        "stale_chunks": stale,
        "orphaned_chunks": orphaned,
        "duplicate_chunks": sum(n - 1 for n in seen.values() if n > 1),
        "wrong_model_chunks": wrong_model,
        "ahead_chunks": ahead,
        "missing_entities": missing,
    }
