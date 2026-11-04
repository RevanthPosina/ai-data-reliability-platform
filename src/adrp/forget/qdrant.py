"""Qdrant side of the forget workflow (G8): delete a subject's points, make the optimizer rewrite the segments,
and say what bytes could still be on disk.

Facts from the G8 spike (Qdrant v1.19.2):
  * delete-by-filter removes points logically; the bytes stay in the segment's payload storage and in the WAL.
  * The vacuum optimizer rewrites a segment when its deleted share passes `deleted_threshold`, but only for
    segments with at least `vacuum_min_vector_number` vectors, and that setting cannot go below 100. Segments with
    fewer than 100 vectors are never vacuumed: their deleted bytes stay (a bound).
  * The WAL (`wal_capacity_mb`, 32 MiB segments, not changeable by PATCH) keeps the operation payloads until about
    twice that much later write volume has rolled them out. There is no API to force it (a bound).
  * Vacuum shows up as a changed segment directory uuid under .../segments.

REST over http://127.0.0.1:6333 (as scripts/g6_check.py); raw-byte greps and directory listings go through
`docker compose exec qdrant`. Counts and booleans only.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from adrp.forget.rawscan import GrepCount, Subject, compose_exec, grep_files, wait_until

BASE = "http://127.0.0.1:6333"
COLLECTION = "text_chunks"
STORAGE = "/qdrant/storage"
SNAPSHOTS = "/qdrant/snapshots"
MIN_VECTORS_FOR_VACUUM = 100
BATCH = 200  # entity ids per filter
_NAME = re.compile(r"[A-Za-z0-9_-]+")


def _req(method: str, path: str, body: Any = None, timeout: float = 60) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"qdrant {method} {path} -> {e.code}: {e.read().decode(errors='replace')[:300]}") from None


def _name(collection: str) -> str:
    if not _NAME.fullmatch(collection):
        raise ValueError(f"bad collection name: {collection!r}")
    return collection


def entity_filter(entity_ids: Sequence[str]) -> dict[str, Any]:
    """Payload filter matching points whose entity_id is any of `entity_ids` (`support_tickets:123`)."""
    return {"must": [{"key": "entity_id", "match": {"any": list(entity_ids)}}]}


def _batches(items: Sequence[str]) -> Iterable[list[str]]:
    for i in range(0, len(items), BATCH):
        yield list(items[i : i + BATCH])


# ---- logical ---------------------------------------------------------------------------------------------


def count_entities(entity_ids: Sequence[str], collection: str = COLLECTION) -> int:
    """Points in the collection whose entity_id is in `entity_ids`: a filtered scroll (payload and vectors off),
    paged to the end. 0 means the subject is logically absent from the index."""
    total = 0
    for batch in _batches(list(entity_ids)):
        offset: Any = None
        while True:
            body: dict[str, Any] = {
                "filter": entity_filter(batch),
                "limit": 1000,
                "with_payload": False,
                "with_vector": False,
            }
            if offset is not None:
                body["offset"] = offset
            res = _req("POST", f"/collections/{_name(collection)}/points/scroll", body)["result"]
            total += len(res["points"])
            offset = res.get("next_page_offset")
            if offset is None:
                break
    return total


def delete_entities(entity_ids: Sequence[str], collection: str = COLLECTION) -> int:
    """Delete-by-filter with wait=true (returns once applied). Returns the number of points that were present
    just before (counted first). Call only after the silver tombstones exist: the embedder re-creates points for
    an entity whose latest silver row still has content."""
    n = count_entities(entity_ids, collection)
    for batch in _batches(list(entity_ids)):
        _req("POST", f"/collections/{_name(collection)}/points/delete?wait=true", {"filter": entity_filter(batch)})
    return n


# ---- optimizer -------------------------------------------------------------------------------------------


VACUUM_CONFIG = {"deleted_threshold": 0.0001, "vacuum_min_vector_number": MIN_VECTORS_FOR_VACUUM}


def set_vacuum_aggressive(collection: str = COLLECTION) -> None:
    """PATCH optimizers_config so a segment with any deletions (>= 0.01 % deleted) is vacuumed, down to the
    smallest size Qdrant allows (100 vectors; 1 is rejected). Idempotent; the optimizer re-evaluates at once."""
    _req("PATCH", f"/collections/{_name(collection)}", {"optimizers_config": VACUUM_CONFIG})


def collection_info(collection: str = COLLECTION) -> dict[str, Any]:
    out: dict[str, Any] = _req("GET", f"/collections/{_name(collection)}")["result"]
    return out


def wait_green(collection: str = COLLECTION, timeout_s: float = 120, every_s: float = 1.0) -> bool:
    """Collection status green and optimizer ok (no optimization pending or running)."""

    def ok() -> bool:
        info = collection_info(collection)
        return info.get("status") == "green" and info.get("optimizer_status") == "ok"

    return wait_until(ok, timeout_s, every_s)[0]


def segment_dirs(collection: str = COLLECTION) -> list[str]:
    """Segment directory names (uuids) of every shard, sorted. A vacuum or merge replaces a directory."""
    r = compose_exec(
        "qdrant", ["sh", "-c", f"ls -d {STORAGE}/collections/{_name(collection)}/*/segments/* 2>/dev/null"], timeout=60
    )
    return sorted(ln.rsplit("/", 1)[-1] for ln in r.text.splitlines() if ln.strip())


@dataclass(frozen=True)
class VacuumObservation:
    changed: bool  # at least one segment directory was replaced
    replaced: int  # directories in `before` that are gone
    elapsed_s: float
    green: bool


def observe_vacuum(
    before: Sequence[str], collection: str = COLLECTION, timeout_s: float = 60, settle_s: float = 4.0
) -> VacuumObservation:
    """Wait for the segment directories to differ from `before` (the vacuum optimizer ran), then until the set has
    been stable for `settle_s` (the replaced directories are deleted only when the new segment is in place) and
    the collection is green. `changed=False` after the timeout means nothing was eligible: either the vacuum
    already ran before `before` was read, or every segment is below the 100-vector floor (see bounds())."""
    t0 = time.monotonic()
    ok, _ = wait_until(lambda: set(segment_dirs(collection)) != set(before), timeout_s, 1.0)
    if ok:
        stable_since, last = time.monotonic(), set(segment_dirs(collection))
        while time.monotonic() - t0 < timeout_s and time.monotonic() - stable_since < settle_s:
            time.sleep(1.0)
            now = set(segment_dirs(collection))
            if now != last:
                stable_since, last = time.monotonic(), now
    green = wait_green(collection, timeout_s=max(timeout_s - (time.monotonic() - t0), 10))
    gone = len(set(before) - set(segment_dirs(collection)))
    return VacuumObservation(ok, gone, time.monotonic() - t0, green)


def segment_vector_counts(collection: str = COLLECTION) -> list[int]:
    """`num_vectors` of each local segment (telemetry, details level 4)."""
    tel = _req("GET", "/telemetry?details_level=4")["result"]["collections"]["collections"]
    out: list[int] = []
    for c in tel:
        if c.get("id") != collection:
            continue
        for shard in c.get("shards", []):
            for seg in shard.get("local", {}).get("segments", []):
                out.append(int(seg["info"]["num_vectors"]))
    return out


# ---- raw bytes -------------------------------------------------------------------------------------------


def subject_patterns(subject: Subject) -> list[bytes]:
    """What Qdrant stores about a subject. Each point's payload is JSON text with `"entity_id":"<table>:<pk>"` and
    the chunk `text` (which holds names and emails: pass them as Subject.canaries to search for them).

    Patterns: every canary; the quoted JSON value `"support_tickets:123"` (the closing quote stops
    `support_tickets:12` matching `support_tickets:123`); and the bare entity string for ids >= 10**12, which also
    finds the payload-index copy of the value. For a small id the index copy is not searched (a bare
    `support_tickets:12` would match `support_tickets:123`); the filtered scroll and the quoted payload pattern
    cover that id. Point ids are uuid5 hashes of those strings and are not searched."""
    out = {c.encode() for c in subject.canaries}
    for e in subject.entity_strings():
        out.add(f'"{e}"'.encode())
        if int(e.rsplit(":", 1)[1]) >= 10**12:
            out.add(e.encode())
    return sorted(out)


@dataclass(frozen=True)
class QdrantRaw:
    segment_files: int  # payload storage / vector / index files under .../segments/
    wal_files: int  # files under .../wal/
    other_files: int
    errors: int

    @property
    def total(self) -> int:
        return self.segment_files + self.wal_files + self.other_files

    @property
    def absent(self) -> bool:
        return self.total == 0 and self.errors == 0


def classify(g: GrepCount) -> QdrantRaw:
    """Split grep hits by where they are: segments, wal, other (e.g. payload_index.json)."""
    seg = wal = other = 0
    for p in g.paths:
        if "/wal/" in p:
            wal += 1
        elif "/segments/" in p:
            seg += 1
        else:
            other += 1
    return QdrantRaw(seg, wal, other, g.errors)


def raw_grep(patterns: Iterable[bytes], root: str = STORAGE) -> QdrantRaw:
    """grep -rlaF over /qdrant/storage, classified by location. Needles go in on stdin."""
    return classify(grep_files("qdrant", [root], patterns))


def snapshots_empty() -> bool:
    """No snapshot is registered (collection and storage-level API lists) and /qdrant/snapshots holds no file.
    A snapshot is a full copy of the collection, deleted points included, so it must not exist."""
    api = _req("GET", "/snapshots")["result"]
    per = [
        _req("GET", f"/collections/{_name(c['name'])}/snapshots")["result"]
        for c in _req("GET", "/collections")["result"]["collections"]
    ]
    r = compose_exec("qdrant", ["sh", "-c", f"find {SNAPSHOTS} -type f 2>/dev/null | head -5"], timeout=60)
    return not api and not any(per) and not r.text.strip()


# ---- bounds ----------------------------------------------------------------------------------------------


def bound_strings(small_segments: int, wal_capacity_mb: int, raw: QdrantRaw) -> list[str]:
    """The structured `bound` statements for Qdrant. Pure. Stated only for the part that is still present:
    nothing is claimed removed that the grep still finds; nothing is flagged that the grep shows clean."""
    out: list[str] = []
    if raw.segment_files:
        reason = (
            f"{small_segments} segment(s) hold fewer than {MIN_VECTORS_FOR_VACUUM} vectors; the vacuum optimizer never "
            f"rewrites those (vacuum_min_vector_number cannot go below {MIN_VECTORS_FOR_VACUUM})"
            if small_segments
            else "no segment is below the vacuum floor, so the optimizer has not yet rewritten the matching segment"
        )
        out.append(
            f"qdrant segments: {raw.segment_files} file(s) still contain subject bytes; {reason}. Bounded, not removed."
        )
    if raw.wal_files:
        out.append(
            f"qdrant WAL: {raw.wal_files} file(s) still contain subject bytes; they are rolled out after about "
            f"2 x wal_capacity_mb = {2 * wal_capacity_mb} MiB of later writes (segments of {wal_capacity_mb} MiB; "
            "wal_capacity_mb is not changeable at runtime and there is no API to force it). Bounded, not removed."
        )
    if raw.other_files:
        out.append(f"qdrant other: {raw.other_files} file(s) outside segments/wal contain subject bytes. Not removed.")
    return out


def bounds(raw: QdrantRaw, collection: str = COLLECTION) -> list[str]:
    """bound_strings() with the live collection's small-segment count and WAL capacity."""
    small = sum(1 for n in segment_vector_counts(collection) if n < MIN_VECTORS_FOR_VACUUM)
    wal_mb = int(collection_info(collection)["config"]["wal_config"]["wal_capacity_mb"])
    return bound_strings(small, wal_mb, raw)


def physical_pass(subject: Subject, collection: str = COLLECTION, timeout_s: float = 60) -> tuple[QdrantRaw, list[str]]:
    """The physical procedure in one call: aggressive vacuum config, observe the vacuum, grep, bounds.
    Run after delete_entities(). Returns (raw grep result, bound statements for whatever is still present)."""
    before = segment_dirs(collection)
    set_vacuum_aggressive(collection)
    observe_vacuum(before, collection, timeout_s)
    raw = raw_grep(subject_patterns(subject))
    return raw, bounds(raw, collection)
