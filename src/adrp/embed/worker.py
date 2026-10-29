"""Embedding worker (PLAN G6): keeps the Qdrant index in step with the text tables.

Driven by incremental reads of *bronze* (append-only change log; the pyiceberg incremental append scan returns
only rows added by append snapshots, so compaction, expiry and the delete snapshots G8 produces are skipped).
For each changed entity it looks up the current version and is_deleted in *silver*; the decision per entity is in
adrp.embed.logic. The pointer into bronze moves only after Qdrant has acknowledged (wait=true) every write of the
poll, so a crash re-does work, never skips it; re-doing is a no-op.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyiceberg.expressions import In
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

from adrp.embed import logic
from adrp.embed.lake import catalog, silver_lookup
from adrp.embed.store import QdrantStore

log = logging.getLogger("embedder")
POLL_S = float(os.environ.get("POLL_INTERVAL_S", "2"))
STUCK_AFTER_S = 900  # a change silver never reaches (e.g. it was quarantined) is reported, not dropped
SOURCE_TABLES = [f"public.{t}" for t in logic.TEXT_TABLES]


class Encoder:
    def __init__(self, model: SentenceTransformer) -> None:
        self.model = model

    def encode(self, texts: list[str]) -> list[list[float]]:
        return [v.tolist() for v in self.model.encode(texts, normalize_embeddings=True, batch_size=32)]


class Worker:
    def __init__(self) -> None:
        name, rev = os.environ["EMBED_MODEL"], os.environ["EMBED_MODEL_REVISION"]
        self.model_version = f"{name}@{rev[:12]}"
        model = SentenceTransformer(name, revision=rev, device="cpu")
        self.encoder = Encoder(model)
        self.store = QdrantStore(QdrantClient(url=os.environ["QDRANT_URL"]), model.get_embedding_dimension())
        state_dir = Path(os.environ.get("STATE_DIR", "/state"))
        self.state_path = state_dir / "embedder.json"
        self.events_path = Path(os.environ.get("EVENT_LOG_DIR", "/eventlog")) / "embedder" / "actions.jsonl"
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self.state: dict[str, Any] = {"snapshot": None, "pending": {}, "since": {}, "batch": 0}
        if self.state_path.exists():
            self.state.update(json.loads(self.state_path.read_text()))

    def save(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.replace(self.state_path)

    def changed_since_pointer(self, bronze: Any, snap_id: int) -> dict[str, int]:
        flt = In("source_table", SOURCE_TABLES)
        cols = ("source_table", "pk", "lsn")
        last = self.state["snapshot"]
        if last is None or bronze.snapshot_by_id(last) is None:
            # first run, or the pointer's snapshot was expired: rescan everything still in bronze
            reason = "first run" if last is None else "pointer snapshot expired"
            log.info("full bronze scan (%s)", reason)
            rows = bronze.scan(row_filter=flt, selected_fields=cols, snapshot_id=snap_id).to_arrow().to_pylist()
        else:
            scan = bronze.incremental_append_scan(
                from_snapshot_id_exclusive=last, to_snapshot_id_inclusive=snap_id, row_filter=flt, selected_fields=cols
            )
            rows = scan.to_arrow().to_pylist()
        return logic.bronze_changes(rows)

    def log_action(self, a: logic.Action, ack_ns: int, batch: int) -> None:
        record = {
            "batch": batch, "entity": a.entity, "lsn": a.version, "kind": a.kind, "chunks": a.chunks,
            "embedded": a.embedded, "reused": a.reused, "ack_ns": ack_ns,
        }  # fmt: skip
        with open(self.events_path, "a") as fh:
            fh.write(json.dumps(record) + "\n")

    def poll_once(self, cat: Any) -> dict[str, int]:
        bronze = cat.load_table("bronze.cdc_events")
        snap = bronze.current_snapshot()
        if snap is None:
            return {}
        new: dict[str, int] = {}
        if snap.snapshot_id != self.state["snapshot"]:
            new = self.changed_since_pointer(bronze, snap.snapshot_id)
        pending = {k: int(v) for k, v in self.state["pending"].items()}
        required = logic.merge_required(pending, new)
        if not required and snap.snapshot_id == self.state["snapshot"]:
            return {}
        by_table: dict[str, list[int]] = {}
        for eid in required:
            t, rid = logic.split_entity(eid)
            by_table.setdefault(t, []).append(rid)
        silver: dict[str, dict[str, Any]] = {}
        for t, ids in by_table.items():
            silver.update(silver_lookup(cat, t, ids))
        ready, still = logic.split_ready(required, silver)
        self.state["batch"] += 1
        batch = self.state["batch"]
        counts = {
            "changed": len(new),
            "ready": len(ready),
            "upsert": 0,
            "delete": 0,
            "unchanged": 0,
            "embedded": 0,
            "reused": 0,
        }
        for eid in ready:
            a = logic.process_entity(eid, silver[eid], self.store, self.encoder, self.model_version,
                                     datetime.now(timezone.utc).isoformat())  # fmt: skip
            ack = time.time_ns()  # every Store write above returned only after Qdrant acked (wait=true)
            counts[a.kind] += 1
            counts["embedded"] += a.embedded
            counts["reused"] += a.reused
            self.log_action(a, ack, batch)
        now = time.time()
        since = {k: self.state["since"].get(k, now) for k in still}
        self.state.update(snapshot=snap.snapshot_id, pending={k: int(v) for k, v in still.items()}, since=since)
        self.save()
        stuck = sum(1 for t in since.values() if now - t > STUCK_AFTER_S)
        counts["pending"] = len(still)
        counts["stuck"] = stuck
        if stuck:
            log.warning("%d change(s) have waited more than %d s for silver (quarantined?)", stuck, STUCK_AFTER_S)
        return counts

    def run(self) -> None:
        cat = catalog()
        while True:
            try:
                counts = self.poll_once(cat)
                if counts:
                    log.info("poll %s", json.dumps(counts))
            except Exception as e:  # noqa: BLE001 - keep the loop alive; tables may not exist yet
                log.warning("poll failed: %s: %s", type(e).__name__, str(e)[:300])
            time.sleep(POLL_S)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    Worker().run()
