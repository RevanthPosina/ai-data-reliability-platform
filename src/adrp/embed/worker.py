"""G0 embedding worker: keep Qdrant in sync with silver.help_articles.

G0 polls the silver snapshot and diffs against local state. G6 replaces this with
incremental bronze reads + silver lookups, and adds the stale/orphan/duplicate checks.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyiceberg.catalog import load_catalog
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

from adrp.common import chunk_hash, chunk_text, point_id

log = logging.getLogger("embedder")
COLLECTION = "help_articles"
TABLE = "silver.help_articles"
POLL_S = float(os.environ.get("POLL_INTERVAL_S", "2"))


def catalog() -> Any:
    env = os.environ
    return load_catalog(
        "lake",
        type="rest",
        uri=env["CATALOG_URI"],
        warehouse=env["WAREHOUSE"],
        **{
            "s3.endpoint": env["S3_ENDPOINT"],
            "s3.access-key-id": env["S3_ACCESS_KEY"],
            "s3.secret-access-key": env["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
            "s3.region": env.get("AWS_REGION", "us-east-1"),
        },
    )


def entity_filter(entity_id: str, below_version: int | None = None) -> models.Filter:
    must: list[models.Condition] = [models.FieldCondition(key="entity_id", match=models.MatchValue(value=entity_id))]
    if below_version is not None:
        must.append(models.FieldCondition(key="source_version", range=models.Range(lt=below_version)))
    return models.Filter(must=must)


class Worker:
    def __init__(self) -> None:
        self.model_name = os.environ["EMBED_MODEL"]
        self.model_version = f"{self.model_name}@{os.environ['EMBED_MODEL_REVISION'][:12]}"
        self.model = SentenceTransformer(self.model_name, revision=os.environ["EMBED_MODEL_REVISION"], device="cpu")
        self.qdrant = QdrantClient(url=os.environ["QDRANT_URL"])
        self.state_path = Path(os.environ.get("STATE_DIR", "/state")) / "embedder.json"
        self.state: dict[str, Any] = (
            json.loads(self.state_path.read_text()) if self.state_path.exists() else {"snapshot": None, "versions": {}}
        )
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        if not self.qdrant.collection_exists(COLLECTION):
            dim = self.model.get_embedding_dimension()
            self.qdrant.create_collection(
                COLLECTION, vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE)
            )
            self.qdrant.create_payload_index(COLLECTION, "entity_id", models.PayloadSchemaType.KEYWORD, wait=True)
            self.qdrant.create_payload_index(COLLECTION, "source_version", models.PayloadSchemaType.INTEGER, wait=True)

    def _save(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.replace(self.state_path)

    def upsert_version(self, entity_id: str, version: int, title: str, body: str) -> None:
        chunks = chunk_text(f"{title}\n\n{body}")
        vectors = self.model.encode(chunks, normalize_embeddings=True)
        now = datetime.now(timezone.utc).isoformat()
        points = [
            models.PointStruct(
                id=point_id(entity_id, version, i),
                vector=vec.tolist(),
                payload={
                    "entity_id": entity_id,
                    "source_version": version,
                    "chunk_no": i,
                    "chunk_hash": chunk_hash(text),
                    "embed_model_version": self.model_version,
                    "ingested_at": now,
                    "text": text,
                },
            )
            for i, (text, vec) in enumerate(zip(chunks, vectors, strict=True))
        ]
        # New version first, then remove older versions (PLAN G6 ordering).
        self.qdrant.upsert(COLLECTION, points=points, wait=True)
        self.qdrant.delete(COLLECTION, points_selector=entity_filter(entity_id, below_version=version), wait=True)
        log.info("upserted version=%s chunks=%d ack=%s", version, len(points), datetime.now(timezone.utc).isoformat())

    def delete_entity(self, entity_id: str) -> None:
        self.qdrant.delete(COLLECTION, points_selector=entity_filter(entity_id), wait=True)
        log.info("deleted entity chunks ack=%s", datetime.now(timezone.utc).isoformat())

    def poll_once(self, table: Any) -> None:
        table.refresh()
        snap = table.current_snapshot()
        if snap is None or snap.snapshot_id == self.state["snapshot"]:
            return
        rows = table.scan(selected_fields=("id", "title", "body", "lsn", "is_deleted")).to_arrow().to_pylist()
        versions: dict[str, int] = self.state["versions"]
        for row in rows:
            entity_id = str(row["id"])
            known = versions.get(entity_id)
            if row["is_deleted"]:
                if known is not None:
                    self.delete_entity(entity_id)
                    del versions[entity_id]
            elif known is None or row["lsn"] > known:
                self.upsert_version(entity_id, row["lsn"], row["title"], row["body"])
                versions[entity_id] = row["lsn"]
        self.state["snapshot"] = snap.snapshot_id
        self._save()

    def run(self) -> None:
        cat = catalog()
        table = None
        while True:
            try:
                if table is None:
                    table = cat.load_table(TABLE)
                self.poll_once(table)
            except Exception as e:  # noqa: BLE001 - keep the loop alive; table may not exist yet
                log.warning("poll failed: %s: %s", type(e).__name__, e)
                table = None
            time.sleep(POLL_S)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    Worker().run()
