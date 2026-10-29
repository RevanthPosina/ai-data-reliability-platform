"""Index consistency checks (PLAN G6): stale, orphaned, duplicate and wrong-model chunks.

Compares Qdrant with silver directly (it does not go through the worker's state), so it also catches a worker bug.
CLI (run inside the embedder container):
  python -m adrp.embed.checks            ->  CHECKS {json}
  python -m adrp.embed.checks silver     ->  SILVER {entity: [lsn, is_deleted]}   (pyiceberg view of silver)
  python -m adrp.embed.checks points     ->  POINTS {point_id: [entity, source_version, chunk_hash, ingested_at, model]}
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

from qdrant_client import QdrantClient

from adrp.embed import logic
from adrp.embed.lake import catalog
from adrp.embed.logic import evaluate


def all_points(client: QdrantClient) -> list[Any]:
    out, offset = [], None
    while True:
        pts, offset = client.scroll(logic.COLLECTION, limit=1000, offset=offset, with_payload=True, with_vectors=False)
        out += pts
        if offset is None:
            return out


def silver_state(cat: Any) -> dict[str, tuple[int, bool, bool]]:
    """entity -> (lsn, is_deleted, has_text)"""
    out: dict[str, tuple[int, bool, bool]] = {}
    for table, cols in logic.TEXT_TABLES.items():
        t = cat.load_table(f"silver.{table}")
        for r in t.scan(selected_fields=("id", "lsn", "is_deleted", *cols)).to_arrow().to_pylist():
            doc = logic.document(table, r) if not r["is_deleted"] else ""
            out[logic.entity_id(table, int(r["id"]))] = (int(r["lsn"]), bool(r["is_deleted"]), bool(doc.strip()))
    return out


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "checks"
    client = QdrantClient(url=os.environ["QDRANT_URL"])
    cat = catalog()
    if cmd == "silver":
        print("SILVER " + json.dumps({k: [v[0], v[1]] for k, v in sorted(silver_state(cat).items())}))
        return
    pts = all_points(client)
    if cmd == "points":
        body = {
            str(p.id): [
                p.payload["entity_id"],
                p.payload["source_version"],
                p.payload["chunk_hash"],
                p.payload["ingested_at"],
                p.payload["embed_model_version"],
            ]
            for p in pts
            if p.payload
        }
        print("POINTS " + json.dumps(body))
        return
    name, rev = os.environ["EMBED_MODEL"], os.environ["EMBED_MODEL_REVISION"]
    result = evaluate([p.payload for p in pts if p.payload], silver_state(cat), f"{name}@{rev[:12]}")
    print("CHECKS " + json.dumps(result))


if __name__ == "__main__":
    main()
