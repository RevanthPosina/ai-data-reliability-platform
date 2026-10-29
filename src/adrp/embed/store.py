"""Qdrant implementation of the embedding Store (all writes use wait=true)."""

from __future__ import annotations

from typing import Any

from qdrant_client import QdrantClient, models

from adrp.embed.logic import COLLECTION, Point


def entity_filter(eid: str, below_version: int | None = None) -> models.Filter:
    must: list[models.Condition] = [models.FieldCondition(key="entity_id", match=models.MatchValue(value=eid))]
    if below_version is not None:
        must.append(models.FieldCondition(key="source_version", range=models.Range(lt=below_version)))
    return models.Filter(must=must)


class QdrantStore:
    def __init__(self, client: QdrantClient, dim: int) -> None:
        self.client = client
        if not client.collection_exists(COLLECTION):
            client.create_collection(
                COLLECTION, vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE)
            )
            client.create_payload_index(COLLECTION, "entity_id", models.PayloadSchemaType.KEYWORD, wait=True)
            client.create_payload_index(COLLECTION, "source_version", models.PayloadSchemaType.INTEGER, wait=True)

    def _entity_points(self, eid: str, with_vectors: bool) -> list[Any]:
        out, offset = [], None
        while True:
            points, offset = self.client.scroll(
                COLLECTION, scroll_filter=entity_filter(eid), limit=256, offset=offset,
                with_payload=True, with_vectors=with_vectors,
            )  # fmt: skip
            out += points
            if offset is None:
                return out

    def current_matches(self, eid: str, version: int, hashes: list[str], model: str) -> bool:
        from adrp.common import point_id

        ids = [point_id(eid, version, i) for i in range(len(hashes))]
        got = {str(p.id): p.payload for p in self.client.retrieve(COLLECTION, ids=ids, with_payload=True)}
        return len(got) == len(ids) and all(
            got[ids[i]] is not None
            and got[ids[i]]["chunk_hash"] == hashes[i]
            and got[ids[i]]["embed_model_version"] == model
            for i in range(len(ids))
        )

    def vectors_by_hash(self, eid: str, model: str) -> dict[str, list[float]]:
        out: dict[str, list[float]] = {}
        for p in self._entity_points(eid, with_vectors=True):
            if p.payload and p.payload.get("embed_model_version") == model and isinstance(p.vector, list):
                out[p.payload["chunk_hash"]] = p.vector
        return out

    def upsert(self, points: list[Point]) -> None:
        self.client.upsert(
            COLLECTION,
            points=[models.PointStruct(id=p.id, vector=p.vector, payload=p.payload) for p in points],
            wait=True,
        )

    def delete_older(self, eid: str, below_version: int) -> None:
        self.client.delete(COLLECTION, points_selector=entity_filter(eid, below_version), wait=True)

    def delete_entity(self, eid: str) -> None:
        self.client.delete(COLLECTION, points_selector=entity_filter(eid), wait=True)
