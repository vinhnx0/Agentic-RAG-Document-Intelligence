# vectorstore/stores.py

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any
from uuid import uuid5, NAMESPACE_URL

from qdrant_client import QdrantClient, models

from schemas.documents import EmbeddedChunk


class BaseVectorStore(ABC):
    @abstractmethod
    def create_collection(
        self,
        collection_name: str,
        vector_size: int,
        distance: str,
        recreate: bool = False,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_embeddings(
        self,
        collection_name: str,
        embedded_chunks: list[EmbeddedChunk],
        batch_size: int,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_collection_info(self, collection_name: str) -> dict[str, Any]:
        raise NotImplementedError


class QdrantVectorStore(BaseVectorStore):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6333,
        timeout: float = 60.0,
    ) -> None:
        self.client = QdrantClient(
            host=host,
            port=port,
            timeout=timeout,
        )

    def create_collection(
        self,
        collection_name: str,
        vector_size: int,
        distance: str,
        recreate: bool = False,
    ) -> None:
        if vector_size <= 0:
            raise ValueError("vector_size must be > 0")

        distance_metric = self._resolve_distance(distance)

        if recreate:
            self.client.recreate_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=distance_metric,
                ),
            )
            return

        if self.client.collection_exists(collection_name=collection_name):
            return

        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=distance_metric,
            ),
        )

    def upsert_embeddings(
        self,
        collection_name: str,
        embedded_chunks: list[EmbeddedChunk],
        batch_size: int,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        total_upserted = 0

        for batch in self._batched(embedded_chunks, batch_size):
            points = [
                models.PointStruct(
                    id=self._build_point_id(chunk.chunk_id),
                    vector=chunk.embedding,
                    payload=self._build_payload(chunk),
                )
                for chunk in batch
            ]

            self.client.upsert(
                collection_name=collection_name,
                points=points,
            )

            total_upserted += len(points)

        return total_upserted

    def get_collection_info(self, collection_name: str) -> dict:
        collection_info = self.client.get_collection(collection_name)

        return {
            "collection_name": collection_name,
            "status": str(collection_info.status),
            "points_count": getattr(collection_info, "points_count", None),
            "vectors_count": getattr(collection_info, "vectors_count", None),
            "indexed_vectors_count": getattr(collection_info, "indexed_vectors_count", None),
        }

    @staticmethod
    def _resolve_distance(distance: str) -> models.Distance:
        normalized = distance.strip().lower()

        if normalized == "cosine":
            return models.Distance.COSINE
        if normalized == "dot":
            return models.Distance.DOT
        if normalized == "euclid":
            return models.Distance.EUCLID
        if normalized == "manhattan":
            return models.Distance.MANHATTAN

        raise ValueError(f"Unsupported Qdrant distance metric: {distance}")

    @staticmethod
    def _build_point_id(chunk_id: str) -> str:
        """
        Qdrant point IDs must be an unsigned integer or UUID.
        Existing chunk_id is a stable string, so convert it into a stable UUID.
        """
        return str(uuid5(NAMESPACE_URL, chunk_id))

    @staticmethod
    def _build_payload(chunk: EmbeddedChunk) -> dict[str, Any]:
        return {
            "chunk_id": chunk.chunk_id,
            "doc_id": chunk.doc_id,
            "corpus": chunk.corpus,
            "text": chunk.text,
            "chunk_index": chunk.chunk_index,
            "section_title": chunk.section_title,
            "section_path": chunk.section_path,
            "page_number": chunk.page_number,
            "embedding_model": chunk.embedding_model,
            "metadata": chunk.metadata,
            "embedded_at": chunk.embedded_at,
        }

    @staticmethod
    def _batched(
        items: list[EmbeddedChunk],
        batch_size: int,
    ) -> Iterable[list[EmbeddedChunk]]:
        for start in range(0, len(items), batch_size):
            yield items[start:start + batch_size]


class VectorStoreRegistry:
    def __init__(
        self,
        provider: str,
        host: str,
        port: int,
    ) -> None:
        if provider != "qdrant":
            raise ValueError(f"Unsupported vectorstore provider: {provider}")

        self._store = QdrantVectorStore(
            host=host,
            port=port,
        )

    def get_store(self) -> BaseVectorStore:
        return self._store