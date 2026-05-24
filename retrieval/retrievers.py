# retrieval/retrievers.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from qdrant_client import QdrantClient, models


class BaseRetriever(ABC):
    @abstractmethod
    def retrieve(
        self,
        collection_name: str,
        query_vector: list[float],
        top_k: int,
        score_threshold: float | None = None,
        metadata_filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError


class QdrantRetriever(BaseRetriever):
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

    def retrieve(
        self,
        collection_name: str,
        query_vector: list[float],
        top_k: int,
        score_threshold: float | None = None,
        metadata_filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if top_k <= 0:
            raise ValueError("top_k must be > 0")

        query_filter = self._build_qdrant_filter(metadata_filters)

        query_response = self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
            with_vectors=False,
        )

        search_results = query_response.points

        return [
            self._normalize_result(rank=rank, result=result)
            for rank, result in enumerate(search_results, start=1)
        ]

    @staticmethod
    def _normalize_result(rank: int, result: Any) -> dict[str, Any]:
        payload = result.payload or {}

        return {
            "rank": rank,
            "score": float(result.score),
            "point_id": str(result.id),
            "chunk_id": payload.get("chunk_id"),
            "doc_id": payload.get("doc_id"),
            "corpus": payload.get("corpus"),
            "text": payload.get("text"),
            "chunk_index": payload.get("chunk_index"),
            "section_title": payload.get("section_title"),
            "section_path": payload.get("section_path", []),
            "page_number": payload.get("page_number"),
            "source_path": payload.get("metadata", {}).get("source_path"),
            "metadata": payload.get("metadata", {}),
        }
    
    @staticmethod
    def _build_qdrant_filter(
        metadata_filters: dict[str, Any] | None,
    ) -> models.Filter | None:
        if not metadata_filters:
            return None

        conditions: list[models.FieldCondition] = []

        for key, value in metadata_filters.items():
            if value is None:
                continue

            conditions.append(
                models.FieldCondition(
                    key=f"metadata.{key}",
                    match=models.MatchValue(value=value),
                )
            )

        if not conditions:
            return None

        return models.Filter(must=conditions)


class RetrieverRegistry:
    def __init__(
        self,
        provider: str,
        host: str,
        port: int,
    ) -> None:
        if provider != "qdrant":
            raise ValueError(f"Unsupported retriever provider: {provider}")

        self._retriever = QdrantRetriever(
            host=host,
            port=port,
        )

    def get_retriever(self) -> BaseRetriever:
        return self._retriever