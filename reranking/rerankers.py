# reranking/rerankers.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sentence_transformers import CrossEncoder


class BaseReranker(ABC):
    @abstractmethod
    def rerank(
        self,
        query: str,
        results: list[dict[str, Any]],
        final_top_k: int,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_model_name(self) -> str:
        raise NotImplementedError


class CrossEncoderReranker(BaseReranker):
    def __init__(
        self,
        model_name: str,
        batch_size: int = 16,
    ) -> None:
        if not model_name:
            raise ValueError("model_name must not be empty")
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        self.model_name = model_name
        self.batch_size = batch_size
        self.model = CrossEncoder(model_name)

    def rerank(
        self,
        query: str,
        results: list[dict[str, Any]],
        final_top_k: int,
    ) -> list[dict[str, Any]]:
        clean_query = query.strip()

        if not clean_query:
            raise ValueError("query must not be empty")
        if final_top_k <= 0:
            raise ValueError("final_top_k must be > 0")
        if not results:
            return []

        pairs = [
            [clean_query, self._build_candidate_text(result)]
            for result in results
        ]

        scores = self.model.predict(
            pairs,
            batch_size=self.batch_size,
            show_progress_bar=True,
        )

        scored_results: list[dict[str, Any]] = []

        for result, score in zip(results, scores, strict=True):
            reranked_result = {
                **result,
                "retrieval_rank": result.get("rank"),
                "retrieval_score": result.get("score"),
                "rerank_score": float(score),
            }
            scored_results.append(reranked_result)

        scored_results.sort(
            key=lambda item: item["rerank_score"],
            reverse=True,
        )

        final_results: list[dict[str, Any]] = []
        for rank, result in enumerate(scored_results[:final_top_k], start=1):
            final_results.append(
                {
                    **result,
                    "rerank_rank": rank,
                }
            )

        return final_results

    def get_model_name(self) -> str:
        return self.model_name

    @staticmethod
    def _build_candidate_text(result: dict[str, Any]) -> str:
        section_title = result.get("section_title") or ""
        section_path = result.get("section_path") or []
        text = result.get("text") or ""

        path_text = " > ".join(section_path)

        return (
            f"Section title: {section_title}\n"
            f"Section path: {path_text}\n\n"
            f"{text}"
        ).strip()


class RerankerRegistry:
    def __init__(
        self,
        provider: str,
        model_name: str,
        batch_size: int = 16,
    ) -> None:
        if provider != "cross_encoder":
            raise ValueError(f"Unsupported reranker provider: {provider}")

        self._reranker = CrossEncoderReranker(
            model_name=model_name,
            batch_size=batch_size,
        )

    def get_reranker(self) -> BaseReranker:
        return self._reranker