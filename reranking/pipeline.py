# reranking/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from reranking.rerankers import RerankerRegistry
from retrieval.pipeline import RetrievalPipeline


class RerankingPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

        self.reranking_config = self.config.get("reranking", {})
        self.retrieval_config = self.config.get("retrieval", {})

        self.retrieval_pipeline = RetrievalPipeline(config_path)
        self.reranker_registry = self._build_reranker_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_reranker_registry(self) -> RerankerRegistry:
        provider = self.reranking_config.get("provider", "cross_encoder")
        model_name = self.reranking_config.get("model_name")

        if not model_name:
            raise ValueError("Missing reranking.model_name in config")

        return RerankerRegistry(
            provider=provider,
            model_name=model_name,
            batch_size=self.reranking_config.get("batch_size", 16),
        )

    def run(
        self,
        query: str,
        retrieval_top_k: int | None = None,
        final_top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> dict[str, Any]:
        clean_query = query.strip()

        if not clean_query:
            raise ValueError("query must not be empty")

        resolved_retrieval_top_k = (
            retrieval_top_k
            or self.retrieval_config.get("top_k", 20)
        )
        resolved_final_top_k = (
            final_top_k
            or self.reranking_config.get("final_top_k", 5)
        )

        if resolved_final_top_k > resolved_retrieval_top_k:
            raise ValueError(
                "final_top_k must be <= retrieval_top_k. "
                f"Got final_top_k={resolved_final_top_k}, "
                f"retrieval_top_k={resolved_retrieval_top_k}"
            )

        retrieval_output = self.retrieval_pipeline.run(
            query=clean_query,
            top_k=resolved_retrieval_top_k,
            score_threshold=score_threshold,
        )

        reranker = self.reranker_registry.get_reranker()
        reranked_results = reranker.rerank(
            query=clean_query,
            results=retrieval_output["results"],
            final_top_k=resolved_final_top_k,
        )

        return {
            "query": clean_query,
            "collection_name": retrieval_output["collection_name"],
            "embedding_model": retrieval_output["embedding_model"],
            "reranker_model": reranker.get_model_name(),
            "retrieval_top_k": resolved_retrieval_top_k,
            "final_top_k": resolved_final_top_k,
            "retrieval_result_count": retrieval_output["result_count"],
            "reranked_result_count": len(reranked_results),
            "retrieval_results": retrieval_output["results"],
            "reranked_results": reranked_results,
        }

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)