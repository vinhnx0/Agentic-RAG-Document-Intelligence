# retrieval/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from embedding.embedders import EmbedderRegistry
from retrieval.retrievers import RetrieverRegistry


class RetrievalPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

        self.embedding_config = self.config.get("embedding", {})
        self.vectorstore_config = self.config.get("vectorstore", {})
        self.retrieval_config = self.config.get("retrieval", {})

        self.embedder_registry = self._build_embedder_registry()
        self.retriever_registry = self._build_retriever_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_embedder_registry(self) -> EmbedderRegistry:
        model_name = self.embedding_config.get("model_name")
        if not model_name:
            raise ValueError("Missing embedding.model_name in config")

        return EmbedderRegistry(
            model_name=model_name,
            batch_size=self.embedding_config.get("batch_size", 32),
            normalize_embeddings=self.embedding_config.get(
                "normalize_embeddings",
                True,
            ),
        )

    def _build_retriever_registry(self) -> RetrieverRegistry:
        provider = self.vectorstore_config.get("provider", "qdrant")
        host = self.vectorstore_config.get("host", "localhost")
        port = self.vectorstore_config.get("port", 6333)

        return RetrieverRegistry(
            provider=provider,
            host=host,
            port=port,
        )

    def run(
        self,
        query: str,
        top_k: int | None = None,
        score_threshold: float | None = None,
        metadata_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_query = query.strip()
        if not clean_query:
            raise ValueError("query must not be empty")

        collection_name = self.vectorstore_config.get("collection_name")
        if not collection_name:
            raise ValueError("Missing vectorstore.collection_name in config")

        resolved_top_k = top_k or self.retrieval_config.get("top_k", 5)
        resolved_score_threshold = (
            score_threshold
            if score_threshold is not None
            else self.retrieval_config.get("score_threshold")
        )

        embedder = self.embedder_registry.get_embedder()
        query_vectors = embedder.embed_texts([clean_query])

        if len(query_vectors) != 1:
            raise RuntimeError(
                f"Expected exactly 1 query vector, got {len(query_vectors)}"
            )

        retriever = self.retriever_registry.get_retriever()
        results = retriever.retrieve(
            collection_name=collection_name,
            query_vector=query_vectors[0],
            top_k=resolved_top_k,
            score_threshold=resolved_score_threshold,
            metadata_filters=metadata_filters,
        )

        return {
            "query": clean_query,
            "collection_name": collection_name,
            "embedding_model": embedder.get_model_name(),
            "top_k": resolved_top_k,
            "score_threshold": resolved_score_threshold,
            "result_count": len(results),
            "results": results,
            "metadata_filters": metadata_filters or {},
        }

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)