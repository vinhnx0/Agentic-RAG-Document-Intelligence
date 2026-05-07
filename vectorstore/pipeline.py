# vectorstore/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from schemas.documents import EmbeddedChunk
from vectorstore.stores import VectorStoreRegistry


class VectorStorePipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.vectorstore_config = self.config.get("vectorstore", {})
        self.registry = self._build_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_registry(self) -> VectorStoreRegistry:
        provider = self.vectorstore_config.get("provider", "qdrant")
        host = self.vectorstore_config.get("host", "localhost")
        port = self.vectorstore_config.get("port", 6333)

        return VectorStoreRegistry(
            provider=provider,
            host=host,
            port=port,
        )

    def run(self, embedded_chunks: list[EmbeddedChunk]) -> dict[str, Any]:
        if not embedded_chunks:
            raise ValueError("No embedded chunks provided to VectorStorePipeline")

        collection_name = self.vectorstore_config.get("collection_name")
        if not collection_name:
            raise ValueError("Missing vectorstore.collection_name in config")

        distance = self.vectorstore_config.get("distance", "cosine")
        recreate_collection = self.vectorstore_config.get("recreate_collection", False)
        batch_size = self.vectorstore_config.get("batch_size", 64)

        embedding_dimensions = {
            len(chunk.embedding)
            for chunk in embedded_chunks
        }

        if len(embedding_dimensions) != 1:
            raise ValueError(
                "All embeddings must have the same dimension. "
                f"Found dimensions: {sorted(embedding_dimensions)}"
            )

        vector_size = embedding_dimensions.pop()

        store = self.registry.get_store()

        store.create_collection(
            collection_name=collection_name,
            vector_size=vector_size,
            distance=distance,
            recreate=recreate_collection,
        )

        upserted_count = store.upsert_embeddings(
            collection_name=collection_name,
            embedded_chunks=embedded_chunks,
            batch_size=batch_size,
        )

        collection_info = store.get_collection_info(collection_name)

        return {
            "provider": self.vectorstore_config.get("provider", "qdrant"),
            "collection_name": collection_name,
            "distance": distance,
            "recreate_collection": recreate_collection,
            "embedding_dimension": vector_size,
            "embedded_chunk_count": len(embedded_chunks),
            "upserted_vector_count": upserted_count,
            "collection_info": collection_info,
        }

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)