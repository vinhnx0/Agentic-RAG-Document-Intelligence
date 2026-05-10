# embedding/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from embedding.embedders import EmbedderRegistry
from schemas.documents import DocumentChunk, EmbeddedChunk


class EmbeddingPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.registry = self._build_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_registry(self) -> EmbedderRegistry:
        embedding_config = self.config.get("embedding", {})
        model_name = embedding_config.get("model_name")

        if not model_name:
            raise ValueError("Missing embedding.model_name in config")

        return EmbedderRegistry(
            model_name=model_name,
            batch_size=embedding_config.get("batch_size", 32),
            normalize_embeddings=embedding_config.get("normalize_embeddings", True),
        )

    def run(self, chunks: list[DocumentChunk]) -> list[EmbeddedChunk]:
        embedder = self.registry.get_embedder()

        texts = [chunk.text for chunk in chunks]
        vectors = embedder.embed_texts(texts)

        if len(chunks) != len(vectors):
            raise RuntimeError(
                "Embedding count mismatch: "
                f"{len(chunks)} chunks but {len(vectors)} vectors"
            )

        embedded_chunks: list[EmbeddedChunk] = []

        for chunk, vector in zip(chunks, vectors, strict=True):
            embedded_chunk = EmbeddedChunk(
                chunk_id=chunk.chunk_id,
                doc_id=chunk.doc_id,
                corpus=chunk.corpus,
                text=chunk.text,
                embedding=vector,
                embedding_model=embedder.get_model_name(),
                chunk_index=chunk.chunk_index,
                section_title=chunk.section_title,
                section_path=chunk.section_path,
                page_number=chunk.page_number,
                metadata={
                    **chunk.metadata,
                    "embedding_dimension": len(vector),
                    "embedding_normalized": self.config.get("embedding", {}).get(
                        "normalize_embeddings",
                        True,
                    ),
                },
            )
            embedded_chunks.append(embedded_chunk)

        return embedded_chunks

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)