# chunking/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from chunking.chunkers import ChunkerRegistry
from schemas.documents import DocumentChunk, ParsedDocument


class ChunkingPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.registry = self._build_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_registry(self) -> ChunkerRegistry:
        chunking_config = self.config.get("chunking", {})
        strategy = chunking_config.get("strategy")

        if not strategy:
            raise ValueError("Missing chunking.strategy in config")

        if strategy != "section_then_tokens":
            raise ValueError(f"Unsupported chunking.strategy: {strategy}")

        return ChunkerRegistry(
            max_tokens=chunking_config.get("max_tokens", 350),
            overlap_tokens=chunking_config.get("overlap_tokens", 50),
            min_chunk_chars=chunking_config.get("min_chunk_chars", 150),
        )

    def run(self, documents: list[ParsedDocument]) -> list[DocumentChunk]:
        chunker = self.registry.get_chunker()

        chunks: list[DocumentChunk] = []
        for document in documents:
            document_chunks = chunker.chunk(document)
            chunks.extend(document_chunks)

        return chunks

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)