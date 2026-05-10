# run_embedding.py

from __future__ import annotations

from collections import Counter
from typing import Any

from chunking.pipeline import ChunkingPipeline
from embedding.pipeline import EmbeddingPipeline
from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


def build_summary(embedded_chunks: list[Any]) -> dict[str, Any]:
    doc_counts = Counter(chunk.doc_id for chunk in embedded_chunks)

    embedding_dimensions = {
        len(chunk.embedding)
        for chunk in embedded_chunks
    }

    model_names = sorted(
        {
            chunk.embedding_model
            for chunk in embedded_chunks
        }
    )

    return {
        "embedded_chunk_count": len(embedded_chunks),
        "document_count_with_embeddings": len(doc_counts),
        "embedding_models": model_names,
        "embedding_dimensions": sorted(embedding_dimensions),
        "chunks_per_document": dict(sorted(doc_counts.items())),
        "embeddings_preview": [
            {
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.doc_id,
                "chunk_index": chunk.chunk_index,
                "section_title": chunk.section_title,
                "embedding_model": chunk.embedding_model,
                "embedding_dimension": len(chunk.embedding),
                "text_preview": chunk.text[:200],
            }
            for chunk in embedded_chunks[:10]
        ],
    }


def main() -> None:
    ingestion_pipeline = IngestionPipeline(CONFIG_PATH)
    raw_documents = ingestion_pipeline.run()

    parsing_pipeline = ParsingPipeline(CONFIG_PATH)
    parsed_documents = parsing_pipeline.run(raw_documents)

    chunking_pipeline = ChunkingPipeline(CONFIG_PATH)
    chunks = chunking_pipeline.run(parsed_documents)

    embedding_pipeline = EmbeddingPipeline(CONFIG_PATH)
    embedded_chunks = embedding_pipeline.run(chunks)

    processed_data_dir = embedding_pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "embedding")

    write_json(
        output_dir / "embedding_summary.json",
        build_summary(embedded_chunks),
    )
    write_json(
        output_dir / "embeddings.json",
        [to_jsonable(chunk) for chunk in embedded_chunks],
    )

    print(f"Embedded {len(chunks)} chunks")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {output_dir / 'embedding_summary.json'}")


if __name__ == "__main__":
    main()