# run_chunking.py

from __future__ import annotations

from collections import Counter
from typing import Any

from chunking.pipeline import ChunkingPipeline
from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


def build_summary(chunks: list[Any]) -> dict[str, Any]:
    doc_counts = Counter(chunk.doc_id for chunk in chunks)

    return {
        "chunk_count": len(chunks),
        "document_count_with_chunks": len(doc_counts),
        "chunks_per_document": dict(sorted(doc_counts.items())),
        "chunks_preview": [
            {
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.doc_id,
                "chunk_index": chunk.chunk_index,
                "section_title": chunk.section_title,
                "section_path": chunk.section_path,
                "text_preview": chunk.text[:200],
            }
            for chunk in chunks[:10]
        ],
    }


def main() -> None:
    ingestion_pipeline = IngestionPipeline(CONFIG_PATH)
    raw_documents = ingestion_pipeline.run()

    parsing_pipeline = ParsingPipeline(CONFIG_PATH)
    parsed_documents = parsing_pipeline.run(raw_documents)

    chunking_pipeline = ChunkingPipeline(CONFIG_PATH)
    chunks = chunking_pipeline.run(parsed_documents)

    processed_data_dir = chunking_pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "chunking")

    write_json(output_dir / "chunking_summary.json", build_summary(chunks))
    write_json(
        output_dir / "chunks.json",
        [to_jsonable(chunk) for chunk in chunks],
    )

    print(f"Chunked {len(parsed_documents)} documents into {len(chunks)} chunks")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {output_dir / 'chunking_summary.json'}")


if __name__ == "__main__":
    main()