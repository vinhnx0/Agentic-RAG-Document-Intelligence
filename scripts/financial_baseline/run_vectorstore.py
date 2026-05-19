# run_vectorstore.py

from __future__ import annotations
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from chunking.pipeline import ChunkingPipeline
from embedding.pipeline import EmbeddingPipeline
from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline
from utils.io import ensure_stage_output_dir, write_json
from vectorstore.pipeline import VectorStorePipeline


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


def main() -> None:
    ingestion_pipeline = IngestionPipeline(CONFIG_PATH)
    raw_documents = ingestion_pipeline.run()

    parsing_pipeline = ParsingPipeline(CONFIG_PATH)
    parsed_documents = parsing_pipeline.run(raw_documents)

    chunking_pipeline = ChunkingPipeline(CONFIG_PATH)
    chunks = chunking_pipeline.run(parsed_documents)

    embedding_pipeline = EmbeddingPipeline(CONFIG_PATH)
    embedded_chunks = embedding_pipeline.run(chunks)

    vectorstore_pipeline = VectorStorePipeline(CONFIG_PATH)
    summary = vectorstore_pipeline.run(embedded_chunks)

    processed_data_dir = vectorstore_pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "vectorstore")

    summary_path = output_dir / "vectorstore_summary.json"
    write_json(summary_path, summary)

    print(f"Upserted {summary['upserted_vector_count']} vectors")
    print(f"Collection: {summary['collection_name']}")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {summary_path}")


if __name__ == "__main__":
    main()