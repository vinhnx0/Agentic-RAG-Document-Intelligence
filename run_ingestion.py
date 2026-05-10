# run_ingestion.py

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ingestion.pipeline import IngestionPipeline
from schemas.documents import RawDocument
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_document_output(output_dir: Path, document: RawDocument) -> str:
    file_name = f"{document.doc_id}.json"
    output_path = output_dir / file_name
    write_json(output_path, to_jsonable(document))
    return file_name


def build_summary(
    corpus_name: str,
    config_path: str,
    output_dir: Path,
    documents: list[RawDocument],
    saved_files: list[str],
) -> dict:
    source_type_counts: dict[str, int] = {}
    for doc in documents:
        source_type_counts[doc.source_type] = (
            source_type_counts.get(doc.source_type, 0) + 1
        )

    return {
        "corpus_name": corpus_name,
        "config_path": config_path,
        "generated_at": utc_now_compact(),
        "output_dir": str(output_dir),
        "document_count": len(documents),
        "source_type_counts": source_type_counts,
        "documents": [
            {
                "doc_id": doc.doc_id,
                "title": doc.title,
                "source_type": doc.source_type,
                "source_path": doc.source_path,
                "output_file": saved_file,
            }
            for doc, saved_file in zip(documents, saved_files, strict=True)
        ],
    }


def save_summary(output_dir: Path, summary: dict) -> Path:
    summary_path = output_dir / "ingestion_summary.json"
    write_json(summary_path, summary)
    return summary_path


def main() -> None:
    pipeline = IngestionPipeline(CONFIG_PATH)
    documents = pipeline.run()

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "ingestion")

    saved_files: list[str] = []
    for doc in documents:
        saved_file = save_document_output(output_dir, doc)
        saved_files.append(saved_file)

    summary = build_summary(
        corpus_name=pipeline.config["corpus_name"],
        config_path=CONFIG_PATH,
        output_dir=output_dir,
        documents=documents,
        saved_files=saved_files,
    )
    summary_path = save_summary(output_dir, summary)

    print(f"Ingested {len(documents)} documents")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {summary_path}\n")


if __name__ == "__main__":
    main()