# run_ingestion.py

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from ingestion.pipeline import IngestionPipeline
from schemas.documents import RawDocument


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_output_dir(base_dir: str | Path) -> Path:
    output_dir = Path(base_dir) / "ingestion"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_document_output(output_dir: Path, document: RawDocument) -> str:
    file_name = f"{document.doc_id}.json"
    output_path = output_dir / file_name

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(document), f, ensure_ascii=False, indent=2)

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
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary_path


def main() -> None:
    pipeline = IngestionPipeline(CONFIG_PATH)
    documents = pipeline.run()

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_output_dir(processed_data_dir)

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