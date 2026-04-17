# run_parsing.py

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline


def _to_jsonable(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, list):
        return [_to_jsonable(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _to_jsonable(value) for key, value in obj.items()}
    return obj


def _ensure_output_dir(base_dir: str | Path) -> Path:
    output_dir = Path(base_dir) / "parsing"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _build_summary(parsed_documents: list[Any]) -> dict[str, Any]:
    return {
        "document_count": len(parsed_documents),
        "documents": [
            {
                "doc_id": doc.doc_id,
                "title": doc.title,
                "source_type": doc.source_type,
                "source_path": doc.source_path,
                "section_count": len(doc.sections),
                "sections_preview": [
                    {
                        "section_id": section.section_id,
                        "title": section.title,
                        "level": section.level,
                        "parent_section_id": section.parent_section_id,
                        "preview": section.text[:150].replace("\n", " "),
                    }
                    for section in doc.sections[:5]
                ],
            }
            for doc in parsed_documents
        ],
    }


def main() -> None:
    config_path = "configs/corpora/tech_docs.yaml"

    ingestion_pipeline = IngestionPipeline(config_path)
    raw_documents = ingestion_pipeline.run()

    parsing_pipeline = ParsingPipeline(config_path)
    parsed_documents = parsing_pipeline.run(raw_documents)

    processed_data_dir = parsing_pipeline.config.get("processed_data_dir", "data/processed")
    output_dir = _ensure_output_dir(processed_data_dir)
    
    summary_payload = _build_summary(parsed_documents)
    _write_json(output_dir / "parsing_summary.json", summary_payload)

    for doc in parsed_documents:
        doc_payload = _to_jsonable(doc)
        _write_json(output_dir / f"{doc.doc_id}.json", doc_payload)

    print(f"Parsed {len(parsed_documents)} documents")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {output_dir / 'parsing_summary.json'}")


if __name__ == "__main__":
    main()