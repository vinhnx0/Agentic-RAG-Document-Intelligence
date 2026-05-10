# run_parsing.py

from __future__ import annotations
from typing import Any

from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


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
    output_dir = ensure_stage_output_dir(processed_data_dir, "parsing")

    summary_payload = _build_summary(parsed_documents)
    write_json(output_dir / "parsing_summary.json", summary_payload)

    for doc in parsed_documents:
        doc_payload = to_jsonable(doc)
        write_json(output_dir / f"{doc.doc_id}.json", doc_payload)

    print(f"Parsed {len(parsed_documents)} documents")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {output_dir / 'parsing_summary.json'}")


if __name__ == "__main__":
    main()