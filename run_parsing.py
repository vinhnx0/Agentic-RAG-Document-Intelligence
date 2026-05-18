# run_parsing.py

from __future__ import annotations
from typing import Any

from ingestion.pipeline import IngestionPipeline
from parsing.pipeline import ParsingPipeline
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


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
                "parser": doc.metadata.get("parser"),
                "validation": (
                    doc.metadata
                    .get("parsing_audit", {})
                    .get("validation", {})
                ),
                "sections_preview": [
                    {
                        "section_id": section.section_id,
                        "title": section.title,
                        "level": section.level,
                        "section_type": section.metadata.get("section_type"),
                        "form_item": section.metadata.get("form_item"),
                        "part": section.metadata.get("part"),
                        "subheading_count": section.metadata.get(
                            "subheading_count",
                            0,
                        ),
                        "subheadings_preview": section.metadata.get(
                            "subheadings",
                            [],
                        )[:5],
                        "parent_section_id": section.parent_section_id,
                        "preview": section.text[:150].replace("\n", " "),
                    }
                    for section in doc.sections[:8]
                ],
            }
            for doc in parsed_documents
        ],
    }


def _build_parsing_audit_report(
    parsed_documents: list[Any],
) -> dict[str, Any]:
    audits = []

    for doc in parsed_documents:
        audit = doc.metadata.get("parsing_audit")
        if audit:
            audits.append(audit)

    passed_count = sum(
        1
        for audit in audits
        if audit.get("validation", {}).get("passed") is True
    )

    failed_count = len(audits) - passed_count

    return {
        "document_count": len(parsed_documents),
        "audit_count": len(audits),
        "passed_count": passed_count,
        "failed_count": failed_count,
        "documents": audits,
    }


def main() -> None:
    config_path = CONFIG_PATH

    ingestion_pipeline = IngestionPipeline(config_path)
    raw_documents = ingestion_pipeline.run()

    parsing_pipeline = ParsingPipeline(config_path)
    parsed_documents = parsing_pipeline.run(raw_documents)

    processed_data_dir = parsing_pipeline.config.get(
        "processed_data_dir",
        "data/processed",
    )
    output_dir = ensure_stage_output_dir(processed_data_dir, "parsing")

    summary_payload = _build_summary(parsed_documents)
    write_json(output_dir / "parsing_summary.json", summary_payload)

    audit_report = _build_parsing_audit_report(parsed_documents)
    write_json(output_dir / "parsing_audit_report.json", audit_report)

    for doc in parsed_documents:
        doc_payload = to_jsonable(doc)
        write_json(output_dir / f"{doc.doc_id}.json", doc_payload)

    print(f"Parsed {len(parsed_documents)} documents")
    print(f"Saved outputs to: {output_dir}")
    print(f"Summary file: {output_dir / 'parsing_summary.json'}")
    print(f"Audit file: {output_dir / 'parsing_audit_report.json'}")

    print("\nParsing validation:")
    print(f"- Passed: {audit_report['passed_count']}")
    print(f"- Failed: {audit_report['failed_count']}")


if __name__ == "__main__":
    main()