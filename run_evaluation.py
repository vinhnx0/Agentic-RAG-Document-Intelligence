# run_evaluation.py

from __future__ import annotations

from typing import Any

from evaluation.pipeline import EvaluationPipeline
from utils.io import ensure_stage_output_dir, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


def build_summary(evaluation_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "query_count": evaluation_report["query_count"],
        "retrieval_summary": evaluation_report["retrieval_summary"],
        "reranking_summary": evaluation_report["reranking_summary"],
        "rag_summary": evaluation_report["rag_summary"],
        "failed_or_weak_queries": [
            {
                "query_id": report["query_id"],
                "query": report["query"],
                "retrieval_hit_at_5": report["retrieval"]["hit_at_5"],
                "reranking_hit_at_5": report["reranking"]["hit_at_5"],
                "top_reranking_preview": report["top_reranking_preview"][:3],
            }
            for report in evaluation_report["query_reports"]
            if not report["reranking"]["hit_at_5"]
        ],
    }


def main() -> None:
    pipeline = EvaluationPipeline(CONFIG_PATH)
    evaluation_report = pipeline.run()

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(
        processed_data_dir,
        pipeline.get_output_stage_name(),
    )

    report_path = output_dir / "evaluation_report.json"
    summary_path = output_dir / "evaluation_summary.json"

    write_json(report_path, evaluation_report)
    write_json(summary_path, build_summary(evaluation_report))

    print(f"Evaluated {evaluation_report['query_count']} queries")
    print(f"Saved outputs to: {output_dir}")
    print(f"Report file: {report_path}")
    print(f"Summary file: {summary_path}")

    print("\nRetrieval Summary:")
    for metric, value in evaluation_report["retrieval_summary"].items():
        print(f"- {metric}: {value:.4f}")

    print("\nReranking Summary:")
    for metric, value in evaluation_report["reranking_summary"].items():
        print(f"- {metric}: {value:.4f}")

    print("\nRAG Summary:")
    for metric, value in evaluation_report["rag_summary"].items():
        print(f"- {metric}: {value:.4f}")


if __name__ == "__main__":
    main()