# run_agentic_evaluation.py

from __future__ import annotations

from typing import Any

from evaluation.agentic_pipeline import AgenticEvaluationPipeline
from utils.io import ensure_stage_output_dir, write_json


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


def build_summary(evaluation_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "query_count": evaluation_report["query_count"],
        "retrieval_summary": evaluation_report["retrieval_summary"],
        "rag_summary": evaluation_report["rag_summary"],
        "agentic_summary": evaluation_report["agentic_summary"],
        "failed_or_weak_queries": [
            {
                "query_id": report["query_id"],
                "query": report["query"],
                "expected_behavior": report["expected_behavior"],
                "predicted_behavior": report["predicted_behavior"],
                "behavior_correct": report["behavior_correct"],
                "evidence_check": report["evidence_check"],
                "metadata_filters": report["metadata_filters"],
                "retrieval_hit_at_5": report["retrieval"]["hit_at_5"],
                "top_context_preview": report["top_context_preview"][:3],
            }
            for report in evaluation_report["query_reports"]
            if not report["retrieval"]["hit_at_5"] or not report["behavior_correct"]
        ],
    }


def main() -> None:
    pipeline = AgenticEvaluationPipeline(CONFIG_PATH)
    evaluation_report = pipeline.run()

    output_dir = ensure_stage_output_dir(
        pipeline.get_processed_data_dir(),
        pipeline.get_output_stage_name(),
    )

    report_path = output_dir / "agentic_v1_report.json"
    summary_path = output_dir / "agentic_v1_summary.json"

    write_json(report_path, evaluation_report)
    write_json(summary_path, build_summary(evaluation_report))

    print(f"Evaluated {evaluation_report['query_count']} queries")
    print(f"Saved outputs to: {output_dir}")
    print(f"Report file: {report_path}")
    print(f"Summary file: {summary_path}")

    print("\nRetrieval Summary:")
    for metric, value in evaluation_report["retrieval_summary"].items():
        print(f"- {metric}: {value:.4f}")

    print("\nRAG Summary:")
    for metric, value in evaluation_report["rag_summary"].items():
        print(f"- {metric}: {value:.4f}")

    print("\nAgentic Summary:")
    for metric, value in evaluation_report["agentic_summary"].items():
        if value is None:
            print(f"- {metric}: None")
        else:
            print(f"- {metric}: {value:.4f}")


if __name__ == "__main__":
    main()